"""
AKA cumberbatch.py

Check to see if CSV file is internally valid
See which EIDs would be added
Update existing records
Import new records
Register newly added EIDs
"""

import codecs
import csv
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import traceback
from typing import Dict

from DDR import config
from DDR import changelog
from DDR import commands
from DDR import csvfile
from DDR import dvcs
from DDR import fileio
from DDR import identifier
from DDR import idservice
from DDR import ingest
from DDR import models
from DDR import modules
from DDR import util
from DDR import vocab

COLLECTION_FILES_PREFIX = 'files'

# TODO get these from ddr-defs/repo_modules/file.py
FILE_UPDATE_IGNORE_FIELDS = ['sha1', 'sha256', 'md5', 'size']

# fields that only appear in File objects
# TODO get these from ddr-defs/repo_modules/file.py
FILE_ONLY_FIELDS = [
    'external', 'basename_orig', 'role', 'sort', 'mimetype', 'tech_notes',
]


class Exporter():
    
    @staticmethod
    def _make_tmpdir(tmpdir):
        """Make tmp dir if doesn't exist.
        
        @param tmpdir: Absolute path to dir
        """
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)

    @staticmethod
    def export(json_paths, model, csv_path, required_only=False):
        """Write the specified objects' data to CSV.
        
        IMPORTANT: All objects in json_paths must have the same set of fields!
        
        TODO let user specify which fields to write
        TODO confirm that each identifier's class matches object_class
        
        @param json_paths: list of .json files
        @param model: str
        @param csv_path: Absolute path to CSV data file.
        @param required_only: boolean Only required fields.
        """
        object_class = identifier.class_for_name(
            identifier.MODEL_CLASSES[model]['module'],
            identifier.MODEL_CLASSES[model]['class']
        )
        module = modules.Module(identifier.module_for_name(
            identifier.MODEL_REPO_MODELS[model]['module']
        ))
        
        if hasattr(object_class, 'xmp') and not hasattr(object_class, 'mets'):
            # File or subclass
            json_paths = models.common.sort_file_paths(json_paths)
        else:
            # Entity or subclass
            json_paths = util.natural_sort(json_paths)
        json_paths_len = len(json_paths)
        
        Exporter._make_tmpdir(os.path.dirname(csv_path))
        
        headers = module.csv_export_fields(required_only)
        # make sure we export 'id' if it's not in model FIELDS (ahem, files)
        if 'id' not in headers:
            headers.insert(0, 'id')
        
        with codecs.open(csv_path, 'wb', 'utf-8') as csvfile:
            writer = fileio.csv_writer(csvfile)
            # headers in first line
            writer.writerow(headers)
            for n,json_path in enumerate(json_paths):
                i = identifier.Identifier(json_path)
                logging.info('%s/%s - %s' % (n+1, json_paths_len, i.id))
                obj = object_class.from_identifier(i)
                if obj:
                    csv = obj.dump_csv(fields=headers)
                    try:
                        writer.writerow(csv)
                    except UnicodeDecodeError as err:
                        nicer_unicode_decode_error(headers, obj, csv)
        return csv_path
    
    @staticmethod
    def export_field_csv(json_paths, model, fieldname):
        """Export specified field values from all entities in a collection to CSV
        
        @param json_paths: list of .json files
        @param model: str
        @param fieldname: str
        """
        module = modules.Module(identifier.module_for_name(
            identifier.MODEL_REPO_MODELS[model]['module']
        ))
        headers = ['object_id', 'fieldname', 'value',]
        yield fileio.write_csv_str(headers)
        for json_path in json_paths:
            oi = identifier.Identifier(json_path)
            fieldvalues = models.common.prep_csv(
                oi.object(), module, fields=[fieldname]
            )
            row = [oi.id, fieldname, fieldvalues[0]]
            yield fileio.write_csv_str(row)


def nicer_unicode_decode_error(headers, obj, csv):
    """Nicer Exception msg pointing out location of UnicodeDecodeError
    
    List the object ID, field name, and index within the field where the error
    can be found -- a great help to users puzzling over why their export failed.
    
    @param headers: list of fieldnames
    @param obj: DDRObject
    @param csv: list of CSV row cells
    """
    for n,field in enumerate(csv):
        if isinstance(field, str):
            try:
                utf8 = field.decode('utf8', 'strict')
            except UnicodeEncodeError as err:
                msg = 'UnicodeEncodeError in {}, "{}" field, position {}'.format(
                    obj.identifier.id,
                    headers[n],
                    err.start
                )
                raise Exception(msg)


class InvalidCSVException(Exception):
    pass

class Checker():

    @staticmethod
    def check_repository(cidentifier):
        """Load repository, check for staged or modified files
        
        Entity.add_files will not work properly if the repo contains staged
        or modified files.
        
        Results dict includes:
        - 'passed': boolean
        - 'repo': GitPython repository
        - 'staged': list of staged files
        - 'modified': list of modified files
        
        @param cidentifier: Identifier
        @returns: list of staged and modified files
        """
        logging.info('Checking repository')
        repo = dvcs.repository(cidentifier.path_abs())
        logging.info(repo)
        return dvcs.list_staged(repo), dvcs.list_modified(repo)

    @staticmethod
    def check_csv(model, csv_path, rowds, headers, csv_errs, cidentifier, vocabs_url):
        """Load CSV, validate headers and rows
        
        An import file must be a valid CSV file.
        Each row must contain a valid DDR identifier.
        All rows must represent the same kind of object.
        All files must be children of the same kind of parent (entity,segment).
        
        Results dict includes:
        - 'passed'
        - 'headers'
        - 'rowds'
        - 'csv_errs'
        - 'header_errs'
        - 'rowds_errs'
        
        @param model: str
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param vocabs_url: str URL or path to vocabs
        @param session: requests.session object
        @returns: list of validation errors
        """
        logging.info('Checking CSV file')
        # Validate format of CSV and DDR IDs
        logging.info('%s rows' % len(rowds))
        id_errs = []
        for rowd in rowds:
            if rowd.get('id'):
                try:
                    rowd['identifier'] = identifier.Identifier(rowd['id'])
                except identifier.InvalidIdentifierException:
                    id_errs.append(f'Bad Identifier: {rowd["id"]}')
            else:
                rowd['identifier'] = None
        # Validate file content
        module = Checker._get_module(model)
        vocabs = vocab.get_vocabs(config.VOCABS_URL)
        validation_errs = Checker._validate_csv_file(
            model, csv_path, rowds, headers, module, vocabs
        )
        return csv_errs,id_errs,validation_errs
    
    @staticmethod
    def check_eids(rowds, cidentifier, idservice_client):
        """Check CSV for already existing and registered IDs
        
        List IDs that are registered,unregistered with ID service.
        List IDs that already exist in the repository.
        
        Results dict includes:
        - passed
        - csv_eids
        - registered
        
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param idservice_client: idservice.IDServiceClient
        @returns: CheckResult
        """
        logging.info('Confirming all entity IDs available')
        passed = False
        csv_eids = [rowd['id'] for rowd in rowds]
        status,reason,registered,unregistered = idservice_client.check_eids(
            cidentifier, csv_eids
        )
        logging.info('%s %s' % (status,reason))
        if status != 200:
            raise Exception('%s %s' % (status,reason))
        logging.info('%s registered' % len(registered))
        logging.info('%s NOT registered' % len(unregistered))
        # confirm file entities not in repo
        logging.info('Checking for locally existing IDs')
        already_added = Checker._ids_in_local_repo(
            rowds, cidentifier.model, cidentifier.path_abs()
        )
        logging.debug('%s locally existing' % len(already_added))
        if already_added:
            logging.error('The following entities already exist: %s' % already_added)
        if (unregistered == csv_eids) \
        and (not registered) \
        and (not already_added):
            passed = True
            logging.info('ok')
        else:
            logging.error('FAIL')
        return {
            'passed': True,
            'csv_eids': csv_eids,
            'registered': registered,
            'unregistered': unregistered,
        }

    # ----------------------------------------------------------------------

    @staticmethod
    def _get_module(model):
        return modules.Module(
            identifier.module_for_name(
                identifier.MODEL_REPO_MODELS[model]['module']
            )
        )

    @staticmethod
    def _ids_in_local_repo(rowds, model, collection_path):
        """Lists which IDs in CSV are present in local repo.
        
        @param rowds: list of dicts
        @param model: str
        @param collection_path: str Absolute path to collection repo.
        @returns: list of IDs.
        """
        metadata_paths = util.find_meta_files(
            collection_path,
            model=model,
            recursive=True, force_read=True
        )
        existing_ids = [
            identifier.Identifier(path=path).id
            for path in metadata_paths
        ]
        new_ids = [rowd['id'] for rowd in rowds]
        already = [i for i in new_ids if i in existing_ids]
        return already

    @staticmethod
    def _prep_valid_values(vocabs):
        """Prepares dict of acceptable values for controlled-vocab fields.
        
        >>> vocabs = {
        ...     'status': {'id': 'status', 'terms': [
        ...         {'id': 'inprocess', 'title': 'In Progress'},
        ...         {'id': 'completed', 'title': 'Completed'}
        ...     ]},
        ...     'language': {'id': 'language', 'terms': [
        ...         {'id': 'eng', 'title': 'English'},
        ...         {'id': 'jpn', 'title': 'Japanese'},
        ...     ]}
        ... }
        >>> batch._prep_valid_values(vocabs)
        {'status': ['inprocess', 'completed'], 'language': ['eng', 'jpn']}
        
        @param vocabs: dict Output of DDR.vocab.get_vocabs()
        @returns: dict
        """
        valid_values = {}
        for key,data in vocabs.items():
            field = data['id']
            values = [term['id'] for term in data['terms']]
            if values:
                valid_values[field] = values
        return valid_values

    @staticmethod
    def _validate_csv_file(model, csv_path, rowds, headers, module, vocabs):
        """Validate CSV headers and data against schema/field definitions
        
        @param model: str
        @param rowds: list
        @param csv_path: Absolute path to CSV data file.
        @param module: modules.Module
        @param vocabs: dict Output of _prep_valid_values()
        @returns: list [header_errs, rowds_errs]
        """
        # gather data
        field_names = module.field_names()
        # Files don't have an 'id' field but we have to have one in CSV
        if 'id' not in field_names:
            field_names.insert(0, 'id')
        nonrequired_fields = module.module.REQUIRED_FIELDS_EXCEPTIONS
        required_fields = module.required_fields(nonrequired_fields)
        valid_values = Checker._prep_valid_values(vocabs)
        # check

        logging.info('Validating headers')
        header_errs = csvfile.validate_headers(
            headers, field_names, exceptions=nonrequired_fields,
            additional=['access_path'],  # used for custom access files
        )
        logging.info('Validating rows')
        find_dupes = True
        if model and (model == 'file'):
            find_dupes = False
        try:
            rowds_errs = csvfile.validate_rowds(
                module, headers, required_fields, valid_values, rowds, find_dupes
            )
        except identifier.InvalidIdentifierException:
            rowds_errs = []
        file_errs = []
        if model == 'file' and 'basename_orig' in headers:
            logging.info('Validating file imports')
            for rowd in rowds:
                if rowd.get('external') and rowd['external']:
                    # no need to check for existence of external files
                    continue
                path = os.path.join(os.path.dirname(csv_path), rowd['basename_orig'])
                if not os.path.exists(path):
                    file_errs.append({'Missing file': path}); continue
                if not os.path.isfile(path):
                    file_errs.append({'Not a file': path}); continue
                if not os.access(path, os.R_OK):
                    file_errs.append({'Not readable': path}); continue
        return header_errs,rowds_errs,file_errs

class ModifiedFilesError(Exception):
    pass

class UncommittedFilesError(Exception):
    pass

class Importer():

    @staticmethod
    def _fidentifier_parent(fidentifier):
        """Returns entity Identifier for either 'file' or 'file-role'
        
        We want to support adding new files and updating existing ones.
        New file IDs have no SHA1, thus they are actually file-roles.
        Identifier.parent() returns different objects depending on value of 'stubs'.
        This function ensures that the parent of 'fidentifier' will always be an Entity.
        
        @param fidentifier: Identifier
        @returns: boolean
        """
        is_stub = fidentifier.object_class() == models.common.Stub
        return fidentifier.parent(stubs=is_stub)

    @staticmethod
    def _write_entity_changelog(entity, git_name, git_mail, agent):
        msg = 'Updated entity file {}'
        messages = [
            msg.format(entity.json_path),
            '@agent: %s' % agent,
        ]
        changelog.write_changelog_entry(
            entity.changelog_path, messages,
            user=git_name, email=git_mail)

    @staticmethod
    def _write_file_changelogs(entities, git_name, git_mail, agent):
        """Writes entity update/add changelogs, returns list of changelog paths
        
        Assembles appropriate changelog messages and then updates changelog for
        each entity.  update_files() adds lists of updated and added File objects
        to entities in list.
        
        TODO should this go in DDR.changelog.py?
        
        @param entities: list of Entity objects.
        @param git_name:
        @param git_mail:
        @param agent:
        @returns: list of paths relative to repository base
        """
        git_files = []
        for entity in entities:
            messages = []
            if getattr(entity, 'changelog_updated', None):
                for f in entity.changelog_updated:
                    messages.append('Updated entity file {}'.format(f.json_path_rel))
            #if getattr(entity, 'changelog_added', None):
            #    for f in entity.changelog_added:
            #        messages.append('Added entity file {}'.format(f.json_path_rel))
            messages.append('@agent: %s' % agent)
            changelog.write_changelog_entry(
                entity.changelog_path,
                messages,
                user=git_name,
                email=git_mail)
            git_files.append(entity.changelog_path_rel)
        return git_files

    # ----------------------------------------------------------------------

    @staticmethod
    def import_entities(csv_path, cidentifier, vocabs_url, git_name, git_mail, agent, dryrun=False):
        """Adds or updates entities from a CSV file
        
        Running function multiple times with the same CSV file is idempotent.
        After the initial pass, files will only be modified if the CSV data
        has been updated.
        
        This function writes and stages files but does not commit them!
        That is left to the user or to another function.
        
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param vocabs_url: str URL or path to vocabs
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param dryrun: boolean
        @returns: list of updated entities
        """
        logging.info('------------------------------------------------------------------------')
        logging.info('batch import entity')
        model = 'entity'
        
        repository = dvcs.repository(cidentifier.path_abs())
        logging.info(repository)
        
        logging.info('Reading %s' % csv_path)
        headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv_path))
        logging.info('%s rows' % len(rowds))
        
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        logging.info('Importing')
        start_updates = datetime.now(config.TZ)
        git_files = []
        updated = []
        elapsed_rounds = []
        obj_metadata = None
        
        if dryrun:
            logging.info('Dry run - no modifications')
        for n,rowd in enumerate(rowds):
            logging.info('%s/%s - %s' % (n+1, len(rowds), rowd['id']))
            start_round = datetime.now(config.TZ)
            
            eidentifier = identifier.Identifier(id=rowd['id'], base_path=cidentifier.basepath)
            # if there is an existing object it will be loaded
            try:
                entity = eidentifier.object()
            except IOError:
                entity = None
            if not entity:
                entity = models.Entity.new(eidentifier)
            modified = entity.load_csv(rowd)
            # Getting obj_metadata takes about 1sec each time
            # TODO caching works as long as all objects have same metadata...
            if not obj_metadata:
                obj_metadata = models.common.object_metadata(
                    eidentifier.fields_module(),
                    repository.working_dir
                )
            
            if dryrun:
                pass
            elif modified:
                # write files
                if not os.path.exists(entity.path_abs):
                    os.makedirs(entity.path_abs)
                logging.debug('    writing %s' % entity.json_path)
                
                exit,status,updated_files = entity.save(
                    git_name, git_mail, agent,
                    collection=cidentifier.object(),
                    #cleaned_data=rowd,
                    commit=False
                )
                
                # stage
                git_files.append(updated_files)
                updated.append(entity)
            
            elapsed_round = datetime.now(config.TZ) - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('| %s (%s)' % (eidentifier, elapsed_round))
    
        if dryrun:
            logging.info('Dry run - no modifications')
        elif updated:
            logging.info('Staging %s modified files' % len(git_files))
            start_stage = datetime.now(config.TZ)
            dvcs.stage(repository, git_files)
            for path in util.natural_sort(dvcs.list_staged(repository)):
                if path in git_files:
                    logging.debug('+ %s' % path)
                else:
                    logging.debug('| %s' % path)
            elapsed_stage = datetime.now(config.TZ) - start_stage
            logging.debug('ok (%s)' % elapsed_stage)
        
        elapsed_updates = datetime.now(config.TZ) - start_updates
        logging.debug('%s updated in %s' % (len(elapsed_rounds), elapsed_updates))
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        
        return updated

    @staticmethod
    def _csv_load(module, rowds):
        return [models.common.csvload_rowd(module, rowd) for rowd in rowds]

    @staticmethod
    def _fidentifiers(rowds, cidentifier):
        """Make dict of File Identifiers by file ID; exclude entity/segments
        
        Note: Rows for *existing* files have file IDs.
        Rows for *new* files don't have IDs yet (no SHA yet) so have the IDs
        of the *parents*.
        """
        oids = {}
        for rowd in rowds:
            fi = identifier.Identifier(
                id=rowd['id'],
                base_path=cidentifier.basepath
            )
            if fi.model == 'file':
                oids[fi.id] = fi
        return oids
    
    @staticmethod
    def _fid_parents(fidentifiers, rowds, cidentifier):
        """Make dict of File Identifier parents (entities) by file ID.
        
        Note: Rows for *existing* files have file IDs. We can get parent IDs.
        Rows for *new* files don't have IDs yet (no SHA yet) so the IDs
        are those of the *parents*.
        
        @param fidentifiers: dict of File Identifiers by ID
        @param rowds: list of dicts
        @param cidentifier: Identifier
        """
        # existing files (these will be actual File identifiers)
        # note: fidentifiers will be {} if all rows are for new files
        oids = {
            fi.id: Importer._fidentifier_parent(fi)
            for fi in fidentifiers.values()
        }
        # new files (these will be the Files' parent Entity identifiers)
        for rowd in rowds:
            fi = identifier.Identifier(
                id=rowd['id'],
                base_path=cidentifier.basepath
            )
            if fi.model != 'file':
                oids[fi.id] = fi
        return oids
    
    @staticmethod
    def _eidentifiers(fid_parents):
        """deduplicated list of Entity Identifiers."""
        return list([
            e for e in fid_parents.values()
        ])
    
    @staticmethod
    def _existing_bad_entities(eidentifiers):
        """dict of Entity Identifiers by entity ID; list of bad entities.
        
        "Bad" entities are those for which no entity.json in filesystem.
        @returns: (dict, list)
        """
        entities = {}
        bad_entities = []
        for eidentifier in eidentifiers:
            if os.path.exists(eidentifier.path_abs()):
                entities[eidentifier.id] = eidentifier.object()
            elif eidentifier.id not in bad_entities:
                bad_entities.append(eidentifier.id)
        return entities,bad_entities
    
    @staticmethod
    def _file_objects(fidentifiers):
        """dict of File objects by file ID."""
        # File objects will be used to determine if the Files "exist"
        # e.g. whether they are local/normal or external/metadata-only
        return {
            # TODO don't hard-code object class!!!
            fid: fi.object()
            for fid,fi in fidentifiers.items()
        }
    
    @staticmethod
    def _rowds_new_existing(rowds, files):
        """separates rowds into new,existing lists
        
        This is more complicated than before because the "files" may actually be
        Stubs, which don't have .jsons or .exists() methods.
        """
        new = []
        existing = []
        for n,rowd in enumerate(rowds):
            # gather facts
            has_id = rowd.get('id')
            obj = files.get(rowd['id'], None)
            obj_is_node = False
            if obj and (obj.identifier.model in identifier.NODES):
                obj_is_node = True
            if obj and obj_is_node:
                json_exists = obj.exists()
            else:
                json_exists = False
            # decide
            if has_id and obj and obj_is_node and json_exists:
                existing.append(rowd)
            else:
                new.append(rowd)
        return new,existing

    @staticmethod
    def _rowd_is_external(rowd):
        """indicates whether or not rowd represents an external file."""
        if rowd.get('external') and isinstance(rowd['external'], str) and rowd['external'].isdigit():
            rowd['external'] = int(rowd['external'])
        if rowd.get('external', 0):
            return True
        return False
    
    @staticmethod
    def import_files(csv_path, rowds, cidentifier, vocabs_url, git_name, git_mail,
                     agent, row_start=0, row_end=9999999,
                     tmp_dir=config.MEDIA_BASE, log_path=None, dryrun=False):
        """Adds or updates files from a CSV file
        
        TODO how to handle excluded fields like XMP???
        
        @param csv_path: Absolute path to CSV data file.
        @param rowds: list of rowd dicts
        @param cidentifier: Identifier
        @param vocabs_url: str URL or path to vocabs
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param log_path: str Absolute path to addfile log for all files
        @param dryrun: boolean
        @returns: list git_files
        """
        if log_path:
            log = util.FileLogger(log_path=log_path)
        else:
            cpath = Path(csv_path)
            log = util.FileLogger(log_path=cpath.with_name(f'{cpath.stem}.log'))
        
        log.info('batch import files ----------------------------')
        
        # TODO hard-coded model name...
        model = 'file'
        csv_dir = os.path.dirname(csv_path)
        # TODO this still knows too much about entities and files...
        entity_class = identifier.class_for_name(
            identifier.MODEL_CLASSES['entity']['module'],
            identifier.MODEL_CLASSES['entity']['class']
        )
        repository = dvcs.repository(cidentifier.path_abs())
        log.debug(f'{csv_dir=}')
        log.debug(f'{entity_class=}')
        log.debug(repository)
        
        log.info(f'{len(rowds)} rows')
        log.info(f'csv_load rowds')
         # Apply module's csvload_* methods to rowd data
        rowds = Importer._csv_load(Checker._get_module(model), rowds)
        # add file abs path; enables importing from subdirs of /tmp/ddrshared
        for rowd in rowds:
            rowd['path_abs'] = Path(csv_dir) / rowd['basename_orig']

        # various dicts and lists instantiated here so we don't do it
        # multiple times later
        fidentifiers = Importer._fidentifiers(rowds, cidentifier)
        fid_parents = Importer._fid_parents(fidentifiers, rowds, cidentifier)
        eidentifiers = Importer._eidentifiers(fid_parents)
        entities,bad_entities = Importer._existing_bad_entities(eidentifiers)
        files = Importer._file_objects(fidentifiers)
        rowds_new,rowds_existing = Importer._rowds_new_existing(rowds, files)
        if bad_entities:
            for f in bad_entities:
                log.error(f'    {f} missing')
            raise Exception(
                f'{len(bad_entities)} entities could not be loaded! - IMPORT CANCELLED!'
            )
        
        log.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        log.info('Updating existing files')
        git_files = Importer._update_existing_files(
            rowds_existing,
            fid_parents, entities, files, models, repository,
            git_name, git_mail, agent,
            log, dryrun
        )
        
        log.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        log.info('Adding new files')
        git_files2 = Importer._add_new_files(
            rowds_new,
            fid_parents, entities, files,
            git_name, git_mail, agent,
            log, dryrun,
            tmp_dir=tmp_dir
        )
        log.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        
        return git_files
    
    @staticmethod
    def _update_existing_files(rowds, fid_parents, entities, files, models, repository, git_name, git_mail, agent, log, dryrun):
        start = datetime.now(config.TZ)
        elapsed_rounds = []
        git_files = []
        annex_files = []
        updated = []
        staged = []
        obj_metadata = None
        len_rowds = len(rowds)
        for n,rowd in enumerate(rowds):
            log.info('+ %s/%s - %s (%s)' % (
                n+1, len_rowds, rowd['id'], rowd['basename_orig']
            ))
            start_round = datetime.now(config.TZ)
            
            # remove ignored fields (hashes)
            # Users should not be able to modify hash and file size values
            # after initial file import.
            for field in FILE_UPDATE_IGNORE_FIELDS:
                rowd.pop(field)
            
            fid = rowd['id']
            eid = fid_parents[fid].id
            entity = entities[eid]
            file_ = files[fid]
            modified = file_.load_csv(rowd)
            # Getting obj_metadata takes about 1sec each time
            # TODO caching works as long as all objects have same metadata...
            if not obj_metadata:
                obj_metadata = models.common.object_metadata(
                    file_.identifier.fields_module(),
                    repository.working_dir
                )

            # Custom access files
            if rowd.get('access_path'):
                log.debug('    replacing access file {}'.format(
                    rowd.get('access_path')))
                new_annex_files = ingest.replace_access(
                    repository, file_, rowd.get('access_path')
                )
                annex_files.append(new_annex_files)
            
            if modified and not dryrun:
                log.debug('    writing %s' % file_.json_path)

                exit,status,updated_files = file_.save(
                    git_name=git_name,
                    git_mail=git_mail,
                    agent=agent,
                    commit=False
                )
                
                # stage
                git_files.append(updated_files)
                updated.append(file_)
            
            elapsed_round = datetime.now(config.TZ) - start_round
            elapsed_rounds.append(elapsed_round)
            log.debug('| %s (%s)' % (file_.identifier, elapsed_round))
        
        elapsed = datetime.now(config.TZ) - start
        log.debug('%s updated in %s' % (len(elapsed_rounds), elapsed))
                
        if (git_files or annex_files) and not dryrun:
            log.info('Staging %s modified files' % len(git_files))
            start_stage = datetime.now(config.TZ)
            # Stage annex files (binaries) before non-binary git files
            # else binaries might end up in .git/objects/ which would be BAD
            dvcs.annex_stage(repository, annex_files)
            # If git_files contains binaries they are already staged by now.
            dvcs.stage(repository, git_files)
            staged = util.natural_sort(dvcs.list_staged(repository))
            for path in staged:
                if path in git_files:
                    log.debug('+ %s' % path)
                else:
                    log.debug('| %s' % path)
            elapsed_stage = datetime.now(config.TZ) - start_stage
            log.debug('ok (%s)' % elapsed_stage)
            log.debug('%s staged in %s' % (len(staged), elapsed_stage))
        
        return git_files
    
    @staticmethod
    def _add_new_files(rowds, fid_parents, entities, files, git_name,
                       git_mail, agent, log, dryrun,
                       tmp_dir=config.MEDIA_BASE):
        log.info(f'addfile log to {log.path}')
        git_files = []
        failures = []
        start = datetime.now(config.TZ)
        elapsed_rounds = []
        len_rowds = len(rowds)
        for n,rowd in enumerate(rowds):
            log.info('+ %s/%s - %s (%s)' % (
                n+1, len_rowds, rowd['id'], rowd['basename_orig']
            ))
            start_round = datetime.now(config.TZ)

            fid = rowd['id']
            parent_id = fid_parents[fid].id
            file_ = files.get(fid)  # Note: no File object yet for new files
            parent = entities[parent_id]
            # If the actual file ID is not specified in the rowd
            # (ex: SHA1 not yet known),
            # the ID in the CSV will be the ID of the *parent* object.
            # In this case, file_ and parent vars will likely be wrong.
            # TODO refactor up the chain somewhere.
            # NOTE: no File object yet for new files
            if file_ and (file_.identifier.model not in identifier.NODES):
                parent = file_
            
            log.debug('| parent %s' % (parent))
            
            if not dryrun:
                file_,repo2,log2 = ingest.add_file(
                    rowd, parent,
                    git_name, git_mail, agent,
                    tmp_dir=tmp_dir, log_path=log.path, show_staged=False
                )
                # TODO integrate into ingest.add_file
                if rowd.get('access_path'):
                    file_,repo3,log3,status = ingest.add_access(
                        parent, file_, rowd['access_path'],
                        git_name, git_mail, agent,
                        log_path=log.path, show_staged=False
                    )
                git_files.append(file_)
            
            elapsed_round = datetime.now(config.TZ) - start_round
            elapsed_rounds.append(elapsed_round)
            log.debug('| file   %s' % (file_))
            log.debug('| %s' % (elapsed_round))
                  
        elapsed = datetime.now(config.TZ) - start
        log.debug('%s added in %s' % (len(elapsed_rounds), elapsed))
        if failures:
            log.error('************************************************************************')
            for e in failures:
                log.error(e)
            log.error('************************************************************************')
        return git_files,failures
    
    @staticmethod
    def register_entity_ids(csv_path, cidentifier, idservice_client, dryrun=True):
        """
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param idservice_client: idservice.IDServiceCrequests.session object
        @param register: boolean Whether or not to register IDs
        @returns: nothing
        """
        logging.info('-----------------------------------------------')
        logging.info('Reading %s' % csv_path)
        headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv_path))
        logging.info('%s rows' % len(rowds))
        
        logging.info('Looking up already registered IDs')
        csv_eids = [rowd['id'] for rowd in rowds]
        status1,reason1,registered,unregistered = idservice_client.check_eids(cidentifier, csv_eids)
        logging.info('%s %s' % (status1,reason1))
        if status1 != 200:
            raise Exception('%s %s' % (status1,reason1))
        
        num_unregistered = len(unregistered)
        logging.info('%s IDs to register.' % num_unregistered)
        
        if unregistered and dryrun:
            logging.info('These IDs would be registered if not --dryrun')
            for n,eid in enumerate(unregistered):
                logging.info('| %s/%s %s' % (n, num_unregistered, eid))
        
        elif unregistered:
            logging.info('Registering IDs')
            for n,eid in enumerate(unregistered):
                logging.info('| %s/%s %s' % (n, num_unregistered, eid))
            status2,reason2,created = idservice_client.register_eids(cidentifier, unregistered)
            logging.info('%s %s' % (status2,reason2))
            if status2 != 201:
                raise Exception('%s %s' % (status2,reason2))
            logging.info('%s registered' % len(created))
        
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')


class UpdaterMetrics():
    cid = None
    verdict = 'unknown'
    objects = 0
    objects_saved = 0
    updated: Dict[str,str] = {}
    files_updated = 0
    load_errs: Dict[str,str] = {}
    save_errs: Dict[str,str] = {}
    bad_exits: Dict[str,str] = {}
    failures = 0
    fail_rate = 0.0
    committed = None
    kept = None
    elapsed = None
    per_object = 'n/a'
    error = ''
    traceback = ''
    
    def headers(self):
        return [
            'id',
            'verdict',
            'failures',
            'fail_rate',
            'objects',
            'objects_saved',
            'files_updated',
            'elapsed',
            'per_object',
            'committed',
            'kept',
            'error',
            'traceback',
            'load_errs',
            'save_errs',
            'bad_exits',
        ]
    
    def row(self):
        load_errs = [':'.join([key,val]) for key,val in self.load_errs.items()]
        save_errs = [':'.join([key,val]) for key,val in self.save_errs.items()]
        bad_exits = [':'.join([key,val]) for key,val in self.bad_exits.items()]
        return [
            self.cid,
            self.verdict,
            self.failures,
            self.fail_rate,
            self.objects,
            self.objects_saved,
            self.files_updated,
            self.elapsed,
            self.per_object,
            self.committed,
            self.kept,
            self.error,
            self.traceback,
            '\n'.join(load_errs),
            '\n'.join(save_errs),
            '\n'.join(bad_exits),
        ]


class Updater():
    
    AGENT = 'ddr-transform'
    TODO = 'todo'
    THIS = 'this'
    DONE = 'done.csv'
    COLLECTION_LOG = '%s.log'

    @staticmethod
    def update_collection(cidentifier, user, mail, commit=False):
        if commit and ((not user) or (not mail)):
            logging.error('You must specify a user and email address! >:-0')
            sys.exit(1)
        else:
            logging.info('Not committing changes')
            commit = False
        
        start = datetime.now()
        response = Updater._update_collection_objects(cidentifier, user, mail, commit)
        end = datetime.now()
        return Updater._analyze(start, end, response)
    
    @staticmethod
    def update_multi(basedir, source, user, mail, commit=False, keep=False):
        """
        @param user: str User name
        @param mail: str User email
        @param basedir: str Absolute path to base dir.
        @param source: str Absolute path to list file.
        @param commit: boolean
        @param keep: boolean
        @return:
        """
        logging.info('========================================================================')
        logging.info('Prepping collections list')
        cids_path = Updater._prep_todo(basedir, source)
        cids = Updater._read_todo(cids_path)
        
        completed = 0
        successful = 0
        total_failures = 0
        total_load_errs = 0
        total_save_errs = 0
        total_bad_exits = 0
        total_objects_saved = 0
        total_files_updated = 0
        per_objects = []
        while(cids):
            logging.info('------------------------------------------------------------------------')
            # rm current cid from TODO, update TODO and THIS
            cid = cids.pop(0)
            logging.info(cid)
            Updater._write_todo(cids, cids_path)
            Updater._write_this(basedir, cid)
            
            collection_log = os.path.join(basedir, Updater.COLLECTION_LOG % cid)
            collection_path = os.path.join(basedir, cid)
            cidentifier = identifier.Identifier(cid, base_path=basedir)
            
            # clone
            if os.path.exists(collection_path):
                logging.info('Removing existing repo: %s' % collection_path)
                shutil.rmtree(collection_path)
            logging.info('Cloning %s' % collection_path)
            try:
                clone_exit,clone_status = commands.clone(
                    user, mail,
                    cidentifier,
                    collection_path
                )
                logging.info('ok')
            except:
                metrics = UpdaterMetrics()
                metrics.cid = cid
                metrics.verdict = 'FAIL'
                metrics.error = 'clone failed'
                metrics.traceback = traceback.format_exc().strip()
            
            # transform
            try:
                metrics = Updater.update_collection(cidentifier, user, mail, commit=commit)
            except:
                metrics = UpdaterMetrics()
                metrics.cid = cid
                metrics.verdict = 'FAIL'
                metrics.error = 'update failed'
                metrics.traceback = traceback.format_exc().strip()
                
            if metrics.verdict == 'ok':
                logging.info(metrics.verdict)
                successful += 1
            else:
                logging.error(metrics)
            logging.info('objects_saved: %s' % metrics.objects_saved)
            logging.info('files_updated: %s' % metrics.files_updated)
            logging.info('s/object:      %s' % metrics.per_object)
            logging.info('failures:      %s (%s)' % (metrics.failures, metrics.fail_rate))
            logging.info('load_errs:     %s' % len(metrics.load_errs))
            logging.info('save_errs:     %s' % len(metrics.save_errs))
            logging.info('bad_exits:     %s' % len(metrics.bad_exits))
            total_objects_saved += metrics.objects_saved
            total_files_updated += metrics.files_updated
            total_failures += metrics.failures
            total_load_errs += len(list(metrics.load_errs.keys()))
            total_save_errs += len(list(metrics.save_errs.keys()))
            total_bad_exits += len(list(metrics.bad_exits.keys()))
            delta = metrics.per_object
            per_objects.append(delta)
            
            if commit:
                if metrics.load_errs or metrics.save_errs or metrics.bad_exits:
                    logging.error('We have errors! Cannot commit!')
                    metrics.committed = False
                else:
                    repo = dvcs.repository(
                        collection_path,
                        user_name=user, user_mail=mail
                    )
                    # stage
                    stage_these = []
                    if metrics.updated:
                        for f in metrics.updated.values():
                            stage_these.extend(f)
                    logging.info('%s files changed' % (len(stage_these)))
                    if stage_these:
                        logging.info('Staging...')
                        staged = dvcs.stage(repo, git_files=stage_these)
                        logging.info('Committing...')
                        committed = dvcs.commit(
                            repo,
                            "Batch updated all objects in collection",
                            agent=Updater.AGENT
                        )
                        logging.info('commit %s' % committed)
                        metrics.committed = str(committed)[:10]
                        # remove remotes so you can't sync
                        # (remotes will return next time it's modded tho)
                        for name in dvcs.repos_remotes(repo):
                            repo.remove_remote(remote)
                        logging.info('ok')
                    else:
                        metrics.committed = 'nochanges'
            else:
                metrics.committed = 'nocommit'
            
            if os.path.exists(collection_path) and not keep:
                logging.info('Deleting %s' % collection_path)
                shutil.rmtree(collection_path)
                logging.info('ok')
                metrics.kept = 'nokeep'
            else:
                logging.info('Keeping %s' % collection_path)
                metrics.kept = 'kept'
            
            Updater._write_done(basedir, metrics)
            # update THIS, not writing this collection any more
            Updater._write_this(basedir, '')
            completed += 1
            logging.info('')

        return {
            'collections': completed,
            'successful': successful,
            'failures': total_failures,
            'objects_saved': total_objects_saved,
            'files_updated': total_files_updated,
            'per_objects': per_objects,
        }
    
    @staticmethod
    def _consolidate_paths(updated_files):
        updated = []
        for oid,paths in updated_files.items():
            for path in paths:
                if path not in updated:
                    updated.append(path)
        return updated
    
    @staticmethod
    def _update_collection_objects(cidentifier, git_user, git_mail, commit=False):
        """Loads and saves each object in collection
        """
        logging.info('Loading collection')
        collection = cidentifier.object()
        logging.info(collection)
        
        logging.info('Finding metadata files %s' % cidentifier.path_abs())
        paths = util.find_meta_files(
            cidentifier.path_abs(),
            recursive=True, force_read=True
        )
        num = len(paths)
        logging.info('%s paths' % num)
        logging.info('Writing')
        load_errs = {}
        save_errs = {}
        bad_exits = {}
        statuses = {}
        updated_files = {}
        for n,path in enumerate(paths):
            logging.info('%s/%s %s' % (n, num, path))
            try:
                o = identifier.Identifier(path).object()
            except:
                load_errs[o.identifier.id] = traceback.format_exc().strip().splitlines()[-1]
                logging.error('ERROR: instantiation')
                continue
            try:
                exit,status,updated = o.save(
                    git_user, git_mail, agent=Updater.AGENT, commit=False
                )
            except:
                save_errs[o.identifier.id] = traceback.format_exc().strip().splitlines()[-1]
                logging.error('ERROR: save')
                continue
            if exit != 0:
                bad_exits[o.identifier.id] = exit
                logging.error('ERROR: bad exit')
            if status != 'ok':
                statuses[o.identifier.id] = status
            updated_files[o.identifier.id] = updated
        
        return {
            'cid': cidentifier.id,    # str collection ID
            'num': num,               # int raw number of objects
            'load_errs': load_errs,   # dict load errs by object ID
            'save_errs': save_errs,   # dict save errs by object ID
            'bad_exits': bad_exits,   # dict non-zero exits by object ID
            'statuses': statuses,     # dict non-ok statuses by object ID
            'updated_files': updated_files, # dict updated files by object ID
        }
    
    @staticmethod
    def _analyze(start, end, response):
        """
        @param start: datetime
        @param end: datetime
        @param response: dict
        @returns: UpdaterMetrics
        """
        metrics = UpdaterMetrics()
        metrics.cid  =  response['cid']
        metrics.objects  =  response['num']
        metrics.objects_saved  =  response['num']
        metrics.load_errs  =  response['load_errs']
        metrics.save_errs  =  response['save_errs']
        metrics.bad_exits  =  response['bad_exits']
        metrics.updated = response['updated_files']
        metrics.files_updated  =  len(metrics.updated)
        
        metrics.failures = len(list(metrics.load_errs.keys())) + len(list(metrics.save_errs.keys())) + len(list(metrics.bad_exits.keys()))
        metrics.fail_rate = (metrics.failures * 1.0) / metrics.objects
        
        metrics.elapsed = end - start
        if metrics.elapsed and response.get('num'):
            metrics.per_object = metrics.elapsed / response['num']
        
        if metrics.load_errs or metrics.save_errs or metrics.bad_exits \
        or (metrics.objects_saved == 0) \
        or (metrics.files_updated == 0):
            metrics.verdict = 'FAIL'
        else:
            metrics.verdict = 'ok'
        
        return metrics
    
    @staticmethod
    def _prep_todo(basedir, source):
        """
        read cids list from SOURCE
        read cids from DONE
        remove DONE cids from TODO cids
        write TODO cids to TODO
        """
        path = os.path.join(basedir, Updater.TODO)
        logging.info('Prepping TODO file: %s' % path)
        # Read cids list from SOURCE
        logging.info('Getting collections list from %s' % source)
        if os.path.exists(source):
            cids = Updater._read_todo(source)
        # get list from Gitolite
        elif '@' in source:
            pass
        else:
            cids = []
        # Read cids from DONE
        if not os.path.join(basedir, Updater.DONE):
            Updater._write_done(basedir, None)
        done_headers,done_rows = Updater._read_done(basedir)
        done = [row[0] for row in done_rows]
        # Remove DONE cids from TODO cids
        completed = []
        for cid in done:
            if cid in cids:
                completed.append(cid)
                cids.remove(cid)
        logging.info('Removed completed collections: %s' % completed)
        # Write TODO cids to TODO
        Updater._write_todo(cids, path)
        return path
    
    @staticmethod
    def _read_todo(path):
        #logging.debug('_read_todo(%s)' % path)
        text = fileio.read_text(path)
        cids = [line.strip() for line in text.strip().split('\n') if line.strip()]
        return cids
    
    @staticmethod
    def _write_todo(cids, path):
        #logging.debug('_write_todo(%s, %s)' % (cids, path))
        fileio.write_text(
            '\n'.join(cids),
            path
        )
    
    @staticmethod
    def _read_this(basedir):
        text = fileio.read_text(path)
        return text.strip()
    
    @staticmethod
    def _write_this(basedir, cid):
        path = os.path.join(basedir, Updater.THIS)
        fileio.write_text(cid, path)
    
    @staticmethod
    def _read_done(basedir):
        path = os.path.join(basedir, Updater.DONE)
        headers = None
        rows = []
        if os.path.exists(path):
            rows = fileio.read_csv(path)
            if rows:
                headers = rows.pop(0)
        return headers,rows
    
    @staticmethod
    def _write_done(basedir, metrics=UpdaterMetrics()):
        path = os.path.join(basedir, Updater.DONE)
        headers = []
        rows = []
        if os.path.exists(path):
            rows = fileio.read_csv(path)
            if rows:
                headers = rows.pop(0)
        if not headers:
            headers = metrics.headers()
        if metrics.cid:
            rows.append(metrics.row())
        fileio.write_csv(path, headers, rows)
