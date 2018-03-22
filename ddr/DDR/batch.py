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
import shutil
import traceback

import requests

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

COLLECTION_FILES_PREFIX = 'files'


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
            json_paths = models.sort_file_paths(json_paths)
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
                    writer.writerow(obj.dump_csv(fields=headers))
        
        return csv_path


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
        @returns: dict
        """
        logging.info('Checking repository')
        passed = False
        repo = dvcs.repository(cidentifier.path_abs())
        logging.info(repo)
        staged = dvcs.list_staged(repo)
        if staged:
            logging.error('*** Staged files in repo %s' % repo.working_dir)
            for f in staged:
                logging.error('*** %s' % f)
        modified = dvcs.list_modified(repo)
        if modified:
            logging.error('Modified files in repo: %s' % repo.working_dir)
            for f in modified:
                logging.error('*** %s' % f)
        if repo and (not (staged or modified)):
            passed = True
            logging.info('ok')
        else:
            logging.error('FAIL')
        return {
            'passed': passed,
            'repo': repo,
            'staged': staged,
            'modified': modified,
        }

    @staticmethod
    def check_csv(csv_path, cidentifier, vocabs_path):
        """Load CSV, validate headers and rows
        
        Results dict includes:
        - 'passed'
        - 'headers'
        - 'rowds'
        - 'header_errs'
        - 'rowds_errs'
        
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param vocabs_path: Absolute path to vocab dir
        @param session: requests.session object
        @returns: nothing
        """
        logging.info('Checking CSV file')
        passed = False
        headers,rowds = csvfile.make_rowds(fileio.read_csv(csv_path))
        for rowd in rowds:
            if rowd.get('id'):
                rowd['identifier'] = identifier.Identifier(rowd['id'])
            else:
                rowd['identifier'] = None
        logging.info('%s rows' % len(rowds))
        model,model_errs = Checker._guess_model(rowds)
        module = Checker._get_module(model)
        vocabs = Checker._get_vocabs(module)
        header_errs,rowds_errs = Checker._validate_csv_file(
            module, vocabs, headers, rowds
        )
        if (not model_errs) and (not header_errs) and (not rowds_errs):
            passed = True
            logging.info('ok')
        else:
            logging.error('FAIL')
        return {
            'passed': passed,
            'headers': headers,
            'rowds': rowds,
            'model_errs': model_errs,
            'header_errs': header_errs,
            'rowds_errs': rowds_errs,
        }
    
    @staticmethod
    def check_eids(rowds, cidentifier, idservice_client):
        """
        
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
    def _guess_model(rowds):
        """Loops through rowds and guesses model
        
        # TODO guess schema too
        
        @param rowds: list
        @returns: str model keyword
        """
        logging.debug('Guessing model based on %s rows' % len(rowds))
        models = []
        errors = []
        for n,rowd in enumerate(rowds):
            if rowd.get('identifier'):
                if rowd['identifier'].model not in models:
                    models.append(rowd['identifier'].model)
            else:
                errors.append('No Identifier for row %s!' % (n))
        if not models:
            errors.append('Cannot guess model type!')
        if len(models) > 1:
            errors.append('More than one model type in imput file!')
        model = models[0]
        # TODO should not know model name
        if model == 'file-role':
            model = 'file'
        logging.debug('model: %s' % model)
        return model,errors

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
            identifier.Identifier(path=path)
            for path in metadata_paths
        ]
        new_ids = [rowd['id'] for rowd in rowds]
        already = [i for i in new_ids if i in existing_ids]
        return already

    @staticmethod
    def _load_vocab_files(vocabs_path):
        """Loads vocabulary term files in the 'ddr' repository
        
        @param vocabs_path: Absolute path to dir containing vocab .json files.
        @returns: list of raw text contents of files.
        """
        json_paths = []
        for p in os.listdir(vocabs_path):
            path = os.path.join(vocabs_path, p)
            if os.path.splitext(path)[1] == '.json':
                json_paths.append(path)
        json_texts = [
            fileio.read_text(path)
            for path in json_paths
        ]
        return json_texts

    @staticmethod
    def _get_vocabs(module):
        logging.info('Loading vocabs from API (%s)' % config.VOCAB_TERMS_URL)
        urls = [
            config.VOCAB_TERMS_URL % field.get('name')
            for field in module.module.FIELDS
            if field.get('vocab')
        ]
        vocabs = [
            requests.get(url).text
            for url in urls
        ]
        logging.info('ok')
        return vocabs

    @staticmethod
    def _prep_valid_values(json_texts):
        """Prepares dict of acceptable values for controlled-vocab fields.
        
        TODO should be method of DDR.modules.Module
        
        Loads choice values from FIELD.json files in the 'ddr' repository
        into a dict:
        {
            'FIELD': ['VALID', 'VALUES', ...],
            'status': ['inprocess', 'completed'],
            'rights': ['cc', 'nocc', 'pdm'],
            ...
        }
        
        >>> json_texts = [
        ...     '{"terms": [{"id": "advertisement"}, {"id": "album"}, {"id": "architecture"}], "id": "genre"}',
        ...     '{"terms": [{"id": "eng"}, {"id": "jpn"}, {"id": "chi"}], "id": "language"}',
        ... ]
        >>> batch._prep_valid_values(json_texts)
        {u'genre': [u'advertisement', u'album', u'architecture'], u'language': [u'eng', u'jpn', u'chi']}
        
        @param json_texts: list of raw text contents of files.
        @returns: dict
        """
        valid_values = {}
        for text in json_texts:
            data = json.loads(text)
            field = data['id']
            values = [term['id'] for term in data['terms']]
            if values:
                valid_values[field] = values
        return valid_values

    @staticmethod
    def _validate_csv_file(module, vocabs, headers, rowds):
        """Validate CSV headers and data against schema/field definitions
        
        @param module: modules.Module
        @param vocabs: dict Output of _prep_valid_values()
        @param headers: list
        @param rowds: list
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
        header_errs = csvfile.validate_headers(headers, field_names, nonrequired_fields)
        if header_errs.keys():
            for name,errs in header_errs.iteritems():
                if errs:
                    logging.error(name)
                    for err in errs:
                        logging.error('* %s' % err)
            logging.error('FAIL')
        else:
            logging.info('ok')
        logging.info('Validating rows')
        rowds_errs = csvfile.validate_rowds(module, headers, required_fields, valid_values, rowds)
        if rowds_errs.keys():
            for name,errs in rowds_errs.iteritems():
                if errs:
                    logging.error(name)
                    for err in errs:
                        logging.error('* %s' % err)
            logging.error('FAIL')
        else:
            logging.info('ok')
        return [header_errs, rowds_errs]

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
        is_stub = fidentifier.object_class() == models.Stub
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
    def import_entities(csv_path, cidentifier, vocabs_path, git_name, git_mail, agent, dryrun=False):
        """Adds or updates entities from a CSV file
        
        Running function multiple times with the same CSV file is idempotent.
        After the initial pass, files will only be modified if the CSV data
        has been updated.
        
        This function writes and stages files but does not commit them!
        That is left to the user or to another function.
        
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param vocabs_path: Absolute path to vocab dir
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
        headers,rowds = csvfile.make_rowds(fileio.read_csv(csv_path))
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
                entity = models.Entity.create(eidentifier.path_abs(), eidentifier)
            modified = entity.load_csv(rowd)
            # Getting obj_metadata takes about 1sec each time
            # TODO caching works as long as all objects have same metadata...
            if not obj_metadata:
                obj_metadata = models.object_metadata(
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
        return [models.csvload_rowd(module, rowd) for rowd in rowds]

    @staticmethod
    def _fidentifiers(rowds, cidentifier):
        """dict of File Identifiers by file ID."""
        return {
            rowd['id']: identifier.Identifier(
                id=rowd['id'],
                base_path=cidentifier.basepath
            )
            for rowd in rowds
        }
    
    @staticmethod
    def _fid_parents(fidentifiers):
        """dict of File Identifier parents (entities) by file ID."""
        return {
            fi.id: Importer._fidentifier_parent(fi)
            for fi in fidentifiers.itervalues()
        }
    
    @staticmethod
    def _eidentifiers(fid_parents):
        """deduplicated list of Entity Identifiers."""
        return list(
            set([
                e for e in fid_parents.itervalues()
            ])
        )
    
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
            for fid,fi in fidentifiers.iteritems()
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
        if int(rowd.get('external', 0)):
            return True
        return False
    
    @staticmethod
    def import_files(csv_path, cidentifier, vocabs_path, git_name, git_mail, agent, row_start=0, row_end=9999999, log_path=None, dryrun=False):
        """Adds or updates files from a CSV file
        
        TODO how to handle excluded fields like XMP???
        
        @param csv_path: Absolute path to CSV data file.
        @param cidentifier: Identifier
        @param vocabs_path: Absolute path to vocab dir
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param log_path: str Absolute path to addfile log for all files
        @param dryrun: boolean
        """
        logging.info('batch import files ----------------------------')
        
        # TODO hard-coded model name...
        model = 'file'
        csv_dir = os.path.dirname(csv_path)
        # TODO this still knows too much about entities and files...
        entity_class = identifier.class_for_name(
            identifier.MODEL_CLASSES['entity']['module'],
            identifier.MODEL_CLASSES['entity']['class']
        )
        repository = dvcs.repository(cidentifier.path_abs())
        logging.debug('csv_dir %s' % csv_dir)
        logging.debug('entity_class %s' % entity_class)
        logging.debug(repository)
        
        logging.info('Reading %s' % csv_path)
        headers,rowds = csvfile.make_rowds(fileio.read_csv(csv_path), row_start, row_end)
        logging.info('%s rows' % len(rowds))
        logging.info('csv_load rowds')
        module = Checker._get_module(model)
        rowds = Importer._csv_load(module, rowds)
        
        # various dicts and lists instantiated here so we don't do it
        # multiple times later
        fidentifiers = Importer._fidentifiers(rowds, cidentifier)
        fid_parents = Importer._fid_parents(fidentifiers)
        eidentifiers = Importer._eidentifiers(fid_parents)
        entities,bad_entities = Importer._existing_bad_entities(eidentifiers)
        files = Importer._file_objects(fidentifiers)
        rowds_new,rowds_existing = Importer._rowds_new_existing(rowds, files)
        if bad_entities:
            for f in bad_entities:
                logging.error('    %s missing' % f)
            raise Exception(
                '%s entities could not be loaded! - IMPORT CANCELLED!' % len(bad_entities)
            )
        
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        logging.info('Updating existing files')
        git_files = Importer._update_existing_files(
            rowds_existing,
            fid_parents, entities, files, models, repository,
            git_name, git_mail, agent,
            dryrun
        )
        
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        logging.info('Adding new files')
        git_files2 = Importer._add_new_files(
            rowds_new,
            fid_parents, entities, files,
            git_name, git_mail, agent,
            log_path, dryrun
        )
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        
        return git_files
    
    @staticmethod
    def _update_existing_files(rowds, fid_parents, entities, files, models, repository, git_name, git_mail, agent, dryrun):
        start = datetime.now(config.TZ)
        elapsed_rounds = []
        git_files = []
        updated = []
        staged = []
        obj_metadata = None
        len_rowds = len(rowds)
        for n,rowd in enumerate(rowds):
            logging.info('+ %s/%s - %s (%s)' % (
                n+1, len_rowds, rowd['id'], rowd['basename_orig']
            ))
            start_round = datetime.now(config.TZ)

            fid = rowd['id']
            eid = fid_parents[fid].id
            entity = entities[eid]
            file_ = files[fid]
            modified = file_.load_csv(rowd)
            # Getting obj_metadata takes about 1sec each time
            # TODO caching works as long as all objects have same metadata...
            if not obj_metadata:
                obj_metadata = models.object_metadata(
                    file_.identifier.fields_module(),
                    repository.working_dir
                )
            
            if modified and not dryrun:
                logging.debug('    writing %s' % file_.json_path)

                exit,status,updated_files = file_.save(
                    git_name, git_mail, agent,
                    cleaned_data=obj_metadata,
                    commit=False
                )
                
                # stage
                git_files.append(updated_files)
                updated.append(file_)
            
            elapsed_round = datetime.now(config.TZ) - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('| %s (%s)' % (file_.identifier, elapsed_round))
        
        elapsed = datetime.now(config.TZ) - start
        logging.debug('%s updated in %s' % (len(elapsed_rounds), elapsed))
                
        if git_files and not dryrun:
            logging.info('Staging %s modified files' % len(git_files))
            start_stage = datetime.now(config.TZ)
            dvcs.stage(repository, git_files)
            staged = util.natural_sort(dvcs.list_staged(repository))
            for path in staged:
                if path in git_files:
                    logging.debug('+ %s' % path)
                else:
                    logging.debug('| %s' % path)
            elapsed_stage = datetime.now(config.TZ) - start_stage
            logging.debug('ok (%s)' % elapsed_stage)
            logging.debug('%s staged in %s' % (len(staged), elapsed_stage))
        
        return git_files
    
    @staticmethod
    def _add_new_files(rowds, fid_parents, entities, files, git_name, git_mail, agent, log_path, dryrun):
        if log_path:
            logging.info('addfile logging to %s' % log_path)
        git_files = []
        failures = []
        start = datetime.now(config.TZ)
        elapsed_rounds = []
        len_rowds = len(rowds)
        for n,rowd in enumerate(rowds):
            logging.info('+ %s/%s - %s (%s)' % (n+1, len_rowds, rowd['id'], rowd['basename_orig']))
            start_round = datetime.now(config.TZ)

            fid = rowd['id']
            parent_id = fid_parents[fid].id
            file_ = files[fid]
            parent = entities[parent_id]
            # If the actual file ID is not specified in the rowd
            # (ex: SHA1 not yet known),
            # the ID in the CSV will be the ID of the *parent* object.
            # In this case, file_ and parent vars will likely be wrong.
            # TODO refactor up the chain somewhere.
            if file_.identifier.model not in identifier.NODES:
                parent = file_
            
            logging.debug('| parent %s' % (parent))
            
            # external files (no binary except maybe access file)
            if Importer._rowd_is_external(rowd) and not dryrun:
                file_,repo2,log2 = ingest.add_external_file(
                    parent,
                    rowd,
                    git_name, git_mail, agent,
                    log_path=log_path,
                    show_staged=False
                )
                if rowd.get('access_path'):
                    file_,repo3,log3,status = ingest.add_access(
                        parent, file_,
                        rowd['access_path'],
                        git_name, git_mail, agent,
                        log_path=log_path,
                        show_staged=False
                    )
                git_files.append(file_)
            
            # normal files
            elif not dryrun:
                # ingest
                # TODO make sure this updates entity.files
                
                # TODO refactor this?
                # Add role if file.ID doesn't have it
                # This will happen with e.g. transcript files when file_id is
                # actually the Entity/Segment ID and contains no role,
                # and when sha1 field is blank.
                if rowd.get('role') and not file_.identifier.parts.get('role'):
                    file_.identifier.parts['role'] = rowd['role']

                try:
                    file_,repo2,log2 = ingest.add_local_file(
                        parent,
                        rowd['basename_orig'],
                        file_.identifier.parts['role'],
                        rowd,
                        git_name, git_mail, agent,
                        log_path=log_path,
                        show_staged=False
                    )
                    git_files.append(file_)
                except ingest.FileExistsException as e:
                    logging.error('ERROR: %s' % e)
                    failures.append(e)
                except ingest.FileMissingException as e:
                    logging.error('ERROR: %s' % e)
                    failures.append(e)
            
            elapsed_round = datetime.now(config.TZ) - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('|   file %s' % (file_))
            logging.debug('| %s' % (elapsed_round))
                  
        elapsed = datetime.now(config.TZ) - start
        logging.debug('%s added in %s' % (len(elapsed_rounds), elapsed))
        if failures:
            logging.error('************************************************************************')
            for e in failures:
                logging.error(e)
            logging.error('************************************************************************')
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
        headers,rowds = csvfile.make_rowds(fileio.read_csv(csv_path))
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
    updated = {}
    files_updated = 0
    load_errs = {}
    save_errs = {}
    bad_exits = {}
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
        load_errs = [':'.join([key,val]) for key,val in self.load_errs.iteritems()]
        save_errs = [':'.join([key,val]) for key,val in self.save_errs.iteritems()]
        bad_exits = [':'.join([key,val]) for key,val in self.bad_exits.iteritems()]
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
            total_load_errs += len(metrics.load_errs.keys())
            total_save_errs += len(metrics.save_errs.keys())
            total_bad_exits += len(metrics.bad_exits.keys())
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
                        for f in metrics.updated.itervalues():
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
        for oid,paths in updated_files.iteritems():
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
            recursive=True, force_read=True,
            testing=True  # otherwise will be excluded if basedir is under /tmp
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
        
        metrics.failures = len(metrics.load_errs.keys()) + len(metrics.save_errs.keys()) + len(metrics.bad_exits.keys())
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
        with open(path, 'r') as f:
            text = f.read()
        cids = [line.strip() for line in text.strip().split('\n') if line.strip()]
        return cids
    
    @staticmethod
    def _write_todo(cids, path):
        #logging.debug('_write_todo(%s, %s)' % (cids, path))
        with open(path, 'w') as f:
            f.write('\n'.join(cids))
    
    @staticmethod
    def _read_this(basedir):
        with open(path, 'r') as f:
            text = f.read()
        return text.strip()
    
    @staticmethod
    def _write_this(basedir, cid):
        path = os.path.join(basedir, Updater.THIS)
        with open(path, 'w') as f:
            f.write(cid)

    # NOTE: should use DDR.fileio but quoting/delimiters/etc are hardcoded
    
    @staticmethod
    def _csv_reader(csvfile):
        return csv.reader(
            csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
        )
    
    @staticmethod
    def _csv_writer(csvfile):
        return csv.writer(
            csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
        )
    
    @staticmethod
    def _read_csv(path):
        rows = []
        with open(path, 'rU') as f:  # the 'U' is for universal-newline mode
            for row in Updater._csv_reader(f):
                rows.append(row)
        return rows
    
    @staticmethod
    def _write_csv(path, headers, rows):
        with open(path, 'wb') as f:
            writer = Updater._csv_writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
    
    @staticmethod
    def _read_done(basedir):
        path = os.path.join(basedir, Updater.DONE)
        headers = None
        rows = []
        if os.path.exists(path):
            rows = Updater._read_csv(path)
            if rows:
                headers = rows.pop(0)
        return headers,rows
    
    @staticmethod
    def _write_done(basedir, metrics=UpdaterMetrics()):
        path = os.path.join(basedir, Updater.DONE)
        headers = []
        rows = []
        if os.path.exists(path):
            rows = Updater._read_csv(path)
            if rows:
                headers = rows.pop(0)
        if not headers:
            headers = metrics.headers()
        if metrics.cid:
            rows.append(metrics.row())
        Updater._write_csv(path, headers, rows)
