"""
AKA cumberbatch.py

Check to see if CSV file is internally valid
See which EIDs would be added
Update existing records
Import new records
Register newly added EIDs
"""

import codecs
from datetime import datetime
import json
import logging
import os

import requests

from DDR import changelog
from DDR import config
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
                    writer.writerow(obj.dump_csv(headers=headers))
        
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
        start_updates = datetime.now()
        git_files = []
        updated = []
        elapsed_rounds = []
        obj_metadata = None
        
        if dryrun:
            logging.info('Dry run - no modifications')
        for n,rowd in enumerate(rowds):
            logging.info('%s/%s - %s' % (n+1, len(rowds), rowd['id']))
            start_round = datetime.now()
            
            eidentifier = identifier.Identifier(id=rowd['id'], base_path=cidentifier.basepath)
            # if there is an existing object it will be loaded
            entity = eidentifier.object()
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
                entity.write_json(obj_metadata=obj_metadata)
                # TODO better to write to collection changelog?
                # TODO write all additions to changelog at one time
                Importer._write_entity_changelog(entity, git_name, git_mail, agent)
                # stage
                git_files.append(entity.json_path_rel)
                git_files.append(entity.changelog_path_rel)
                updated.append(entity)
            
            elapsed_round = datetime.now() - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('| %s (%s)' % (eidentifier, elapsed_round))
    
        if dryrun:
            logging.info('Dry run - no modifications')
        elif updated:
            logging.info('Staging %s modified files' % len(git_files))
            start_stage = datetime.now()
            dvcs.stage(repository, git_files)
            for path in util.natural_sort(dvcs.list_staged(repository)):
                if path in git_files:
                    logging.debug('+ %s' % path)
                else:
                    logging.debug('| %s' % path)
            elapsed_stage = datetime.now() - start_stage
            logging.debug('ok (%s)' % elapsed_stage)
        
        elapsed_updates = datetime.now() - start_updates
        logging.debug('%s updated in %s' % (len(elapsed_rounds), elapsed_updates))
        logging.info('- - - - - - - - - - - - - - - - - - - - - - - -')
        
        return updated

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
            obj_not_stub = not isinstance(obj, models.Stub)
            if obj and obj_not_stub:
                json_exists = obj.exists()
            else:
                json_exists = False
            # decide
            if has_id and obj and obj_not_stub and json_exists:
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
    def import_files(csv_path, cidentifier, vocabs_path, git_name, git_mail, agent, log_path=None, dryrun=False):
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
        headers,rowds = csvfile.make_rowds(fileio.read_csv(csv_path))
        logging.info('%s rows' % len(rowds))
        
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
        start = datetime.now()
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
            start_round = datetime.now()

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
                file_.write_json(obj_metadata=obj_metadata)
                # TODO better to write to collection changelog?
                Importer._write_entity_changelog(entity, git_name, git_mail, agent)
                # stage
                git_files.append(file_.json_path_rel)
                git_files.append(entity.changelog_path_rel)
                updated.append(file_)
            
            elapsed_round = datetime.now() - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('| %s (%s)' % (file_.identifier, elapsed_round))
        
        elapsed = datetime.now() - start
        logging.debug('%s updated in %s' % (len(elapsed_rounds), elapsed))
                
        if git_files and not dryrun:
            logging.info('Staging %s modified files' % len(git_files))
            start_stage = datetime.now()
            dvcs.stage(repository, git_files)
            staged = util.natural_sort(dvcs.list_staged(repository))
            for path in staged:
                if path in git_files:
                    logging.debug('+ %s' % path)
                else:
                    logging.debug('| %s' % path)
            elapsed_stage = datetime.now() - start_stage
            logging.debug('ok (%s)' % elapsed_stage)
            logging.debug('%s staged in %s' % (len(staged), elapsed_stage))
        
        return git_files
    
    @staticmethod
    def _add_new_files(rowds, fid_parents, entities, files, git_name, git_mail, agent, log_path, dryrun):
        if log_path:
            logging.info('addfile logging to %s' % log_path)
        git_files = []
        start = datetime.now()
        elapsed_rounds = []
        len_rowds = len(rowds)
        for n,rowd in enumerate(rowds):
            logging.info('+ %s/%s - %s (%s)' % (n+1, len_rowds, rowd['id'], rowd['basename_orig']))
            start_round = datetime.now()
            
            fid = rowd['id']
            eid = fid_parents[fid].id
            file_ = files[fid]
            entity = entities[eid]
            logging.debug('| %s' % (entity))
            
            # external files (no binary except maybe access file)
            if Importer._rowd_is_external(rowd) and not dryrun:
                file_,repo2,log2 = ingest.add_external_file(
                    entity,
                    rowd,
                    git_name, git_mail, agent,
                    log_path=log_path,
                    show_staged=False
                )
                if rowd.get('access_path'):
                    file_,repo3,log3,status = ingest.add_access(
                        entity, file_,
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
                file_,repo2,log2 = ingest.add_local_file(
                    entity,
                    rowd['basename_orig'],
                    file_.identifier.parts['role'],
                    rowd,
                    git_name, git_mail, agent,
                    log_path=log_path,
                    show_staged=False
                )
                git_files.append(file_)
            
            elapsed_round = datetime.now() - start_round
            elapsed_rounds.append(elapsed_round)
            logging.debug('| %s (%s)' % (file_, elapsed_round))
        
        elapsed = datetime.now() - start
        logging.debug('%s added in %s' % (len(elapsed_rounds), elapsed))
        return git_files
    
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
