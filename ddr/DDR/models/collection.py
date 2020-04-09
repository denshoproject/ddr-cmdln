from collections import OrderedDict
import json
import logging
logger = logging.getLogger(__name__)
import os
import re

from jinja2 import Template

from DDR import commands
from DDR import config
from DDR.control import CollectionControlFile
from DDR import docstore
from DDR import dvcs
from DDR import fileio
from DDR import format_json
from DDR.identifier import Identifier, ID_COMPONENTS
from DDR import inheritance
from DDR import locking
from DDR.models import common
from DDR.models.entity import ListEntity, Entity
from DDR import modules
from DDR import util

COLLECTION_FILES_PREFIX = 'files'

TEMPLATE_PATH = os.path.join(config.INSTALL_PATH, 'ddr', 'DDR', 'templates')
GITIGNORE_TEMPLATE = os.path.join(TEMPLATE_PATH, 'gitignore.tpl')


class Collection(common.DDRObject):
    root = None
    id = None
    idparts = None
    #collection_id = None
    #parent_id = None
    path_abs = None
    path = None
    #collection_path = None
    #parent_path = None
    json_path = None
    git_path = None
    gitignore_path = None
    annex_path = None
    changelog_path = None
    control_path = None
    ead_path = None
    lock_path = None
    files_path = None
    
    path_rel = None
    json_path_rel = None
    git_path_rel = None
    gitignore_path_rel = None
    annex_path_rel = None
    changelog_path_rel = None
    control_path_rel = None
    ead_path_rel = None
    files_path_rel = None
    
    signature_id = ''
    git_url = None
    _status = ''
    _astatus = ''
    _states = []
    _unsynced = 0
    
    def __init__( self, path_abs, id=None, identifier=None ):
        """
        >>> c = Collection('/tmp/ddr-testing-123')
        >>> c.id
        'ddr-testing-123'
        >>> c.ead_path_rel
        'ead.xml'
        >>> c.ead_path
        '/tmp/ddr-testing-123/ead.xml'
        >>> c.json_path_rel
        'collection.json'
        >>> c.json_path
        '/tmp/ddr-testing-123/collection.json'
        """
        path_abs = os.path.normpath(path_abs)
        if identifier:
            i = identifier
        else:
            i = Identifier(path=path_abs)
        self.identifier = i
        
        self.id = i.id
        self.idparts = list(i.parts.values())
        
        self.path_abs = path_abs
        self.path = path_abs
        
        self.root = os.path.split(self.path)[0]
        self.json_path          = i.path_abs('json')
        self.git_path           = i.path_abs('git')
        self.gitignore_path     = i.path_abs('gitignore')
        self.annex_path         = i.path_abs('annex')
        self.changelog_path     = i.path_abs('changelog')
        self.control_path       = i.path_abs('control')
        self.ead_path           = i.path_abs('ead')
        self.lock_path          = i.path_abs('lock')
        self.files_path         = i.path_abs('files')
        
        self.path_rel = i.path_rel()
        self.json_path_rel      = i.path_rel('json')
        self.git_path_rel       = i.path_rel('git')
        self.gitignore_path_rel = i.path_rel('gitignore')
        self.annex_path_rel     = i.path_rel('annex')
        self.changelog_path_rel = i.path_rel('changelog')
        self.control_path_rel   = i.path_rel('control')
        self.ead_path_rel       = i.path_rel('ead')
        self.files_path_rel     = i.path_rel('files')
        
        self.git_url = '{}:{}'.format(config.GITOLITE, self.id)

    @staticmethod
    def exists(oidentifier, basepath=None, gitolite=None, idservice=None):
        """Indicates whether Identifier exists in filesystem, gitolite, or idservice
        
        from DDR import dvcs
        from DDR import identifier
        from DDR import idservice
        from DDR import models
        ci = identifier.Identifier(id='ddr-test-123', '/var/www/media/ddr')
        g = dvcs.Gitolite()
        g.initialize()
        i = idservice.IDServiceClient()
        i.login('USERNAME','PASSWORD')
        Collection.exists(ci, basepath=ci.basepath, gitolite=g, idservice=i)
        
        @param oidentifier: Identifier
        @param basepath: str Absolute path
        @param gitolite: dvcs.Gitolite (initialized)
        @param idservice: idservice.IDServiceClient (initialized)
        @returns: 
        """
        data = {
            'filesystem': None,
            'gitolite': None,
            'idservice': None,
        }
        
        if basepath:
            logging.debug('Checking for %s in %s' % (oidentifier.id, oidentifier.path_abs()))
            if os.path.exists(oidentifier.path_abs()) and os.path.exists(oidentifier.path_abs('json')):
                data['filesystem'] = True
            else:
                data['filesystem'] = False
        
        if gitolite:
            logging.debug('Checking for %s in %s' % (oidentifier.id, gitolite))
            if not gitolite.initialized:
                raise Exception('%s is not initialized' % gitolite)
            if oidentifier.id in gitolite.repos():
                data['gitolite'] = True
            else:
                data['gitolite'] = False
        
        if idservice:
            logging.debug('Checking for %s in %s' % (oidentifier.id, idservice))
            if not idservice.token:
                raise Exception('%s is not initialized' % idservice)
            result = idservice.check_object_id(oidentifier.id)
            data['idservice'] = result['registered']
        
        logging.debug(data)
        return data
    
    @staticmethod
    def new(identifier, parent=None):
        """Creates new Collection with default values; does not write/commit.
        
        @param identifier: Identifier
        @param parent: [optional] DDRObject parent object
        @returns: Collection object
        """
        return common.new_object(identifier, parent=parent)
    
    @staticmethod
    def create(identifier, git_name, git_mail, agent='cmdln'):
        """Creates new Collection, writes files, performs initial commit
        
        @param identifier: Identifier
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @returns: exit,status int,str
        """
        exit,status = commands.create(
            user_name=git_name,
            user_mail=git_mail,
            identifier=identifier,
            agent=agent,
        )
        return exit,status
    
    def save(self, git_name, git_mail, agent, inheritables=[], commit=True):
        """Writes specified Collection metadata, stages, and commits.
        
        Returns exit code, status message, and list of updated files.
        Files list is for use by e.g. batch operations that want to commit
        all modified files in one operation rather than piecemeal.  This
        is included in Collection to be consistent with the other objects'
        methods.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param inheritables: list of selected inheritable fields
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        self.set_repo_description()
        
        self.write_json()
        self.write_xml()
        updated_files = [self.json_path, self.ead_path,]
        
        # propagate inheritable changes to child objects
        modified_ids,modified_files = self.update_inheritables(inheritables)
        if modified_files:
            updated_files = updated_files + modified_files

        exit,status = commands.update(
            git_name, git_mail,
            self,
            updated_files,
            agent,
            commit
        )
        return exit,status,updated_files
    
    #TODO def delete(self ...)
    
    @staticmethod
    def from_json(path_abs, identifier=None):
        """Instantiates a Collection object from specified collection.json.
        
        @param path_abs: Absolute path to .json file.
        @param identifier: [optional] Identifier
        @returns: Collection
        """
        return common.from_json(Collection, path_abs, identifier)
    
    #def from_csv
    
    @staticmethod
    def from_identifier(identifier):
        """Instantiates a Collection object using data from Identidier.
        
        @param identifier: Identifier
        @returns: Collection
        """
        return common.from_json(Collection, identifier.path_abs('json'), identifier)
    
    def parent( self ):
        """Returns Collection's parent object.
        """
        return self.identifier.parent().object()
    
    def children( self, quick=False ):
        """Returns list of the Collection's Entity objects.
        
        >>> c = Collection.from_json('/tmp/ddr-testing-123')
        >>> c.children()
        [<Entity ddr-testing-123-1>, <Entity ddr-testing-123-2>, ...]
        
        TODO use util.find_meta_files()
        
        @param quick: Boolean List only titles and IDs
        @param dicts: Boolean List only titles and IDs (dicts)
        @returns: list of Entities or ListEntity
        """
        entity_paths = []
        if os.path.exists(self.files_path):
            # TODO use cached list if available
            for eid in os.listdir(self.files_path):
                path = os.path.join(self.files_path, eid)
                entity_paths.append(path)
        entity_paths = util.natural_sort(entity_paths)
        entities = []
        for path in entity_paths:
            if quick:
                # fake Entity with just enough info for lists
                entity_json_path = os.path.join(path,'entity.json')
                if os.path.exists(entity_json_path):
                    with open(entity_json_path, 'r') as f:
                        data = json.loads(f.read())
                    e = ListEntity()
                    e.identifier = Identifier(path=path)
                    e.id = e.identifier.id
                    for line in data[1:]:
                        if 'title' in list(line.keys()):
                            e.title = line['title']
                        elif 'signature_id' in list(line.keys()):
                            e.signature_id = line['signature_id']
                    e.signature_abs = common.signature_abs(e, self.identifier.basepath)
                    entities.append(e)
            else:
                entity = Entity.from_identifier(Identifier(path=path))
                for lv in entity.labels_values():
                    if lv['label'] == 'title':
                        entity.title = lv['value']
                entities.append(entity)
        return entities
    
    def identifiers(self, model=None, force_read=False):
        """Lists Identifiers for all or subset of Collection's descendents.
        
        TODO how is this different from children?
        
        >>> c = Collection.from_json('/tmp/ddr-testing-123')
        >>> c.descendants()
        [<Entity ddr-testing-123-1>, <Entity ddr-testing-123-2>, ...]
        
        @param model: str Restrict list to model.
        @returns: list of Identifiers
        """
        return [
            Identifier(path)
            for path in util.find_meta_files(
                self.path, recursive=1, model=model, force_read=force_read
            )
        ]
    
    def load_json(self, json_text):
        """Populates Collection from JSON-formatted text.
        
        Goes through COLLECTION_FIELDS, turning data in the JSON file into
        object attributes.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        common.load_json(self, module, json_text)
    
    def dump_json(self, template=False, doc_metadata=False, obj_metadata={}):
        """Dump Collection data to JSON-formatted text.
        
        @param template: [optional] Boolean. If true, write default values for fields.
        @param doc_metadata: boolean. Insert object_metadata().
        @param obj_metadata: dict Cached results of object_metadata.
        @returns: JSON-formatted text
        """
        module = self.identifier.fields_module()
        data = common.dump_json(self, module, template=template)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, common.object_metadata(module, self.path))
        return format_json(data)
    
    def write_json(self, obj_metadata={}):
        """Write Collection JSON file to disk.
        
        @param obj_metadata: dict Cached results of object_metadata.
        """
        if not os.path.exists(self.identifier.path_abs()):
            os.makedirs(self.identifier.path_abs())
        fileio.write_text(
            self.dump_json(doc_metadata=True, obj_metadata=obj_metadata),
            self.json_path
        )
    
    def post_json(self):
        # NOTE: this is same basic code as Docstore.index
        return docstore.Docstore().post(
            self,
            docstore._public_fields().get(self.identifier.model, []),
            {
                'parent_id': self.identifier.parent_id(),
            }
        )
    
    def reindex(self):
        """Reindex Collection objects to Elasticsearch
        """
        ds = docstore.Docstore(config.DOCSTORE_HOST)
        # check for ES connection before going to all the trouble
        health = ds.health()
        index_name = ds.index_name(self.identifier.model)
        index_exists = ds.index_exists(index_name)
        if not index_exists:
            return {
                'error':'Missing Elasticsearch index "%s"' % index_name
            }
        return ds.post_multi(
            self.identifier.path_abs(), recursive=True, force=True
        )
    
    # load_csv
    # dump_csv
    
    def control( self ):
        """Gets Collection control file
        """
        if not os.path.exists(self.control_path):
            CollectionControlFile.create(self.control_path, self.id)
        return CollectionControlFile(self.control_path)
    
    #def ead( self ):
    #    """Returns a ddrlocal.models.xml.EAD object for the collection.
    #    
    #    TODO Do we really need this?
    #    """
    #    if not os.path.exists(self.ead_path):
    #        EAD.create(self.ead_path)
    #    return EAD(self)
    
    def dump_xml(self):
        """Dump Collection data to ead.xml file.
        
        TODO This should not actually write the XML! It should return XML to the code that calls it.
        """
        return Template(
            fileio.read_text(config.TEMPLATE_EAD_JINJA2)
        ).render(object=self)

    def write_xml(self):
        """Write EAD XML file to disk.
        """
        fileio.write_text(self.dump_xml(), self.ead_path)
    
    # specific to Collection
    
    def gitignore( self ):
        if not os.path.exists(self.gitignore_path):
            fileio.write_text(
                fileio.read_text(GITIGNORE_TEMPLATE),
                self.gitignore_path
            )
        return fileio.read_text(self.gitignore_path)
    
    @staticmethod
    def collection_paths( collections_root, repository, organization ):
        """Returns collection paths.
        TODO use util.find_meta_files()
        """
        paths = []
        regex = '^{}-{}-[0-9]+$'.format(repository, organization)
        id = re.compile(regex)
        for x in os.listdir(collections_root):
            m = id.search(x)
            if m:
                colldir = os.path.join(collections_root,x)
                if 'collection.json' in os.listdir(colldir):
                    paths.append(colldir)
        return util.natural_sort(paths)
    
    def repo_fetch( self ):
        """Fetch latest changes to collection repo from origin/master.
        """
        result = '-1'
        if os.path.exists(self.git_path):
            result = dvcs.fetch(dvcs.repository(self.path))
        else:
            result = '%s is not a git repository' % self.path
        return result
    
    def repo_status( self ):
        """Get status of collection repo vis-a-vis origin/master.
        
        The repo_(synced,ahead,behind,diverged,conflicted) functions all use
        the result of this function so that git-status is only called once.
        """
        if not self._status and (os.path.exists(self.git_path)):
            status = dvcs.repo_status(dvcs.repository(self.path), short=True)
            if status:
                self._status = status
        return self._status
    
    def repo_annex_status( self ):
        """Get annex status of collection repo.
        """
        if not self._astatus and (os.path.exists(self.git_path)):
            astatus = dvcs.annex_status(dvcs.repository(self.path))
            if astatus:
                self._astatus = astatus
        return self._astatus
    
    def repo_states( self ):
        """Get info on collection's repo state from git-status; cache.
        """
        if not self._states and (os.path.exists(self.git_path)):
            self._states = dvcs.repo_states(self.repo_status())
        return self._states
    
    def repo_synced( self ):     return dvcs.synced(self.repo_status(), self.repo_states())
    def repo_ahead( self ):      return dvcs.ahead(self.repo_status(), self.repo_states())
    def repo_behind( self ):     return dvcs.behind(self.repo_status(), self.repo_states())
    def repo_diverged( self ):   return dvcs.diverged(self.repo_status(), self.repo_states())
    def repo_conflicted( self ): return dvcs.conflicted(self.repo_status(), self.repo_states())

    def set_repo_description(self):
        """Set COLLECTION/.git/description based on self.title
        """
        desc_path = os.path.join(self.git_path, 'description')
        if self.title and os.path.exists(self.git_path) and os.access(desc_path, os.W_OK):
            repo = dvcs.repository(self.path)
            repo.description = self.title
    
    def missing_annex_files(self):
        """List File objects with missing binaries
        
        @returns: list of File objects
        """
        def just_id(oid):
            # some "file IDs" might have config.ACCESS_FILE_APPEND appended.
            # remove config.ACCESS_FILE_APPEND if present
            # NOTE: make sure we're not matching some other part of the ID
            # example: ddr-test-123-456-master-abc123-a
            #                                 ^^
            rindex = oid.rfind(config.ACCESS_FILE_APPEND)
            if rindex > 0:
                stem = oid[:rindex]
                suffix = oid[rindex:]
                if (len(oid) - len(stem)) \
                and (len(suffix) == len(config.ACCESS_FILE_APPEND)):
                    return stem
            return oid
        def add_id_and_hash(item):
            item['hash'] = os.path.splitext(item['keyname'])[0]
            item['id'] = just_id(
                os.path.splitext(os.path.basename(item['file']))[0]
            )
            return item
        return [
            add_id_and_hash(item)
            for item in dvcs.annex_missing_files(dvcs.repository(self.path))
        ]
    
    def child_field_values(self, model, fieldname):
        """Get all values of fieldname from specified model in collection.
        
        @param model str
        @param fieldname str
        """
        rows = []
        paths = util.find_meta_files(self.path_abs, model=model, recursive=True)
        for path in paths:
            o = Identifier(path).object()
            if getattr(o, fieldname):
                rows.append([
                    o.id,
                    fieldname,
                    getattr(o, fieldname),
                ])
        return rows
