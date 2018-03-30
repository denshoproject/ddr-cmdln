from copy import deepcopy
import logging
logger = logging.getLogger(__name__)
import os

import simplejson as json

from DDR import commands
from DDR import config
from DDR import docstore
from DDR import fileio
from DDR.identifier import Identifier, MODULES, VALID_COMPONENTS
from DDR import ingest
from DDR import inheritance
from DDR import locking
from DDR.models import common
from DDR.models.files import File
from DDR import modules
from DDR import util

ENTITY_FILES_PREFIX = 'files'

class ListEntity( object ):
    identifier = None
    id = None
    model = 'entity'
    title=''
    signature_id=''
    signature_abs=''
    # empty class used for quick view
    def __repr__(self):
        return "<DDRListEntity %s>" % (self.id)

# "children": [
#   {
#     "id": "ddr-densho-500-85-1",
#     "title": "Gordon Hirabayashi Interview II Segment 1"
#     "public": "1",
#     "order": 1
#   }
# ],
# "file_groups": [
#   {
#     "role": "master",
#     "files": [
#       {
#         "id": "ddr-densho-23-1-transcript-adb451ffec",
#         "path_rel": "ddr-densho-23-1-transcript-adb451ffec.htm",
#         "label": "Transcript of interview",
#         "record_created": "2016-07-29T18:00:00",
#         "mimetype": "applications/text",
#         "size": "12345",
#         "public": "1",
#         "sort": "1"
#       }
#     ]
#   },
#   {
#     "role": "mezzanine",
#     "files": [...]
#   },
#   {
#     "role": "transcript",
#     "files": [...]
#   },
# ]

def filegroups_to_files(file_groups):
    """Converts file_groups structure to list of files.
    
    Works with either metadata (dict) or File objects.
    
    @param file_groups: list of dicts
    @return: list of File objects
    """
    files = []
    for fg in file_groups:
        files = files + fg['files']
    return files

def files_to_filegroups(files, to_dict=False):
    """Converts list of files to file_groups structure.
    
    Works with either metadata (dict) or File objects.
    
    @param files: list
    @returns: list of dicts
    """
    def get_role(f):
        if isinstance(f, File):
            return getattr(f, 'role')
        elif isinstance(f, dict) and f.get('role'):
            return f.get('role')
        elif isinstance(f, dict) and f.get('path_rel'):
            fid = os.path.basename(os.path.splitext(f['path_rel'])[0])
            fi = Identifier(id=fid)
            return fi.idparts['role']
        return None
    # intermediate format
    fgroups = {}
    for f in files:
        role = get_role(f)
        if not fgroups.get(role):
            fgroups[role] = []
    for f in files:
        role = get_role(f)
        if role:
            if to_dict:
                fgroups[role].append(file_to_filemeta(f))
            else:
                fgroups[role].append(f)
    # final format
    file_groups = [
        {
            'role': role,
            'files': fgroups[role],
        }
        for role in VALID_COMPONENTS['role']
        if fgroups.get(role)
    ]
    return file_groups

ENTITY_ENTITY_KEYS = [
    'id',
    'title',
    'public',
    'sort',
    'signature_id',
]

ENTITY_FILE_KEYS = [
    'id',
    'path_rel',
    'label',
    #'record_created',
    #'mimetype',
    'size',
    'public',
    'sort',
]

def entity_to_childrenmeta(o):
    """Given an Entity object, return the dict used in entity.json
    
    @param o: Entity object
    @returns: dict
    """
    data = {}
    if isinstance(o, dict):
        for key in ENTITY_ENTITY_KEYS:
            val = None
            if hasattr(o, key):
                val = getattr(o, key, None)
            elif o.get(key,None):
                val = o[key]
            if val != None:
                data[key] = val
    elif isinstance(o, Entity):
        for key in ENTITY_ENTITY_KEYS:
            data[key] = getattr(o, key, None)
    return data

def file_to_filemeta(f):
    """Given a File object, return the file dict used in entity.json
    
    @param f: File object
    @returns: dict
    """
    fd = {}
    if isinstance(f, dict):
        for key in ENTITY_FILE_KEYS:
            val = None
            if hasattr(f, key):
                val = getattr(f, key, None)
            elif f.get(key,None):
                val = f[key]
            if val != None:
                fd[key] = val
    elif isinstance(f, File):
        for key in ENTITY_FILE_KEYS:
            fd[key] = getattr(f, key)
    return fd

class Entity( object ):
    root = None
    id = None
    idparts = None
    collection_id = None
    parent_id = None
    path_abs = None
    path = None
    collection_path = None
    parent_path = None
    json_path = None
    changelog_path = None
    control_path = None
    mets_path = None
    files_path = None
    path_rel = None
    json_path_rel = None
    changelog_path_rel = None
    control_path_rel = None
    mets_path_rel = None
    files_path_rel = None
    children_meta = []
    files_dict = {}
    file_groups = []
    _children_objects = 0
    _file_objects = 0
    _file_objects_loaded = 0
    signature_id = ''
    
    def __init__( self, path_abs, id=None, identifier=None ):
        path_abs = os.path.normpath(path_abs)
        if identifier:
            i = identifier
        else:
            i = Identifier(path=path_abs)
        self.identifier = i
        
        self.id = i.id
        self.idparts = i.parts.values()
        
        self.collection_id = i.collection_id()
        self.parent_id = i.parent_id()
        
        self.path_abs = path_abs
        self.path = path_abs
        self.collection_path = i.collection_path()
        self.parent_path = i.parent_path()
        
        self.root = os.path.dirname(self.parent_path)
        self.json_path = i.path_abs('json')
        self.changelog_path = i.path_abs('changelog')
        self.control_path = i.path_abs('control')
        self.mets_path = i.path_abs('mets')
        self.lock_path = i.path_abs('lock')
        self.files_path = i.path_abs('files')
        
        self.path_rel = i.path_rel()
        self.json_path_rel = i.path_rel('json')
        self.changelog_path_rel = i.path_rel('changelog')
        self.control_path_rel = i.path_rel('control')
        self.mets_path_rel = i.path_rel('mets')
        self.files_path_rel = i.path_rel('files')
        
        self._children_objects = []
        self._file_objects = []
    
    def __repr__(self):
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.identifier.model, self.id
        )

    @staticmethod
    def exists(oidentifier, basepath=None, gitolite=None, idservice=None):
        """Indicates whether Identifier exists in filesystem and/or idservice
        
        from DDR import dvcs
        from DDR import identifier
        from DDR import idservice
        from DDR import models
        ei = identifier.Identifier(id='ddr-test-123-456', '/var/www/media/ddr')
        i = idservice.IDServiceClient()
        i.login('USERNAME','PASSWORD')
        Entity.exists(ci, basepath=ci.basepath, idservice=i)
        
        @param oidentifier: Identifier
        @param basepath: str Absolute path
        @param gitolite: dvcs.Gitolite (ignored)
        @param idservice: idservice.IDServiceClient (initialized)
        @returns: 
        """
        data = {
            'filesystem': None,
            'idservice': None,
        }
        
        if basepath:
            logging.debug('Checking for %s in %s' % (oidentifier.id, oidentifier.path_abs()))
            if os.path.exists(oidentifier.path_abs()) and os.path.exists(oidentifier.path_abs('json')):
                data['filesystem'] = True
            else:
                data['filesystem'] = False
        
        if idservice:
            logging.debug('Checking for %s in %s' % (oidentifier.id, idservice))
            if not idservice.token:
                raise Exception('%s is not initialized' % idservice)
            result = idservice.check_object_id(oidentifier.id)
            data['idservice'] = result['registered']
        
        logging.debug(data)
        return data
    
    @staticmethod
    def create(path_abs, identifier=None):
        """Creates a new Entity with initial values from module.FIELDS.
        
        @param path_abs: str Absolute path; must end in valid DDR id.
        @param identifier: [optional] Identifier
        @returns: Entity object
        """
        if not identifier:
            identifier = Identifier(path=path_abs)
        obj = common.create_object(identifier)
        obj.files = []
        return obj
    
    @staticmethod
    def new(identifier, git_name, git_mail, agent='cmdln'):
        """Creates new Entity, writes to filesystem, does initial commit.
        
        @param identifier: Identifier
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @returns: exit,status int,str
        """
        collection = identifier.collection().object()
        if not collection:
            raise Exception('Parent collection for %s does not exist.' % identifier)
        entity = Entity.create(identifier.path_abs(), identifier)
        fileio.write_text(
            entity.dump_json(template=True),
            config.TEMPLATE_EJSON
        )
        exit,status = commands.entity_create(
            git_name, git_mail,
            collection, entity.identifier,
            [collection.json_path_rel, collection.ead_path_rel],
            [config.TEMPLATE_EJSON, config.TEMPLATE_METS],
            agent=agent
        )
        if exit:
            raise Exception('Could not create new Entity: %s, %s' % (exit, status))
        # load Entity object, inherit values from parent, write back to file
        entity = Entity.from_identifier(identifier)
        entity.inherit(collection)
        entity.write_json()
        updated_files = [entity.json_path]
        exit,status = commands.entity_update(
            git_name, git_mail,
            collection, entity,
            updated_files,
            agent=agent
        )
        return exit,status
    
    def save(self, git_name, git_mail, agent, collection=None, cleaned_data={}, commit=True):
        """Writes specified Entity metadata, stages, and commits.
        
        Updates .children and .file_groups if parent is another Entity.
        Returns exit code, status message, and list of updated files.  Files list
        is for use by e.g. batch operations that want to commit all modified files
        in one operation rather than piecemeal.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param collection: Collection
        @param cleaned_data: dict Form data (all fields required)
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        if not collection:
            collection = self.identifier.collection().object()
        parent = self.identifier.parent().object()
        
        if cleaned_data:
            self.form_post(cleaned_data)
        
        self.children(force_read=True)
        self.write_json()
        self.write_mets()
        updated_files = [
            self.json_path,
            self.mets_path,
            self.changelog_path,
        ]
        
        if parent and isinstance(parent, Entity):
            # update parent .children and .file_groups
            parent.children(force_read=True)
            parent.write_json()
            updated_files.append(parent.json_path)
        
        inheritables = self.selected_inheritables(cleaned_data)
        modified_ids,modified_files = self.update_inheritables(inheritables, cleaned_data)
        if modified_files:
            updated_files = updated_files + modified_files

        exit,status = commands.entity_update(
            git_name, git_mail,
            collection, self,
            updated_files,
            agent,
            commit
        )
        return exit,status,updated_files
    
    #TODO def delete(self ...)
    
    @staticmethod
    def from_json(path_abs, identifier=None):
        """Instantiates an Entity object from specified entity.json.
        
        @param path_abs: Absolute path to .json file.
        @param identifier: [optional] Identifier
        @returns: Entity
        """
        return common.from_json(Entity, path_abs, identifier)
    
    @staticmethod
    def from_csv(identifier, rowd):
        """Instantiates a File object from CSV row data.
        
        @param identifier: [optional] Identifier
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: Entity
        """
        return common.from_csv(identifier, rowd)
    
    @staticmethod
    def from_identifier(identifier):
        """Instantiates an Entity object, loads data from entity.json.
        
        @param identifier: Identifier
        @returns: Entity
        """
        return common.from_json(Entity, identifier.path_abs('json'), identifier)
    
#    def parent( self ):
#        """
#        TODO Entity.parent is overridden by a field value
#        """
#        cidentifier = self.identifier.parent()
#        return Collection.from_identifier(cidentifier)
   
    def children( self, role=None, quick=None, force_read=False ):
        """Return list of Entity's children; regenerate list if specified.
        
        @param role: str Restrict list to specified role
        @param quick: bool Not used
        @param force_read: bool Scan entity dir for file jsons
        @returns: list of File objects, sorted
        """
        self.load_children_objects(Identifier, Entity, force_read=force_read)
        self.load_file_objects(Identifier, File, force_read=force_read)
        if role:
            files = [
                f for f in self._file_objects
                if hasattr(f,'role') and (f.role == role)
            ]
        else:
            files = [f for f in self._file_objects]
        self.files = sorted(files, key=lambda f: int(f.sort))
        return self._children_objects + self.files
    
    def signature_abs(self):
        """Absolute path to signature image file, if signature_id present.
        """
        return common.signature_abs(self, self.identifier.basepath)
    
    def labels_values(self):
        """Apply display_{field} functions to prep object data for the UI.
        """
        module = self.identifier.fields_module()
        return modules.Module(module).labels_values(self)
    
    def choices(self, field_name):
        """Returns controlled-vocab choices for specified field, if any
        
        @param field_name: str
        @returns: list or None
        """
        return modules.Module(self.identifier.fields_module()).field_choices(field_name)
    
    def form_prep(self):
        """Apply formprep_{field} functions in Entity module to prep data dict to pass into DDRForm object.
        
        @returns data: dict object as used by Django Form object.
        """
        return common.form_prep(self, self.identifier.fields_module())
    
    def form_post(self, cleaned_data):
        """Apply formpost_{field} functions to process cleaned_data from DDRForm
        
        @param cleaned_data: dict
        """
        common.form_post(self, self.identifier.fields_module(), cleaned_data)

    def inheritable_fields( self ):
        module = self.identifier.fields_module()
        return inheritance.inheritable_fields(module.FIELDS)
    
    def selected_inheritables(self, cleaned_data ):
        """Returns names of fields marked as inheritable in cleaned_data.
        
        Fields are considered selected if dict contains key/value pairs in the form
        'FIELD_inherit':True.
        
        @param cleaned_data: dict Fieldname:value pairs.
        @returns: list
        """
        return inheritance.selected_inheritables(self.inheritable_fields(), cleaned_data)
    
    def update_inheritables( self, inheritables, cleaned_data ):
        """Update specified fields of child objects.
        
        @param inheritables: list Names of fields that shall be inherited.
        @param cleaned_data: dict Fieldname:value pairs.
        @returns: tuple [changed object Ids],[changed objects' JSON files]
        """
        return inheritance.update_inheritables(self, 'entity', inheritables, cleaned_data)
    
    def inherit( self, parent ):
        inheritance.inherit( parent, self )
    
    def lock( self, text ): return locking.lock(self.lock_path, text)
    def unlock( self, text ): return locking.unlock(self.lock_path, text)
    def locked( self ): return locking.locked(self.lock_path)

    def load_json(self, json_text):
        """Populate Entity data from JSON-formatted text.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        json_data = common.load_json(self, module, json_text)
        # special cases
        # files or file_groups -> self.files
        self.files = []
        for fielddict in json_data:
            for fieldname,data in fielddict.iteritems():
                if (fieldname in ['children', 'children_meta']) and data:
                    self.children_meta = data
                elif (fieldname == 'file_groups') and data:
                    self.files_dict = {
                        d['role']: d['files']
                        for d in data
                    }
                    self.file_groups = [
                        {'role': role, 'files': self.files_dict.get(role, [])}
                        for role in VALID_COMPONENTS['role']
                    ]
                    self.files = filegroups_to_files(data)
                elif (fieldname == 'files') and data:
                    self.files = data
        self.rm_file_duplicates()

    def dump_json(self, template=False, doc_metadata=False, obj_metadata={}):
        """Dump Entity data to JSON-formatted text.
        
        @param template: [optional] Boolean. If true, write default values for fields.
        @param doc_metadata: boolean. Insert object_metadata().
        @param obj_metadata: dict Cached results of object_metadata.
        @returns: JSON-formatted text
        """
        module = self.identifier.fields_module()
        data = common.dump_json(self, module,
                         exceptions=['files', 'filemeta'],
                         template=template,)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, object_metadata(module, self.parent_path))
        
        data.append({
            'children': [entity_to_childrenmeta(o) for o in self.children_meta]
        })
        data.append({
            'file_groups': files_to_filegroups(self._file_objects, to_dict=1)
        })
        return common.format_json(data)

    def write_json(self, obj_metadata={}):
        """Write Entity JSON file to disk.
        
        @param obj_metadata: dict Cached results of object_metadata.
        """
        if not os.path.exists(self.identifier.path_abs()):
            os.makedirs(self.identifier.path_abs())
        fileio.write_text(
            self.dump_json(doc_metadata=True, obj_metadata=obj_metadata),
            self.json_path
        )
    
    def post_json(self):
        # NOTE: this is same basic code as docstore.index
        return docstore.Docstore().post(
            common.load_json_lite(self.json_path, self.identifier.model, self.id),
            docstore._public_fields().get(self.identifier.model, []),
            {
                'parent_id': self.parent_id,
            }
        )

    def load_csv(self, rowd):
        """Populate Entity data from CSV-formatted text.
        
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: list of changed fields
        """
        module = modules.Module(self.identifier.fields_module())
        modified = common.load_csv(self, module, rowd)
        ## special cases
        #def parsedt(txt):
        #    d = datetime.now(config.TZ)
        #    try:
        #        d = converters.text_to_datetime(txt)
        #    except:
        #        try:
        #            d = converters.text_to_datetime(txt)
        #        except:
        #            pass
        #    return d
        if not hasattr(self, 'record_created'):
            self.record_created = datetime.now(config.TZ)
        if modified and hasattr(self, 'record_lastmod'):
            self.record_lastmod = datetime.now(config.TZ)
        self.rm_file_duplicates()
        return modified

    def dump_csv(self, headers=[]):
        """Dump Entity data to CSV-formatted text.
        
        @returns: JSON-formatted text
        """
        module = modules.Module(self.identifier.fields_module())
        return common.prep_csv(self, module, headers=headers)
    
    def changelog( self ):
        """Gets Entity changelog
        """
        if os.path.exists(self.changelog_path):
            return open(self.changelog_path, 'r').read()
        return '%s is empty or missing' % self.changelog_path
    
    def control( self ):
        """Gets Entity control file
        """
        if not os.path.exists(self.control_path):
            EntityControlFile.create(self.control_path, self.parent_id, self.id)
        return EntityControlFile(self.control_path)
    
    def dump_xml(self):
        """Dump Entity data to mets.xml file.
        
        TODO This should not actually write the XML! It should return XML to the code that calls it.
        """
        with open(config.TEMPLATE_METS_JINJA2, 'r') as f:
            template = f.read()
        return Template(template).render(object=self)

    def write_xml(self):
        """Write METS XML file to disk.
        """
        fileio.write_text(self.dump_mets(), self.mets_path)
    
    # specific to Entity
    
    @staticmethod
    def checksum_algorithms():
        return ['md5', 'sha1', 'sha256']
    
    def checksums(self, algo, force_read=False):
        """Calculates hash checksums for the Entity's files.
        
        Gets hashes from FILE.json metadata if the file(s) are absent
        from the filesystem (i.e. git-annex file symlinks).
        Overrides DDR.models.Entity.checksums.
        
        @param algo: str
        @param force_read: bool Traverse filesystem if true.
        @returns: list of (checksum, filepath) tuples
        """
        checksums = []
        if algo not in self.checksum_algorithms():
            raise Error('BAD ALGORITHM CHOICE: {}'.format(algo))
        for f in self._file_paths():
            cs = None
            ext = None
            pathname = os.path.splitext(f)[0]
            # from metadata file
            json_path = os.path.join(self.files_path, f)
            for field in json.loads(fileio.read_text(json_path)):
                for k,v in field.iteritems():
                    if k == algo:
                        cs = v
                    if k == 'basename_orig':
                        ext = os.path.splitext(v)[-1]
            fpath = pathname + ext
            if force_read:
                # from filesystem
                # git-annex files are present
                if os.path.exists(fpath):
                    cs = util.file_hash(fpath, algo)
            if cs:
                checksums.append( (cs, os.path.basename(fpath)) )
        return checksums
    
    def _children_paths(self, rel=False):
        """Searches filesystem for (entity) childrens' metadata files, returns relative paths.
        @param rel: bool Return relative paths
        @returns: list
        """
        if os.path.exists(self.files_path):
            prefix_path = 'THISWILLNEVERMATCHANYTHING'
            if rel:
                prefix_path = '{}/'.format(os.path.normpath(self.files_path))
            return sorted(
                [
                    f.replace(prefix_path, '')
                    for f in util.find_meta_files(self.files_path, recursive=True)
                ],
                key=lambda f: util.natural_order_string(f)
            )
        return []
    
    def _file_paths(self, rel=False):
        """Searches filesystem for childrens' metadata files, returns relative paths.
        @param rel: bool Return relative paths
        @returns: list
        """
        if os.path.exists(self.files_path):
            prefix_path = 'THISWILLNEVERMATCHANYTHING'
            if rel:
                prefix_path = '{}/'.format(os.path.normpath(self.files_path))
            return sorted(
                [
                    f.replace(prefix_path, '')
                    for f in util.find_meta_files(self.files_path, recursive=False)
                ],
                key=lambda f: util.natural_order_string(f)
            )
        return []
    
    def load_children_objects(self, identifier_class, object_class, force_read=False):
        """Regenerates list of file info dicts with list of File objects
        
        TODO Don't call in loop - causes all file .JSONs to be loaded!
        
        @param force_read: bool Traverse filesystem if true.
        @returns: None
        """
        if force_read or (not hasattr(self, '_children_objects')):
            self._children_objects = [
                object_class.from_identifier(
                    identifier_class(
                        path=os.path.dirname(json_path)
                    )
                )
                for json_path in self._children_paths()
                if util.path_matches_model(json_path, self.identifier.model)
            ]
            self.children_meta = [
                entity_to_childrenmeta(o)
                for o in self._children_objects
            ]
        return self._children_objects
    
    def load_file_objects(self, identifier_class, object_class, force_read=False):
        """Regenerates list of file info dicts with list of File objects
        
        NOTE: if file_groups contains pointer to nonexistent file, insert a dict
        containing the error message rather than crashing.
        TODO Don't call in loop - causes all file .JSONs to be loaded!
        
        @param force_read: bool Traverse filesystem if true.
        @returns: None
        """
        self._file_objects = []
        if force_read:
            # filesystem
            for json_path in self._file_paths():
                fid = os.path.splitext(os.path.basename(json_path))[0]
                basepath = self.identifier.basepath
                try:
                    file_ = object_class.from_identifier(
                        identifier_class(
                            id=fid,
                            base_path=basepath
                        )
                    )
                except IOError as err:
                    f['error'] = err
                    file_ = {'id': fid, 'error': err}
                self._file_objects.append(file_)
        else:
            for f in self.files:
                if f and f.get('path_rel',None):
                    basename = os.path.basename(f['path_rel'])
                    fid = os.path.splitext(basename)[0]
                    try:
                        file_ = object_class.from_identifier(
                            identifier_class(
                                id=fid,
                                base_path=self.identifier.basepath
                            )
                        )
                    except IOError as err:
                        f['error'] = err
                        file_ = {'id': fid, 'error': err}
                    self._file_objects.append(file_)
        # keep track of how many times this gets loaded...
        self._file_objects_loaded = self._file_objects_loaded + 1
    
    def detect_file_duplicates( self, role ):
        """Returns list of file dicts that appear in Entity.files more than once
        
        NOTE: This function looks only at the list of file dicts in entity.json;
        it does not examine the filesystem.
        """
        duplicates = []
        for x,f in enumerate(self.files):
            for y,f2 in enumerate(self.files):
                if (f != f2) and (f['path_rel'] == f2['path_rel']) and (f2 not in duplicates):
                    duplicates.append(f)
        return duplicates
    
    def rm_file_duplicates( self ):
        """Remove duplicates from the Entity.files (._files) list of dicts.
        
        Technically, it rebuilds the last without the duplicates.
        NOTE: See note for detect_file_duplicates().
        """
        # regenerate files list
        new_files = []
        for f in self.files:
            if f not in new_files:
                new_files.append(f)
        self.files = new_files
        # reload objects
        self.load_file_objects(Identifier, File)
    
    def _children_meta(self):
        """
        children = [
            {
                "id": "ddr-densho-500-85-1",
                "sort": 1,
                "public": "1",
                "title": "Gordon Hirabayashi Interview II Segment 1"
            },
            {
                "id": "ddr-densho-500-85-2",
                "sort": 2,
                "public": "1",
                "title": "Gordon Hirabayashi Interview II Segment 2"
            },
            ...
        ]
        """
        children = []
        for child in self.children():
            if isinstance(child, File):
                children.append({
                    "id": child.id,
                    "order": child.sort,
                    "public": child.public,
                    "title": child.label,
                })
            elif isinstance(child, Entity):
                children.append({
                    "id": child.id,
                    "order": child.sort,
                    "public": child.public,
                    "title": child.title,
                })
        return children
        
    def file( self, role, sha1, newfile=None ):
        """Given a SHA1 hash, get the corresponding file dict.
        
        @param role
        @param sha1
        @param newfile (optional) If present, updates existing file or appends new one.
        @returns 'added', 'updated', File, or None
        """
        self.load_file_objects(Identifier, File)
        # update existing file or append
        if sha1 and newfile:
            for f in self.files:
                if sha1 in f.sha1:
                    f = newfile
                    return 'updated'
            self.files.append(newfile)
            return 'added'
        # get a file
        for f in self._file_objects:
            if (f.sha1[:10] == sha1[:10]) and (f.role == role):
                return f
        # just do nothing
        return None

    def addfile_logger(self):
        return ingest.addfile_logger(self)
    
    def add_local_file(self, src_path, role, data, git_name, git_mail, agent=''):
        return ingest.add_local_file(self, src_path, role, data, git_name, git_mail, agent)
    
    def add_external_file(self, data, git_name, git_mail, agent=''):
        return ingest.add_external_file(self, data, git_name, git_mail, agent)
    
    def add_access(self, ddrfile, src_file, git_name, git_mail, agent=''):
        return ingest.add_access(
            self, ddrfile, src_file, git_name, git_mail, agent=''
        )
    
    def add_file_commit(self, file_, repo, log, git_name, git_mail, agent):
        return ingest.add_file_commit(self, file_, repo, log, git_name, git_mail, agent)

    def prep_rm_file(self, file_):
        """Delete specified file and update Entity.
        
        IMPORTANT: This function modifies entity.json and lists files to remove.
        The actual file removal and commit is done by commands.file_destroy.
        
        @param file_: File
        """
        logger.debug('%s.rm_file(%s)' % (self, file_))
        
        # list of files to be *removed*
        rm_files = [
            f for f in file_.files_rel()
            if os.path.exists(
                os.path.join(self.collection_path, f)
            )
        ]
        logger.debug('rm_files: %s' % rm_files)
        
        # rm file_ from entity metadata
        #
        # entity._file_objects
        self._file_objects = [
            f for f in deepcopy(self._file_objects) if f.id != file_.id
        ]
        #
        # entity.file_groups (probably unnecessary)
        files = filegroups_to_files(self.file_groups)
        for f in files:
            if f.get('path_rel') and not f.get('id'):
                # make sure each file dict has an id
                f['id'] = os.path.basename(os.path.splitext(f['path_rel'])[0])
        self.file_groups = files_to_filegroups(
            # exclude the file
            [f for f in files if f['id'] != file_.id]
        )
        self.write_json()
        
        # list of files to be *updated*
        updated_files = ['entity.json']
        logger.debug('updated_files: %s' % updated_files)
        
        return rm_files,updated_files
