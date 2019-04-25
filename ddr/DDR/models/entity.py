from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from functools import total_ordering
import logging
logger = logging.getLogger(__name__)
import os

from jinja2 import Template
import simplejson as json

from DDR import commands
from DDR import config
from DDR.control import EntityControlFile
from DDR import docstore
from DDR import fileio
from DDR import format_json
from DDR.identifier import Identifier, MODULES
from DDR.identifier import CHILDREN, ID_COMPONENTS, NODES, VALID_COMPONENTS
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

# attrs used in METS Entity file_groups
ENTITY_ENTITY_KEYS = [
    'id',
    'title',
    'public',
    'sort',
    'signature_id',
]

@total_ordering
class Entity(common.DDRObject):
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
    _entities_meta = []
    _files_meta = []
    _children_objects = []
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

    def __lt__(self, other):
        """Enable Pythonic sorting"""
        return self._key() < other._key()
    
    def _key(self):
        """Key for Pythonic object sorting.
        Returns tuple of self.sort,self.identifier.id_sort
        (self.sort takes precedence over ID sort)
        """
        return self.sort,self.identifier.id_sort

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
            logging.debug('Checking for %s in %s' % (
                oidentifier.id, oidentifier.path_abs()
            ))
            if os.path.exists(oidentifier.path_abs()) \
            and os.path.exists(oidentifier.path_abs('json')):
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
    def create(identifier, parent=None):
        """Creates a new Entity with initial values from module.FIELDS.
        
        @param identifier: Identifier
        @param parent: [optional] DDRObject parent object
        @returns: Entity object
        """
        obj = common.create_object(identifier, parent=parent)
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
        entity = Entity.create(identifier)
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
    
    def save(self, git_name, git_mail, agent, collection=None, inheritables=[], commit=True):
        """Writes specified Entity metadata, stages, and commits.
        
        Updates .children if parent is another Entity.
        Returns exit code, status message, and list of updated files.
        Files list is for use by e.g. batch operations that want to commit
        all modified files in one operation rather than piecemeal.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param collection: Collection
        @param inheritables: list of selected inheritable fields
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        if not collection:
            collection = self.identifier.collection().object()
        parent = self.identifier.parent().object()
        
        self.children(force_read=True)
        self.write_json()
        self.write_xml()
        updated_files = [
            self.json_path,
            self.mets_path,
            self.changelog_path,
        ]
        
        if parent and isinstance(parent, Entity):
            # update parent.children
            parent.children(force_read=True)
            parent.write_json()
            updated_files.append(parent.json_path)
        
        # propagate inheritable changes to child objects
        modified_ids,modified_files = self.update_inheritables(inheritables)
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
    
    def delete(self, git_name, git_mail, agent, commit=True):
        """Removes File metadata and file, updates parent entity, and commits.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param commit: boolean
        @returns: exit,status,removed_files,updated_files (int,str,list,list)
        """
        exit = 1; status = 'unknown'; updated_files = []
        parent = self.identifier.parent().object()
        collection = self.identifier.collection().object()
        
        # metadata jsons
        # NOTE: child File objects are deleted in commands.entity_destroy
        
        # parent entity
        parent.remove_child(self.id)
        parent.write_json(force=True)
        updated_files = [
            parent.identifier.path_rel('json'),
        ]
        
        # write files and commit
        return commands.entity_destroy(
            git_name, git_mail,
            self,
            updated_files,
            agent=agent,
            commit=commit
        )

    def dict(self, file_groups=False, json_safe=False):
        """Returns OrderedDict of object data
        
        Overrides common.DDRObject.dict and adds METS Entity children,file_groups
        
        @param json_safe: bool Serialize e.g. datetime to text
        @returns: OrderedDict
        """
        if file_groups:
            return {
                key: getattr(self, key)
                for key in ENTITY_ENTITY_KEYS
            }
        data = common.to_dict(
            self, self.identifier.fields_module(), json_safe=json_safe
        )
        data['children'] = [
            entity_to_childrenmeta(o)
            for o in self.children()
            if o.identifier.model in ['entity', 'segment']
        ]
        data['file_groups'] = files_to_filegroups([
            o for o in self.children()
            if o.identifier.model == 'file'
        ])
        return data
    
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
   
    def children(self, models=None, role=None, quick=None, force_read=False):
        """List Entity's child objects,files; optionally regenerate list
        
        @param model: list Restrict to specified model(s)
        @param role: str Restrict list to specified File role
        @param quick: bool Not used
        @param force_read: bool Scan entity dir for file jsons
        @returns: list of File objects, sorted
        """
        if force_read or not self._children_objects:
            # read objects from filesystem
            self._children_objects = _sort_children([
                Identifier(path).object() for path in self._children_paths()
            ])
        if models:
            return [
                o
                for o in self._children_objects
                if o.identifier.model in models
            ]
        elif role:
            return [
                o
                for o in self._children_objects
                if hasattr(o, 'role') and (o.role == role)
            ]
        return self._children_objects

    def add_child(self, obj):
        """Adds the Entity or File to Entity.children
        """
        assert obj.identifier.model in ['entity', 'segment', 'file']
        self._children_objects.append(obj)
        self._children_objects = _sort_children(self._children_objects)
        
    def children_counts(self):
        """Totals number of (entity) children and each file role
        
        Used to produce tabs in ddrlocal UI
        
        @returns: dict {'children', 1, 'mezzanine':3, 'master':3, ... }
        """
        # set up structure
        counts = OrderedDict()
        counts['children'] = 0
        for role in VALID_COMPONENTS['role']:
            counts[role] = 0
        # non-file objects are 'children'
        for c in self.children():
            if c.identifier.model not in NODES:
                counts['children'] += 1
        # file roles have their own counts
        for c in self.children():
            if c.identifier.idparts.get('role'):
                counts[c.identifier.idparts['role']] += 1
        return counts
        
    def load_json(self, json_text):
        """Populate Entity data from JSON-formatted text.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        json_data = common.load_json(self, module, json_text)

    def dump_json(self, template=False, doc_metadata=False, obj_metadata={}):
        """Dump Entity data to JSON-formatted text.
        
        @param template: [optional] Boolean. If true, write default field values.
        @param doc_metadata: boolean. Insert object_metadata().
        @param obj_metadata: dict Cached results of object_metadata.
        @returns: JSON-formatted text
        """
        module = self.identifier.fields_module()
        self.children(force_read=True)
        data = common.dump_json(self, module,
                         exceptions=['files', 'filemeta'],
                         template=template,)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, common.object_metadata(module, self.parent_path))
        
        data.append({
            'children': [
                entity_to_childrenmeta(o)
                for o in self.children()
                if o.identifier.model in ['entity', 'segment']
            ]
        })
        data.append({
            'file_groups': files_to_filegroups([
                o for o in self.children()
                if o.identifier.model == 'file'
            ])
        })
        return format_json(data)
    
    def post_json(self):
        # NOTE: this is same basic code as docstore.index
        return docstore.Docstore().post(
            self,
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
        return modified

    def dump_csv(self, fields=[]):
        """Dump Entity data to CSV-formatted text.
        
        @returns: JSON-formatted text
        """
        module = modules.Module(self.identifier.fields_module())
        return common.prep_csv(self, module, fields=fields)
    
    
    def control( self ):
        """Gets Entity control file
        """
        if not os.path.exists(self.control_path):
            EntityControlFile.create(self.control_path, self.parent_id, self.id)
        return EntityControlFile(self.control_path)
    
    def dump_xml(self):
        """Dump Entity data to mets.xml file.
        
        TODO This should not actually write the XML! It should return XML
        to the code that calls it.
        """
        return Template(
            fileio.read_text(config.TEMPLATE_METS_JINJA2)
        ).render(object=self)

    def write_xml(self):
        """Write METS XML file to disk.
        """
        fileio.write_text(self.dump_xml(), self.mets_path)
    
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
        """Searches filesystem for (entity) childrens' metadata files, returns relati
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
    
    def detect_children_duplicates(self):
        """Returns list of objects that appear in Entity.children more than once
        
        NOTE: This function looks only at the list of file dicts in entity.json;
        it does not examine the filesystem.
        @returns: list
        """
        duplicates = []
        for x,c in enumerate(self.children()):
            for y,c2 in enumerate(self.children()):
                if (c != c2) and (c.path_rel == c2.path_rel) and (c2 not in duplicates):
                    duplicates.append(c)
        return duplicates
    
    def file( self, role, sha1, newfile=None ):
        """Given a SHA1 hash, get the corresponding file dict.
        
        @param role
        @param sha1
        @param newfile (optional) If present, updates existing file or adds new one.
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
        return ingest.add_local_file(
            self, src_path, role, data, git_name, git_mail, agent
        )
    
    def add_external_file(self, data, git_name, git_mail, agent=''):
        return ingest.add_external_file(self, data, git_name, git_mail, agent)
    
    def add_access(self, ddrfile, src_file, git_name, git_mail, agent=''):
        return ingest.add_access(
            self, ddrfile, src_file, git_name, git_mail, agent=''
        )
    
    def add_file_commit(self, file_, repo, log, git_name, git_mail, agent):
        return ingest.add_file_commit(
            self, file_, repo, log, git_name, git_mail, agent
        )

    def remove_child(self, object_id):
        """Remove child entity from this Entity's children list.
        
        @param object_id: str Child object ID
        """
        logger.debug('%s.remove_child(%s)' % (self, object_id))
        self.children()
        copy_objects = [
            o for o in self._children_objects if not o.id == object_id
        ]
        self._children_objects = copy_objects
    
    def ddrpublic_template_key(self):
        """Combine factors for ddrpublic template selection into key
        
        For use in ddrindex publish to Elasticsearch.
        Generates a key which ddr-public will use to choose a template.
        Finds Entity's signature file, or the first mezzanine file,
        or the Entity's first child's first mezzanine file, etc, etc
        Matches Entity format and file mimetype to template
        
        @returns: signature,key
        """
        entity = self
        try:
            signature = Identifier(
                entity.signature_id, config.MEDIA_BASE
            ).object()
        except:
            signature = None

        # VH entities may not have a valid signature
        if not signature:
            def first_mezzanine(entity):
                for f in entity.children(role='mezzanine'):
                    return f
                return None
                
            # use child entity if exists and has mezzanine file
            if entity.children(models=['entity','segment']):
                for c in entity.children(models=['entity','segment']):
                    if first_mezzanine(c):
                        entity = c
                        break
            # get signature image
            signature = first_mezzanine(entity)
        
        # prepare decision table key
        key = None
        if signature:
            key = ':'.join([
                getattr(entity, 'format', ''),
                signature.mimetype.split('/')[0]
            ])
        return signature,key


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

def _sort_children(objects):
    """Arranges children in the proper order, Entities first, then Files
    
    @param objects: list of Entity and File objects
    @returns: list of objects
    """
    objects = sorted(objects)
    # TODO replace hard-coded models
    # entities,segments
    grouped_objects = [
        o
        for o in objects
        if o.identifier.model in ['entity', 'segment']
    ]
    # files
    for o in objects:
        if o.identifier.model in ['file']:
            grouped_objects.append(o)
    return grouped_objects

def files_to_filegroups(files):
    """Converts list of File objects to METS file_groups structure.
    
    @param files: list
    @returns: list of dicts
    """
    # intermediate format
    fgroups = {}
    for f in files:
        if f.role and not fgroups.get(f.role):
            fgroups[f.role] = []
    for f in files:
        if f.role:
            fgroups[f.role].append(
                f.dict(file_groups=1)
            )
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

def entity_to_childrenmeta(o):
    """Given an Entity object, return the dict used in entity.json
    
    @param o: Entity object
    @returns: dict
    """
    return {
        key: getattr(o, key, None)
        for key in ENTITY_ENTITY_KEYS
    }
