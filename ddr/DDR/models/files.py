from collections import OrderedDict
from functools import total_ordering
import json
import mimetypes
mimetypes.init()
import os

import envoy
from jinja2 import Template

from DDR import commands
from DDR import config
from DDR import docstore
from DDR import fileio
from DDR import format_json
from DDR.identifier import Identifier, ID_COMPONENTS
from DDR import inheritance
from DDR.models import common
from DDR import modules
from DDR import util

ENTITY_FILES_PREFIX = 'files'

FILE_KEYS = ['path_rel',
             'basename', 
             'size', 
             'role', 
             'sha1', 
             'sha256', 
             'md5',
             'basename_orig',
             'public',
             'sort',
             'label',
             'access_rel',
             'xmp',]

# attrs used in METS Entity file_groups
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


@total_ordering
class File(common.DDRObject):
    id = None
    idparts = None
    external = None
    collection_id = None
    parent_id = None
    entity_id = None
    signature_id = ''
    path_abs = None
    path = None
    collection_path = None
    parent_path = None
    entity_path = None
    entity_files_path = None
    json_path = None
    access_abs = None
    path_rel = None
    json_path_rel = None
    access_rel = None
    ext = None
    basename = None
    basename_orig = ''
    mimetype = None
    size = None
    role = None
    sha256 = None
    md5 = None
    public = 0
    sort = 1
    label = ''
    # access file path relative to /
    # not saved; constructed on instantiation
    access_size = None
    xmp = ''
    # entity
    src = None
    links = None
    
    def __init__(self, *args, **kwargs):
        """
        IMPORTANT: If at all possible, use the "path_abs" kwarg!!
        You *can* just pass in an absolute path. It will *appear* to work.
        This horrible function will attempt to infer the path but will
        probably get it wrong and fail silently!
        TODO refactor and simplify this horrible code!
        """
        path_abs = None
        # only accept path_abs
        if kwargs and kwargs.get('path_abs',None):
            path_abs = kwargs['path_abs']
        elif args and args[0] and isinstance(args[0], str):
            path_abs = args[0]  #     Use path_abs arg!!!
        
        i = None
        for arg in args:
            if isinstance(arg, Identifier):
                i = arg
        for key,arg in kwargs.items():
            if isinstance(arg, Identifier):
                i = arg
        self.identifier = i
        
        if self.identifier and not path_abs:
            path_abs = self.identifier.path_abs()
        if not path_abs:
            # TODO accept path_rel plus base_path
            raise Exception("File must be instantiated with an absolute path!")
        path_abs = os.path.normpath(path_abs)
        
        self.id = i.id
        self.idparts = list(i.parts.values())
        self.collection_id = i.collection_id()
        self.parent_id = i.parent_id()
        self.entity_id = self.parent_id
        self.role = i.parts['role']
        
        # IMPORTANT: These paths (set by Identifier) do not have file extension!
        # File extension is added in File.load_json!
        
        self.path_abs = path_abs
        self.path = path_abs
        self.collection_path = i.collection_path()
        self.parent_path = i.parent_path()
        self.entity_path = self.parent_path
        self.entity_files_path = os.path.join(self.entity_path, ENTITY_FILES_PREFIX)
        
        self.json_path = i.path_abs('json')
        self.access_abs = i.path_abs('access')
        
        self.path_rel = i.path_rel()
        self.json_path_rel = i.path_rel('json')
        self.access_rel = i.path_rel('access')
        
        self.basename = os.path.basename(self.path_abs)

    def __lt__(self, other):
        """Enable Pythonic sorting"""
        return self._key() < other._key()
    
    def _key(self):
        """Key for Pythonic object sorting.
        Returns tuple of self.sort,self.identifier.id_sort
        (self.sort takes precedence over ID sort)
        """
        return int(self.sort),self.identifier.id_sort

    #@staticmethod
    #def exists(oidentifier, basepath=None, gitolite=None, idservice=None):
    
    @staticmethod
    def new(identifier=None, parent=None, inherit=True):
        """Creates new File (metadata only) w default values; does not write/commit.
        
        @param identifier: [optional] Identifier
        @param parent: [optional] DDRObject parent object
        @param inherit: boolean Disable in loops to avoid infinite recursion
        @returns: File object
        """
        return common.new_object(identifier, parent=parent, inherit=inherit)
    
    @staticmethod
    def create(identifier, git_name, git_mail, agent='cmdln'):
        """Creates new File (metadata only), writes files, performs initial commit
        
        @param identifier: Identifier
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @returns: exit,status int,str
        """
        parent = identifier.parent().object()
        if not parent:
            raise Exception('Parent for %s does not exist.' % identifier)
        file_ = File.new(identifier)
        file_.write_json()
        
        entity_file_edit(request, collection, file_, git_name, git_mail)

        # load Entity object, inherit values from parent, write back to file
        entity = parent
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
    
    def save(self, git_name, git_mail, agent, collection=None, parent=None, inheritables=[], commit=True):
        """Writes File metadata, stages, and commits.
        
        Updates .children if parent is (almost certainly) an Entity.
        Returns exit code, status message, and list of *updated* files.
        
        Updated files list is for use by e.g. batch operations that want
        to commit all modified files in one operation rather than piecemeal.
        IMPORTANT: This list only includes METADATA files, NOT binaries.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param collection: Collection
        @param parent: Entity or Segment
        @param inheritables: list of selected inheritable fields
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        if not collection:
            collection = self.identifier.collection().object()
        if not parent:
            parent = self.identifier.parent().object()
        
        self.write_json()
        # list of files to stage
        updated_files = [
            self.json_path,
        ]
        if parent and (parent.identifier.model in ['entity','segment']):
            # update parent.children
            parent.children(force_read=True)
            parent.write_json()
            updated_files.append(parent.json_path)
            updated_files.append(parent.changelog_path)
        
        # files have no child object inheritors
        
        exit,status = commands.entity_update(
            git_name, git_mail,
            collection, parent,
            updated_files,
            agent,
            commit
        )
        return exit,status,updated_files

    def delete(self, git_name, git_mail, agent, entity=None, collection=None, commit=True):
        """Removes File metadata and file, updates parent entity, and commits.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param commit: boolean
        @returns: exit,status,removed_files,updated_files (int,str,list,list)
        """
        exit = 1; status = 'unknown'; updated_files = []
        if not entity:
            entity = self.parent()
        if not collection:
            collection = self.identifier.collection().object()
        
        # metadata jsons (rm this file, modify parent entity)
        rm_files = [
            f for f in self.files_rel()
            if os.path.exists(
                os.path.join(self.collection_path, f)
            )
        ]
        
        # binary and access file
        if os.path.exists(self.path_abs) and not (self.path_rel in rm_files):
            rm_files.append(self.path_rel)
        # Note: external files and some others (ex: transcripts) may not *have*
        # access files.
        if os.path.exists(self.access_abs) and not (self.access_rel in rm_files):
            rm_files.append(self.access_rel)
        
        #IMPORTANT: some files use same binary for master,mezz
        #we want to be able to e.g. delete mezz w/out deleting master
        
        # parent entity
        entity.remove_child(self.id)
        entity.write_json(force=True)
        updated_files = [
            entity.identifier.path_rel('json'),
        ]
        
        # write files and commit
        status,message,updated_files = commands.file_destroy(
            git_name, git_mail,
            collection, entity,
            rm_files, updated_files,
            agent=agent,
            commit=commit
        )
        return exit, status, rm_files, updated_files
    
    @staticmethod
    def from_json(path_abs, identifier=None):
        """Instantiates a File object from specified *.json.
        
        @param path_abs: Absolute path to .json file.
        @param identifier: [optional] Identifier
        @returns: DDRFile
        """
        #file_ = File(path_abs=path_abs)
        #file_.load_json(fileio.read_text(file_.json_path))
        #return file_
        return common.from_json(File, path_abs, identifier)
    
    @staticmethod
    def from_csv(identifier, rowd):
        """Instantiates a File object from CSV row data.
        
        @param identifier: [optional] Identifier
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: DDRFile
        """
        return common.from_csv(identifier, rowd)
    
    @staticmethod
    def from_identifier(identifier, inherit=True):
        """Instantiates a File object, loads data from FILE.json or creates new object.
        
        @param identifier: Identifier
        @param inherit: boolean Disable in loops to avoid infinite recursion
        @returns: File
        """
        if os.path.exists(identifier.path_abs('json')):
            return File.from_json(identifier.path_abs('json'), identifier)
        return File.new(identifier, inherit=inherit)
    
    def parent( self ):
        i = Identifier(id=self.parent_id, base_path=self.identifier.basepath)
        return i.object()

    def children( self, quick=None ):
        return []
    
    #def signature_abs(self):
    
    def load_json(self, json_text):
        """Populate File data from JSON-formatted text.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        json_data = common.load_json(self, module, json_text)
        # fill in the blanks
        if self.access_rel:
            access_abs = os.path.join(self.entity_files_path, self.access_rel)
            if os.path.exists(access_abs):
                self.access_abs = access_abs
        # Identifier does not know file extension
        self.ext = os.path.splitext(self.basename_orig)[1]
        self.path = self.path + self.ext
        self.path_abs = self.path_abs + self.ext
        self.path_rel = self.path_rel + self.ext
        self.basename = self.basename + self.ext
        # fix access_rel
        self.access_rel = os.path.join(
            os.path.dirname(self.path_rel),
            os.path.basename(self.access_abs)
        )
    
    def dump_json(self, doc_metadata=False, obj_metadata={}):
        """Dump File data to JSON-formatted text.
        
        @param doc_metadata: boolean. Insert object_metadata().
        @param obj_metadata: dict Cached results of object_metadata.
        @returns: JSON-formatted text
        """
        module = self.identifier.fields_module()
        if self.basename and not self.mimetype:
            self.mimetype = self.get_mimetype(force=True)
        data = common.dump_json(self, module)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, common.object_metadata(module, self.collection_path))
        # what we call path_rel in the .json is actually basename
        data.insert(1, {'path_rel': self.basename})
        return format_json(data)
    
    def post_json(self, public=False):
        # NOTE: this is same basic code as docstore.index
        return docstore.DocstoreManager(
            docstore.INDEX_PREFIX, config.DOCSTORE_HOST, config
        ).post(
            self,
            docstore._public_fields().get(self.identifier.model, []),
            {
                'parent_id': self.parent_id,
                'entity_id': self.parent_id,
            }
        )
    
    def load_csv(self, rowd):
        """Populate File data from JSON-formatted text.
        
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: list of changed fields
        """
        # remove 'id' from rowd because files.FIELDS has no 'id' field
        # TODO files.FIELDS really should have an ID field...
        if 'id' in iter(rowd.keys()):
            rowd.pop('id')
        module = modules.Module(self.identifier.fields_module())
        modified = common.load_csv(self, module, rowd)
        # fill in the blanks
        if self.access_rel:
            access_abs = os.path.join(self.entity_files_path, self.access_rel)
            if os.path.exists(access_abs):
                self.access_abs = access_abs
        # Identifier does not know file extension
        def add_extension(path, ext):
            # add extenstions if not already present
            base,ext = os.path.splitext(path)
            if not ext:
                return path + ext
            return path
        self.ext = os.path.splitext(self.basename_orig)[1]
        self.path_abs = add_extension(self.path_abs, self.ext)
        self.path_rel = add_extension(self.path_rel, self.ext)
        self.basename = add_extension(self.basename, self.ext)
        # fix access_rel
        self.access_rel = os.path.join(
            os.path.dirname(self.path_rel),
            os.path.basename(self.access_abs)
        )
        return modified
    
    def dump_csv(self, fields=[]):
        """Dump File data to list of values suitable for CSV.
        
        @returns: list of values
        """
        # make sure we export 'id' if it's not in model FIELDS (ahem, files)
        if 'id' not in fields:
            fields.insert(0, 'id')
        module = modules.Module(self.identifier.fields_module())
        if self.basename and not self.mimetype:
            self.mimetype = self.get_mimetype(force=True)
        return common.prep_csv(self, module, fields=fields)
    
    # specific to File
    
    def files_rel( self ):
        """Returns list of the file, its metadata JSON, and access file, relative to collection.
        
        @param collection_path
        @returns: list of relative file paths
        """
        return [
            self.path_rel,
            self.json_path_rel,
            self.access_rel,
        ]
    
    def present( self ):
        """Indicates whether or not the original file is currently present in the filesystem.
        """
        if self.path_abs and os.path.exists(self.path_abs):
            return True
        return False
    
    def access_present( self ):
        """Indicates whether or not the access file is currently present in the filesystem.
        """
        if self.access_abs and os.path.exists(self.access_abs):
            return True
        return False
    
    @staticmethod
    def file_name( entity, path_abs, role, sha1=None ):
        """Generate a new name for the specified file; Use only when ingesting a file!
        
        rename files to standard names on ingest:
        %{entity_id%}-%{role}-%{sha1}.%{ext}
        example: ddr-testing-56-101-master-fb73f9de29.jpg
        
        SHA1 is optional so it can be passed in by a calling process that has already
        generated it.
        
        @param entity
        @param path_abs: Absolute path to the file.
        @param role
        @param sha1: SHA1 hash (optional)
        """
        if os.path.exists and os.access(path_abs, os.R_OK):
            ext = os.path.splitext(path_abs)[1]
            if not sha1:
                sha1 = util.file_hash(path_abs, 'sha1')
            if sha1:
                idparts = [a for a in entity.idparts]
                idparts.append(role)
                idparts.append(sha1[:10])
                name = '{}{}'.format(Identifier(parts=idparts).id, ext)
                return name
        return None
    
    def set_path( self, path_rel, entity=None ):
        """
        Reminder:
        self.path_rel is relative to entity
        self.path_abs is relative to filesystem root
        """
        self.path_rel = path_rel
        if entity:
            self.path_rel = self.path_rel.replace(entity.files_path, '')
        if self.path_rel and (self.path_rel[0] == '/'):
            # remove initial slash (ex: '/files/...')
            self.path_rel = self.path_rel[1:]
        if entity:
            self.path_abs = os.path.join(entity.files_path, self.path_rel)
            self.src = os.path.join('base', entity.files_path, self.path_rel)
        if self.path_abs and os.path.exists(self.path_abs):
            self.size = os.path.getsize(self.path_abs)
        self.basename = os.path.basename(self.path_rel)
    
    def set_access( self, access_rel, entity=None ):
        """
        @param access_rel: path relative to entity files dir (ex: 'thisfile.ext')
        @param entity: A Entity object (optional)
        """
        self.access_rel = os.path.basename(access_rel)
        if entity:
            self.access_abs = os.path.join(entity.files_path, self.access_rel)
        if self.access_abs and os.path.exists(self.access_abs):
            self.access_size = os.path.getsize(self.access_abs)
    
    def file( self ):
        """Simulates an entity['files'] dict used to construct file"""
        f = {}
        for key in FILE_KEYS:
            if hasattr(self, key):
                f[key] = getattr(self, key, None)
        return f
        
    def dict(self, file_groups=False, json_safe=False):
        """
        @param file_groups: bool If True return dict for METS Entity file_groups
        @param json_safe: bool Serialize e.g. datetime to text
        @returns: OrderedDict
        """
        if file_groups:
            return {
                key: getattr(self, key)
                for key in ENTITY_FILE_KEYS
            }
        return common.to_dict(
            self, self.identifier.fields_module(), json_safe=json_safe
        )
        
    @staticmethod
    def access_filename( src_abs ):
        """Generate access filename base on source filename.
        
        @param src_abs: Absolute path to source file.
        @returns: Absolute path to access file
        """
        return '%s%s.%s' % (
            os.path.splitext(src_abs)[0],
            config.ACCESS_FILE_APPEND,
            'jpg')
    
    def links_incoming( self ):
        """List of path_rels of files that link to this file.
        """
        incoming = []
        cmd = 'find {} -name "*.json" -print'.format(self.entity_files_path)
        r = envoy.run(cmd)
        jsons = []
        if r.std_out:
            jsons = r.std_out.strip().split('\n')
        for filename in jsons:
            data = json.loads(fileio.read_text(filename))
            path_rel = None
            for field in data:
                if field.get('path_rel',None):
                    path_rel = field['path_rel']
            for field in data:
                linksraw = field.get('links', None)
                if linksraw:
                    for link in linksraw.strip().split(';'):
                        link = link.strip()
                        if self.basename in link:
                            incoming.append(path_rel)
        return incoming
    
    def links_outgoing( self ):
        """List of path_rels of files this file links to.
        """
        if self.links:
            return [link.strip() for link in self.links.strip().split(';')]
        return []
    
    def links_all( self ):
        """List of path_rels of files that link to this file or are linked to from this file.
        """
        links = self.links_outgoing()
        for l in self.links_incoming():
            if l not in links:
                links.append(l)
        return links

    def exists(self):
        """Indicates whether the exits or not; takes File.external into account.
        
        @returns: bool
        """
        FILE_EXISTS = {
           # J   - JSON exists
           # |E  - external == truthy
           # ||F - file exists
            '---': False,
            'J--': False,
           #'-E-'
           #'--F'
            'JE-': True,
            'J-F': True,
           #'-EF'
            'JEF': True,
        }
        score = ''
        if os.path.exists(self.identifier.path_abs('json')):
            score += 'J'
        else:
            score += '-'
        if self.external:
            score += 'E'
        else:
            score += '-'
        if self.path_abs and os.path.exists(self.path_abs):
            score += 'F'
        else:
            score += '-'
        return FILE_EXISTS[score]
    
    def get_mimetype(self, force=False):
        """Gets mimetype based on File.basename_orig.
        
        @param force: bool
        @return: str mimetype
        """
        if self.mimetype and not force:
            return self.mimetype
        # join type and encoding (if available) into str
        if self.basename_orig:
            self.mimetype = '; '.join([
                part
                for part in mimetypes.guess_type(self.basename_orig)
                if part
            ])
        return self.mimetype
