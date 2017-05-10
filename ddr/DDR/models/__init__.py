"""
NOTE: Much of the code in this module used to be in ddr-local
(ddr-local/ddrlocal/ddrlocal/models/__init__.py).  Please refer to that project
for history prior to Feb 2015.

* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

TODO refactor: keep metadata from json_data

TODO refactor: load json_text into an OrderedDict

TODO refactor: put json_data in a object.source dict like ES does.

This way we don't have to worry about field names conflicting with
class methods (e.g. Entity.parent).

ACCESS that dict (actually OrderedDict) via object.source() method.
Lazy loading: don't load unless something needs to access the data

IIRC the only time we need those fields is when we display the object
to the user.

Also we won't have to reload the flippin' .json file multiple times
for things like cmp_model_definition_fields.

TODO indicate in repo_models.MODEL whether to put in .source.
Not as simple as just throwing everything into .source.
Some fields (record_created) are auto-generated.
Others (id) must be first-level attributes of Objects.
Others (status, public) are used in code to do things like inheritance,
yet must be editable in the editor UI.

* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
"""

from copy import deepcopy
from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)
import mimetypes
mimetypes.init()
import os
import re
from StringIO import StringIO

import envoy
from jinja2 import Template

from DDR import VERSION
from DDR import format_json
from DDR import changelog
from DDR import commands
from DDR import config
from DDR.control import CollectionControlFile, EntityControlFile
from DDR import converters
from DDR import docstore
from DDR import dvcs
from DDR import fileio
from DDR.identifier import Identifier, MODULES, VALID_COMPONENTS
from DDR import imaging
from DDR import ingest
from DDR import inheritance
from DDR import locking
from DDR import modules
from DDR import util

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(config.INSTALL_PATH, 'ddr', 'DDR', 'templates')
GITIGNORE_TEMPLATE = os.path.join(TEMPLATE_PATH, 'gitignore.tpl')

MODELS_DIR = '/usr/local/src/ddr-cmdln/ddr/DDR/models'

COLLECTION_FILES_PREFIX = 'files'
ENTITY_FILES_PREFIX = 'files'



# metadata files: finding, reading, writing ----------------------------

def sort_file_paths(json_paths, rank='role-eid-sort'):
    """Sort file JSON paths in human-friendly order.
    
    TODO this belongs in DDR.identifier
    
    @param json_paths: 
    @param rank: 'role-eid-sort' or 'eid-sort-role'
    """
    paths = {}
    keys = []
    while json_paths:
        path = json_paths.pop()
        identifier = Identifier(path=path)
        eid = identifier.parts.get('eid',None)
        role = identifier.parts.get('role',None)
        sha1 = identifier.parts.get('sha1',None)
        sort = 0
        with open(path, 'r') as f:
            for line in f.readlines():
                if 'sort' in line:
                    sort = line.split(':')[1].replace('"','').strip()
        eid = str(eid)
        sha1 = str(sha1)
        sort = str(sort)
        if rank == 'eid-sort-role':
            key = '-'.join([str(eid),sort,role,sha1])
        elif rank == 'role-eid-sort':
            key = '-'.join([role,eid,sort,sha1])
        paths[key] = path
        keys.append(key)
    keys_sorted = [key for key in util.natural_sort(keys)]
    paths_sorted = []
    while keys_sorted:
        val = paths.pop(keys_sorted.pop(), None)
        if val:
            paths_sorted.append(val)
    return paths_sorted

def create_object(identifier):
    """Creates a new object initial values from module.FIELDS.
    
    If identifier.fields_module().FIELDS.field['default'] is non-None
    it is used as the value.
    Use "None" for things like Object.id, which should already be set
    in the object constructor, and which you do not want to overwrite
    with a default value. Ahem.
    
    @param identifier: Identifier
    @returns: object
    """
    object_class = identifier.object_class()
    obj = object_class(
        identifier.path_abs(),
        identifier=identifier
    )
    # set default values
    for f in identifier.fields_module().FIELDS:
        if f['default'] != None:
            setattr(obj, f['name'], f['default'])
        elif hasattr(f, 'name') and hasattr(f, 'initial'):
            setattr(obj, f['name'], f['initial'])
    return obj

def object_metadata(module, repo_path):
    """Metadata for the ddrlocal/ddrcmdln and models definitions used.
    
    @param module: collection, entity, files model definitions module
    @param repo_path: Absolute path to root of object's repo
    @returns: dict
    """
    repo = dvcs.repository(repo_path)
    gitversion = '; '.join([dvcs.git_version(repo), dvcs.annex_version(repo)])
    data = {
        'application': 'https://github.com/densho/ddr-cmdln.git',
        'app_commit': dvcs.latest_commit(config.INSTALL_PATH),
        'app_release': VERSION,
        'models_commit': dvcs.latest_commit(modules.Module(module).path),
        'git_version': gitversion,
    }
    return data

def is_object_metadata(data):
    """Indicate whether json_data field is the object_metadata field.
    
    @param data: list of dicts
    @returns: boolean
    """
    for key in ['app_commit', 'app_release']:
        if key in data.keys():
            return True
    return False

def form_prep(document, module):
    """Apply formprep_{field} functions to prep data dict to pass into DDRForm object.
    
    Certain fields require special processing.  Data may need to be massaged
    and prepared for insertion into particular Django form objects.
    If a "formprep_{field}" function is present in the collectionmodule
    it will be executed.
    
    @param document: Collection, Entity, File document object
    @param module: collection, entity, files model definitions module
    @returns data: dict object as used by Django Form object.
    """
    data = {}
    for f in module.FIELDS:
        if hasattr(document, f['name']) and f.get('form',None):
            fieldname = f['name']
            # run formprep_* functions on field data if present
            field_data = modules.Module(module).function(
                'formprep_%s' % fieldname,
                getattr(document, f['name'])
            )
            data[fieldname] = field_data
    return data
    
def form_post(document, module, cleaned_data):
    """Apply formpost_{field} functions to process cleaned_data from CollectionForm
    
    Certain fields require special processing.
    If a "formpost_{field}" function is present in the entitymodule
    it will be executed.
    NOTE: cleaned_data must contain items for all module.FIELDS.
    
    @param document: Collection, Entity, File document object
    @param module: collection, entity, files model definitions module
    @param cleaned_data: dict cleaned_data from DDRForm
    """
    for f in module.FIELDS:
        if hasattr(document, f['name']) and f.get('form',None):
            fieldname = f['name']
            # run formpost_* functions on field data if present
            field_data = modules.Module(module).function(
                'formpost_%s' % fieldname,
                cleaned_data[fieldname]
            )
            setattr(document, fieldname, field_data)
    # update record_lastmod
    if hasattr(document, 'record_lastmod'):
        document.record_lastmod = datetime.now(config.TZ)

def load_json_lite(json_path, model, object_id):
    """Simply reads JSON file and adds object_id if it's a file
    
    @param json_path: str
    @param model: str
    @param object_id: str
    @returns: list of dicts
    """
    with open(json_path, 'r') as f:
        document = json.loads(f.read())
    if model == 'file':
        document.append( {'id':object_id} )
    return document

def load_json(document, module, json_text):
    """Populates object from JSON-formatted text; applies jsonload_{field} functions.
    
    Goes through module.FIELDS turning data in the JSON file into
    object attributes.
    TODO content fields really should into OBJECT.data OrderedDict or subobject.
    
    @param document: Collection/Entity/File object.
    @param module: collection/entity/file module from 'ddr' repo.
    @param json_text: JSON-formatted text
    @returns: dict
    """
    try:
        json_data = json.loads(json_text)
    except ValueError:
        json_data = [
            {'title': 'ERROR: COULD NOT READ DATA (.JSON) FILE!'},
            {'_error': 'Error: ValueError during read load_json.'},
        ]
    # software and commit metadata
    for field in json_data:
        if is_object_metadata(field):
            setattr(document, 'object_metadata', field)
            break
    # field values from JSON
    for mf in module.FIELDS:
        for f in json_data:
            if hasattr(f, 'keys') and (f.keys()[0] == mf['name']):
                fieldname = f.keys()[0]
                # run jsonload_* functions on field data if present
                field_data = modules.Module(module).function(
                    'jsonload_%s' % fieldname,
                    f.values()[0]
                )
                if isinstance(field_data, basestring):
                    field_data = field_data.strip()
                setattr(document, fieldname, field_data)
    # Fill in missing fields with default values from module.FIELDS.
    # Note: should not replace fields that are just empty.
    for mf in module.FIELDS:
        if not hasattr(document, mf['name']):
            setattr(document, mf['name'], mf.get('default',None))
    # Add timeszone to fields if not present
    apply_timezone(document, module)
    return json_data

def apply_timezone(document, module):
    """Set time zone for datetime fields if not present in datetime fields
    
    If document matches certain criteria, override the timezone with a
    specified alternate timezone.
    """
    # add timezone to any datetime fields missing it
    for mf in module.FIELDS:
        if mf['model_type'] == datetime:
            fieldname = mf['name']
            dt = getattr(document, fieldname)
            if dt and isinstance(dt, datetime) and (not dt.tzinfo):
                # Use default timezone unless...
                if document.identifier.idparts['org'] in config.ALT_TIMEZONES.keys():
                    timezone = config.ALT_TIMEZONES[document.identifier.idparts['org']]
                else:
                    timezone = config.TZ
                setattr(document, fieldname, timezone.localize(dt))

def dump_json(obj, module, template=False,
              template_passthru=['id', 'record_created', 'record_lastmod'],
              exceptions=[]):
    """Arranges object data in list-of-dicts format before serialization.
    
    DDR keeps data in Git is to take advantage of versioning.  Python
    dicts store data in random order which makes it impossible to
    meaningfully compare diffs of the data over time.  DDR thus stores
    data as an alphabetically arranged list of dicts, with several
    exceptions.
    
    The first dict in the list is not part of the object itself but
    contains metadata about the state of the DDR application at the time
    the file was last written: the Git commit of the app, the release
    number, and the versions of Git and git-annex used.
    
    Python data types that cannot be represented in JSON (e.g. datetime)
    are converted into strings.
    
    @param obj: Collection/Entity/File object.
    @param module: modules.Module
    @param template: Boolean True if object to be used as blank template.
    @param template_passthru: list
    @param exceptions: list
    @returns: dict
    """
    data = []
    for mf in module.FIELDS:
        item = {}
        fieldname = mf['name']
        field_data = ''
        if template and (fieldname not in template_passthru) and hasattr(mf,'form'):
            # write default values
            field_data = mf['form']['initial']
        elif hasattr(obj, mf['name']):
            # run jsondump_* functions on field data if present
            field_data = modules.Module(module).function(
                'jsondump_%s' % fieldname,
                getattr(obj, fieldname)
            )
        item[fieldname] = field_data
        if fieldname not in exceptions:
            data.append(item)
    return data

def from_json(model, json_path, identifier):
    """Read the specified JSON file and properly instantiate object.
    
    @param model: LocalCollection, LocalEntity, or File
    @param json_path: absolute path to the object's .json file
    @param identifier: [optional] Identifier
    @returns: object
    """
    document = None
    if not model:
        raise Exception('Cannot instantiate from JSON without a model object.')
    if not json_path:
        raise Exception('Bad path: %s' % json_path)
    if identifier.model in ['file']:
        # object_id is in .json file
        path = os.path.splitext(json_path)[0]
        document = model(path, identifier=identifier)
    else:
        # object_id is in object directory
        document = model(os.path.dirname(json_path), identifier=identifier)
    document_id = document.id  # save this just in case
    document.load_json(fileio.read_text(json_path))
    if not document.id:
        # id gets overwritten if document.json is blank
        document.id = document_id
    return document

def prep_csv(obj, module, headers=[]):
    """Dump object field values to list suitable for a CSV file.
    
    Note: Autogenerated and non-user-editable fields
    (SHA1 and other hashes, file size, etc) should be excluded
    from the CSV file.
    Note: For files these are replaced by File.id which contains
    the role and a fragment of the SHA1 hash.
    
    @param obj_: Collection, Entity, File
    @param module: modules.Module
    @param headers: list If nonblank only export specified fields.
    @returns: list of values
    """
    if headers:
        field_names = headers
    else:
        field_names = module.field_names()
        # TODO field_directives go here!
    # seealso DDR.modules.Module.function
    values = []
    for fieldname in field_names:
        value = ''
        # insert file_id as first column
        if (module.module.MODEL == 'file') and (fieldname == 'file_id'):
            field_data = obj.id
        elif hasattr(obj, fieldname):
            # run csvdump_* functions on field data if present
            field_data = module.function(
                'csvdump_%s' % fieldname,
                getattr(obj, fieldname)
            )
            if field_data == None:
                field_data = ''
        value = util.normalize_text(field_data)
        values.append(value)
    return values

def csvload_rowd(module, rowd):
    """Apply module's csvload_* methods to rowd data
    """
    # In repo_models.object.FIELDS, individual fields can be marked
    # so they are ignored (e.g. not included) when importing.
    # TODO make field_directives ONCE at start of rowds loop
    field_directives = {
        f['name']: f['csv']['import']
        for f in module.module.FIELDS
    }
    data = {}
    for fieldname,value in rowd.iteritems():
        ignored = 'ignore' in field_directives[fieldname]
        if not ignored:
            # run csvload_* functions on field data if present
            field_data = module.function(
                'csvload_%s' % fieldname,
                rowd[fieldname]
            )
            # TODO optimize, normalize only once
            data[fieldname] = util.normalize_text(field_data)
    return data

def load_csv(obj, module, rowd):
    """Populates object from a row in a CSV file.
    
    @param obj: Collection/Entity/File object.
    @param module: modules.Module
    @param rowd: dict Headers/row cells for one line of a CSV file.
    @returns: list of changed fields
    """
    # In repo_models.object.FIELDS, individual fields can be marked
    # so they are ignored (e.g. not included) when importing.
    field_directives = {
        f['name']: f['csv']['import']
        for f in module.module.FIELDS
    }
    # apply module's csvload_* methods to rowd data
    rowd = csvload_rowd(module, rowd)
    obj.modified = []
    for field,value in rowd.iteritems():
        ignored = 'ignore' in field_directives[field]
        if not ignored:
            oldvalue = getattr(obj, field, '')
            value = rowd[field]
            if value != oldvalue:
                obj.modified.append(field)
            setattr(obj, field, value)
    # Add timezone to fields if not present
    apply_timezone(obj, module.module)
    return obj.modified

def from_csv(identifier, rowd):
    """Instantiates a File object from CSV row data.
    
    @param identifier: [optional] Identifier
    @param rowd: dict Headers/row cells for one line of a CSV file.
    @returns: object
    """
    obj = identifier.object()
    obj.load_csv(headers, rowd)
    return obj

def load_xml():
    pass

def prep_xml():
    pass

def from_xml():
    pass


class Path( object ):
    pass


# objects --------------------------------------------------------------


class Stub(object):
    id = None
    idparts = None
    identifier = None

    def __init__(self, identifier):
        self.identifier = identifier
        self.id = self.identifier.id
        self.idparts = self.identifier.parts
    
    @staticmethod
    def from_identifier(identifier):
        return Stub(identifier)
    
    def __repr__(self):
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.identifier.model, self.id
        )
    
    def parent(self, stubs=False):
        return self.identifier.parent(stubs).object()

    def children(self):
        return []
    

class Collection( object ):
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
        self.idparts = i.parts.values()
        
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
    
    def __repr__(self):
        """Returns string representation of object.
        
        >>> c = Collection('/tmp/ddr-testing-123')
        >>> c
        <Collection ddr-testing-123>
        """
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.identifier.model, self.id
        )
    
    @staticmethod
    def create(path_abs, identifier=None):
        """Creates a new Collection with initial values from module.FIELDS.
        
        @param path_abs: str Absolute path; must end in valid DDR id.
        @param identifier: [optional] Identifier
        @returns: Collection object
        """
        if not identifier:
            identifier = Identifier(path=path_abs)
        return create_object(identifier)
    
    @staticmethod
    def new(identifier, git_name, git_mail, agent='cmdln'):
        """Creates new Collection, writes to filesystem, performs initial commit
        
        @param identifier: Identifier
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @returns: exit,status int,str
        """
        collection = Collection.create(identifier.path_abs(), identifier)
        fileio.write_text(
            collection.dump_json(template=True),
            config.TEMPLATE_CJSON
        )
        exit,status = commands.create(
            git_name, git_mail,
            identifier,
            [config.TEMPLATE_CJSON, config.TEMPLATE_EAD],
            agent=agent
        )
        return exit,status
    
    def save(self, git_name, git_mail, agent, cleaned_data={}, commit=True):
        """Writes specified Collection metadata, stages, and commits.
        
        Returns exit code, status message, and list of updated files.  Files list
        is for use by e.g. batch operations that want to commit all modified files
        in one operation rather than piecemeal.  This is included in Collection
        to be consistent with the other objects' methods.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param cleaned_data: dict Form data (all fields required)
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        if cleaned_data:
            self.form_post(cleaned_data)
        
        self.write_json()
        self.write_ead()
        updated_files = [self.json_path, self.ead_path,]
        
        # if inheritable fields selected, propagate changes to child objects
        inheritables = self.selected_inheritables(cleaned_data)
        modified_ids,modified_files = self.update_inheritables(inheritables, cleaned_data)
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
    
    @staticmethod
    def from_json(path_abs, identifier=None):
        """Instantiates a Collection object from specified collection.json.
        
        @param path_abs: Absolute path to .json file.
        @param identifier: [optional] Identifier
        @returns: Collection
        """
        return from_json(Collection, path_abs, identifier)
    
    @staticmethod
    def from_identifier(identifier):
        """Instantiates a Collection object using data from Identidier.
        
        @param identifier: Identifier
        @returns: Collection
        """
        return from_json(Collection, identifier.path_abs('json'), identifier)

    def parent( self ):
        """Returns Collection's parent object.
        """
        return self.identifier.parent().object()
    
    def children( self, quick=None ):
        """Returns list of the Collection's Entity objects.
        
        >>> c = Collection.from_json('/tmp/ddr-testing-123')
        >>> c.children()
        [<Entity ddr-testing-123-1>, <Entity ddr-testing-123-2>, ...]
        
        TODO use util.find_meta_files()
        
        @param quick: Boolean List only titles and IDs
        """
        # empty class used for quick view
        class ListEntity( object ):
            def __repr__(self):
                return "<DDRListEntity %s>" % (self.id)
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
                    for line in fileio.read_text(entity_json_path).split('\n'):
                        if '"title":' in line:
                            e = ListEntity()
                            e.id = Identifier(path=path).id
                            # make a miniature JSON doc out of just title line
                            e.title = json.loads('{%s}' % line)['title']
                            entities.append(e)
                            # stop once we hit 'title' so we don't waste time
                            # and have entity.children as separate ghost entities
                            break
            else:
                entity = Entity.from_identifier(Identifier(path=path))
                for lv in entity.labels_values():
                    if lv['label'] == 'title':
                        entity.title = lv['value']
                entities.append(entity)
        return entities
    
    def signature_abs(self):
        """Absolute path to signature image file, if signature_id present.
        """
        if self.signature_id:
            oi = Identifier(self.signature_id, self.identifier.basepath)
            if oi and oi.model == 'file':
                return File.from_identifier(oi).access_abs
        return None
    
    def identifiers(self, model=None, force_read=False):
        """Lists Identifiers for all or subset of Collection's descendents.
        
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
        """Apply formprep_{field} functions in Collection module to prep data dict to pass into DDRForm object.
        
        @returns data: dict object as used by Django Form object.
        """
        return form_prep(self, self.identifier.fields_module())
    
    def form_post(self, cleaned_data):
        """Apply formpost_{field} functions to process cleaned_data from DDRForm
        
        @param cleaned_data: dict
        """
        form_post(self, self.identifier.fields_module(), cleaned_data)
    
    def inheritable_fields( self ):
        """Returns list of Collection object's field names marked as inheritable.
        
        >>> c = Collection.from_json('/tmp/ddr-testing-123')
        >>> c.inheritable_fields()
        ['status', 'public', 'rights']
        """
        module = self.identifier.fields_module()
        return inheritance.inheritable_fields(module.FIELDS )

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
        return inheritance.update_inheritables(self, 'collection', inheritables, cleaned_data)
    
    def load_json(self, json_text):
        """Populates Collection from JSON-formatted text.
        
        Goes through COLLECTION_FIELDS, turning data in the JSON file into
        object attributes.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        load_json(self, module, json_text)
    
    def dump_json(self, template=False, doc_metadata=False, obj_metadata={}):
        """Dump Collection data to JSON-formatted text.
        
        @param template: [optional] Boolean. If true, write default values for fields.
        @param doc_metadata: boolean. Insert object_metadata().
        @param obj_metadata: dict Cached results of object_metadata.
        @returns: JSON-formatted text
        """
        module = self.identifier.fields_module()
        data = dump_json(self, module, template=template)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, object_metadata(module, self.path))
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
            load_json_lite(self.json_path, self.identifier.model, self.id),
            docstore._public_fields().get(self.identifier.model, []),
            {
                'parent_id': self.identifier.parent_id(),
            }
        )
    
    def lock( self, text ): return locking.lock(self.lock_path, text)
    def unlock( self, text ): return locking.unlock(self.lock_path, text)
    def locked( self ): return locking.locked(self.lock_path)
    
    def changelog( self ):
        if os.path.exists(self.changelog_path):
            return open(self.changelog_path, 'r').read()
        return '%s is empty or missing' % self.changelog_path
    
    def control( self ):
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
    
    def dump_ead(self):
        """Dump Collection data to ead.xml file.
        
        TODO This should not actually write the XML! It should return XML to the code that calls it.
        """
        with open(config.TEMPLATE_EAD_JINJA2, 'r') as f:
            template = f.read()
        return Template(template).render(object=self)

    def write_ead(self):
        """Write EAD XML file to disk.
        """
        fileio.write_text(self.dump_ead(), self.ead_path)
    
    def gitignore( self ):
        if not os.path.exists(self.gitignore_path):
            with open(GITIGNORE_TEMPLATE, 'r') as fr:
                gt = fr.read()
            with open(self.gitignore_path, 'w') as fw:
                fw.write(gt)
        with open(self.gitignore_path, 'r') as f:
            return f.read()
    
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
    def create(path_abs, identifier=None):
        """Creates a new Entity with initial values from module.FIELDS.
        
        @param path_abs: str Absolute path; must end in valid DDR id.
        @param identifier: [optional] Identifier
        @returns: Entity object
        """
        if not identifier:
            identifier = Identifier(path=path_abs)
        obj = create_object(identifier)
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
    
    @staticmethod
    def from_json(path_abs, identifier=None):
        """Instantiates an Entity object from specified entity.json.
        
        @param path_abs: Absolute path to .json file.
        @param identifier: [optional] Identifier
        @returns: Entity
        """
        return from_json(Entity, path_abs, identifier)
    
    @staticmethod
    def from_csv(identifier, rowd):
        """Instantiates a File object from CSV row data.
        
        @param identifier: [optional] Identifier
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: Entity
        """
        return from_csv(identifier, rowd)
    
    @staticmethod
    def from_identifier(identifier):
        """Instantiates an Entity object, loads data from entity.json.
        
        @param identifier: Identifier
        @returns: Entity
        """
        return from_json(Entity, identifier.path_abs('json'), identifier)
    
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
        if self.signature_id:
            oi = Identifier(self.signature_id, self.identifier.basepath)
            if oi and oi.model == 'file':
                return File.from_identifier(oi).access_abs
        return None
    
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
        return form_prep(self, self.identifier.fields_module())
    
    def form_post(self, cleaned_data):
        """Apply formpost_{field} functions to process cleaned_data from DDRForm
        
        @param cleaned_data: dict
        """
        form_post(self, self.identifier.fields_module(), cleaned_data)

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
        json_data = load_json(self, module, json_text)
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
        data = dump_json(self, module,
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
        return format_json(data)

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
            load_json_lite(self.json_path, self.identifier.model, self.id),
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
        modified = load_csv(self, module, rowd)
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
        return prep_csv(self, module, headers=headers)
    
    def changelog( self ):
        if os.path.exists(self.changelog_path):
            return open(self.changelog_path, 'r').read()
        return '%s is empty or missing' % self.changelog_path
    
    def control( self ):
        if not os.path.exists(self.control_path):
            EntityControlFile.create(self.control_path, self.parent_id, self.id)
        return EntityControlFile(self.control_path)

    #def mets( self ):
    #    if not os.path.exists(self.mets_path):
    #        METS.create(self.mets_path)
    #    return METS(self)
    
    def dump_mets(self):
        """Dump Entity data to mets.xml file.
        
        TODO This should not actually write the XML! It should return XML to the code that calls it.
        """
        with open(config.TEMPLATE_METS_JINJA2, 'r') as f:
            template = f.read()
        return Template(template).render(object=self)

    def write_mets(self):
        """Write METS XML file to disk.
        """
        fileio.write_text(self.dump_mets(), self.mets_path)
    
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
             'thumb',
             'access_rel',
             'xmp',]

class File( object ):
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
    thumb = -1
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
        elif args and args[0] and isinstance(args[0], basestring):
            path_abs = args[0]  #     Use path_abs arg!!!
        
        i = None
        for arg in args:
            if isinstance(arg, Identifier):
                i = arg
        for key,arg in kwargs.iteritems():
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
        self.idparts = i.parts.values()
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

    def __repr__(self):
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.identifier.model, self.id
        )
    
    @staticmethod
    def create(path_abs, identifier=None):
        """Creates a new File with initial values from module.FIELDS.
        
        @param path_abs: str Absolute path; must end in valid DDR id.
        @param identifier: [optional] Identifier
        @returns: File object
        """
        if not identifier:
            identifier = Identifier(path=path_abs)
        return create_object(identifier)
    
    @staticmethod
    def new(identifier, git_name, git_mail, agent='cmdln'):
        """Creates new File (metadata only!), writes to filesystem, performs initial commit
        
        @param identifier: Identifier
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @returns: exit,status int,str
        """
        parent = identifier.parent().object()
        if not parent:
            raise Exception('Parent for %s does not exist.' % identifier)
        file_ = File.create(identifier.path_abs(), identifier)
        file_.write_json()
        
        entity_file_edit(request, collection, file_, git_name, git_mail)

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
    
    def save(self, git_name, git_mail, agent, collection=None, parent=None, cleaned_data={}, commit=True):
        """Writes File metadata, stages, and commits.
        
        Updates .children and .file_groups if parent is (almost certainly) an Entity.
        Returns exit code, status message, and list of updated files.  Files list
        is for use by e.g. batch operations that want to commit all modified files
        in one operation rather than piecemeal.
        
        @param git_name: str
        @param git_mail: str
        @param agent: str
        @param collection: Collection
        @param parent: Entity or Segment
        @param cleaned_data: dict Form data (all fields required)
        @param commit: boolean
        @returns: exit,status,updated_files (int,str,list)
        """
        if not collection:
            collection = self.identifier.collection().object()
        if not parent:
            parent = self.identifier.parent().object()
        
        if cleaned_data:
            self.form_post(cleaned_data)
        
        self.write_json()
        updated_files = [
            self.json_path,
        ]
        
        if parent and isinstance(parent, Entity):
            # update parent .children and .file_groups
            parent.children(force_read=True)
            parent.write_json()
            updated_files.append(parent.json_path)
        
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
        
        # remove file from parent entity.file_groups
        rm_files,updated_files = entity.prep_rm_file(self)
        
        # write files and commit
        status,message,updated_files = commands.file_destroy(
            git_name, git_mail,
            collection, entity,
            rm_files, updated_files,
            agent=agent,
            commit=commit
        )
        return exit, status, rm_files, updated_files

    # _lockfile
    # lock
    # unlock
    # locked
    
    # create(path)
    
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
        return from_json(File, path_abs, identifier)
    
    @staticmethod
    def from_csv(identifier, rowd):
        """Instantiates a File object from CSV row data.
        
        @param identifier: [optional] Identifier
        @param rowd: dict Headers/row cells for one line of a CSV file.
        @returns: DDRFile
        """
        return from_csv(identifier, rowd)
    
    @staticmethod
    def from_identifier(identifier):
        """Instantiates a File object, loads data from FILE.json or creates new object.
        
        @param identifier: Identifier
        @returns: File
        """
        if os.path.exists(identifier.path_abs('json')):
            return File.from_json(identifier.path_abs('json'), identifier)
        return File.create(identifier.path_abs('json'), identifier)
    
    def parent( self ):
        i = Identifier(id=self.parent_id, base_path=self.identifier.basepath)
        return Entity.from_identifier(i)

    def children( self, quick=None ):
        return []
    
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
        """Apply formprep_{field} functions in File module to prep data dict to pass into DDRForm object.
        
        @returns data: dict object as used by Django Form object.
        """
        return form_prep(self, self.identifier.fields_module())
    
    def form_post(self, cleaned_data):
        """Apply formpost_{field} functions to process cleaned_data from DDRForm
        
        @param cleaned_data: dict
        """
        form_post(self, self.identifier.fields_module(), cleaned_data)
    
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
    
    def inherit( self, parent ):
        inheritance.inherit( parent, self )
    
    def load_json(self, json_text):
        """Populate File data from JSON-formatted text.
        
        @param json_text: JSON-formatted text
        """
        module = self.identifier.fields_module()
        json_data = load_json(self, module, json_text)
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
        data = dump_json(self, module)
        if obj_metadata:
            data.insert(0, obj_metadata)
        elif doc_metadata:
            data.insert(0, object_metadata(module, self.collection_path))
        # what we call path_rel in the .json is actually basename
        data.insert(1, {'path_rel': self.basename})
        return format_json(data)

    def write_json(self, obj_metadata={}):
        """Write File JSON file to disk.
        
        @param obj_metadata: dict Cached results of object_metadata.
        """
        dirname = os.path.dirname(self.identifier.path_abs())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fileio.write_text(
            self.dump_json(doc_metadata=True, obj_metadata=obj_metadata),
            self.json_path
        )
    
    def post_json(self, public=False):
        # NOTE: this is same basic code as docstore.index
        return docstore.Docstore().post(
            load_json_lite(self.json_path, self.identifier.model, self.id),
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
        if 'id' in rowd.iterkeys():
            rowd.pop('id')
        module = modules.Module(self.identifier.fields_module())
        modified = load_csv(self, module, rowd)
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
    
    def dump_csv(self, headers=[]):
        """Dump File data to list of values suitable for CSV.
        
        @returns: list of values
        """
        # make sure we export 'id' if it's not in model FIELDS (ahem, files)
        if 'id' not in headers:
            headers.insert(0, 'id')
        module = modules.Module(self.identifier.fields_module())
        if self.basename and not self.mimetype:
            self.mimetype = self.get_mimetype(force=True)
        return prep_csv(self, module, headers=headers)
    
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
        
    def dict( self ):
        return self.__dict__
        
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
