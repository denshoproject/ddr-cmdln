from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from functools import total_ordering
import json
import os
import re

from deepdiff import DeepDiff
import elasticsearch_dsl as dsl

from DDR import VERSION
from DDR import archivedotorg
from DDR import config
from DDR import converters
from DDR import docstore
from DDR import dvcs
from DDR import fileio
from DDR.identifier import Identifier, ID_COMPONENTS, MODELS_IDPARTS, MODULES
from DDR.identifier import ELASTICSEARCH_CLASSES_BY_MODEL
from DDR import inheritance
from DDR import locking
from DDR import modules
from DDR import util

INTERVIEW_SIG_PATTERN = r'^denshovh-[a-z_0-9]{1,}-[0-9]{2,2}$'
INTERVIEW_SIG_REGEX = re.compile(INTERVIEW_SIG_PATTERN)

DIFF_IGNORED = [
    'app_commit', 'commit',  # object metadata
    'id',
    'record_created', 'record_lastmod',
]

class Path( object ):
    pass

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
            self.__module__,
            self.__class__.__name__,
            self.identifier.model, self.id
        )
    
    def parent(self, stubs=False):
        return self.identifier.parent(stubs).object()

    def children(self):
        return []


@total_ordering
class DDRObject(object):
    
    def __repr__(self):
        """Returns string representation of object.
        
        >>> c = Collection('/tmp/ddr-testing-123')
        >>> c
        <Collection ddr-testing-123>
        """
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.identifier.model, self.id
        )
    
    def __eq__(self, other):
        """Enable Pythonic sorting"""
        return self.identifier.path_abs() == other.identifier.path_abs()

    def __lt__(self, other):
        """Enable Pythonic sorting"""
        return self.identifier.id_sort < other.identifier.id_sort
    
    #exists
    #create
    #new
    #save
    #delete
    #from_json
    #from_csv
    #from_identifier
    
    def dict(self, json_safe=False):
        """Returns an OrderedDict of object fields data
        
        @param json_safe: bool Serialize e.g. datetime to text
        @returns: OrderedDict
        """
        return to_dict(self, self.identifier.fields_module(), json_safe=json_safe)
    
    def diff(self, other, ignore_fields=[]):
        """Compares object fields w those of another (instantiated) object
        
        NOTE: This function should only be used to tell IF objects differ,
        not HOW they differ.
        NOTE: Output should be treated as a boolean.
        It's currently a dict (unless the datetime error below) but the
        format is subject to change.
        
        NOTE: By the time an object has been instantiated, it has been
        through all the (ddr-defs/repo_models/MODEL:)jsonload_* methods
        and has likely been changed from its original state (e.g. topics).
        If you want to compare an instantiated object with its state
        in the filesystem, use diff_file.
        
        @param other: DDRObject
        @param ignore_fields: list
        @returns: dict
        """
        ignore_fields = DIFF_IGNORED + ignore_fields
        
        def rm_ignored(data, ignore):
            """Remove lines containing the specified fields
            @param data: OrderedDict
            @param ignore: list of ignored fieldnames
            @returns: list of dicts minus ignored fields
            """
            keys = list(data.keys())
            for fieldname in ignore:
                if fieldname in keys:
                    data.pop(fieldname)
            return data
        
        this = rm_ignored(self.dict(), ignore_fields)
        that = rm_ignored(other.dict(), ignore_fields)
        try:
            return DeepDiff(this, that, ignore_order=True)
        except TypeError:
            # DeepDiff crashes when trying to compare timezone-aware and
            # timezone-ignorant datetimes. Let's consider these different
            return True
    
    def diff_file(self, path, ignore_fields=[]):
        """Compares object fields with those of values in .json file
        
        NOTE: This function should only be used to tell IF objects differ,
        not HOW they differ.
        NOTE: Output should be treated as a boolean.
        It's currently a dict (unless the datetime error below) but the
        format is subject to change.
        
        This function compares object's values with those of the raw .json
        that has NOT passed through the various jsonload_* methods.
        
        @param path: str Absolute path to file
        @param ignore_fields: list
        @returns: dict
        """
        ignore_fields = DIFF_IGNORED + ignore_fields
        
        def rm_ignored(data, ignore):
            """Remove lines containing the specified fields
            @param data: OrderedDict
            @param ignore: list of ignored fieldnames
            @returns: list of dicts minus ignored fields
            """
            keys = list(data.keys())
            for fieldname in ignore:
                if fieldname in keys:
                    data.pop(fieldname)
            return data

        def set_empty_defaults(data, module):
            """Set empty fields to correct defaults from module.FIELDS
            Prevents false positives w old objects that don't use current defaults.
            """
            for field in module.FIELDS:
                if (field['name'] in list(data.keys())) and not data[field['name']]:
                    data[field['name']] = field['default']
            return data
        
        # load list of fields from file
        raw = load_json_lite(path, 'entity', 'ddr-densho-12-1')
        # remove initial metadata dict
        raw.pop(0)
        other = OrderedDict()
        for item in raw:
            key = list(item.keys())[0]
            value = item[key]
            other[key] = value
        
        this = rm_ignored(self.dict(), ignore_fields)
        that = rm_ignored(other, ignore_fields)
        set_empty_defaults(this, MODULES[self.identifier.model])
        set_empty_defaults(that, MODULES[self.identifier.model])
        try:
            return DeepDiff(this, that, ignore_order=True)
        except TypeError:
            # DeepDiff crashes when trying to compare timezone-aware and
            # timezone-ignorant datetimes. Let's consider these different
            return True
    
    #parent
    #children
    
    def signature_abs(self):
        """Absolute path to signature image file, if signature_id present.
        """
        return signature_abs(self, self.identifier.basepath)
    
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
        """Returns list of object's inheritable field names 
        
        >>> c = Collection.from_json('/tmp/ddr-testing-123')
        >>> c.inheritable_fields()
        ['status', 'public', 'rights']
        
        @returns: list
        """
        return inheritance.inheritable_fields(
            self.identifier.fields_module().FIELDS
        )

    def selected_inheritables(self, cleaned_data ):
        """Returns names of fields marked as inheritable in cleaned_data.
        
        Fields are considered selected if dict contains key/value pairs
        in the form 'FIELD_inherit':True.
        
        @param cleaned_data: dict Fieldname:value pairs.
        @returns: list
        """
        return inheritance.selected_inheritables(
            self.inheritable_fields(), cleaned_data
        )
    
    def update_inheritables( self, inheritables ):
        """Update specified fields of child objects.
        
        @param inheritables: list Names of fields that shall be inherited.
        @returns: tuple [changed object Ids],[changed objects' JSON files]
        """
        return inheritance.update_inheritables(self, inheritables)
    
    def inherit( self, parent ):
        """Inherit inheritable fields from the specified parent object.
        
        @param parent: DDRObject
        @returns: None
        """
        inheritance.inherit( parent, self )
    
    def lock( self, text ): return locking.lock(self.lock_path, text)
    def unlock( self, text ): return locking.unlock(self.lock_path, text)
    def locked( self ): return locking.locked(self.lock_path)

    #load_json
    #dump_json
    
    def to_esobject(self, public_fields=[], public=True, b2=False):
        """Returns an Elasticsearch DSL version of the object
        
        @param public_fields: list
        @param public: boolean
        @param b2: boolean File uploaded to Backblaze
        @returns: subclass of repo_models.elastic.ESObject
        """
        # instantiate appropriate subclass of ESObject / DocType
        # TODO Devil's advocate: why are we doing this? We already have the object.
        ES_Class = ELASTICSEARCH_CLASSES_BY_MODEL[self.identifier.model]
        fields_module = self.identifier.fields_module()
        if not public_fields:
            public_fields = [
                f['name']
                for f in fields_module.FIELDS
                if f['elasticsearch']['public']
            ]
        
        img_path = ''
        if hasattr(self, 'mimetype') and (self.mimetype == 'text/html'):
            # file with html transcript  TODO test this
            img_path = os.path.join(
                self.identifier.collection_id(),
                '%s%s' % (self.id, self.ext),
            )
        elif hasattr(self, 'access_rel'):
            # file with image            TODO test this
            img_path = os.path.join(
                self.identifier.collection_id(),
                os.path.basename(self.access_rel),
            )
        elif self.signature_id:
            # entity with signature      TODO test this
            img_path = os.path.join(
                self.identifier.collection_id(),
                access_filename(self.signature_id),
            )
        
        d = ES_Class()
        d.meta.id = self.identifier.id
        d.id = self.identifier.id
        d.model = self.identifier.model
        if self.identifier.collection_id() != self.identifier.id:
            # we don't want file-role (a stub) as parent
            d.parent_id = self.identifier.parent_id(stubs=0)
        else:
            # but we do want repository,organization (both stubs)
            d.parent_id = self.identifier.parent_id(stubs=1)
        d.organization_id = self.identifier.organization_id()
        d.collection_id = self.identifier.collection_id()
        d.signature_id = self.signature_id
        # ID components (repo, org, cid, ...) as separate fields
        idparts = deepcopy(self.identifier.idparts)
        idparts.pop('model')
#        for k in ID_COMPONENTS:
#            setattr(d, k, '') # ensure all fields present
        for k,v in idparts.items():
            setattr(d, k, v)
        # links
        d.links_html = self.identifier.id
        d.links_json = self.identifier.id
        d.links_parent = self.identifier.parent_id(stubs=True)
        d.links_children = self.identifier.id
        d.links_img = img_path
        d.links_thumb = img_path
        if (self.identifier.model in ['file']):
            d.links_download = os.path.join(
                self.identifier.collection_id(),
                '%s%s' % (self.id, self.ext),
            )
            if b2:
                d.backblaze = True

        # title,description
        if hasattr(self, 'title'): d.title = self.title
        else: d.title = self.label
        if hasattr(self, 'description'): d.description = self.description
        else: d.description = ''
        # breadcrumbs
        d.lineage = [
            {
                'id': i.id,
                'model': i.model,
                'idpart': str(MODELS_IDPARTS[i.model][-1][-1]),
                'label': str(i.idparts[
                    MODELS_IDPARTS[i.model][-1][-1]
                ]),
            }
            for i in self.identifier.lineage(stubs=0)
        ]
        # module-specific fields
        if hasattr(ES_Class, 'list_fields'):
            setattr(d, '_fields', ES_Class.list_fields())
        # module-specific fields
        for fieldname in docstore.doctype_fields(ES_Class):
            # hide non-public fields if this is public
            if public and (fieldname not in public_fields):
                continue
            # complex fields use repo_models.MODEL.index_FIELD if present
            if hasattr(fields_module, 'index_%s' % fieldname):
                field_data = modules.Module(fields_module).function(
                    'index_%s' % fieldname,
                    getattr(self, fieldname),
                )
            else:
                try:
                    field_data = getattr(self, fieldname)
                except AttributeError as err:
                    field_data = None
            if field_data:
                setattr(d, fieldname, field_data)
        # "special" fields
        if (self.identifier.model in ['entity','segment']):
            # TODO find a way to search on creators.id
            # narrator_id
            for c in self.creators:
                try:
                    d.narrator_id = c['id']
                except:
                    pass
            # topics & facility are too hard to search as nested objects
            # so attach extra 'topics_id' and 'facility_id' fields
            d.topics_id = [item['id'] for item in self.topics]
            d.facility_id = [item['id'] for item in self.facility]
            # A/V object metadata from Internet Archive
            # A/V templates
            if not config.OFFLINE:
                d.ia_meta = archivedotorg.get_ia_meta(self)
                if d.ia_meta:
                    d.template = ':'.join([
                        self.format,
                        d.ia_meta['mimetype'].split('/')[0]
                    ])
        return d
    
    def is_modified(self):
        """Returns True if object non-ignored fields differ from file.
        
        @returns: dict Output of DeepDiff; no diffs -> {} which is Falsey
        """
        if not os.path.exists(self.json_path):
            return True
        return self.diff_file(self.identifier.path_abs('json'))

    def write_json(self, doc_metadata=True, obj_metadata={}, force=False, path=None):
        """Write Collection/Entity JSON file to disk.
        
        @param doc_metadata: boolean
        @param obj_metadata: dict Cached results of object_metadata.
        @param force: boolean Write even nothing looks changed.
        @param path: str Alternate absolute file path
        """
        if force or self.is_modified():
            if not path:
                path = self.json_path
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            fileio.write_text(
                self.dump_json(
                    doc_metadata=doc_metadata, obj_metadata=obj_metadata
                ),
                path
            )
    
    #post_json
    #load_csv
    #dump_csv
    
    def changelog( self ):
        """Gets Collection/Entity changelog
        """
        if os.path.exists(self.changelog_path):
            return open(self.changelog_path, 'r').read()
        return '%s is empty or missing' % self.changelog_path
    
    #control
    #dump_xml
    #write_xml


# helper functions -----------------------------------------------------

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
        for line in fileio.read_text(path).splitlines():
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

def new_object(identifier, parent=None, inherit=True):
    """Creates a new object initial values from module.FIELDS.
    
    If identifier.fields_module().FIELDS.field['default'] is non-None
    it is used as the value.
    Use "None" for things like Object.id, which should already be set
    in the object constructor, and which you do not want to overwrite
    with a default value. Ahem.
    
    @param identifier: Identifier
    @param parent: DDRObject (optional)
    @param inherit: boolean Disable in loops to avoid infinite recursion
    @returns: object
    """
    object_class = identifier.object_class()
    # instantiate a raw object
    obj = object_class(
        identifier.path_abs(),
        identifier=identifier
    )
    # set default values
    for f in identifier.fields_module().FIELDS:
        if f['default'] != None:
            # some defaults are functions (e.g. datetime.now)
            if callable(f['default']):
                setattr(
                    obj,
                    f['name'],
                    f['default']()  # call function with no args
                )
            # most are just values (e.g. 'unknown', -1)
            else:
                setattr(
                    obj,
                    f['name'],
                    f['default']  # just the default value
                )
        elif hasattr(f, 'name') and hasattr(f, 'initial'):
            setattr(obj, f['name'], f['initial'])
    # inherit defaults from parent
    # Disable within loops to avoid infinite recursion
    # Ex: Loading File may trigger loading an Entity which loads Files, etc
    if inherit and (not parent) and identifier.parent():
        try:
            parent = identifier.parent().object()
        except IOError:
            parent = None
    if parent:
        inheritance.inherit(parent, obj)
    obj.identifier = identifier
    obj.id = identifier.id
    return obj

def object_metadata(module, repo_path):
    """Metadata for the ddrlocal/ddrcmdln and models definitions used.
    
    @param module: collection, entity, files model definitions module
    @param repo_path: Absolute path to root of object's repo
    @returns: dict
    """
    if not config.APP_METADATA:
        repo = dvcs.repository(repo_path)
        config.APP_METADATA['git_version'] = '; '.join([
            dvcs.git_version(repo),
            dvcs.annex_version(repo)
        ])
        # ddr-cmdln
        url = 'https://github.com/densho/ddr-cmdln.git'
        config.APP_METADATA['application'] = url
        config.APP_METADATA['app_path'] = config.INSTALL_PATH
        config.APP_METADATA['app_commit'] = dvcs.latest_commit(
            config.INSTALL_PATH
        )
        config.APP_METADATA['app_release'] = VERSION
        # ddr-defs
        config.APP_METADATA['defs_path'] = modules.Module(module).path
        config.APP_METADATA['defs_commit'] = dvcs.latest_commit(
            modules.Module(module).path
        )
    return config.APP_METADATA

def is_object_metadata(data):
    """Indicate whether json_data field is the object_metadata field.
    
    @param data: list of dicts
    @returns: boolean
    """
    for key in ['app_commit', 'app_release']:
        if key in list(data.keys()):
            return True
    return False

def to_dict(document, module, json_safe=False):
    """Returns an OrderedDict containing the object fields and values.
    
    All fields of the object *definition* are included.  Fields missing
    from the provided object are included as blank strings ('').
    
    @param document: Collection, Entity, File document object
    @param module: collection, entity, files model definitions module
    @param json_safe: bool Serialize Python objects e.g. datetime to text
    @returns: OrderedDict
    """
    data = OrderedDict()
    for f in module.FIELDS:
        fieldname = f['name']
        field_data = getattr(document, f['name'], '')
        if json_safe:
            if isinstance(field_data, Identifier):
                field_data = str(field_data)
            elif isinstance(field_data, datetime):
                field_data = converters.datetime_to_text(field_data)
        data[fieldname] = field_data
    return data

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
    document = json.loads(fileio.read_text(json_path))
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
    json_data = json.loads(json_text)
    # software and commit metadata
    for field in json_data:
        if is_object_metadata(field):
            setattr(document, 'object_metadata', field)
            break
    # field values from JSON
    for mf in module.FIELDS:
        for f in json_data:
            if hasattr(f, 'keys') and (list(f.keys())[0] == mf['name']):
                fieldname = list(f.keys())[0]
                # run jsonload_* functions on field data if present
                field_data = modules.Module(module).function(
                    'jsonload_%s' % fieldname,
                    list(f.values())[0]
                )
                if isinstance(field_data, str):
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
                if document.identifier.idparts['org'] in list(config.ALT_TIMEZONES.keys()):
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

def from_json(model, json_path, identifier, inherit=True):
    """Read the specified JSON file and properly instantiate object.
    
    @param model: LocalCollection, LocalEntity, or File
    @param json_path: absolute path to the object's .json file
    @param identifier: [optional] Identifier
    @param inherit: boolean Disable in loops to avoid infinite recursion
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

def prep_csv(obj, module, fields=[]):
    """Dump object field values to list suitable for a CSV file.
    
    Note: Autogenerated and non-user-editable fields
    (SHA1 and other hashes, file size, etc) should be excluded
    from the CSV file.
    Note: For files these are replaced by File.id which contains
    the role and a fragment of the SHA1 hash.
    
    @param obj_: Collection, Entity, File
    @param module: modules.Module
    @param fields: list If nonblank only export specified fields.
    @returns: list of values
    """
    if fields:
        field_names = fields
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
    # Other CSV fields like `access_path` (for importing custom access files)
    # are not in repo_models.object.FIELDS at all but we still need them.
    field_directives = {
        f['name']: f['csv']['import']
        for f in module.module.FIELDS
    }
    data = {}
    for fieldname,value in rowd.items():
        try:
            ignored = 'ignore' in field_directives[fieldname]
        except KeyError:
            # Ignore rowd fields not in module.FIELDS
            ignored = None
        if ignored != True:
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
    # Other CSV fields like `access_path` (for importing custom access files)
    # are not in repo_models.object.FIELDS at all but we still need them.
    field_directives = {
        f['name']: f['csv']['import']
        for f in module.module.FIELDS
    }
    # apply module's csvload_* methods to rowd data
    rowd = csvload_rowd(module, rowd)
    obj.modified_fields = []
    for field,value in rowd.items():
        try:
            ignored = 'ignore' in field_directives[field]
        except KeyError:
            # Ignore rowd fields not in module.FIELDS
            ignored = None
        if ignored != True:
            oldvalue = getattr(obj, field, '')
            value = rowd[field]
            if value != oldvalue:
                obj.modified_fields.append(field)
            setattr(obj, field, value)
    # Add timezone to fields if not present
    apply_timezone(obj, module.module)
    return obj.modified_fields

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

def signature_abs(obj, basepath):
    """Absolute path to signature image file, if signature_id present.
    
    Expects obj.signature_id to be either a valid file ID
    or a special interview signature image
    (ex. "denshovh-aart-03", "denshovh-hlarry_g-02")
    
    @returns: str absolute path to signature img, or None
    """
    if isinstance(obj, dict):
        sid = obj.get('signature_id')
    else:
        sid = getattr(obj, 'signature_id', None)
    # ignore interview signature ID
    if sid and INTERVIEW_SIG_REGEX.match(sid):
        return None
    if sid:
        try:
            oi = Identifier(sid, basepath)
        except:
            oi = None
        if oi and oi.model == 'file':
            return oi.path_abs('access')
    return None

def access_filename(file_id):
    """
    TODO This is probably redundant. D-R-Y!
    """
    if file_id:
        return '%s-a.jpg' % file_id
    return file_id
