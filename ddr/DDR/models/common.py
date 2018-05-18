from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
import os

import elasticsearch_dsl as dsl
import simplejson as json

from DDR import VERSION
from DDR import config
from DDR import docstore
from DDR import dvcs
from DDR import fileio
from DDR.identifier import Identifier, ID_COMPONENTS, MODELS_IDPARTS
from DDR.identifier import ELASTICSEARCH_CLASSES_BY_MODEL
from DDR import inheritance
from DDR import locking
from DDR import modules
from DDR import util


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

    #exists
    #create
    #new
    #save
    #delete
    #from_json
    #from_csv
    #from_identifier
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
        """Returns list of Collection/Entity object's field names marked as inheritable.
        
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
    
    #update_inheritables
    #inherit
    
    def lock( self, text ): return locking.lock(self.lock_path, text)
    def unlock( self, text ): return locking.unlock(self.lock_path, text)
    def locked( self ): return locking.locked(self.lock_path)

    #load_json
    #dump_json
    
    def to_esobject(self, public_fields=[], public=True):
        """Returns an Elasticsearch DSL version of the object
        
        @param public_fields: list
        @param public: boolean
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
        if hasattr(self, 'mimetype') and (self.mimetype == 'text/html'):  # TODO knows too much!!!
            img_path = os.path.join(
                self.identifier.collection_id(),
                '%s.htm' % self.id,
            )
        elif hasattr(self, 'access_rel'):
            img_path = os.path.join(
                self.identifier.collection_id(),
                os.path.basename(self.access_rel),
            )
        elif self.signature_id:
            img_path = os.path.join(
                self.identifier.collection_id(),
                access_filename(self.signature_id),
            )
        
        download_path = ''
        if (self.identifier.model in ['file']):
            download_path = os.path.join(
                self.identifier.collection_id(),
                '%s%s' % (self.id, self.ext),
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
        for k in ID_COMPONENTS:
            setattr(d, k, '') # ensure all fields present
        for k,v in idparts.iteritems():
            setattr(d, k, v)
        # links
        d.links_html = self.identifier.id
        d.links_json = self.identifier.id
        d.links_parent = self.identifier.parent_id(stubs=True)
        d.links_children = self.identifier.id
        d.links_img = img_path
        d.links_thumb = img_path
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
        elif (self.identifier.model in ['file']):
            if download_path:
                d.links_download = download_path
        return d
    
    def write_json(self, obj_metadata={}):
        """Write Collection/Entity JSON file to disk.
        
        @param obj_metadata: dict Cached results of object_metadata.
        """
        if not os.path.exists(self.identifier.path_abs()):
            os.makedirs(self.identifier.path_abs())
        fileio.write_text(
            self.dump_json(doc_metadata=True, obj_metadata=obj_metadata),
            self.json_path
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
        'defs_path': modules.Module(module).path,
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

def signature_abs(obj, basepath):
    """Absolute path to signature image file, if signature_id present.
    """
    if isinstance(obj, dict):
        sid = obj.get('signature_id')
    else:
        sid = getattr(obj, 'signature_id', None)
    if sid:
        oi = Identifier(sid, basepath)
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
