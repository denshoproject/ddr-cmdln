# coding: utf-8

from collections import OrderedDict
import importlib
import os
import re
import string
from urlparse import urlparse



class Definitions():
    """Functions for parsing and extracting useful data from IDENTIFIERS
    """

    @staticmethod
    def models(identifiers):
        # Models in this Repository
        models = [i['model'] for i in identifiers]
        models.reverse()
        return models
    
    @staticmethod
    def modules(identifiers):                                                                         
        return {key:None for key in Definitions.models(identifiers)}
    
    @staticmethod
    def import_modules(identifiers, modules):
        """Imports modules in IDENTIFIERS[N]['module'] and adds to MODULES
        
        @param identifiers: list
        @param modules: dict
        @returns: dict modules
        """
        models_modules = {
            i['model']: i['module']
            for i in identifiers
            if i['module']
        }
        couldnt = []
        for model,module in models_modules.iteritems():
            try:
                modules[model] = importlib.import_module(module)
            except ImportError:
                couldnt.append(module)
        if couldnt:
            raise Exception(
                'Could not import module(s) %s. ' \
                'May indicate problem in definitions module or dependency (e.g. lxml).' % (
                    couldnt
            ))
        return modules

    @staticmethod
    def model_classes(identifiers):
        """map model names to DDR python classes
        
        >>> identifiers = [
        ...     {'model': 'collection', 'class':'DDR.models.Collection'},
        ...     {'model': 'entity',     'class':'DDR.models.Entity'},
        ...     {'model': 'file-role',  'class':'DDR.models.Stub'},
        ...     {'model': 'file',       'class':'DDR.models.File'},
        ... ]
        >>> model_classes(identifiers)
        {
            'collection': {'class': 'Collection', 'module': 'DDR.models'},
            'entity':     {'class': 'Entity',     'module': 'DDR.models'},
            'file-role':  {'class': 'Stub',       'module': 'DDR.models'},
            'file':       {'class': 'File',       'module': 'DDR.models'}
        }
        """
        return {
            i['model']: {
                'module': i['class'][0:i['class'].rindex('.')],
                'class': i['class'][i['class'].rindex('.')+1:],
            }
            for i in identifiers
        }

    @staticmethod
    def models_modules(modules):
        """map model names to module files in ddr repo's repo_models
        """
        return {
            model: {
                'module': module.__name__,
                'class': model,
                'as': '%smodule' % model,
            }
            for model,module in modules.iteritems() if module
        }

    @staticmethod
    def collection_models(identifiers):
        """Models that are part of collection repositories.
        
        Repository and organizations are above the level of the collection
        and are thus excluded.
        """
        return [i['model'] for i in identifiers if i['level'] >= 0]

    @staticmethod
    def containers(identifiers):
        """Models that can contain other models.
        """
        containers = [
            i['model']
            for i in identifiers
            if i['children'] or i['children_all']
        ]
        containers.reverse()
        return containers

    @staticmethod
    def models_parents(identifiers):
        """Pointers from models to their parent models
        
        >>> identifiers = [
        ...     {'model': 'collection', 'parents': []},
        ...     {'model': 'entity',     'parents': ['collection']},
        ...     {'model': 'file',       'parents': ['entity']},
        ... ]
        >>> models_parents(identifiers)
        {
           'entity': ['collection'],
           'file': ['entity'],
        }
        """
        parents = {}
        for i in identifiers:
            if 'Stub' not in i['class']:
                for parent in i['parents']:
                    if not parents.get(i['model']):
                        parents[i['model']] = []
                    parents[i['model']].append(parent)
        return parents

    @staticmethod
    def models_parents_all(identifiers):
        """Pointers from models to their parent models
        
        >>> identifiers = [
        ...     {'model': 'repository',   'parents_all': []},
        ...     {'model': 'organization', 'parents_all': ['repository']},
        ...     {'model': 'collection',   'parents_all': ['organization]},
        ...     {'model': 'entity',       'parents_all': ['collection']},
        ...     {'model': 'file-role',    'parents_all': ['entity']},
        ...     {'model': 'file',         'parents_all': ['file-role']},
        ... ]
        >>> models_parents_all(identifiers)
        {
            'organization': ['repository'],
            'collection': ['organization'],
            'entity': ['collection'],
            'file-role': ['entity'],
            'file': ['file-role'],
        }
        """
        parents = {}
        for i in identifiers:
            for parent in i['parents_all']:
                if not parents.get(i['model']):
                    parents[i['model']] = []
                parents[i['model']].append(parent)
        return parents

    @staticmethod
    def children(parents):
        children = {}
        for child,folks in parents.iteritems():
            if folks:
                for parent in folks:
                    if not children.get(parent):
                        children[parent] = []
                    children[parent].append(child)
        return children

    @staticmethod
    def endpoints(identifiers, fieldname):
        """Nodes, leafs, endpoints. Things with no children or parents.
        """
        return [
            i['model']
            for i in identifiers
            if i[fieldname] == []
        ]

    @staticmethod
    def id_components(identifiers):
        """Keywords that can legally appear in IDs
        
        TODO list should include 'ext'?
        """
        return [
            i['component']['name']
            for i in identifiers
        ]

    @staticmethod
    def component_types(identifiers):
        """Python data type for each component
        """
        return {
            i['component']['name']: i['component']['type']
            for i in identifiers
        }

    @staticmethod
    def component_formfields(identifiers):
        """Django form field info for each component
        """
        return {
            i['component']['name']: i['component']['form']
            for i in identifiers
        }

    @staticmethod
    def valid_components(identifiers):
        """Components in VALID_COMPONENTS.keys() must appear in VALID_COMPONENTS[key] to be valid.
        """
        return {
            i['component']['name']: i['component']['valid']
            for i in identifiers
            if i['component']['valid']
        }

    @staticmethod
    def nextable_models(identifiers):
        """Models whose components are sequential
        """
        return [
            i['model']
            for i in identifiers
            if i['component']['type'] == int
        ]

    @staticmethod
    def filename_regexes(identifiers):
        """List of regexes for matching metadata filenames
        
        META_FILENAME_REGEX = {
            'collection': re.compile('collection.json'),
            'entity': re.compile('entity.json'),
            'segment': re.compile('entity.json'),
            'file': re.compile('-([\d]+)-([\w]+)-([\w\d]+).json'),
        }
        """
        return {
            i['model']: re.compile(i.get('filename_regex'))
            for i in identifiers
            if i.get('filename_regex')
        }

    @staticmethod
    def models_idparts(identifiers):
        mi = {
            i['model']: i['templates']['id']
            for i in identifiers
        }
        return {
            model: [
                template.replace('{','').replace('}','').split('-')
                for template in templates
            ]
            for model,templates in mi.iteritems()
        }

    # ----------------------------------------------------------------------
    # Regex patterns used to link raw IDs/URLs/paths to models
    #
    # Regex patterns used to match IDs, paths, and URLs and extract model and tokens
    # Record format: (regex, memo, model)
    # TODO are we using ID_PATTERNS memos?
    #

    @staticmethod
    def id_patterns(identifiers):
        # (regex, memo, model) NOTE: 'memo' is not used for anything yet
        patterns = []
        for i in identifiers:
            for regex in i['patterns']['id']:
                p = (re.compile(regex), '', i['model'])
                patterns.append(p)
        patterns.reverse()
        return patterns

    @staticmethod
    def path_patterns(identifiers):
        # TODO are we using PATH_PATTERNS memos?
        # In the current path scheme, collection and entity ID components are repeated.
        # Fields can't appear multiple times in regexes so redundant fields have numbers.
        # (regex, memo, model) note: 'memo' is not used for anything yet
        patterns = []
        for i in identifiers:
            for regex in i['patterns']['path']:
                p = (re.compile(regex), '', i['model'])
                patterns.append(p)
        patterns.reverse()
        return patterns

    @staticmethod
    def url_patterns(identifiers):
        # TODO check
        # (regex, memo, model) note: 'memo' is not used for anything yet
        patterns = []
        for i in identifiers:
            for regex in i['patterns']['url']:
                p = (re.compile(regex), '', i['model'])
                patterns.append(p)
        patterns.reverse()
        return patterns

    # ----------------------------------------------------------------------
    # Templates used to generate IDs, paths, and URLs from model and tokens
    #
    # There may be multiple possible templates for a model.  This is to account
    # for multiple levels due to things like segments.  When generating an
    # ID/URL/PATH, templates will be tried in order until one works. Therefore,
    # put templates with the MOST components EARLIEST in the list.
    #

    @staticmethod
    def id_templates(identifiers):
        """
        {
            'collection': [
                '{repo}-{org}-{cid}',
            ],
            'entity': [
                '{repo}-{org}-{cid}-{eid}',
            ],
            'file': [
                '{repo}-{org}-{cid}-{eid}-{sid}-{role}-{sha1}',
                '{repo}-{org}-{cid}-{eid}-{role}-{sha1}',
            ],
        }
        """
        templates = {}
        for i in identifiers:
            if not templates.get(i['model']):
                templates[i['model']] = []
            templates[i['model']] = [t for t in i['templates']['id']]
        return templates

    @staticmethod
    def path_templates(identifiers):
        """
        {
            'entity-rel': [
                'files/{repo}-{org}-{cid}-{eid}',
            ],
            'entity-abs': [
                '{basepath}/{repo}-{org}-{cid}/files/{repo}-{org}-{cid}-{eid}',
            ],
            'collection-abs': [
                '{basepath}/{repo}-{org}-{cid}',
            ],
        }
        """
        templates = {}
        for i in identifiers:
            for k,v in i['templates']['path'].iteritems():
                if v:
                    key = '%s-%s' % (i['model'],k)
                    templates[key] = v
        return templates

    @staticmethod
    def url_templates(identifiers):
        """
        {
            'editor': {
                'file': [
                    '/ui/{repo}-{org}-{cid}-{eid}-{sid}-{role}-{sha1}',
                    '/ui/{repo}-{org}-{cid}-{eid}-{role}-{sha1}',
                ],
                'entity': [
                    '/ui/{repo}-{org}-{cid}-{eid}',
                ],
                'collection': [
                    '/ui/{repo}-{org}-{cid}',
                ],
            },
            'public': {
                'file': [
                    '/{repo}/{org}/{cid}/{eid}/{sid}/{role}/{sha1}',
                    '/{repo}/{org}/{cid}/{eid}/{role}/{sha1}',
                ],
                'entity': [
                    '/{repo}/{org}/{cid}/{eid}',
                ],
                'collection': [
                    '/{repo}/{org}/{cid}',
                ],
            }
        }
        """
        templates = {}
        for i in identifiers:
            for k,v in i['templates']['url'].iteritems():
                if not templates.get(k):
                    templates[k] = {}
                templates[k][i['model']] = [u for u in i['templates']['url'][k]]
        return templates

    @staticmethod
    def additional_paths(identifiers):
        """Additional file types that may be present in a repo
        """
        return {
            i['model']: i['files']
            for i in identifiers
        }


try:
    from repo_models.identifier import IDENTIFIERS
except ImportError:
    raise Exception(
        'Could not import Identifier definitions! ' \
        'May indicate problem in definitions module or dependency (e.g. lxml).'
    )

MODELS = Definitions.models(IDENTIFIERS)
MODULES = Definitions.import_modules(IDENTIFIERS, Definitions.modules(IDENTIFIERS))
MODEL_CLASSES = Definitions.model_classes(IDENTIFIERS)
MODEL_REPO_MODELS = Definitions.models_modules(MODULES)
COLLECTION_MODELS = Definitions.collection_models(IDENTIFIERS)
CONTAINERS = Definitions.containers(IDENTIFIERS)
PARENTS = Definitions.models_parents(IDENTIFIERS)
PARENTS_ALL = Definitions.models_parents_all(IDENTIFIERS)
CHILDREN = Definitions.children(PARENTS)
CHILDREN_ALL = Definitions.children(PARENTS_ALL)
ROOTS = Definitions.endpoints(IDENTIFIERS, 'parents_all')
NODES = Definitions.endpoints(IDENTIFIERS, 'children_all')
ID_COMPONENTS = Definitions.id_components(IDENTIFIERS)
COMPONENT_TYPES = Definitions.component_types(IDENTIFIERS)
COMPONENT_FORMFIELDS = Definitions.component_formfields(IDENTIFIERS)
VALID_COMPONENTS = Definitions.valid_components(IDENTIFIERS)
NEXTABLE_MODELS = Definitions.nextable_models(IDENTIFIERS)
# Bits of file paths that uniquely identify file types.
# Suitable for use on command-line e.g. in git-annex-whereis.
# TODO these should be in ddr-defs/repo_models/identifier.py
FILETYPE_MATCH_ANNEX = {
    'access': '*-a.jpg',
    'master': '*-master-*',
    'mezzanine': '*-mezzanine-*',
}
META_FILENAME_REGEX = Definitions.filename_regexes(IDENTIFIERS)
MODELS_IDPARTS = Definitions.models_idparts(IDENTIFIERS)
ID_PATTERNS = Definitions.id_patterns(IDENTIFIERS)
PATH_PATTERNS = Definitions.path_patterns(IDENTIFIERS)
URL_PATTERNS = Definitions.url_patterns(IDENTIFIERS)
ID_TEMPLATES = Definitions.id_templates(IDENTIFIERS)
PATH_TEMPLATES = Definitions.path_templates(IDENTIFIERS)
URL_TEMPLATES = Definitions.url_templates(IDENTIFIERS)
ADDITIONAL_PATHS = Definitions.additional_paths(IDENTIFIERS)


# ----------------------------------------------------------------------

def render_models_digraph(output_path):
    """Generates DAG of model child->parent relationships
    
    @param path: str Absolute path to output file
    @returns: str path to output file
    """
    import graphviz
    
    filename,extension = os.path.splitext(output_path)
    extension = extension.replace('.', '')
    
    g = graphviz.Digraph(format=extension)
    
    for model in MODELS:
        g.node(model)
    
    for model,parents in PARENTS_ALL.iteritems():
        if model and parents:
            for parent in parents:
                g.edge(model, parent)
    
    return g.render(filename)


def identify_object(text, patterns):
    """Split ID, path, or URL into model and tokens and assign to Identifier
    
    Like Django URL router, look for pattern than matches the given text.
    Patterns match to a model and the fields correspond to components of
    a legal object ID.
    Component names and values are assigned as attributes of the object.
    
    @param i: Identifier object
    @param text: str Text string to look for
    @param patterns: list Patterns in which to look
    @returns: dict groupdict resulting from successful regex match
    """
    model = None
    memo = None
    groupdict = None
    for tpl in patterns:
        m = re.match(tpl[0], text)
        if m:
            pattern,memo,model = tpl
            groupdict = m.groupdict()
            break
    ## validate components
    #for key in VALID_COMPONENTS.keys():
    #    val = groupdict.get(key, None)
    #    if val and (val not in VALID_COMPONENTS[key]):
    #        raise Exception('Invalid ID keyword: "%s"' % val)
    return model,memo,groupdict

def identify_filepath(path):
    """Indicates file role or if path is an access file.
    
    TODO use VALID_COMPONENTS or similar rather than hard-coding
    Probably better to do the access file matching separately
    
    @param text: str Text string to look for
    @returns: str File type or None
    """
    ftype = None
    if   '-a.'    in path: ftype = 'access'
    elif 'mezzan' in path: ftype = 'mezzanine'
    elif 'master' in path: ftype = 'master'
    elif 'transcript' in path: ftype = 'transcript'
    return ftype

def set_idparts(i, groupdict, components=ID_COMPONENTS, types=COMPONENT_TYPES):
    """Sets keys,values of groupdict as attributes of identifier.
    
    Each component is coerced to the data type specified in
    IDENTIFIERS['component']['type'].  See Definitions.component_types.
    
    @param i: Identifier
    @param groupdict: dict
    @param components: list [optional]
    @param types: dict
    """
    i.basepath = groupdict.get('basepath', None)
    if i.basepath:
        i.basepath = os.path.normpath(i.basepath)
    # list of object ID components
    id_components = [
        (key, groupdict[key])
        for key in components
        if groupdict.get(key)
    ]
    i.parts = OrderedDict(id_components)
    id_components.insert(0, ('model',i.model))
    i.idparts = OrderedDict(id_components)
    # set ID parts to their assigned type
    for key,val in i.parts.items():
        i.parts[key] = types[key](val)

class IdentifierFormatException(Exception):
    pass

def format_id(i, model, templates=ID_TEMPLATES):
    """Format ID for the requested model using ID_TEMPLATES.
    
    Hint: you can get parent IDs from an Identifier.
    
    @param i: Identifier
    @param model: str A legal model keyword
    @param templates: [optional] dict of str templates keyed to models
    @returns: str
    """
    for template in templates[model]:
        # first one that works is the ID (probably)
        try:
            return template.format(**i.parts)
        except KeyError:
            pass
    raise IdentifierFormatException('Could not format ID for %s' % i.parts)

def format_path(i, model, path_type, templates=PATH_TEMPLATES):
    """Format absolute or relative path using PATH_TEMPLATES.
    
    @param i: Identifier
    @param model: str A legal model keyword
    @param path_type: str 'abs' or 'rel'
    @param templates: [optional] dict of str templates keyed to models
    @returns: str or None
    """
    if path_type and (path_type == 'abs') and (not i.basepath):
        raise MissingBasepathException('%s basepath not set.'% i)
    key = '-'.join([model, path_type])
    for template in templates[key]:
        kwargs = {key: val for key,val in i.parts.items()}
        kwargs['basepath'] = i.basepath
        # TODO put in try/except, first one that works is the path
        try:
            return template.format(**kwargs)
        except KeyError:
            pass
    raise IdentifierFormatException('Could not format path for %s' % i.parts)

def format_url(i, model, url_type, templates=URL_TEMPLATES):
    """Format URL using URL_TEMPLATES.
    
    @param i: Identifier
    @param model: str A legal model keyword
    @param url_type: str 'public' or 'editor'
    @param templates: [optional] dict of str templates keyed to models
    @returns: str
    """
    for template in templates[url_type][model]:
        # TODO put in try/except, first one that works is the URL
        try:
            return template.format(**i.parts)
        except KeyError:
            pass
    raise IdentifierFormatException('Could not format URL for %s' % i.parts)

def matches_pattern(text, patterns):
    """True if text matches one of patterns
    
    Used for telling what kind of pattern (id, path, url) an arg is.
    
    @param text: str
    @returns: dict of idparts including model
    """
    for tpl in patterns:
        pattern = tpl[0]
        model = tpl[-1]
        m = re.match(pattern, text)
        if m:
            idparts = {k:v for k,v in m.groupdict().iteritems()}
            idparts['model'] = model
            return idparts
    return {}

def _is_id(text):
    """
    @param text: str
    @returns: dict of idparts including model
    """
    return matches_pattern(text, ID_PATTERNS)

def _is_path(text):
    """
    @param text: str
    @returns: dict of idparts including model
    """
    return matches_pattern(text, PATH_PATTERNS)

def _is_url(text):
    """
    @param text: str
    @returns: dict of idparts including model
    """
    return matches_pattern(text, URL_PATTERNS)

def _is_abspath(text):
    if isinstance(text, basestring) and os.path.isabs(text):
        return True
    return False

def _parse_args_kwargs(keys, args, kwargs):
    """Attempts to convert Identifier.__init__ args to kwargs.
    
    @param keys: list Whitelist of accepted kwargs
    @param args: list
    @param kwargs: dict
    """
    # TODO there's probably something in stdlib for this...
    blargs = {key:None for key in keys}
    if args:
        arg = None
        if len(args) >= 2: blargs['base_path'] = args[1]
        if len(args) >= 1: arg = args[0]
        if arg:
            # TODO refactor: lots of regex that's duplicated in identify_object
            if isinstance(arg, dict): blargs['parts'] = arg
            elif _is_id(arg): blargs['id'] = arg
            elif _is_url(arg): blargs['url'] = arg
            elif _is_abspath(arg): blargs['path'] = arg
    # kwargs override args
    if kwargs:
        for key,val in kwargs.items():
            if val and (key in keys):
                blargs[key] = val
    return blargs

def module_for_name(module_name):
    """Returns specified module.
    
    @param module_name: str
    @returns: module
    """
    # load the module, will raise ImportError if module cannot be loaded
    return importlib.import_module(module_name)

def class_for_name(module_name, class_name):
    """Returns specified class from specified module.
    
    @param module_name: str
    @param class_name: str
    @returns: class
    """
    c = getattr(
        module_for_name(module_name),
        class_name
    )
    return c

def _field_names(template):
    """Extract field names from string formatting template.
    """
    return [v[1] for v in string.Formatter().parse(template)]

def first_id(model, i):
    """Returns first child Identifier in series
    
    No guarantee that it's a legal Identifier...
    
    @param model: str Model of desired Identifier
    @param i: Identifier
    @returns: Identifier
    """
    parts = {k:v for k,v in i.parts.iteritems()}
    next_component = _field_names(ID_TEMPLATES[model][0]).pop()
    parts[next_component] = 1
    parts['model'] = model
    new = Identifier(parts=parts)
    return new

def max_id(model, identifiers):
    """Returns highest *existing* ID for the specied model
    
    @param model: str
    @param i: Identifier
    @returns: Identifier
    """
    if not identifiers:
        raise Exception('Cannot pick max if list is empty')
    for i in identifiers:
        if not isinstance(i, Identifier):
            raise Exception('List must consist of Identifiers.')
    list_models = [i.model for i in identifiers]
    if len(set(list_models)) > 1:
        raise Exception('All identifiers must be of same type.')
    list_parents = [i.parent_id() for i in identifiers]
    if len(set(list_parents)) > 1:
        raise Exception('All identifiers must have same parent.')
    
    # name of desired ID component ('eid' for entity)
    component = _field_names(ID_TEMPLATES[model][0]).pop()
    # list of just these ID components, sorted
    existing = sorted([i.parts[component] for i in identifiers])
    max_component = existing[-1]
    # construct new Identifier with max_component
    # seems better to pick the matching one from identifiers list
    # but currently no way to make ID from parts outside of constructor
    parts = identifiers[0].idparts
    parts[component] = max_component
    oi = Identifier(parts=parts)
    if oi.id not in [i.id for i in identifiers]:
        raise Exception('New id not in given list: %s' % oi)
    return oi

def add_ids(num_new, model, identifiers, startwith=None):
    """Add {num} {model} IDs to {list} starting with {n}; complain if duplicates
    
    >>> model = 'entity'
    >>> c = Collection('/PATH/TO/ddr-test-123')
    >>> identifiers = [i.id for i in c.family(model=model)]
    >>> add_ids(10, model, identifiers, 42)
    [42, 43, 44, 45, 46, 47, 48, 49, 50, 51]
    
    @param num_new: int
    @param model: str
    @param identifiers: list of Identifiers
    @param startwith: int
    @returns: dict {'success', 'max_id', 'new', 'taken'}
    """
    # TODO what if there is more than one ID_TEMPLATE???
    template = ID_TEMPLATES[model][0]
    # Get name of the ID component from the model's ID_TEMPLATE
    # e.g. for Entity we want 'eid'
    # This is the part we will increment
    component = _field_names(template).pop()
    # then get {component} from each Identifier.parts
    existing = sorted([i.parts[component] for i in identifiers])
    max_id = existing[-1] + 1
    
    if startwith:
        start = startwith
    else:
        start = max_id + 1
    new = range(start, start + num_new)
    
    taken = [x for x in set(new).intersection(existing)]
    return {
        'max_id': max_id,
        'new': new,
        'taken': taken,
        'success': taken == [],
    }

def available(existing, new):
    """Add new list to existing list; successful if no clashes.
    
    @param existing: list 
    @param new: list 
    @returns: {'new': list, 'taken': list, 'success': boolean}
    """
    data = {
        'overlap': [x for x in set(new).intersection(existing)],
    }
    data['success'] = data['overlap'] == []
    return data
    

class MissingBasepathException(Exception):
    pass

class BadPathException(Exception):
    pass

class MalformedIDException(Exception):
    pass

class MalformedPathException(Exception):
    pass

class MalformedURLException(Exception):
    pass

class InvalidInputException(Exception):
    pass

KWARG_KEYS = [
    'id',
    'parts',
    'path',
    'url',
    'base_path',
]

class Identifier(object):
    raw = None
    method = None
    model = None
    parts = OrderedDict()
    basepath = None
    id = None
    
    @staticmethod
    def wellformed(idtype, text, models=MODELS):
        """Checks if text is well-formed ID of specified type and (optionally) model.
        
        @param idtype: str one of ['id', 'path', 'url']
        @param text: str
        @param models: list
        @returns: dict (populated if well-formed)
        """
        idparts = None
        if idtype == 'id': idparts = _is_id(text)
        elif idtype == 'path': idparts = _is_path(text)
        elif idtype == 'url': idparts = _is_url(text)
        if idparts and (idparts['model'] in models):
            return idparts
        return {}

    #@staticmethod
    #def valid(idparts, components=VALID_COMPONENTS):
    #    """Checks if all non-int ID components are valid.
    #    
    #    @param idparts: dict
    #    @param components: dict 
    #    @returns: True or dict containing name of invalid component
    #    """
    #    invalid = [
    #        key for key in components.iterkeys()
    #        if idparts.get(key) and (idparts[key] not in components[key])
    #    ]
    #    if not invalid:
    #        return True
    #    return invalid
    
    def __init__(self, *args, **kwargs):
        """
        NOTE: You will get faster performance with kwargs
        """
        blargs = _parse_args_kwargs(KWARG_KEYS, args, kwargs)
        if blargs['id']: self._from_id(blargs['id'], blargs['base_path'])
        elif blargs['parts']: self._from_idparts(blargs['parts'], blargs['base_path'])
        elif blargs['path']: self._from_path(blargs['path'], blargs['base_path'])
        elif blargs['url']: self._from_url(blargs['url'], blargs['base_path'])
        else:
            raise InvalidInputException('Could not grok Identifier input: %s' % blargs)

    def _from_id(self, object_id, base_path=None):
        """Make Identifier from object ID.
        
        >>> Identifier(id='ddr-testing-123-456')
        <Identifier ddr-testing-123-456>
        
        @param object_id: str
        @param base_path: str Absolute path to Store's parent dir
        @returns: Identifier
        """
        if base_path and not os.path.isabs(base_path):
            raise BadPathException('Base path is not absolute: %s' % base_path)
        if base_path:
            base_path = os.path.normpath(base_path)
        self.method = 'id'
        self.raw = object_id
        self.id = object_id
        model,memo,groupdict = identify_object(object_id, ID_PATTERNS)
        if not groupdict:
            raise MalformedIDException('Malformed ID: "%s"' % object_id)
        self.model = model
        set_idparts(self, groupdict)
        if base_path and not self.basepath:
            self.basepath = base_path
    
    def _from_idparts(self, idparts, base_path=None):
        """Make Identifier from dict of parts.
        
        >>> parts = {'model':'entity', 'repo':'ddr', 'org':'testing', 'cid':123, 'eid':456}
        >>> Identifier(parts=parts)
        <Identifier ddr-testing-123-456>
        
        @param parts: dict
        @param base_path: str Absolute path to Store's parent dir
        @returns: Identifier
        """
        if base_path and not os.path.isabs(base_path):
            raise BadPathException('Base path is not absolute: %s' % base_path)
        if base_path:
            base_path = os.path.normpath(base_path)
        self.method = 'parts'
        self.raw = idparts
        self.model = idparts['model']
        id_components = [
            (key, idparts[key])
            for key in ID_COMPONENTS
            if idparts.get(key)
        ]
        self.parts = OrderedDict(id_components)
        id_components.insert(0, ('model', self.model))
        self.idparts = OrderedDict(id_components)
        self.id = format_id(self, self.model)
        if base_path and not self.basepath:
            self.basepath = base_path
    
    def _from_path(self, path_abs, base_path=None):
        """Make Identifier from absolute path.
        
        >>> path = '/tmp/ddr-testing-123/files/ddr-testing-123-456/entity.json
        >>> Identifier(path=path)
        <Identifier ddr-testing-123-456>
        
        @param path_abs: str
        @param base_path: str Absolute path to Store's parent dir
        @returns: Identifier
        """
        path_abs = os.path.normpath(path_abs)
        if not os.path.isabs(path_abs):
            raise BadPathException('Path is not absolute: %s' % path_abs)
        if base_path:
            base_path = os.path.normpath(base_path)
        self.method = 'path'
        self.raw = path_abs
        model,memo,groupdict = identify_object(path_abs, PATH_PATTERNS)
        if not groupdict:
            raise MalformedPathException('Malformed path: "%s"' % path_abs)
        self.model = model
        set_idparts(self, groupdict)
        self.id = format_id(self, self.model)
    
    def _from_url(self, url, base_path=None):
        """Make Identifier from URL or URI.
        
        >>> Identifier(url='http://ddr.densho.org/ddr/testing/123/456')
        <Identifier ddr-testing-123-456>
        >>> Identifier(url='http://ddr.densho.org/ddr/testing/123/456/')
        <Identifier ddr-testing-123-456>
        >>> Identifier(url='http://192.168.56.101/ui/ddr-testing-123-456')
        <Identifier ddr-testing-123-456>
        >>> Identifier(url='http://192.168.56.101/ui/ddr-testing-123-456/files/')
        <Identifier ddr-testing-123-456>
        
        @param path_abs: str
        @param base_path: str Absolute path to Store's parent dir
        @returns: Identifier
        """
        if base_path and not os.path.isabs(base_path):
            raise BadPathException('Base path is not absolute: %s' % base_path)
        if base_path:
            base_path = os.path.normpath(base_path)
        self.method = 'url'
        self.raw = url
        urlpath = urlparse(url).path  # ignore domain and queries
        urlpath = os.path.normpath(urlpath)
        model,memo,groupdict = identify_object(urlpath, URL_PATTERNS)
        if not groupdict:
            raise MalformedURLException('Malformed URL: "%s"' % url)
        self.model = model
        set_idparts(self, groupdict)
        self.id = format_id(self, self.model)
        if base_path and not self.basepath:
            self.basepath = base_path
    
    def __repr__(self):
        return "<%s.%s %s:%s>" % (self.__module__, self.__class__.__name__, self.model, self.id)
    
    def _key(self):
        """Key for Pythonic object sorting.
        Integer components are returned as ints, enabling natural sorting.
        """
        return self.parts.values()
    
    def __lt__(self, other):
        """Enables Pythonic sorting; see Identifier._key.
        """
        return self._key() < other._key()

    @staticmethod
    def nextable(model):
        if model in NEXTABLE_MODELS:
            return True
        return False
        
    def next(self):
        """Returns next Identifier if last ID component is numeric
        
        @returns: Identifier
        """
        partsd = {'model': self.model}
        for k,v in self.parts.iteritems():
            partsd[k] = v
        if not isinstance(v, int):
            raise Exception('Not a next-able model: %s' % (self.model))
        # increment the last component of the ID
        partsd[k] = v + 1
        return Identifier(parts=partsd, base_path=self.basepath)
    
    def components(self):
        """Model and parts of the ID as a list.
        """
        parts = [val for val in self.parts.itervalues()]
        parts.insert(0, self.model)
        return parts

    def fields_module(self, mappings=MODEL_REPO_MODELS):
        """Identifier's fields definitions module from repo_models.
        """
        return module_for_name(
            mappings[self.model]['module']
        )
        
    def object_class(self, mappings=MODEL_CLASSES):
        """Identifier's object class according to mappings.
        """
        return class_for_name(
            mappings[self.model]['module'],
            mappings[self.model]['class']
        )
    
    def object(self, mappings=MODEL_CLASSES):
        """The object identified by the Identifier.
        """
        return self.object_class(mappings).from_identifier(self)
    
    def collection_id(self):
        """ID of the collection to which the Identifier belongs, if any.
        """
        if not self.model in COLLECTION_MODELS:
            raise Exception('%s objects do not have collection IDs' % self.model.capitalize())
        return format_id(self, 'collection')
    
    def collection_path(self):
        """Absolute path of the collection to which the Identifier belongs, if any.
        """
        if not self.model in COLLECTION_MODELS:
            raise Exception('%s objects do not have collection paths' % self.model.capitalize())
        if not self.basepath:
            raise MissingBasepathException('%s basepath not set.'% self)
        return os.path.normpath(format_path(self, 'collection', 'abs'))
    
    def collection(self):
        """Collection object to which the Identifier belongs, if any.
        """
        return self.__class__(id=self.collection_id(), base_path=self.basepath)
    
    def parent_id(self, stubs=False):
        """ID of the Identifier's parent, if any.
        
        @param stubs: boolean Whether or not to include Stub objects.
        """
        parent = self.parent(stubs=stubs)
        if parent:
            return parent.id
        return None
    
    def parent_path(self, stubs=False):
        """Absolute path to parent object
        
        @param stubs: boolean Whether or not to include Stub objects.
        """
        parent = self.parent(stubs=stubs)
        if parent:
            return parent.path_abs()
        return None

    def _parent_parts(self):
        return {
            k:v
            for k,v in [
                x for x in self.parts.items()
            ][:-1]
        }

    def _parent_models(self, stubs=False):
        if stubs:
            return PARENTS_ALL.get(self.model, [])
        return PARENTS.get(self.model, [])
        
    def parent(self, stubs=False):
        """Parent of the Identifier
        
        @param stub: boolean An archival object not just a Stub
        """
        parent_parts = self._parent_parts()
        for model in self._parent_models(stubs):
            idparts = parent_parts
            idparts['model'] = model
            try:
                return Identifier(idparts, base_path=self.basepath)
            except IdentifierFormatException:
                pass
        return None
    
    def lineage(self, stubs=False):
        """Identifier's lineage, starting with the Identifier itself.
        
        @param stubs: boolean Whether or not to include Stub objects.
        """
        i = self
        identifiers = [i]
        while(i.parent(stubs=stubs)):
            i = i.parent(stubs=stubs)
            identifiers.append(i)
        return identifiers
    
    def child_models(self, stubs=False):
        if stubs:
            return CHILDREN_ALL.get(self.model, [])
        return CHILDREN.get(self.model, [])
        
    def child(self, model, idparts, base_path=None):
        """Returns a *new* child Identifier with specified properties.
        
        Does not return an existing child of the Identifier!
        Useful for when you need Identifier info before Object is created.
        
        @param model: str Model of the child object
        @param idparts: dict Additional idpart(s) to add.
        """
        if not model in CHILDREN[self.model]:
            raise Exception('%s can not have child of type %s.' % (self.model, model))
        child_parts = {key:val for key,val in self.parts.iteritems()}
        child_parts['model'] = model
        for key,val in idparts.iteritems():
            if key in ID_COMPONENTS:
                child_parts[key] = val
        return self.__class__(child_parts, base_path=base_path)

    def path_abs(self, append=None):
        """Return absolute path to object with optional file appended.
        
        @param append: str File descriptor. Must be present in ADDITIONAL_PATHS!
        @returns: str
        """
        if not self.basepath:
            raise MissingBasepathException('%s basepath not set.'% self)
        path = format_path(self, self.model, 'abs')
        if append:
            filename = ADDITIONAL_PATHS.get(self.model,None).get(append,None)
            if filename and (self.model in NODES):
                # For files, bits are appended to file ID using string formatter
                dirname,basename = os.path.split(path)
                filename = filename.format(id=self.id)
                path = os.path.join(dirname, filename)
            elif filename:
                # For everything else you just append the file or dirname
                # to the end of the path
                path = os.path.join(path, filename)
            else:
                return None
        return os.path.normpath(path)
    
    def path_rel(self, append=None):
        """Return relative path to object with optional file appended.
        
        Note: paths relative to repository, intended for use in Git commands.
        
        @param append: str Descriptor of file in ADDITIONAL_PATHS!
        @returns: str
        """
        path = format_path(self, self.model, 'rel')
        if append:
            if self.model == 'file':
                filename = ADDITIONAL_PATHS[self.model][append].format(id=self.id)
                path = os.path.dirname(path)
            else:
                filename = ADDITIONAL_PATHS[self.model][append]
            if path:
                path = os.path.join(path, filename)
            else:
                path = filename
        if path:
            path = os.path.normpath(path)
        return path
    
    def urlpath(self, url_type):
        """Return object URL or URI.
        
        @param url_type: str 'public' or 'editor'
        @returns: str
        """
        return format_url(self, self.model, url_type)
