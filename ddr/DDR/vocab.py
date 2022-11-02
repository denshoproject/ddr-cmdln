"""
DDR.vocab.py -- controlled vocabulary tools

Manages vocabulary used for the Topic field in ddr-local and ddr-public.
The data file lives in the main Repository repo for the installation,
which should be present on each Store.::

    /var/www/media/base/ddr/facets/topics.json
    /media/DRIVELABEL/ddr/ddr/facets/topics.json

ddr-local looks for topics.json at "local:vocab_terms_url", specified
in `/etc/ddr/ddr.cfg`.::

    http://partner.densho.org/vocab/api/0.2/topics.json

ddrindex is used to upload topics.json to Elasticsearch for use by
ddr-public.

Once in Elasticsearch it is available at this URL.::

    http://HOST:PORT/INDEX/facet/topics/

IMPORTANT: Only tested with small vocabularies (<200 terms).
- Use pointers to Term.ancestors/siblings/children instead of objects.
- Use a real tree structure.

Working directly with the objects::

    >>> from DDR.vocab import Index, Term
    >>> index = Index()
    >>> index.add( Term( id=1, parent_id=0, title='music') )
    >>> index.add( Term( id=2, parent_id=1, title='classical') )
    >>> index.add( Term( id=3, parent_id=1, title='jazz') )
    >>> index.add( Term( id=4, parent_id=1, title='electronic') )
    >>> index.add( Term( id=5, parent_id=2, title='romantic') )
    >>> index.add( Term( id=6, parent_id=2, title='modern') )
    >>> index.add( Term( id=7, parent_id=3, title='traditional') )
    >>> index.add( Term( id=8, parent_id=3, title='fusion') )
    >>> index.add( Term( id=9, parent_id=4, title='dance') )
    >>> index.add( Term(id=10, parent_id=4, title='experimental') )
    
    >>> music = index.get(id=1)
    >>> index._parent(music)
    >>> index._siblings(music)
    >>> index._children(music)
    [<Term 2: classical>, <Term 3: jazz>, <Term 4: electronic>]
    
    >>> electronic = index.get(title='electronic')
    >>> index._parent(electronic)
    <Term 1: music>
    >>> index._siblings(electronic)
    [<Term 2: classical>, <Term 3: jazz>]
    >>> index._children(electronic)
    [<Term 9: dance>, <Term 10: experimental>]
    
    >>> experimental = index.get(title='experimental')
    >>> index._parent(experimental)
    <Term 4: electronic>
    >>> index._siblings(experimental)
    [<Term 9: dance>]
    >>> index._children(experimental)
    []

Sample edit workflow.  Topics are exported to CSV, edited in Google Docs,
and reimported from CSV.::

    # Load JSON file and export to CSV.
    $ ./manage.py shell
    >>> from DDR import vocab
    >>> index = vocab.Index()
    >>> index.read('/opt/densho-vocab/api/0.2/topics.json')
    >>> index.write('/tmp/topics-exported.csv')
    
    # Import to and export from Google Docs using the default settings.
    
    # Re-import from CSV and update JSON file.
    $ ./manage.py shell
    >>> from DDR import vocab
    >>> index = vocab.Index()
    >>> index.read('/tmp/topics-finished.csv')
    >>> index.write('/opt/densho-vocab/api/0.2/topics.json')

::
    
    # Generate Django form menu from index.
    >>> index.menu_choices()
    
    # Generate Django form menu with topic "paths".
    >>> index.path_choices()
"""

from collections import defaultdict, OrderedDict
from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)
import multiprocessing
import os
import io
import sys
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union
import urllib.parse

from dateutil import parser
import requests

from DDR import config
from DDR import converters
from DDR import format_json
from DDR import fileio

CSV_HEADERS = [
    'id',
    '_title',
    'title',
    'parent_id',
    'weight',
    'created',
    'modified',
    'encyc_urls',
    'description',
]

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


class Index( object ):
    id = ''
    title = ''
    description = ''
    ids: List[Any] = []
    _terms_by_id: Dict[str,str] = {}
    _titles_to_ids: Dict[str,str] = {}
    _parents_to_children: Dict[str,str] = {}
    
    # TODO type hinting
    def add( self, term ):
        """Adds a Term to the Index.
        """
        if not term.id in self.ids:
            self.ids.append(term.id)
        if not term in self._terms_by_id:
            self._terms_by_id[term.id] = term
        # enables retrieve by title
        if not term.id in self._titles_to_ids:
            self._titles_to_ids[term.title] = term.id
        # lists of children for each parent
        if not self._parents_to_children.get(term.parent_id):
            self._parents_to_children[term.parent_id] = []
        if (term.id not in self._parents_to_children[term.parent_id]):
            self._parents_to_children[term.parent_id].append(term.id)
    
    def terms(self) -> List[Optional[Union[str, object]]]:
        return [self._terms_by_id.get(tid, None) for tid in self.ids]
    
    def get(self, id: str=None, title: str=None) -> Optional[Union[str, object]]:
        """Returns Term matching the id or title, or None.
        """
        if title and not id:
            id = self._titles_to_ids.get(title, None)
        if id:
            return self._terms_by_id.get(id, None)
        raise Exception('id or title is required')
    
    def _get_by_term_ids(self, term_ids: List[str]) -> List[str]:
        terms = []
        for id in term_ids:
            term = self._terms_by_id.get(id, None)
            if term:
                terms.append(term)
        return terms
    
    # TODO type hinting
    def _parent(self, term):
        """Term for term.parent_id or None.
        """
        return self._terms_by_id.get(term.parent_id, None) 
    
    # TODO type hinting
    def _children(self, term):
        """List of terms that have term.id as their parent_id.
        """
        term_ids = self._parents_to_children.get(term.id, [])
        term_ids.sort()
        return self._get_by_term_ids(term_ids)
   
    # TODO type hinting
    def _siblings( self, term ):
        """List of other terms with same parent_id.
        """
        parent = self._parent(term)
        if parent:
            return [t for t in self._children(parent) if t != term]
        return []
    
    # TODO type hinting
    def _ancestors( self, term ):
        """List of term IDs leading from term to root.
        @param term
        @returns: list of term IDs, from root to leaf.
        """
        term.path = ''
        ancestors = []
        t = term
        while t.parent_id:
            t = self._parent(t)
            ancestors.append(t.id)
        ancestors.reverse()
        return ancestors
    
    # TODO type hinting
    def _path(self, term):
        path = [term.title]
        t = term
        while t.parent_id:
            t = self._parent(t)
            path.append(t.title)
        path.reverse()
        return config.VOCABS_PRECOORD_PATH_SEP.join(path)
    
    # TODO type hinting
    def _format(self, term):
        """Generates thesaurus-style text output for each Term.
        """
        bt = self._parent(term)
        nt = self._children(term)
        rt = self._siblings(term)
        lines = []
        lines.append(term.title)
        if term.description: lines.append('  %s' % term.description)
        if bt: lines.append('BT %s' % bt.title)
        if nt: lines.append('NT %s' % ', '.join([t.title for t in nt]))
        if rt: lines.append('RT %s' % ', '.join([t.title for t in rt]))
        return '\n'.join(lines)
    
    # TODO type hinting
    def _build( self, terms ):
        """Adds list of raw Term objects to Index, adds parent/siblings/children to each Term.
        """
        for term in terms:
            self.add(term)
        for term in self.terms():
            #term.parent = self._parent(term)
            term.siblings = self._siblings(term)
            term.children = self._children(term)
            term.ancestors = self._ancestors(term)
            term.path = self._path(term)
            #term.format = self._format(term)
    
    def read(self, path: str):
        """Read from the specified file (.json or .csv).
        
        @param path: Absolute path to file; must be .json or .csv.
        @returns: Index object with terms
        """
        extension = os.path.splitext(path)[1]
        if not extension in ['.json', '.csv']:
            raise Exception('Index.read only reads .json and .csv files.')
        if extension.lower() == '.json':
            self.load_json(fileio.read_text(path))
        elif extension.lower() == '.csv':
            self.load_csv(fileio.read_text(path))
    
    def write(self, path: str):
        """Write to the specified file (.json or .csv).
        
        @param path: Absolute path to file; must be .json or .csv.
        """
        extension = os.path.splitext(path)[1]
        if not extension in ['.json', '.csv']:
            raise Exception('Index.read only writes .json and .csv files.')
        if extension.lower() == '.json':
            fileio.write_text(self.dump_json(), path)
        elif extension.lower() == '.csv':
            fileio.write_text(self.dump_csv(), path)
            
    def load_json(self, text: str):
        """Load terms from a JSON file.
        
        Sample JSON format (fields will not be in same order):
            {
                "id": "topics",
                "title": "Topics",
                "description": "DDR Topics",
                "terms": [
                    {
                        "id": 120,
                        "parent_id": 0,
                        "ancestors": [],
                        "siblings": [],
                        "children": [
                            233,
                            234,
                            235
                        ],
                        "weight": 0,
                        "path": "Activism and involvement",
                        "encyc_urls": [],
                        "created": "1969-12-31T00:00:00-0800" [or null],
                        "modified": "1969-12-31T00:00:00-0800" [or null],
                        "title": "Activism and involvement",
                        _"title": "Activism and involvement [120]",
                        "description": "",
                        "format": "Activism and involvement NT Civil liberties, Civil rights, Politics"
                    },
                    ...
                ]
            }
        
        NOTE: When reading, the following fields are are generated using parent_id
        and are ignored: "ancestors", "children", "siblings", "format"
        
        @param text: JSON-formatted text
        @returns: Index object with terms
        """
        data = json.loads(text)
        self.id = data['id']
        self.title = data['title']
        self.description = data['description']
        terms = []
        for t in data['terms']:
            term = Term.from_dict(t)
            terms.append(term)
        self._build(terms)

    def _parse_csv_urls(self, text: str) -> List[str]:
        """Parses URLs, removes domain, returns list of URIs
        @param text: str
        @returns: list
        """
        urls = []
        for part in text.split(';'):
            if part.strip():
                urls.append(part.strip())
        uris = []
        for url in urls:
            if url and urllib.parse.urlparse(url).path:
                uris.append(urllib.parse.urlparse(url).path)
        return uris
    
    def load_csv(self, text: str):
        """Load terms from a CSV file.
        
            id, topics
            title, Topics
            description,"DDR Topics"
            id,_title,title,parent_id,weight,encyc_urls,description,created,modified
            120,"Activism and involvement [120]","Activism and involvement",0,0,"","","1969-12-31T00:00:00-0800","1969-12-31T00:00:00-0800"

        @param text: str Raw contents of CSV file
        @returns: Index object with terms
        """
        pseudofile = io.StringIO(text)
        reader = fileio.csv_reader(pseudofile)
        terms = []
        for n,row in enumerate(reader):
            if (n == 0): self.id = row[1].strip()
            elif (n == 1): self.title = row[1].strip()
            elif (n == 2): self.description = row[1].strip()
            elif (n == 3):
                if (row != CSV_HEADERS):
                    print('Expected these headers:')
                    print('    %s' % CSV_HEADERS)
                    print('Got these:')
                    print('    %s' % row)
                    raise Exception("Sorry not smart enough to rearrange the headers myself... (;-_-)")
            else:
                # convert row to dict
                t = {}
                for c,col in enumerate(CSV_HEADERS):
                    t[col] = row[c].strip()
                    # special processing for certain columns
                    if col == 'encyc_urls':
                        t[col] = self._parse_csv_urls(t[col])
                term = Term.from_dict(t)
                terms.append(term)
        self._build(terms)
    
    # TODO type hinting
    def dump_json( self ):
        """JSON format of the entire index.
        
        Terms list plus a keyword, title, and description.
        This is the same format used for Elasticsearch facets.
        
        @param id
        @param title
        @param description
        """
        data = {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'terms': [term._flatten_json() for term in self.terms()],
        }
        return format_json(data)
    
    # TODO type hinting
    def dump_csv(self):
        """Write terms to a CSV file.
        
        @returns: CSV formatted text
        """
        output = io.StringIO()
        writer = fileio.csv_writer(output)
        # metadata
        writer.writerow(['id', self.id])
        writer.writerow(['title', self.title])
        writer.writerow(['description', self.description])
        # headers
        writer.writerow(CSV_HEADERS)
        # terms
        for term in self.terms():
            writer.writerow(term.csv())
        return output.getvalue()
    
    # TODO type hinting
    def dump_graphviz( self, term_len=50 ):
        """Dumps contents of index to a Graphviz file.
        
        digraph terms {
          rankdir=RL;
          2-classical -> 1-music;
          3-jazz -> 1-music;
        }
        
        Render thusly:
        $ dot -Tpng -o /tmp/terms.png /tmp/terms.gv
        """
        lines = [
            'digraph G {',
            '  rankdir=RL;'
        ]
        for tid,term in self._terms_by_id.items():
            parent = self._parent(term)
            if parent:
                src = '"%s-%s"' % (term.id, term.title[:term_len].replace('"',''))
                dest = '"%s-%s"' % (parent.id, parent.title[:term_len].replace('"',''))
                line = '  %s -> %s;' % (src, dest)
                lines.append(line)
        lines.append('}')
        return '\n'.join(lines)
    
    # TODO type hinting
    def dump_text(self):
        """Text format of the entire index.
        """
        terms = [self._format(term) for id,term in self._terms_by_id.items()]
        return '\n\n'.join(terms)
        
    # TODO type hinting
    def menu_choices(self):
        """List of (id,title) tuples suitable for use in Django multiselect menu.
        """
        return [(term.id,term.title) for term in self.terms()]

    # TODO type hinting
    def path_choices(self):
        """List of (id,title) tuples suitable for use in Django multiselect menu.
        """
        return [('%s [%s]' % (term.path, term.id)) for term in self.terms()]


class Term( object ):
    id = None
    parent_id = 0
    siblings: List[Any] = []
    children: List[Any] = []
    created = None   # Date fields must contain dates or "null",
    modified = None  # or Elasticsearch will throw a parsing error.
    title = ''
    _title = ''
    description = ''
    weight = 0
    encyc_urls: List[str] = []
    path = ''

    def __init__(self, id=None, parent_id=0, created=None, modified=None, title='', _title='', description='', weight=0, encyc_urls=[]):
        self.id = id
        self.parent_id = parent_id
        self.created = created
        self.modified = modified
        self.title = title
        self._title = _title
        self.description = description
        self.weight = weight
        self.encyc_urls = encyc_urls
    
    def __repr__(self) -> str:
        return "<Term %s: %s>" % (self.id, self.title)
    
    # TODO type hinting
    @staticmethod
    def from_dict(t):
        """
        @param t: dict
        @returns: Term object
        """
        def getstr(t: Dict[str,str],
                   fieldname: str,
                   default: Optional[str]) -> Optional[str]:
            if t.get(fieldname):
                return t[fieldname].strip()
            return default
        
        term = Term()
        term.id = int(t['id'])
        term.parent_id = int(t['parent_id'])
        term.created = parser.parse(getstr(t, 'created', ''))
        term.modified = parser.parse(getstr(t, 'modified', ''))
        term._title = getstr(t, '_title', '')
        term.title = getstr(t, 'title', '')
        term.description = getstr(t, 'description', '')
        # parse list from string in CSV
        encyc_urls = t.get('encyc_urls', [])
        if encyc_urls:
            if isinstance(encyc_urls, str):
                term.encyc_urls = encyc_urls.strip().split(',')
            elif isinstance(encyc_urls, list):
                term.encyc_urls = encyc_urls
        if t.get('weight',None):
            term.weight = int(t['weight'])
        else:
            term.weight = 0
        return term

    def _flatten_json(self) -> Dict[str,str]:
        """Converts Term into a dict suitable for writing to JSON.
        """
        data = {}
        for key,val in self.__dict__.items():
            if (key in ['parent_id']) and val:
                val = getattr(self, key)
            elif (key in ['siblings', 'children']) and val:
                val = []
                for t in getattr(self, key):
                    val.append(t.id)
            elif (key in ['created', 'modified']):
                val_before = val
                if isinstance(val, datetime):
                    val = converters.datetime_to_text(val)
                elif isinstance(val, str):
                    pass
                else:
                    val = None
            data[key] = val
        return data
    
    def csv(self) -> List[List[str]]:
        """Converts Term into a list suitable for writing to CSV.
        """
        data = []
        for key in CSV_HEADERS:
            val = getattr(self, key)
            if (key in ['created', 'modified']):
                if val:
                    val = converters.text_to_datetime(val)
                else:
                    val = ''
            elif (key == 'encyc_urls'):
                if val:
                    val = ','.join(val)
                else:
                    val = ''
            data.append(val)
        return data


# Functions for getting single or multiple vocabs from URL or filesystem

def _get_vocab_fs(path: str) -> Dict[str,str]:
    """Loads vocabulary data from filesystem.
    
    @param path: str Absolute path to vocabulary file (.json)
    """
    logging.info('getting vocab: %s' % path)
    return json.loads(fileio.read_text(path))

def _get_vocab_http(url: str) -> Dict[str,str]:
    """Loads vocabulary data from vocab API.
    
    @param path: str URL of vocabulary file (.json)
    """
    logging.info('getting vocab: %s' % url)
    r = requests.get(url, timeout=config.REQUESTS_TIMEOUT)
    if r.status_code != 200:
        raise Exception(
            'vocabulary file missing: %s' % (url))
    return json.loads(r.text)

def _get_vocabs_all_fs(base, exclude=['index','narrators']):
    """Get JSON vocab files, excluding index and narrators.
    @param base: str Absolute path to vocabs
    @param exclude: list
    @returns: dict
    """
    data = {}
    for vocab in os.listdir(base):
        is_json = '.json' in vocab
        excluded = False
        for e in exclude:
            if e in vocab:
                excluded = True
        if is_json and not excluded:
            key = vocab.replace('.json','')
            val = _get_vocab_fs(os.path.join(base, vocab))
            data[key] = val
    return data

def _get_vocabs_all_http(base_url, exclude=['index','narrators']):
    """Get JSON vocab files, excluding index and narrators.
    @param base: str Absolute path to vocabs
    @param exclude: list
    @returns: dict
    """
    url = os.path.join(base_url, 'index.json')
    r = requests.get(url, timeout=config.REQUESTS_TIMEOUT)
    if r.status_code != 200:
        raise Exception(
            'Cannot load vocabulary index: %s' % (url))
    vocabs = json.loads(r.text)
    
    # get each vocab
    def worker(vocab, url, return_dict):
        return_dict[vocab] = _get_vocab_http(url)
    return_dict = multiprocessing.Manager().dict()
    jobs = []
    for vocab,filename in vocabs.items():
        if vocab not in exclude:
            url = os.path.join(base_url, filename)
            p = multiprocessing.Process(
                target=worker,
                args=(vocab, url, return_dict)
            )
            jobs.append(p)
            p.start()
    for proc in jobs:
        proc.join()
    
    # turn return_dict into a regular dict
    return {key:val for key,val in list(return_dict.items())}

# TODO type hinting
def get_vocabs(vocabs_url: str):
    """Loads data for multiple vocabularies from URL or from filesystem.
    
    Works with JSON files generated by DDR.vocab.Index.dump_terms_json().
    
    @param vocabs_url: str URL or filesystem path
    @param vocab: str
    """
    cached = config.VOCABS
    if not cached:
        if ('https://' in vocabs_url) or ('http://' in vocabs_url):
            cached = _get_vocabs_all_http(vocabs_url)
        else:
            cached = _get_vocabs_all_fs(vocabs_url)
        config.VOCABS = cached
    return cached


# this is a pointer to the module object instance itself.
THIS_MODULE = sys.modules[__name__]
# global var so we don't have to retrieve topics for every entity
TOPICS: Dict[str,str]={}

# TODO type hinting
def repair_topicdata(data, facet):
    """Repair damaged topics data
    # see https://github.com/densho/ddr-cmdln/issues/43
    
    Many topics were munged such that part of the term (label)
    was transferred from the term field to the id field
    
    >>> vocab.repair_topicdata([
        {'id': u'38', 'term': u'racial relations',},
        {"id": "277", "term": "i"},
        ])
    [
        {'term': u'Race and racism: Cross-racial relations', 'id': u'38'}
        {'term': u"Geographic communities: Hawai'i", 'id': '277'}
    ]
    
    @param data: list of dicts
    @param facet: dict Output of get_vocabs()[FIELDNAME]
    @return: list of dicts
    """
    topics = facet
    # get topics so we can repair topic term (path) field
    # keep it so we only retrieve it once
    if not THIS_MODULE.TOPICS:
        THIS_MODULE.TOPICS = {
            str(term['id']): term['path']
            for term in topics['terms']
        }
    for item in data:
        if isinstance(item, dict) and item.get('id'):
            # 'id' field is supposed to be an integer in a str
            if (not item['id'].isdigit()) and (':' in item['id']):
                tid = item['id'].split(':')[-1]
                if tid.isdigit():
                    item['id'] = tid
                    item['term'] = THIS_MODULE.TOPICS[tid]
            # refresh term even if it's not 'bad'
            item['term'] = THIS_MODULE.TOPICS[item['id']]
    return data

# TODO type hinting
def TEMP_scrub_topicdata(data):
    """Pretty much the same as repair_topicdata, from repo_models
    """
    # TEMPORARY function for fixing bad data
    # see https://github.com/densho/ddr-cmdln/issues/43
    if not THIS_MODULE.TOPICS:
        # get topics so we can repair topic term (path) field
        logging.debug('getting topics')
        THIS_MODULE.TOPICS = {
            str(term['id']): term['path']
            for term in get_vocabs(config.VOCABS_URL)['topics']['terms']
        }
        logging.debug('ok')
    for item in data:
        # 'id' field is supposed to be an integer in a str
        if (not item['id'].isdigit()) and (':' in item['id']):
            logging.debug('Fixing topic ID')
            logging.debug('BEFORE %s' % item)
            tid = item['id'].split(':')[-1]
            if tid.isdigit():
                item['id'] = tid
                item['term'] = THIS_MODULE.TOPICS[tid]
            logging.debug('    -> %s' % item)
        if THIS_MODULE.TOPICS.get(item['id']):
            item['term'] = THIS_MODULE.TOPICS[item['id']]
    return data


# TODO type hinting
def topics_choices(facet, FacetTermClass):
    """List of topicID,path used in ddrpublic search forms topics fields.
    
    @param facet: dict Output of get_vocabs()[FIELDNAME]
    @param FacetTermClass: class DDR.identifier.ELASTICSEARCH_CLASSES_BY_MODEL['facetterm']
    @returns: list [(term.id, term.path), ...]
    """
    fid = facet['id']
    terms = []
    for t in facet['terms']:
        tid = t.get('id')
        facetterm_id = '-'.join([
            str(fid),
            str(tid),
        ])
        term = FacetTermClass()
        term.meta.id = facetterm_id
        term.facet = fid
        term.term_id = t.get('id')
        for field in list(FacetTermClass._doc_type.mapping.to_dict()['properties'].keys()):
            if t.get(field):
                setattr(term, field, t[field])
        term.id = facetterm_id  # overwrite id from original
        terms.append(term)
    return [
        (term.id, term.path)
        for term in make_tree(terms)
    ]

# TODO type hinting
def make_tree(terms_list):
    """Rearranges terms list into hierarchical list.
    
    Uses term['ancestors'] to generate a tree structure
    then "prints out" the tree to a list with indent (depth) indicators.
    More specifically, it adds term['depth'] attribs and reorders
    terms so they appear in the correct places in the hierarchy.
    source: https://gist.github.com/hrldcpr/2012250
    
    @param terms_list: list
    @returns: list
    """
    
    def tree() -> defaultdict:
        """Define a tree data structure
        """
        return defaultdict(tree)
    
    def add(tree_: defaultdict, path: List[str]):
        """
        @param tree_: defaultdict
        @param path: list of ancestor term IDs
        """
        for node in path:
            tree_ = tree_[node]
    
    # TODO type hinting
    def populate(terms_list):
        """Create and populate tree structure
        by iterating through list of terms and referencing ancestor/path keys
        
        @param terms_list: list of dicts
        @returns: defaultdict
        """
        tree_ = tree()
        for term in terms_list:
            path = []
            if hasattr(term, 'ancestors') and term.ancestors:
                for tid in term.ancestors:
                    path.append(tid)
            path.append(term['term_id'])
            add(tree_, path)
        return tree_
    
    def flatten(tree_: defaultdict, depth: int=0):
        """Takes tree dict and returns list of terms with depth values
        
        Variation on ptr() from the gist
        Recursively gets term objects from terms_dict, adds depth,
        and appends to list of terms.
        
        @param tree_: defaultdict Tree
        @param depth: int Depth of indents
        """
        for key in sorted(tree_.keys()):
            term = terms_dict[key]
            term['depth'] = depth
            terms.append(term)
            depth += 1
            flatten(tree_[key], depth)
            depth -= 1
    
    terms_dict = {t['term_id']: t for t in terms_list}
    terms_tree = populate(terms_list)
    terms: List[str] = []
    flatten(terms_tree)
    return terms

def form_vocab_choices(facet: dict, fieldname: str) -> List[Tuple[str,str]]:
    """List of keyword,label used in ddrpublic search multichoice fields.
    
    @param facet: dict Output of get_vocabs()[FIELDNAME]
    @param fieldname: str
    @returns: list [(term.id, term.path), ...]
    """
    terms = sorted(facet['terms'], key=lambda term: term['title'])
    return [
        (
            '-'.join([
                fieldname, term['id'],
            ]),
            term['title']
        )
        for term in terms
    ]
