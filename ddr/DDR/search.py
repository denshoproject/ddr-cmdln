from collections import OrderedDict
from copy import deepcopy
import json
import logging
logger = logging.getLogger(__name__)
import os
import urlparse

from elasticsearch_dsl import Index, Search, A, Q, A
from elasticsearch_dsl.query import Match, MultiMatch, QueryString
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.result import Result

from DDR import config
from DDR import docstore
from DDR import identifier
from DDR import vocab

# set default hosts and index
DOCSTORE = docstore.Docstore()

# whitelist of params recognized in URL query
# TODO derive from ddr-defs/repo_models/
SEARCH_PARAM_WHITELIST = [
    'fulltext',
    'model',
    'models',
    'parent',
    'status',
    'public',
    'topics',
    'facility',
    'contributor',
    'creators',
    'format',
    'genre',
    'geography',
    'language',
    'location',
    'mimetype',
    'persons',
    'rights',
]

# fields where the relevant value is nested e.g. topics.id
# TODO derive from ddr-defs/repo_models/
SEARCH_NESTED_FIELDS = [
    'facility',
    'topics',
]

# TODO derive from ddr-defs/repo_models/
SEARCH_AGG_FIELDS = {
    'model': 'model',
    'status': 'status',
    'public': 'public',
    'contributor': 'contributor',
    'creators': 'creators.namepart',
    'facility': 'facility.id',
    'format': 'format',
    'genre': 'genre',
    'geography': 'geography.term',
    'language': 'language',
    'location': 'location',
    'mimetype': 'mimetype',
    'persons': 'persons',
    'rights': 'rights',
    'topics': 'topics.id',
}

# TODO derive from ddr-defs/repo_models/
SEARCH_MODELS = [
    'repository','organization','collection','entity','file'
]

# fields searched by query
# TODO derive from ddr-defs/repo_models/
SEARCH_INCLUDE_FIELDS = [
    'model',
    'status',
    'public',
    'title',
    'description',
    'contributor',
    'creators',
    'facility',
    'format',
    'genre',
    'geography',
    'label',
    'language',
    'location',
    'persons',
    'rights',
    'topics',
]

# TODO derive from ddr-defs/repo_models/
SEARCH_FORM_LABELS = {
    'model': 'Model',
    'status': 'Status',
    'public': 'Public',
    'contributor': 'Contributor',
    'creators.namepart': 'Creators',
    'facility': 'Facility',
    'format': 'Format',
    'genre': 'Genre',
    'geography.term': 'Geography',
    'language': 'Language',
    'location': 'Location',
    'mimetype': 'Mimetype',
    'persons': 'Persons',
    'rights': 'Rights',
    'topics': 'Topics',
}

# TODO derive from ddr-defs/repo_models/
def _vocab_choice_labels(field):
    return {
        str(term['id']): term['title']
        for term in vocab.get_vocabs(config.VOCABS_URL)[field]['terms']
    }
VOCAB_TOPICS_IDS_TITLES = {
    'facility': _vocab_choice_labels('facility'),
    'format': _vocab_choice_labels('format'),
    'genre': _vocab_choice_labels('genre'),
    'language': _vocab_choice_labels('language'),
    'public': _vocab_choice_labels('public'),
    'rights': _vocab_choice_labels('rights'),
    'status': _vocab_choice_labels('status'),
    'topics': _vocab_choice_labels('topics'),
}


def es_offset(pagesize, thispage):
    """Convert Django pagination to Elasticsearch limit/offset
    
    >>> es_offset(pagesize=10, thispage=1)
    0
    >>> es_offset(pagesize=10, thispage=2)
    10
    >>> es_offset(pagesize=10, thispage=3)
    20
    
    @param pagesize: int Number of items per page
    @param thispage: int The current page (1-indexed)
    @returns: int offset
    """
    page = thispage - 1
    if page < 0:
        page = 0
    return pagesize * page

def start_stop(limit, offset):
    """Convert Elasticsearch limit/offset to Python slicing start,stop
    
    >>> start_stop(10, 0)
    0,9
    >>> start_stop(10, 1)
    10,19
    >>> start_stop(10, 2)
    20,29
    """
    start = int(offset)
    stop = (start + int(limit)) - 1
    return start,stop
    
def django_page(limit, offset):
    """Convert Elasticsearch limit/offset pagination to Django page
    
    >>> django_page(limit=10, offset=0)
    1
    >>> django_page(limit=10, offset=10)
    2
    >>> django_page(limit=10, offset=20)
    3
    
    @param limit: int Number of items per page
    @param offset: int Start of current page
    @returns: int page
    """
    return divmod(offset, limit)[0] + 1


class SearchResults(object):
    """Nicely packaged search results for use in API and UI.
    
    >>> from rg import search
    >>> q = {"fulltext":"minidoka"}
    >>> sr = search.run_search(request_data=q, request=None)
    """
    query = {}
    aggregations = None
    objects = []
    total = 0
    limit = config.ELASTICSEARCH_MAX_SIZE
    offset = 0
    start = 0
    stop = 0
    prev_offset = 0
    next_offset = 0
    prev_api = u''
    next_api = u''
    page_size = 0
    this_page = 0
    prev_page = 0
    next_page = 0
    prev_html = u''
    next_html = u''
    errors = []

    def __init__(self, mappings, query={}, count=0, results=None, objects=[], limit=config.ELASTICSEARCH_DEFAULT_LIMIT, offset=0):
        self.mappings = mappings
        self.query = query
        self.limit = int(limit)
        self.offset = int(offset)
        
        if results:
            # objects
            self.objects = [hit for hit in results]
            if results.hits.total:
                self.total = int(results.hits.total)

            # aggregations
            self.aggregations = {}
            if hasattr(results, 'aggregations'):
                results_aggregations = results.aggregations
                for field in results.aggregations.to_dict().keys():
                    
                    # nested aggregations
                    if field == 'topics':
                        buckets = results.aggregations['topics']['topic_ids'].buckets
                    elif field == 'facility':
                        buckets = results.aggregations['facility']['facility_ids'].buckets
                    # simple aggregations
                    else:
                        buckets = results.aggregations[field].buckets

                    if VOCAB_TOPICS_IDS_TITLES.get(field):
                        self.aggregations[field] = []
                        for bucket in buckets:
                            if bucket['key'] and bucket['doc_count']:
                                self.aggregations[field].append({
                                    'key': bucket['key'],
                                    'label': VOCAB_TOPICS_IDS_TITLES[field].get(str(bucket['key'])),
                                    'doc_count': str(bucket['doc_count']),
                                })
                                # print topics/facility errors in search results
                                # TODO hard-coded
                                if (field in ['topics', 'facility']) \
                                and not (
                                    isinstance(bucket['key'], int) \
                                    or bucket['key'].isdigit()
                                ):
                                    self.errors.append(bucket)

                    else:
                        self.aggregations[field] = [
                            {
                                'key': bucket['key'],
                                'label': bucket['key'],
                                'doc_count': str(bucket['doc_count']),
                            }
                            for bucket in buckets
                            if bucket['key'] and bucket['doc_count']
                        ]

        elif objects:
            # objects
            self.objects = objects
            self.total = len(objects)

        else:
            self.total = count

        # elasticsearch
        self.prev_offset = self.offset - self.limit
        self.next_offset = self.offset + self.limit
        if self.prev_offset < 0:
            self.prev_offset = None
        if self.next_offset >= self.total:
            self.next_offset = None

        # django
        self.page_size = self.limit
        self.this_page = django_page(self.limit, self.offset)
        self.prev_page = u''
        self.next_page = u''
        # django pagination
        self.page_start = (self.this_page - 1) * self.page_size
        self.page_next = self.this_page * self.page_size
        self.pad_before = range(0, self.page_start)
        self.pad_after = range(self.page_next, self.total)
    
    def __repr__(self):
        return u"<SearchResults '%s' [%s]>" % (
            self.query, self.total
        )
    
    def to_dict(self, list_function):
        """Express search results in API and Redis-friendly structure
        returns: dict
        """
        return self._dict({}, list_function)
    
    def ordered_dict(self, list_function, pad=False):
        """Express search results in API and Redis-friendly structure
        returns: OrderedDict
        """
        return self._dict(OrderedDict(), list_function, pad=pad)
    
    def _dict(self, data, list_function, pad=False):
        data['total'] = self.total
        data['limit'] = self.limit
        data['offset'] = self.offset
        data['prev_offset'] = self.prev_offset
        data['next_offset'] = self.next_offset
        data['page_size'] = self.page_size
        data['this_page'] = self.this_page

        data['objects'] = []
        
        # pad before
        if pad:
            data['objects'] += [
                {'n':n} for n in range(0, self.page_start)
            ]
        
        # page
        for o in self.objects:
            data['objects'].append(
                list_function(
                    identifier.Identifier(
                        o['id'], base_path=config.MEDIA_BASE
                    ),
                    o.to_dict(),
                    is_detail=False,
                )
            )
        
        # pad after
        if pad:
            data['objects'] += [
                {'n':n} for n in range(self.page_next, self.total)
            ]
        
        data['query'] = self.query
        data['aggregations'] = self.aggregations
        return data


def format_object(oi, d, is_detail=False):
    """Format detail or list objects for command-line
    
    Certain fields are always included (id, title, etc and links).
    Everything else is determined by what fields are in the result dict.
    
    d is basically an elasticsearch_dsl.Result, packaged by
    search.SearchResults.
    
    @param oi: Identifier
    @param d: dict
    @param is_detail: boolean
    """
    try:
        collection_id = oi.collection_id()
    except:
        collection_id = None
    
    data = OrderedDict()
    data['id'] = d.pop('id')
    data['model'] = oi.model
    data['collection_id'] = collection_id
    data['links'] = make_links(
        oi, d, source='es', is_detail=is_detail
    )
    DETAIL_EXCLUDE = []
    for key,val in d.items():
        if key not in DETAIL_EXCLUDE:
            data[key] = val
    return data

def make_links(oi, d, source='fs', is_detail=False):
    """Make the 'links pod' at the top of detail or list objects.
    
    @param oi: Identifier
    @param d: dict
    @param request: None
    @param source: str 'fs' (filesystem) or 'es' (elasticsearch)
    @param is_detail: boolean
    @returns: dict
    """
    assert source in ['fs', 'es']
    try:
        collection_id = oi.collection_id()
        child_models = oi.child_models(stubs=False)
    except:
        collection_id = None
        child_models = oi.child_models(stubs=True)
    
    img_url = ''
    if d.get('signature_id'):
        img_url = identifier.Identifier(d['signature_id'])
    elif d.get('access_rel'):
        img_url = oi
    elif oi.model in ['repository','organization']:
        img_url = '%s%s/%s' % (
            settings.MEDIA_URL,
            oi.path_abs().replace(settings.MEDIA_ROOT, ''),
            'logo.png'
        )
    
    links = OrderedDict()
    
    if is_detail:
        # objects above the collection level are stubs and do not have collection_id
        # collections have collection_id but have to point up to parent stub
        # API does not include stubs inside collections (roles)
        if collection_id and (collection_id != oi.id):
            parent_id = oi.parent_id(stubs=0)
        else:
            parent_id = oi.parent_id(stubs=1)
        if parent_id:
            links['parent'] = parent_id

    links['img'] = img_url
    
    return links


class Searcher(object):
    """
    >>> s = Searcher(index, mappings=DOCTYPE_CLASS, fields=SEARCH_LIST_FIELDS)
    >>> s.prep(request_data)
    'ok'
    >>> r = s.execute()
    'ok'
    >>> d = r.to_dict(request)
    """
    index = DOCSTORE.indexname
    search_results_class = SearchResults
    mappings = {}
    fields = []
    q = OrderedDict()
    query = {}
    sort_cleaned = None
    s = None
    
    def __init__(self, mappings, fields, search=None):
        self.mappings = mappings
        self.fields = fields
        self.s = search

    def prepare(self, fulltext='', models=SEARCH_MODELS, parent='', filters={}):
        """assemble elasticsearch_dsl.Search object
        
        @param fulltext: str
        @param models: list of str
        @param parent: str
        @param filters: dict
        @returns: elasticsearch_dsl.Search
        """
        s = Search(
            using=DOCSTORE.es,
            index=DOCSTORE.indexname,
            doc_type=models,
        ).source(include=identifier.ELASTICSEARCH_LIST_FIELDS)
        
        # fulltext query
        if fulltext:
            # MultiMatch chokes on lists
            if isinstance(fulltext, list) and (len(fulltext) == 1):
                fulltext = fulltext[0]
            # fulltext search
            s = s.query(
                QueryString(
                    query=fulltext,
                    fields=SEARCH_INCLUDE_FIELDS,
                    analyze_wildcard=False,
                    allow_leading_wildcard=False,
                    default_operator='AND',
                )
            )

        if parent:
            parent = '%s*' % parent
            s = s.query("wildcard", id=parent)
        
        # filters
        for key,val in filters.items():
            
            if key in SEARCH_NESTED_FIELDS:
    
                # search for *ALL* the topics (AND)
                for term_id in val:
                    s = s.filter(
                        Q('bool',
                          must=[
                              Q('nested',
                                path=key,
                                query=Q(
                                    'term',
                                    **{'%s.id' % key: term_id}
                                )
                              )
                          ]
                        )
                    )
                
                ## search for *ANY* of the topics (OR)
                #s = s.query(
                #    Q('bool',
                #      must=[
                #          Q('nested',
                #            path=key,
                #            query=Q('terms', **{'%s.id' % key: val})
                #          )
                #      ]
                #    )
                #)
    
            elif key in SEARCH_PARAM_WHITELIST:
                s = s.filter('terms', **{key: val})
        
        # aggregations
        for fieldname,field in SEARCH_AGG_FIELDS.items():
            
            # nested aggregation (Elastic docs: https://goo.gl/xM8fPr)
            if fieldname == 'topics':
                s.aggs.bucket(
                    'topics', 'nested', path='topics'
                ).bucket(
                    'topic_ids', 'terms', field='topics.id', size=1000
                )
            elif fieldname == 'facility':
                s.aggs.bucket(
                    'facility', 'nested', path='facility'
                ).bucket(
                    'facility_ids', 'terms', field='facility.id',
                    size=1000
                )
                # result:
                # results.aggregations['topics']['topic_ids']['buckets']
                #   {u'key': u'69', u'doc_count': 9}
                #   {u'key': u'68', u'doc_count': 2}
                #   {u'key': u'62', u'doc_count': 1}
            
            # simple aggregations
            else:
                s.aggs.bucket(fieldname, 'terms', field=field)
        
        self.s = s
    
    def execute(self, limit, offset):
        """Execute a query and return SearchResults
        
        @param limit: int
        @param offset: int
        @returns: SearchResults
        """
        if not self.s:
            raise Exception('Searcher has no ES Search object.')
        start,stop = start_stop(limit, offset)
        response = self.s[start:stop].execute()
        return self.search_results_class(
            mappings=self.mappings,
            query=self.s.to_dict(),
            results=response,
            limit=limit,
            offset=offset,
        )
