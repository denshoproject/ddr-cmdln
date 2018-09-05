from datetime import datetime
import json
import os

from nose.tools import assert_raises
from nose.plugins.attrib import attr

from DDR import config
from DDR import docstore
from DDR import identifier

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'docstore')
#if not os.path.exists(TESTING_BASE_DIR):
#    os.makedirs(TESTING_BASE_DIR)

"""
NOTE: You can disable tests requiring Elasticseach server:

    $ nosetests -a '!elasticsearch'

"""


HOSTS = [{'host':'127.0.0.1', 'port':9200}]


#@attr('elasticsearch')
#def test_get_connection():
#    d = docstore.Docstore(hosts=HOSTS, index=None)
#    assert d.es
#    assert d.es.cat.client.ping() == True

def test_make_index_name():
    assert docstore.make_index_name('abc-def_ghi.jkl/mno\\pqr stu') == 'abc-def_ghi.jkl-mno-pqrstu'
    assert docstore.make_index_name('qnfs/kinkura/gold') == 'qnfs-kinkura-gold'

## index_exists
## index_names
## create_index
## delete_index
#@attr('elasticsearch')
#def test_index():
#    index = 'test%s' % datetime.now(config.TZ).strftime('%Y%m%d%H%M%S')
#    d = docstore.Docstore(HOSTS, index)
#    exists_initial = d.index_exists(index)
#    created = d.create_index()
#    names = d.index_names()
#    exists_created = d.index_exists(index)
#    deleted = d.delete_index(index)
#    exists_deleted = d.index_exists(index)
#    assert exists_initial == False
#    assert created == {u'acknowledged': True}
#    assert index in names
#    assert exists_created == True
#    assert deleted == {u'acknowledged': True}
#    assert exists_deleted == False

# set_alias
# target_index
def test_parse_cataliases():
    cataliases = u'ddrworkstation documents0 \nwd5000bmv-2 documents0 \n'
    expected = [('ddrworkstation','documents0'), ('wd5000bmv-2','documents0')]
    assert docstore._parse_cataliases(cataliases) == expected

# put_mappings

# post_facets
# list_facets
# facet_terms

def test_filter_payload():
    data = [{'id': 'ddr-testing-123-1'}, {'title': 'Title'}, {'secret': 'this is a secret'}]
    public_fields = ['id', 'title']
    docstore._filter_payload(data, public_fields)
    assert data == [{'id': 'ddr-testing-123-1'}, {'title': 'Title'}]

def test_clean_controlled_vocab():
    data0 = ['Facility [123]']
    data1 = 'Facility [123]'
    data2 = ['123']
    data3 = [123]
    data4 = '123'
    data5 = 123
    expected = ['123']
    assert docstore._clean_controlled_vocab(data0) == expected
    assert docstore._clean_controlled_vocab(data1) == expected
    assert docstore._clean_controlled_vocab(data2) == expected
    assert docstore._clean_controlled_vocab(data3) == expected
    assert docstore._clean_controlled_vocab(data4) == expected
    assert docstore._clean_controlled_vocab(data5) == expected

def test_clean_dict():
    d = {'a': 'abc', 'b': 'bcd', 'x':'' }
    docstore._clean_dict(d)
    assert d == {'a': 'abc', 'b': 'bcd'}

def test_clean_payload():
    data = [
        {'what': 'app version, commit data, etc'},
        {'id': 'ddr-testing-141'},
        {'record_created': '2013-09-13T14:49:43'},
        {'creators': [u'Mitsuoka, Norio: photographer']},
        {'facility': 'Tule Lake [10]'},
        {'parent': {'href':'', 'uuid':'', 'label':'ddr-testing-123'}},
        {'topics': ['Topics [123]']},
        {'none': None},
        {'empty': ''},
    ]
    docstore._clean_payload(data)
    expected = [
        {'what': 'app version, commit data, etc'},
        {'id': 'ddr-testing-141'},
        {'record_created': '2013-09-13T14:49:43'},
        {'creators': [u'Mitsuoka, Norio: photographer']},
        {'facility': 'Tule Lake [10]'},
        {'parent': {'href': '', 'uuid': '', 'label': 'ddr-testing-123'}},
        {'topics': ['Topics [123]']},
        {},
        {}
    ]
    assert data == expected

# post
# exists
# get

def test_all_list_fields():
    expected = [
        'id', 'title', 'description', 'url',
        'basename_orig', 'label', 'access_rel', 'sort'
    ]
    assert docstore.all_list_fields() == expected

def test_validate_number():
    assert_raises(docstore.PageNotAnInteger, docstore._validate_number, None, 10)
    assert_raises(docstore.PageNotAnInteger, docstore._validate_number, 'x', 10)
    assert docstore._validate_number(1, 10) == 1
    assert docstore._validate_number(10, 10) == 10
    assert_raises(docstore.EmptyPage, docstore._validate_number, 0, 10)
    assert_raises(docstore.EmptyPage, docstore._validate_number, 11, 10)

def test_page_bottom_top():
    # _page_bottom_top(total, index, page_size) -> bottom,top,num_pages
    # index within bounds
    assert docstore._page_bottom_top(100,  1, 10) == (0, 10, 10)
    assert docstore._page_bottom_top(100,  2, 10) == (10, 20, 10)
    assert docstore._page_bottom_top(100,  9, 10) == (80, 90, 10)
    assert docstore._page_bottom_top(100, 10, 10) == (90, 100, 10)
    assert docstore._page_bottom_top( 10,  1,  5) == (0, 5, 2)
    assert docstore._page_bottom_top( 10,  2,  5) == (5, 10, 2)
    # index out of bounds
    assert_raises(docstore.EmptyPage, docstore._page_bottom_top, 10,0,5)
    assert_raises(docstore.EmptyPage, docstore._page_bottom_top, 10,3,5)

MASSAGE_QUERY_RESULTS	= {
    'hits': {
        'hits': [
            {
                '_id': 'ddr-test-123',
                '_index': 'fakeindex',
                '_type': 'collection',
                'fields': {
                    'index': 'fakeindex',
                    'model': 'collection',
                    'type': 'collection',
                    'id': 'ddr-test-123',
                    'title': 'TITLE TEXT',
                    'description': 'DESCRIPTION TEXT',
                    'signature_file': 'ddr-test-123-1-master-a1b2c3'
                }
            },
            {
                '_id': 'ddr-test-123-1',
                '_index': 'fakeindex',
                '_type': 'entity',
                'fields': {
                    'index': 'fakeindex',
                    'model': 'entity',
                    'type': 'entity',
                    'id': 'ddr-test-123-1',
                    'title': 'TITLE TEXT',
                    'description': 'DESCRIPTION TEXT',
                    'signature_file': 'ddr-test-123-1-master-a1b2c3'
                }
            },
            {
                '_id': 'ddr-test-123-1-master-a1b2c3',
                '_index': 'fakeindex',
                '_type': 'file',
                '_source': {
                    'index': 'fakeindex',
                    'model': 'file',
                    'type': 'file',
                    'id': 'ddr-test-123-1-master-a1b2c3',
                    'title': 'TITLE TEXT',
                    'description': 'DESCRIPTION TEXT',
                    'signature_file': 'ddr-test-123-1-master-a1b2c3'
                }
            },
            {
                '_id': 'ddr-test-123-2',
                '_index': 'fakeindex',
                '_type': 'entity',
                'fields': {
                    'index': 'fakeindex',
                    'model': 'entity',
                    'type': 'entity',
                    'id': 'ddr-test-123-2',
                    'title': 'TITLE TEXT',
                    'description': 'DESCRIPTION TEXT',
                    'signature_file': 'ddr-test-123-1-master-a1b2c3'
                }
            },
        ],
        'total': 7
    },
}
MASSAGE_EXPECTED0 = [
    {'index': 'fakeindex', 'description': 'DESCRIPTION TEXT', 'title': 'TITLE TEXT', 'model': 'collection', 'type': 'collection', 'id': 'ddr-test-123', 'signature_file': 'ddr-test-123-1-master-a1b2c3'},
    {'index': 'fakeindex', 'description': 'DESCRIPTION TEXT', 'title': 'TITLE TEXT', 'model': 'entity', 'type': 'entity', 'id': 'ddr-test-123-1', 'signature_file': 'ddr-test-123-1-master-a1b2c3'},
    {'placeholder': True, 'id': 'ddr-test-123-1-master-a1b2c3', 'n': 2},
    {'placeholder': True, 'id': 'ddr-test-123-2', 'n': 3}
]
MASSAGE_EXPECTED1 = [
    {'placeholder': True, 'id': 'ddr-test-123', 'n': 0},
    {'placeholder': True, 'id': 'ddr-test-123-1', 'n': 1},
    {'index': 'fakeindex', 'description': 'DESCRIPTION TEXT', 'title': 'TITLE TEXT', 'model': 'file', 'type': 'file', 'id': 'ddr-test-123-1-master-a1b2c3', 'signature_file': 'ddr-test-123-1-master-a1b2c3'},
    {'index': 'fakeindex', 'description': 'DESCRIPTION TEXT', 'title': 'TITLE TEXT', 'model': 'entity', 'type': 'entity', 'id': 'ddr-test-123-2', 'signature_file': 'ddr-test-123-1-master-a1b2c3'}
]

def test_massage_query_results():
    objects0 = docstore.massage_query_results(MASSAGE_QUERY_RESULTS, page_size=2, thispage=1)
    objects1 = docstore.massage_query_results(MASSAGE_QUERY_RESULTS, page_size=2, thispage=2)
    assert objects0 == MASSAGE_EXPECTED0
    assert objects1 == MASSAGE_EXPECTED1

def test_clean_sort():
    data0 = 'whatever'
    data1 = [['a', 'asc'], ['b', 'asc'], 'whatever']
    data2 = [['a', 'asc'], ['b', 'asc']]
    expected0 = ''
    expected1 = ''
    expected2 = 'a:asc,b:asc'
    assert docstore._clean_sort(data0) == expected0
    assert docstore._clean_sort(data1) == expected1
    assert docstore._clean_sort(data2) == expected2

# search
# delete
# _model_fields


def test_public_fields():
    
    class PublicFieldsModule(object):
        pass

    entity = PublicFieldsModule()
    entity.FIELDS = [
        {"elasticsearch": {"public": True}, "name": "id"},
        {"elasticsearch": {"public": True}, "name": "title"},
        {"elasticsearch": {"public": False}, "name": "notes"},
        {"name": "noelastic"}
    ]
    file_ = PublicFieldsModule()
    file_.FIELDS = [
        {"elasticsearch": {"public": True}, "name": "id"},
        {"elasticsearch": {"public": True}, "name": "title"},
        {"elasticsearch": {"public": False}, "name": "notes"},
        {"name": "noelastic"}
    ]
    MODULES = {
        'entity': entity,
        'file': file_,
    }
    EXPECTED = {
        'entity': ['id', 'title'],
        'file': ['id', 'title', 'path_rel', 'id'],
    }
    assert docstore._public_fields(MODULES) == EXPECTED

# _parents_status

def test_file_parent_ids():
    i0 = identifier.Identifier('ddr-testing-123')
    i1 = identifier.Identifier('ddr-testing-123-1')
    i2 = identifier.Identifier('ddr-testing-123-1-master-a1')
    expected0 = [None, 'ddr-testing-123']
    expected1 = ['ddr-testing-123', 'ddr-testing-123']
    expected2 = ['ddr-testing-123-1', 'ddr-testing-123']
    assert docstore._file_parent_ids(i0) == expected0
    assert docstore._file_parent_ids(i1) == expected1
    assert docstore._file_parent_ids(i2) == expected2

# _has_access_file
# _store_signature_file
# _choose_signatures
# load_document_json

def test_post_multi():
    hosts = [{'host': '127.0.0.1', 'port': 9999}]
    index = 'fakeindex'
    results = docstore.Docstore(hosts, index).post_multi('/tmp', recursive=True, force=True)
    print('results %s' % results)
    expected = {'successful': 0, 'skipped': 0, 'total': 0, 'bad': []}
    assert results == expected
                       
