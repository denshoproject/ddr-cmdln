from datetime import datetime
import json
import os
import sys

from elasticsearch.connection.base import TransportError
from nose.tools import assert_raises
from nose.plugins.attrib import attr
import pytest
import requests
from requests import ConnectionError

from DDR import config
from DDR import docstore
from DDR import dvcs
from DDR import identifier
from DDR import models

HOSTS = [{'host':'127.0.0.1', 'port':9200}]
HOST_CHECK_URL = 'http://{}'.format(config.DOCSTORE_HOST)
DISABLE_SKIP = False

def no_cluster():
    """Returns True if cannot contact cluster; use to skip tests
    """
    if DISABLE_SKIP:
        return False
    try:
        r = requests.get(HOST_CHECK_URL, timeout=1)
        if r.status_code == 200:
            return False
    except ConnectionError:
        print('ConnectionError')
        return True
    except TransportError:
        print('TransportError')
        return True
    return True


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

# put_mappings

# post_facets
# list_facets
# facet_terms

def test_clean_dict():
    d = {'a': 'abc', 'b': 'bcd', 'x':'' }
    docstore._clean_dict(d)
    assert d == {'a': 'abc', 'b': 'bcd'}

# post
# exists
# get

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

# _store_signature_file
# _choose_signatures
# load_document_json

GIT_USER = 'gjost'
GIT_MAIL = 'gjost@densho.org'
AGENT = 'pytest'

COLLECTION_IDS = [
    'ddr-testing-123',
    'ddr-testing-123-1',
    'ddr-testing-123-1-1',
    'ddr-testing-123-1-master-abc123',
    'ddr-testing-123-1-1-master-abc123',
]

@pytest.fixture(scope="session")
def publishable_objects(tmpdir_factory):
    fn = tmpdir_factory.mktemp('repo').join(COLLECTION_IDS[0])
    repo_path = str(fn)
    repo = dvcs.initialize_repository(
        repo_path, GIT_USER, GIT_MAIL
    )
    basepath = os.path.dirname(repo_path)
    objects = []
    for oid in COLLECTION_IDS:
        oi = identifier.Identifier(oid, basepath)
        model_class = identifier.class_for_name(
            identifier.MODEL_CLASSES[oi.model]['module'],
            identifier.MODEL_CLASSES[oi.model]['class']
        )
        o = model_class.new(oi)
        if o.identifier.model == 'file':
            o.sha1 = o.identifier.idparts['sha1']
        o.save(GIT_USER, GIT_MAIL, AGENT)
        objects.append(o)
    return objects


PUBLISHABLE_IDS = [
    'ddr-testing-123',
    'ddr-testing-123-1',
    'ddr-testing-123-1-master-abc123',
]
PUBLISHABLE_INPUTS_EXPECTED = [
    # collection, entity, file        
    (('publish', 'publish', 'publish'), ('POST', 'POST', 'POST')),
    (('publish', 'publish', '-------'), ('POST', 'POST', '----')),
    (('publish', '-------', '-------'), ('POST', '----', '----')),
    (('-------', '-------', '-------'), ('----', '----', '----')),
    (('-------', '-------', 'publish'), ('----', '----', '----')),
    (('-------', 'publish', 'publish'), ('----', '----', '----')),
    (('publish', 'publish', 'publish'), ('POST', 'POST', 'POST')),
    # publishable items with unpublishable parents are not publishable
    (('publish', 'publish', '-------'), ('POST', 'POST', '----')),
    (('publish', '-------', 'publish'), ('POST', '----', '----')),
    (('-------', 'publish', 'publish'), ('----', '----', '----')),
]

def test_publishable(publishable_objects):
    test_these = [o for o in publishable_objects if o.id in PUBLISHABLE_IDS]
    print('test_these %s' % test_these)
    # test combinations of settings of objects at diff places in hierarchy
    ct = 0; total = len(PUBLISHABLE_INPUTS_EXPECTED)
    for status_public,expectations in PUBLISHABLE_INPUTS_EXPECTED:
        ct += 1
        #print('{}/{}'.format(ct,total))
        # set expectations for this round
        expected = []
        for x in expectations:
            if x == '----':
                x = 'SKIP'
            expected.append(x)
        # write test data to objects
        for n,o in enumerate(test_these):
            status = status_public[n]
            if status == 'publish':
                o.status = 'completed'
                o.public = 1
            else:
                o.status = 'inprocess'
                o.public = 0
            o.write_json()
            #print(o.id, o.status, o.public)
        paths = [o.path_abs for o in test_these]
        #print('paths %s' % paths)
        # this code will call docstore.publishable
        identifiers = [identifier.Identifier(path) for path in paths]
        parents = {
            oi.id: oi.object()
            for oi in identifiers
            if oi.model is not 'file' # TODO is not leaf
        }
        results = docstore.publishable(identifiers, parents, force=0)
        # package results and assert
        out = [r['action'] for r in results]
        print(
            status_public,
            expectations,
            out,
            out == expected
        )
        assert out == expected

POST_OBJECT_IDS = [
    'ddr-testing-123',
    'ddr-testing-123-1',
    'ddr-testing-123-1-master-abc123',
]

@pytest.mark.skipif(no_cluster(), reason="Elasticsearch cluster not available.")
def test_post(publishable_objects):
    """Right now this only tests if you can post() without raising exceptions
    """
    ds = docstore.Docstore(config.DOCSTORE_HOST)
    post_these = [o for o in publishable_objects if o.id in POST_OBJECT_IDS]
    for oid in post_these:
        print(o)
        o.status = 'completed'
        o.public = 1
        o.write_json()
        status = ds.post(o)
        print(status)

@pytest.mark.skipif(no_cluster(), reason="Elasticsearch cluster not available.")
def test_post_multi(publishable_objects):
    """Right now this only tests if you can post() without raising exceptions
    """
    ds = docstore.Docstore(config.DOCSTORE_HOST)
    print(ds)
    post_these = [o for o in publishable_objects if o.id in POST_OBJECT_IDS]
    collection_path = post_these[0].identifier.collection_path()
    print(collection_path)
    # make all objects publishable
    for o in post_these:
        o.status = 'completed'
        o.public = 1
        o.write_json()
    # post
    result = ds.post_multi(collection_path, recursive=False)
    print(result)
    result = ds.post_multi(collection_path, recursive=True)
    print(result)
    
# this should come last...
@pytest.mark.skipif(no_cluster(), reason="Elasticsearch cluster not available.")
def test_delete(publishable_objects):
    ds = docstore.Docstore(config.DOCSTORE_HOST)
    print(ds)
    # delete single
    f = None
    for o in publishable_objects:
        if o.identifier.model == 'file':
            f = o
    print(f)
    result = ds.delete(f.id, recursive=False)
    print(result)
    #
    c = None
    for o in publishable_objects:
        if o.identifier.model == 'collection':
            c = o
    print(c)
    result = ds.delete(c.id, recursive=True)
    print(result)
