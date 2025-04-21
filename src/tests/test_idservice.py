from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest

from DDR import config
from DDR import identifier
from DDR import idservice
from DDR.www import httpx_client


def mkurl(fragment):
    u = urlparse(config.IDSERVICE_API_BASE)
    path = Path(u.path) / fragment
    return urlunparse([u.scheme, u.netloc, str(path), '', '', ''])

SKIP_REASON = 'No [cmdln]idservice_username/password in configs.'
def no_username_password():
    if not (hasattr(config,'IDSERVICE_USERNAME') or hasattr(config,'IDSERVICE_PASSWORD')):
        return True
    if not (config.IDSERVICE_USERNAME or config.IDSERVICE_PASSWORD):
        return True
    return False

NO_IDSERVICE_ERR = 'ID service is not available.'
def no_idservice():
    """Returns True if cannot contact ID service; use to skip tests
    """
    try:
        print(config.IDSERVICE_API_BASE)
        client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
        r = client.get(config.IDSERVICE_API_BASE, timeout=1)
        print(r.status_code)
        if r.status_code == 200:
            return False
    except ConnectionError:
        print('ConnectionError')
        return True
    return True

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_groups():
    url = mkurl('groups')
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    response = client.get(url)
    assert response.status_code == 200

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_group():
    url = mkurl('groups/1')
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    response = client.get(url)
    assert response.status_code == 200

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_users():
    url = mkurl('users')
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    response = client.get(url)
    assert response.status_code == 401

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_user():
    url = mkurl('users/1')
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    response = client.get(url)
    assert response.status_code == 401

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_login():
    ic = idservice.IDServiceClient()
    print(config.IDSERVICE_USERNAME)
    print(config.IDSERVICE_PASSWORD)
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    print(code,status)
    assert code == 200
    assert status == 'OK'

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_resume():
    ic = idservice.IDServiceClient()
    print(config.IDSERVICE_USERNAME)
    print(config.IDSERVICE_PASSWORD)
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    print(code,status)
    assert code == 200
    assert status == 'OK'
    code,status = ic.resume(ic.token)
    assert code == 200
    assert status == 'OK'

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_detail():
    ic = idservice.IDServiceClient()
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    assert code == 200
    print(code,status)
    url = mkurl('objectids/ddr-testing-1/')
    print(url)
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    r = client.get(url)
    print(r.status_code,r.reason)
    assert r.status_code == 200

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_children():
    """Test that app returns list of children
    """
    ic = idservice.IDServiceClient()
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    assert code == 200
    url = mkurl('objectids/ddr-testing-1/children/')
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    r = client.get(url)
    assert r.status_code == 200
    print(r.text)
    data = r.json()
    print(data)
    assert isinstance(data, list)
    assert len(data)
    assert data[0]['group'] == 'testing'
    assert data[0]['model'] in ['entity','segment']
    assert 'ddr-testing-' in data[0]['id']
    #assert data[1]['group'] == 'testing'
    #assert data[1]['model'] in ['entity','segment']
    #assert 'ddr-testing-' in data[1]['id']

#@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
#@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
#def test_check_collections():
#    ic = idservice.IDServiceClient()
#    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
#    assert code == 200
#    url = mkurl('objectids/ddr-testing-1/check/')
#    print(url)
#    data = {"object_ids": ["ddr-testing-1","ddr-testing-2"]}
#    r = requests.post(url, data=data)
#    print(r.status_code)
#    print(r.reason)
#    assert r.status_code == 200
#    #print(r.text)
#    data = r.json()
#    assert isinstance(data, dict)
#    assert isinstance(data['registered'], list)
#    assert isinstance(data['unregistered'], list)
#    assert len(data['registered']) == 1
#    assert len(data['unregistered']) == 1
#    assert 'ddr-testing-1' in data['registered']
#    assert 'ddr-testing-2' in data['unregistered']

#@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
#@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
#def test_check_entities():
#    ic = idservice.IDServiceClient()
#    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
#    assert code == 200
#    url = mkurl('objectids/ddr-testing-1/check/')
#    data = {'object_ids': [
#        'ddr-testing-1-1','ddr-testing-1-2','ddr-testing-1-3',
#    ]}
#    r = requests.post(url, data=data)
#    print(r.status_code)
#    print(r.reason)
#    assert r.status_code == 200
#    data = r.json()
#    print(data)
#    assert isinstance(data, dict)
#    assert isinstance(data['registered'], list)
#    assert isinstance(data['unregistered'], list)
#    assert len(data['registered']) == 2
#    assert len(data['unregistered']) == 1
#    assert 'ddr-testing-1-1' in data['registered']
#    assert 'ddr-testing-1-2' in data['registered']
#    assert 'ddr-testing-1-3' in data['unregistered']

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_next_collection():
    """Test if app can get or post next collection
    """
    ic = idservice.IDServiceClient()
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    assert code == 200
    url = mkurl('objectids/ddr-testing/next/collection/')
    # GET
    print(url)
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    r = client.get(url)
    assert r.status_code == 200
    print(r.status_code)
    print(r.text)
    data = r.json()
    assert isinstance(data, dict)
    assert data['group'] == 'testing'
    assert data['model'] == 'collection'
    assert 'ddr-testing-' in data['id']
    ## POST
    #print(url)
    #r = client.post(url)
    #assert r.status_code == 201
    #print(r.data)
    #assert isinstance(r.data, dict)
    #assert r.data['group'] == 'testing'
    #assert r.data['model'] == 'collection'
    #assert r.data['id'] == 'ddr-testing-2'
    #o = models.ObjectID.objects.get(id='ddr-testing-2')
    #print(o)

@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
def test_next_entity():
    """Test if app can get or post next entity
    """
    ic = idservice.IDServiceClient()
    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
    assert code == 200
    url = mkurl('objectids/ddr-testing-1/next/entity/')
    print(url)
    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
    r = client.get(url)
    assert r.status_code == 200
    print(r.status_code)
    print(r.text)
    data = r.json()
    assert isinstance(data, dict)
    assert data['group'] == 'testing'
    assert data['model'] == 'entity'
    assert 'ddr-testing-1-' in data['id']
    ## POST
    #print(url)
    #r = client.post(url)
    #assert r.status_code == 201
    #print(r.data)
    #assert isinstance(r.data, dict)
    #assert r.data['group'] == 'testing'
    #assert r.data['model'] == 'entity'
    #assert r.data['id'] == 'ddr-testing-1-2'

#@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
#@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
#def test_create():
#    ic = idservice.IDServiceClient()
#    code,status = ic.login(config.IDSERVICE_USERNAME, config.IDSERVICE_PASSWORD)
#    assert code == 200
#    print(code,status)
#    url = mkurl('objectids/ddr-testing-1/')
#    print(url)
#    client = httpx_client(cafile=config.IDSERVICE_SSL_CERTFILE)
#    r = client.get(url)
#    print(r.status_code,r.reason)
#    assert r.status_code == 200

#@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
#@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
#def test_create_collections(client, create_user):
#    # setup
#    make_objectid('testing', 'collection', 'ddr-testing-1')
#    models.ObjectID.objects.get(id='ddr-testing-1')
#    admin_user = create_user(
#        username=USERNAME, password=PASSWORD, is_staff=1, is_superuser=1
#    )
#    # test
#    client.login(username=USERNAME, password=PASSWORD)
#    url = IDSERVICE_API_BASE + '/objectids/{}/create/'.format('ddr-testing')
#    data = {'object_ids': [
#        'ddr-testing-2','ddr-testing-3'
#    ]}
#    r = client.post(url, data=data)
#    print(r.data)
#    assert isinstance(r.data, dict)
#    assert isinstance(r.data['created'], list)
#    assert len(r.data['created']) == 2
#    assert 'ddr-testing-2' in r.data['created']
#    assert 'ddr-testing-3' in r.data['created']

#@pytest.mark.skipif(no_username_password(), reason=SKIP_REASON)
#@pytest.mark.skipif(no_idservice(), reason=NO_IDSERVICE_ERR)
#def test_create_entities(client, create_user):
#    # setup
#    make_objectid('testing', 'collection', 'ddr-testing-1')
#    make_objectid('testing', 'entity', 'ddr-testing-1-1')
#    models.ObjectID.objects.get(id='ddr-testing-1')
#    models.ObjectID.objects.get(id='ddr-testing-1-1')
#    admin_user = create_user(
#        username=USERNAME, password=PASSWORD, is_staff=1, is_superuser=1
#    )
#    # test
#    client.login(username=USERNAME, password=PASSWORD)
#    url = IDSERVICE_API_BASE + '/objectids/{}/create/'.format('ddr-testing-1')
#    data = {'object_ids': [
#        'ddr-testing-1-2','ddr-testing-1-3',
#    ]}
#    r = client.post(url, data=data)
#    print(r.data)
#    assert isinstance(r.data, dict)
#    assert isinstance(r.data['created'], list)
#    assert len(r.data['created']) == 2
#    assert 'ddr-testing-1-2' in r.data['created']
#    assert 'ddr-testing-1-3' in r.data['created']
