import os

from bs4 import BeautifulSoup

from DDR import config
from DDR import identifier
from DDR import idservice

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'idservice')
#if not os.path.exists(TESTING_BASE_DIR):
#    os.makedirs(TESTING_BASE_DIR)

# TODO test_session
# TODO test__get_csrf_token

LOGGED_IN_HTML = """<html>
<head>
<title>logged in</title>
</head>
</html>"""

LOGGED_OUT_HTML = """<html>
<head>
<title>Log in | Densho Digital Repository</title>
</head>
</html>"""

#def test_needs_login():
#    soup0 = BeautifulSoup(LOGGED_IN_HTML, 'html.parser')
#    soup1 = BeautifulSoup(LOGGED_OUT_HTML, 'html.parser')
#    assert idservice._needs_login(soup0) == False
#    assert idservice._needs_login(soup1) == True

# TODO test_login
# TODO test_logout

COLLECTION_IDS_HTML = """
<table id="collections" class="table">
  <tr>
    <td><a class="collection" href="/kiroku/ddr-densho-1/">ddr-densho-1</a></td>
  </tr>
  <tr>
    <td><a class="collection" href="/kiroku/ddr-densho-2/">ddr-densho-2</a></td>
  </tr>
</table>
"""
COLLECTION_IDS = ['ddr-densho-1', 'ddr-densho-2']

ENTITY_IDS_HTML = """
<table id="entities" class="table">
  <tr class="entity">
    <td class="eid">ddr-densho-1-1</td>
    <td class="timestamp">2013-01-11T20:09:38.331830-08:00td></tr>
  <tr class="entity">
    <td class="eid">ddr-densho-1-2</td>
    <td class="timestamp">2013-03-14T14:51:43.344370-07:00</td>
  </tr>
</table>
"""
ENTITY_IDS = ['ddr-densho-1-1', 'ddr-densho-1-2']

#def test__object_ids_existing():
#    out0 = idservice._object_ids_existing(
#        BeautifulSoup(COLLECTION_IDS_HTML),
#        ('a','collection')
#    )
#    assert out0 == COLLECTION_IDS
#    out1 = idservice._object_ids_existing(
#        BeautifulSoup(ENTITY_IDS_HTML),
#        ('td', 'eid')
#    )
#    assert out1 == ENTITY_IDS

#def test_get_ancestor():
#    ei = identifier.Identifier('ddr-test-123-456')
#    ci = identifier.Identifier('ddr-test-123')
#    oi = identifier.Identifier('ddr-test')
#    assert idservice.get_ancestor(ei, 'collection').id == ci.id
#    assert idservice.get_ancestor(ei, 'organization').id == oi.id

# TODO test_collections
# TODO test_entities
# TODO test_objects_next

NEXT_COLLECTION_HTML = """
<!DOCTYPE html>
<html lang="en">
<body>
<table id="collections" class="table">
  <tr>
    <td>
      <a class="collection" href="/workbench/kiroku/ddr-testing-123/">ddr-testing-123</a>
    </td>
  </tr>
  <tr>
    <td>
      <a class="collection" href="/workbench/kiroku/ddr-testing-124/">ddr-testing-124</a>
    </td>
  </tr>
</table>
</body>
</html>
"""

NEXT_ENTITY_HTML = """
<!DOCTYPE html>
<html lang="en">
<body>
<table id="entities" class="table">
  <tr class="entity">
    <td class="eid">ddr-testing-124-1</td>
    <td class="timestamp">2015-11-16T15:27:26.467049-08:00</td>
  </tr>
  <tr class="entity">
    <td class="eid">ddr-testing-124-2</td>
    <td class="timestamp">2015-11-16T15:27:33.447116-08:00</td>
  </tr>
  <tr class="entity">
    <td class="eid">ddr-testing-124-3</td>
    <td class="timestamp">2015-11-16T15:27:33.462868-08:00</td>
  </tr>
</table>
</body>
</html>
"""

#def test__objects_next_process():
#    new_ids_url = 'NEW_IDS_URL'
#    find0 = ['a', 'collection']
#    out0 = idservice._objects_next_process(new_ids_url, NEXT_COLLECTION_HTML, find0, 1)
#    assert out0 == ['ddr-testing-124']
#    
#    find1 = ['td', 'eid']
#    out1 = idservice._objects_next_process(new_ids_url, NEXT_ENTITY_HTML, find1, 1)
#    assert out1 == ['ddr-testing-124-3']


# TODO test_collections_next
# TODO test_entities_next
# TODO test_register_entity_ids



