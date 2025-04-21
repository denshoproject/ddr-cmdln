import os
import shutil

import pytest

from DDR import identifier
from DDR import inheritance
from DDR import models

MODEL_FIELDS_INHERITABLE = [
    {'model': 'collection', 'name':'id',},
    {'model': 'collection', 'name':'record_created',},
    {'model': 'collection', 'name':'record_lastmod',},
    {'model': 'collection', 'name':'status', 'inheritable':True,},
    {'model': 'collection', 'name':'public', 'inheritable':True,},
    {'model': 'collection', 'name':'title',},
]
def test_Inheritance_inheritable_fields():
    expected = ['status','public']
    out = inheritance.inheritable_fields(MODEL_FIELDS_INHERITABLE)
    assert out == expected

# TODO inheritance_inherit
# TODO inheritance_selected_inheritables
# TODO inheritance_update_inheritables


CHILD_JSONS_DIRS = [
    '.git',
    'files/ddr-test-123-1',
    'files/ddr-test-123-2',
    'files/ddr-test-123-2/files',
]
CHILD_JSONS_FILES = [
    'collection.json',
    '.git/config',
    'files/ddr-test-123-1/entity.json',
    'files/ddr-test-123-1/changelog',
    'files/ddr-test-123-2/entity.json',
    'files/ddr-test-123-2/control',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.jpg',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.json',
]
CHILD_JSONS_EXPECTED = [
    'files/ddr-test-123-1/entity.json',
    'files/ddr-test-123-2/entity.json',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.json',
]

def test_child_jsons(tmpdir):
    pass
    basedir = str(tmpdir / 'child_jsons')
    if os.path.exists(basedir):
        shutil.rmtree(basedir, ignore_errors=1)
    
    # build sample repo
    sampledir = os.path.join(basedir, 'ddr-test-123')
    for d in CHILD_JSONS_DIRS:
        path = os.path.join(sampledir, d)
        os.makedirs(path)
        print('path %s' % path)
    for fn in CHILD_JSONS_FILES:
        path = os.path.join(sampledir, fn)
        print('path %s' % path)
        with open(path, 'w') as f:
            f.write('testing')
    
    def clean(paths):
        base = '%s/' % sampledir
        cleaned = [path.replace(base, '') for path in paths]
        cleaned.sort()
        return cleaned
    
    paths0 = clean(inheritance._child_jsons(sampledir, testing=1))
    print('paths0 %s' % paths0)
    assert paths0 == CHILD_JSONS_EXPECTED

def test_selected_field_values():
    ci = identifier.Identifier('/var/www/media/ddr/ddr-testing-123')
    parent = models.Collection(ci.path_abs(), identifier=ci)
    parent.title = 'Testing'
    parent.status = 'inprogress'
    parent.public = 0
    parent.description = 'testing'
    inheritables = ['status', 'public']
    expected = [
        ('collection.status', 'inprogress'),
        ('collection.public', 0),
    ]
    out = inheritance._selected_field_values(parent, inheritables)
    assert out == expected

SELECTED_INHERITABLES = ['a', 'b']
SELECTED_DATA = {
    'a': False,
    'b': True,
    'a_inherit': False,
    'b_inherit': True,
}
SELECTED_EXPECTED = ['b']

def test_selected_inheritables():
    selected = inheritance.selected_inheritables(SELECTED_INHERITABLES, SELECTED_DATA)
    assert selected == SELECTED_EXPECTED

def test_inherit():
    # TODO this test depends on particular repo_models modules and fields
    ci = identifier.Identifier('/var/www/media/ddr/ddr-testing-123')
    ei = identifier.Identifier('/var/www/media/ddr/ddr-testing-123/files/ddr-testing-123-456')
    fi = identifier.Identifier('/var/www/media/ddr/ddr-testing-123/files/ddr-testing-123-456/files/ddr-testing-123-456-master-abc123')
    collection = models.Collection(ci.path_abs(), identifier=ci)
    entity = models.Entity(ei.path_abs(), identifier=ei)
    file_ = models.File(fi.path_abs(), identifier=fi)
    
    collection.public = True
    entity.public = False
    assert collection.public == True
    assert entity.public == False
    inheritance.inherit(collection, entity)
    assert collection.public == True
    assert entity.public == True
    
    entity.public = True
    file_.public = False
    assert entity.public == True
    assert file_.public == False
    inheritance.inherit(entity, file_)
    assert entity.public == True
    assert file_.public == True
    
    collection.public = True
    file_.public = False
    assert collection.public == True
    assert file_.public == False
    inheritance.inherit(collection, file_)
    assert collection.public == True
    assert file_.public == True
