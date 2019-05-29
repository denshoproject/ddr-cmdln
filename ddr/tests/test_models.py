from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
import json
import os
import random
import shutil

from deepdiff import DeepDiff
import pytest

from DDR import models
from DDR import identifier


class TestModule(object):
    __name__ = 'TestModule'
    __file__ = 'ddr/repo_models'
    FIELDS = [
        {
            'name': 'id',
            'model_type': str,
            'form': {
                'label': 'Object ID',
            },
            'default': '',
        },
        {
            'name': 'timestamp',
            'model_type': datetime,
            'form': {
                'label': 'Last Modified',
            },
            'default': '',
        },
        {
            'name': 'status',
            'model_type': str,
            'form': {
                'label': 'Status',
            },
            'default': '',
        },
        {
            'name': 'title',
            'model_type': str,
            'form': {
                'label': 'Title',
            },
            'default': '',
        },
        {
            'name': 'description',
            'model_type': str,
            'form': {
                'label': 'Description',
            },
            'default': '',
        },
    ]

class TestDocument():
    pass

TEST_DOCUMENT = """[
    {
        "application": "https://github.com/densho/ddr-local.git",
        "commit": "52155f819ccfccf72f80a11e1cc53d006888e283  (HEAD, repo-models) 2014-09-16 16:30:42 -0700",
        "git": "git version 1.7.10.4; git-annex version: 3.20120629",
        "models": "",
        "release": "0.10"
    },
    {"id": "ddr-test-123"},
    {"timestamp": "2014-09-19T03:14:59"},
    {"status": 1},
    {"title": "TITLE"},
    {"description": "DESCRIPTION"}
]"""


# TODO sort_file_paths
# TODO object_metadata
# TODO is_object_metadata

def test_load_json():
    class Document(object):
        pass
    
    document = Document()
    module = TestModule()
    models.common.load_json(document, module, TEST_DOCUMENT)
    assert document.id == 'ddr-test-123'
    assert document.timestamp == u'2014-09-19T03:14:59'
    assert document.status == 1
    assert document.title == 'TITLE'
    assert document.description == 'DESCRIPTION'

# TODO prep_json
# TODO from_json
# TODO load_xml
# TODO prep_xml
# TODO from_xml


# TODO Stub


# Collection

def test_Collection__init__(tmpdir):
    cid = 'ddr-testing-123'
    path_abs = str(tmpdir / cid)
    c = models.Collection(path_abs)
    assert c.root == str(tmpdir)
    assert c.id == 'ddr-testing-123'
    assert c.path == path_abs
    assert c.path_abs == path_abs
    assert c.gitignore_path == os.path.join(path_abs, '.gitignore')
    assert c.annex_path == os.path.join(path_abs, '.git/annex')
    assert c.files_path == os.path.join(path_abs, 'files')
    assert c.lock_path == os.path.join(path_abs, 'lock')
    assert c.control_path == os.path.join(path_abs, 'control')
    assert c.changelog_path == os.path.join(path_abs, 'changelog')
    assert (c.path_rel == None) or (c.path_rel == '')
    assert c.gitignore_path_rel == '.gitignore'
    assert c.annex_path_rel == '.git/annex'
    assert c.files_path_rel == 'files'
    assert c.control_path_rel == 'control'
    assert c.changelog_path_rel == 'changelog'
    # TODO assert c.git_url

# TODO Collection.__repr__
# TODO Collection.create
# TODO Collection.from_identifier
# TODO Collection.from_json
# TODO Collection.parent
# TODO Collection.children
# TODO Collection.labels_values
# TODO Collection.inheritable_fields
# TODO Collection.selected_inheritables
# TODO Collection.update_inheritables
# TODO Collection.load_json
# TODO Collection.dump_json
# TODO Collection.write_json
# TODO Collection.post_json

# Collection.lock
# Collection.unlock
# Collection.locked
def test_Collection_locking(tmpdir):
    cid = 'ddr-testing-123'
    path_abs = str(tmpdir / cid)
    c = models.Collection(path_abs)
    text = 'testing'
    # prep
    if os.path.exists(path_abs):
        shutil.rmtree(path_abs)
    os.makedirs(c.path)
    # before locking
    assert c.locked() == False
    assert not os.path.exists(c.lock_path)
    # locking
    assert c.lock(text) == 'ok'
    # locked
    assert c.locked() == text
    assert os.path.exists(c.lock_path)
    # unlocking
    assert c.unlock(text) == 'ok'
    assert c.locked() == False
    assert not os.path.exists(c.lock_path)
    # clean up
    if os.path.exists(path_abs):
        shutil.rmtree(path_abs)

# TODO Collection.changelog
# TODO Collection.control
# TODO Collection.ead
# TODO Collection.dump_ead
# TODO Collection.write_ead
# TODO Collection.gitignore
# TODO Collection.collection_paths
# TODO Collection.repo_fetch
# TODO Collection.repo_status
# TODO Collection.repo_annex_status
# TODO Collection.repo_synced
# TODO Collection.repo_ahead
# TODO Collection.repo_behind
# TODO Collection.repo_diverged
# TODO Collection.repo_conflicted


# dict of file info dicts so we can test against the following data structures
FILEMETA_DATA = {
    'ddr-densho-23-1-master-adb451ffec': {
        "md5": "7c17eb2b0e838c8d7e2324ba5dd462d6",
        "path_rel": "ddr-densho-23-1-master-adb451ffec.tif",
        "public": "1",
        "sha1": "adb451ffece389d175c57f55caec6abcd688ca0f",
        "sha256": "0f4c964f1c8db557d17a6c9d2732cfa5b80805079ee01cc89173e379e144c19f",
        "order": 1
    },
    'ddr-densho-23-1-mezzanine-adb451ffec': {
        "md5": "7c17eb2b0e838c8d7e2324ba5dd462d6",
        "path_rel": "ddr-densho-23-1-mezzanine-adb451ffec.htm",
        "public": "1",
        "sha1": "adb451ffece389d175c57f55caec6abcd688ca0f",
        "sha256": "0f4c964f1c8db557d17a6c9d2732cfa5b80805079ee01cc89173e379e144c19f",
        "order": 1
    },
    'ddr-testing-141-1-master-96c048001e': {
        'md5': "67267fa285abd746db72a8b5782c7aa8",
        'path_rel': "ddr-testing-141-1-master-96c048001e.pdf",
        'public': "1",
        'role': "master",
        'sha1': "96c048001e06bd1146d964ea16616d55344a514b",
        'sha256': "77d8d47a3316d88e2c759638f22df45c3b9b1e21a20de50fb0706df3777364d6"
    },
    'ddr-testing-141-1-master-c774ed4657': {
        'md5': "1cfa88f0105b90030d677400a59982e4",
        'path_rel': "ddr-testing-141-1-master-c774ed4657.jpg",
        'public': "1",
        'role': "master",
        'sha1': "c774ed4657b88eb7aea3269ebb9dd63f5807e7df",
        'sha256': "75414667f22a71c20f6822c933ce44d5cf3adaf3db26d7dad617a34f8a44da03"
    },
}
# dict of file objects so we can test against the following data structures
# ** * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# ** filegroups testing disabled bc depends on nonexistent collection repo
# ** TODO generate a repo for testing
# ** * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
#FILEGROUPS_DATA = {
#    fid: identifier.Identifier(id=fid, base_path=MEDIA_BASE).object()
#    for fid in [
#        'ddr-densho-23-1-master-adb451ffec',
#        'ddr-densho-23-1-mezzanine-adb451ffec',
#        'ddr-testing-141-1-master-96c048001e',
#        'ddr-testing-141-1-master-c774ed4657',
#    ]
#}
# 
#FILEGROUPS_META = [
#    {
#        "role": "mezzanine",
#        "files": [FILEMETA_DATA['ddr-densho-23-1-mezzanine-adb451ffec']]
#    },
#    {
#        "role": "master",
#        "files": [FILEMETA_DATA['ddr-densho-23-1-master-adb451ffec']]
#    },
#]
#FILES_META = [
#    FILEMETA_DATA['ddr-densho-23-1-mezzanine-adb451ffec'],
#    FILEMETA_DATA['ddr-densho-23-1-master-adb451ffec'],
#]
#FILEGROUPS_OBJECTS = [
#    {
#        "role": "mezzanine",
#        "files": [FILEGROUPS_DATA['ddr-densho-23-1-mezzanine-adb451ffec']]
#    },
#    {
#        "role": "master",
#        "files": [FILEGROUPS_DATA['ddr-densho-23-1-master-adb451ffec']]
#    },
#]
#FILES_OBJECTS = [
#    FILEGROUPS_DATA['ddr-densho-23-1-mezzanine-adb451ffec'],
#    FILEGROUPS_DATA['ddr-densho-23-1-master-adb451ffec'],
#]
    
def test_Entity__init__(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    e = models.Entity(path_abs)
    assert e.parent_path == collection_path
    assert e.parent_id == collection_id
    assert e.root == str(tmpdir)
    assert e.id == 'ddr-testing-123-456'
    assert e.path == path_abs
    assert e.path_abs == path_abs
    assert e.files_path == os.path.join(path_abs, 'files')
    assert e.lock_path == os.path.join(path_abs, 'lock')
    assert e.control_path == os.path.join(path_abs, 'control')
    assert e.changelog_path == os.path.join(path_abs, 'changelog')
    assert e.path_rel == 'files/ddr-testing-123-456'
    assert e.files_path_rel == 'files/ddr-testing-123-456/files'
    assert e.control_path_rel == 'files/ddr-testing-123-456/control'
    assert e.changelog_path_rel == 'files/ddr-testing-123-456/changelog'

# TODO Entity.__repr__
# TODO Entity.create
# TODO Entity.from_identifier
# TODO Entity.from_json
# TODO Entity.from_csv

ENTITY_DICT = OrderedDict([
    ('id', 'ddr-testing-123-456'),
    ('record_created', datetime(2018, 9, 20, 12, 23, 21, 227561)),
    ('record_lastmod', datetime(2018, 9, 20, 12, 23, 21, 227582)),
    ('status', ''), ('public', ''), ('sort', 1),
    ('title', ''), ('description', ''),
    ('creation', ''), ('location', ''), ('creators', ''),
    ('language', ''), ('genre', ''), ('format', ''), ('extent', ''),
    ('contributor', ''), ('alternate_id', ''), ('digitize_person', ''),
    ('digitize_organization', ''), ('digitize_date', ''), ('credit', ''),
    ('rights', ''), ('rights_statement', ''), ('topics', ''),
    ('persons', ''), ('facility', ''), ('chronology', ''),
    ('geography', ''), ('parent', ''), ('signature_id', ''), ('notes', ''),
    #('children', []),
    #('file_groups', []),
])

def test_Entity_dict(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    ei = identifier.Identifier(path_abs)
    o = models.Entity.create(ei)
    o.record_created = datetime(2018, 9, 20, 12, 23, 21, 227561)
    o.record_lastmod = datetime(2018, 9, 20, 12, 23, 21, 227582)
    out = o.dict()
    # exclude .children and .file_groups from comparison
    out.pop('children')
    out.pop('file_groups')
    assert out == ENTITY_DICT

def test_Entity_diff(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    ei = identifier.Identifier(path_abs)
    o1 = models.Entity.create(ei)
    o2 = deepcopy(o1)
    # identical
    out0 = o1.diff(o2)
    print('out0 %s' % out0)
    assert out0 == {}
    # change lastmod
    o2.notes = 'these are some notes'
    out1 = o1.diff(o2)
    print('out1 %s' % out1)
    assert out1        # diffs present
    out2 = o1.diff(o2, ignore_fields=['notes'])
    print('out2 %s' % out2)
    assert out2 == {}  # no diffs

def test_Entity_is_modified(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    
    print('NEW DOCUMENT')
    o0 = models.Entity(path_abs)
    print('o0 %s' % o0)
    # nonexistent documents are considered modified
    print('o0.is_modified() %s' % o0.is_modified())
    assert o0.is_modified() == True
    now = datetime.now()
    o0.record_created = now
    o0.record_lastmod = now
    o0.title = 'TITLE'
    o0.write_json(doc_metadata=False)
    
    print('EXISTING DOCUMENT')
    o1 = identifier.Identifier(id=entity_id, base_path=str(tmpdir)).object()
    print('o1 %s' % o1)
    # freshly loaded object should not be modified
    out1 = o1.is_modified()
    print('out1 %s' % out1)
    assert out1 == {}
    
    o1.title = 'new title'
    assert o1.title == 'new title'
    print('o1.is_modified() %s' % o1.is_modified())
    assert o1.is_modified()
    assert o1.title == 'new title'
    o1.write_json(doc_metadata=False)
    
    # existing document
    o2 = identifier.Identifier(id=entity_id, base_path=str(tmpdir)).object()
    # freshly loaded object should not be modified
    print('o2.is_modified() %s' % o2.is_modified())
    assert not o2.is_modified()
    assert o2.title == 'new title'

# TODO Entity.parent
# TODO Entity.labels_values
# TODO Entity.inheritable_fields
# TODO Entity.selected_inheritables
# TODO Entity.update_inheritables
# TODO Entity.inherit

# Entity.lock
# Entity.unlock
# Entity.locked
def test_Entity_locking(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    e = models.Entity(path_abs)
    text = 'testing'
    # prep
    if os.path.exists(path_abs):
        shutil.rmtree(path_abs)
    os.makedirs(e.path)
    # before locking
    assert e.locked() == False
    assert not os.path.exists(e.lock_path)
    # locking
    assert e.lock(text) == 'ok'
    # locked
    assert e.locked() == text
    assert os.path.exists(e.lock_path)
    # unlocking
    assert e.unlock(text) == 'ok'
    assert e.locked() == False
    assert not os.path.exists(e.lock_path)
    # clean up
    if os.path.exists(path_abs):
        shutil.rmtree(path_abs)

# TODO Entity.load_json
# TODO Entity.dump_json
# TODO Entity.write_json
# TODO Entity.post_json
# TODO Entity.load_csv
# TODO Entity.dump_csv

def test_Entity_changelog(tmpdir):
    collection_id = 'ddr-testing-123'
    entity_id = 'ddr-testing-123-456'
    collection_path = str(tmpdir / collection_id)
    path_abs = os.path.join(collection_path, 'files', entity_id)
    e = models.Entity(path_abs)
    changelog_path = os.path.join(path_abs, 'changelog')
    assert e.changelog() == '%s is empty or missing' % changelog_path
    # TODO test reading changelog

# TODO Entity.control
# TODO Entity.mets
# TODO Entity.dump_xml
# TODO Entity.write_xml

def test_Entity_checksum_algorithms():
    assert models.Entity.checksum_algorithms() == ['md5', 'sha1', 'sha256']

# TODO Entity.checksum_algorithms
# TODO Entity.checksums
# TODO Entity.file_paths
# TODO Entity.detect_children_duplicates
# TODO Entity.file
# TODO Entity.addfile_logger
# TODO Entity.add_local_file
# TODO Entity.add_external_file
# TODO Entity.add_access
# TODO Entity.add_file_commit

CHILDREN_ENTITY = models.entity.Entity(
    '/var/www/media/ddr/ddr-test-123/files/ddr-test-123-456'
)
CHILDREN_ENTITY.public = 1
CHILDREN_ENTITY.signature_id = 'ddr-test-123-456-mezzanine-abc123'
CHILDREN_ENTITY.sort = 123
CHILDREN_ENTITY.title = 'Some Entity!'

CHILDREN_FIDS = [
    'ddr-testing-123-456-mezzanine-a1b2c3',
    'ddr-testing-123-456-mezzanine-abc123',
    'ddr-testing-123-456-master-a1b2c3',
    'ddr-testing-123-456-master-abc123',
    'ddr-testing-123-456-transcript-abc123',
]
CHILDREN_FILES = [
    models.files.File(fi)
    for fi in [
        identifier.Identifier(os.path.join(CHILDREN_ENTITY.files_path, fid))
        for fid in CHILDREN_FIDS
    ]
]

CHILDREN_COUNTS = OrderedDict()
CHILDREN_COUNTS['children'] = 0
CHILDREN_COUNTS['mezzanine'] = 2
CHILDREN_COUNTS['master'] = 2
CHILDREN_COUNTS['transcript'] = 1
CHILDREN_COUNTS['gloss'] = 0
CHILDREN_COUNTS['preservation'] = 0
CHILDREN_COUNTS['administrative'] = 0

CHILDREN_FILEGROUPS = [
    {
        'role': 'mezzanine',
        'files': [
            {
                'id': 'ddr-testing-123-456-mezzanine-a1b2c3',
                'label': '', 'public': 0, 'size': None, 'sort': 1,
                'path_rel': 'files/ddr-testing-123-456/files/ddr-testing-123-456-mezzanine-a1b2c3',
            },
            {
                'id': 'ddr-testing-123-456-mezzanine-abc123',
                'label': '', 'public': 0, 'size': None, 'sort': 1,
                'path_rel': 'files/ddr-testing-123-456/files/ddr-testing-123-456-mezzanine-abc123',
            },
        ],
    },
    {
        'role': 'master',
        'files': [
            {
                'id': 'ddr-testing-123-456-master-a1b2c3',
                'label': '', 'public': 0, 'size': None, 'sort': 1,
                'path_rel': 'files/ddr-testing-123-456/files/ddr-testing-123-456-master-a1b2c3',
            },
            {
                'id': 'ddr-testing-123-456-master-abc123',
                'label': '', 'public': 0, 'size': None, 'sort': 1,
                'path_rel': 'files/ddr-testing-123-456/files/ddr-testing-123-456-master-abc123',
            },
        ],
    },
    {
        'role': 'transcript',
        'files': [
            {
                'id': 'ddr-testing-123-456-transcript-abc123',
                'label': '', 'public': 0, 'size': None, 'sort': 1,
                'path_rel': 'files/ddr-testing-123-456/files/ddr-testing-123-456-transcript-abc123',
            }
        ],
    }
]

def test_ddrpublic_template_key():
    # normal(?)
    e0 = deepcopy(CHILDREN_ENTITY)
    e0.format = 'img'
    e0._children_objects = deepcopy(CHILDREN_FILES)
    for f in e0._children_objects:
        f.mimetype == 'image/jpeg'
    signature0,key0 = e0.ddrpublic_template_key()
    print(signature0,key0)
    assert signature0.id == 'ddr-test-123-456-mezzanine-abc123'
    assert key0 == 'img:'

    # entity.signature_id is blank
    e1 = deepcopy(CHILDREN_ENTITY)
    e1.signature_id = None
    e1.format = 'img'
    e1._children_objects = deepcopy(CHILDREN_FILES)
    for f in e1.children(role='mezzanine'):
        f.mimetype = 'image/jpeg'
    signature1,key1 = e1.ddrpublic_template_key()
    print(signature1,key1)
    assert signature1.id == 'ddr-testing-123-456-mezzanine-a1b2c3'
    assert key1 == 'img:image'

    # no mezzanines
    e2 = deepcopy(CHILDREN_ENTITY)
    e2.signature_id = None
    e2.format = 'img'
    children = [
        f for f in CHILDREN_FILES
        if 'mezzanine' not in f.id
    ]
    e2._children_objects = children
    signature2,key2 = e2.ddrpublic_template_key()
    print(signature2,key2)
    assert signature2 == None
    assert key2 == None

    # TODO signature belongs to segment mezzanine


def test_children():
    e = deepcopy(CHILDREN_ENTITY)
    e._children_objects = deepcopy(CHILDREN_FILES)
    e.add_child(CHILDREN_ENTITY)
    # models=
    out0 = e.children(models=['entity','segments'])
    assert out0 == [CHILDREN_ENTITY]
    # role=
    expected1 = [CHILDREN_FILES[-1]]
    out1 = e.children(role='transcript')
    assert out1 == expected1

def test_add_child():
    new_child = CHILDREN_FILES[0]
    e = deepcopy(CHILDREN_ENTITY)
    before = deepcopy(e._children_objects)
    e.add_child(new_child)
    after = deepcopy(e._children_objects)
    diff = DeepDiff(before, after, ignore_order=True)
    assert diff
    assert diff.get('iterable_item_added')
    assert diff['iterable_item_added'].get('root[0]')
    added = diff['iterable_item_added']['root[0]']
    assert added not in before
    assert added in after
    assert added.id == new_child.id

def test_remove_child():
    e = deepcopy(CHILDREN_ENTITY)
    e._children_objects = [
        CHILDREN_FILES[0]
    ]
    e.remove_child(CHILDREN_FILES[0].id)
    assert e._children_objects == []

def test_children_counts():
    e = deepcopy(CHILDREN_ENTITY)
    e._children_objects = deepcopy(CHILDREN_FILES)
    e.add_child(CHILDREN_ENTITY)
    CHILDREN_COUNTS['children'] = 1
    out = e.children_counts()
    print(out)
    assert out == CHILDREN_COUNTS

def test_sort_children():
    objects = [CHILDREN_ENTITY] + CHILDREN_FILES
    object_ids = [CHILDREN_ENTITY.id] + [o.id for o in CHILDREN_FILES]
    shuffled = deepcopy(objects)
    random.shuffle(shuffled)
    objects_sorted = models.entity._sort_children(shuffled)
    sorted_ids = [o.id for o in objects_sorted]
    print(sorted_ids)
    assert sorted_ids == object_ids

def test_files_to_filegroups():
    out = models.entity.files_to_filegroups(CHILDREN_FILES)
    assert out == CHILDREN_FILEGROUPS
    
def test_entity_to_childrenmeta():
    expected = {
        'id': CHILDREN_ENTITY.id,
        'public': CHILDREN_ENTITY.public,
        'signature_id': CHILDREN_ENTITY.signature_id,
        'sort': CHILDREN_ENTITY.sort,
        'title': CHILDREN_ENTITY.title,
    }
    out = models.entity.entity_to_childrenmeta(CHILDREN_ENTITY)
    assert out == expected


# TODO File.__init__
# TODO File.__repr__
# TODO File.from_identifer
# TODO File.from_json
# TODO File.parent
# TODO File.children
# TODO File.labels_values
# TODO File.files_rel
# TODO File.present
# TODO File.access_present
# TODO File.inherit
# TODO File.load_json
# TODO File.dump_json
# TODO File.write_json
# TODO File.post_json
# TODO File.file_name
# TODO File.set_path
# TODO File.set_access
# TODO File.file
# TODO File.access_filename
# TODO File.links_incoming
# TODO File.links_outgoing
# TODO File.links_all
