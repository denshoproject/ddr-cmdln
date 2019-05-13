# -*- coding: utf-8 -*-

import os

import git
import pytest

from DDR import dvcs
from DDR import identifier
from DDR import models
from DDR import signatures
from DDR import util

GIT_USER = 'gjost'
GIT_MAIL = 'gjost@densho.org'
AGENT = 'pytest'

COLLECTION_ID = 'ddr-testing-123'
ENTITY_IDS = [
    ('ddr-testing-123-1', True, 'completed'),
    ('ddr-testing-123-2', True, 'completed'),
    ('ddr-testing-123-3', True, 'completed'),
]
FILE_IDS = [
    ('ddr-testing-123-1-mezzanine-abc123', True, 'completed'),
    ('ddr-testing-123-1-master-abc123',    True, 'completed'),
    ('ddr-testing-123-2-mezzanine-abc124', True, 'completed'),
    ('ddr-testing-123-2-master-abc124',    True, 'completed'),
    ('ddr-testing-123-3-mezzanine-a1b2c3', True, 'completed'),
    ('ddr-testing-123-3-master-a1b2c3',    True, 'completed'),
]

SIGNATURES = [
    #('ddr-testing-123', 'ddr-testing-123-1-mezzanine-abc123'),
    ('ddr-testing-123-1', 'ddr-testing-123-1-mezzanine-abc123'),
    ('ddr-testing-123-2', 'ddr-testing-123-2-mezzanine-abc124'),
    ('ddr-testing-123-3', 'ddr-testing-123-3-mezzanine-a1b2c3'),
]

@pytest.fixture(scope="session")
def collection(tmpdir_factory):
    fn = tmpdir_factory.mktemp('repo').join(COLLECTION_ID)
    collection_path = str(fn)
    collection_json = os.path.join(collection_path, 'collection.json')
    repo = dvcs.initialize_repository(
        collection_path, GIT_USER, GIT_MAIL
    )
    ci = identifier.Identifier(collection_path)
    collection = models.collection.Collection.create(ci)
    collection.write_json()
    for oid,public,status in ENTITY_IDS:
        oi = identifier.Identifier(id=oid, base_path=collection.identifier.basepath)
        o = models.entity.Entity.create(oi)
        o.public = public
        o.status = status
        o.write_json()
    for oid,public,status in FILE_IDS:
        oi = identifier.Identifier(id=oid, base_path=collection.identifier.basepath)
        o = models.files.File.create(oi)
        o.public = public
        o.status = status
        o.sha1 = oi.idparts['sha1']
        o.write_json()
    return collection

def test_00_pick_signatures(tmpdir, collection):
    paths = util.find_meta_files(
        collection.path_abs, recursive=True, force_read=True, testing=True
    )
    parents = signatures.choose(paths)
    updates = signatures.find_updates(parents)
    files_written = signatures.write_updates(updates)

def test_01_check_signatures(tmpdir, collection):
    for oid,sid in SIGNATURES:
        oi = identifier.Identifier(id=oid, base_path=collection.identifier.basepath)
        o = oi.object()
        assert sid == o.signature_id
