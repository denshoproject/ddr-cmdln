# -*- coding: utf-8 -*-

import git
import pytest

from DDR import identifier
from DDR.models import collection, entity, files
from DDR import signatures
from DDR import util

COLLECTION_ID = 'ddr-testing-123'
ENTITY_IDS = [
    ('ddr-testing-123-1', True, 'completed'),
    ('ddr-testing-123-2', True, 'completed'),
    ('ddr-testing-123-3', True, 'completed'),
]
FILE_IDS = [
    ('ddr-testing-123-1-mezzanine-abc123', True, 'completed'),
    ('ddr-testing-123-1-master-abc123', True, 'completed'),
    ('ddr-testing-123-2-mezzanine-abc124', True, 'completed'),
    ('ddr-testing-123-2-master-abc124', True, 'completed'),
    ('ddr-testing-123-3-mezzanine-a1b2c3', True, 'completed'),
    ('ddr-testing-123-3-master-a1b2c3', True, 'completed'),
]

SIGNATURES = [
    ('ddr-testing-123', 'ddr-testing-123-1-mezzanine-abc123'),
    ('ddr-testing-123-1', 'ddr-testing-123-1-mezzanine-abc123'),
    ('ddr-testing-123-2', 'ddr-testing-123-2-mezzanine-abc124'),
    ('ddr-testing-123-3', 'ddr-testing-123-3-mezzanine-a1b2c3'),
]

def test_setup_test_collection(tmpdir):
    oid = COLLECTION_ID
    cpath = str(tmpdir / oid)
    repo = git.Repo.init(cpath)
    oi = identifier.Identifier(id=oid, base_path=str(tmpdir))
    o = collection.Collection.create(oi)
    o.public = True
    o.status = 'completed'
    o.write_json()
    
    for oid,public,status in ENTITY_IDS:
        oi = identifier.Identifier(id=oid, base_path=str(tmpdir))
        o = entity.Entity.create(oi)
        o.public = public
        o.status = status
        o.write_json()
    
    for oid,public,status in FILE_IDS:
        oi = identifier.Identifier(id=oid, base_path=str(tmpdir))
        o = files.File.create(oi)
        o.public = public
        o.status = status
        o.sha1 = oi.idparts['sha1']
        o.write_json()

def test_pick_signatures(tmpdir):
    cpath = str(tmpdir / COLLECTION_ID)
    paths = util.find_meta_files(cpath, recursive=True, force_read=True, testing=True)
    parents = signatures.choose(paths)
    updates = signatures.find_updates(parents)
    files_written = signatures.write_updates(updates)
    return files_written

def test_check_signatures(tmpdir):
    for oid,sid in SIGNATURES:
        o = identifier.Identifier(id=oid, base_path=str(tmpdir)).object()
        print(oid,sid,o.signature_id)
        assert sid == o.signature_id
