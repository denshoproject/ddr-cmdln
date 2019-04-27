# -*- coding: utf-8 -*-

import os
import shutil

import git
import pytest

from DDR import batch
from DDR import config
from DDR import dvcs
from DDR import identifier
from DDR.models import Collection

COLLECTION_ID = 'ddr-testing-123'
VOCABS_URL = config.VOCABS_URL
GIT_USER = 'gjost'
GIT_MAIL = 'gjost@densho.org'
AGENT = 'pytest'
TEST_FILES_DIR = os.path.join(
    os.getcwd(), 'ddr-cmdln/ddr/tests/ddrimport'
)
TEST_FILES_TMP = 'ddrimport'


@pytest.fixture(scope="session")
def test_files_dir(tmpdir_factory):
    fn = tmpdir_factory.mktemp(TEST_FILES_TMP)
    dest_dir = str(fn)
    #shutil.copytree(TEST_FILES_DIR, dest_dir, dirs_exist_ok=True)
    for f in os.listdir(TEST_FILES_DIR):
        shutil.copy(
            os.path.join(TEST_FILES_DIR, f),
            dest_dir
        )
    return dest_dir

@pytest.fixture(scope="session")
def collection(tmpdir_factory):
    fn = tmpdir_factory.mktemp('repo').join(COLLECTION_ID)
    collection_path = str(fn)
    collection_json = os.path.join(collection_path, 'collection.json')
    if os.path.exists(collection_json):
        return identifier.Identifier(collection_path).object()
    else:
        repo = dvcs.initialize_repository(
            collection_path, GIT_USER, GIT_MAIL
        )
        ci = identifier.Identifier(collection_path)
        collection = Collection.create(ci)
        collection.save(GIT_USER, GIT_MAIL, AGENT)
        return collection

EXPECTED_ENTITY_IDS = [
    'ddr-testing-123-1',
    'ddr-testing-123-2',
    'ddr-testing-123-3',
    'ddr-testing-123-4',
    'ddr-testing-123-4-1',
    'ddr-testing-123-5',
    'ddr-testing-123-6',
]

def test_import_entities(tmpdir, collection, test_files_dir):
    entity_csv_path = os.path.join(
        test_files_dir, 'ddrimport-entity-new.csv'
    )
    out = batch.Importer.import_entities(
        entity_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT
    )
    print(out)
    out_ids = [o.id for o in out]
    assert out_ids == EXPECTED_ENTITY_IDS

#def test_update_entities(tmpdir, collection, test_files_dir):
#def test_import_files(tmpdir, collection, test_files_dir):
#def test_update_files(tmpdir, collection, test_files_dir):
