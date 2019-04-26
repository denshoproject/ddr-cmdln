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

@pytest.fixture
def test_files_dir(tmpdir):
    dest_dir = str(tmpdir / TEST_FILES_TMP)
    shutil.copytree(TEST_FILES_DIR, dest_dir)
    return dest_dir

@pytest.fixture
def collection(tmpdir):
    collection_path = str(tmpdir / COLLECTION_ID)
    if os.path.exists(collection_path):
        return identifier.Identifier(collection_path).object()
    else:
        repo = dvcs.initialize_repository(
            collection_path, GIT_USER, GIT_MAIL
        )
        ci = identifier.Identifier(COLLECTION_ID, str(tmpdir))
        collection = Collection.create(ci)
        collection.save(GIT_USER, GIT_MAIL, AGENT)
        return collection

#def test_import_entities(tmpdir, collection, test_files_dir):
#def test_update_entities(tmpdir, collection, test_files_dir):
#def test_import_files(tmpdir, collection, test_files_dir):
#def test_update_files(tmpdir, collection, test_files_dir):
