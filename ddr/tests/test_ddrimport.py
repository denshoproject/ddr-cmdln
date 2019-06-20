# -*- coding: utf-8 -*-

import os
import shutil

from deepdiff import DeepDiff
import git
import pytest

from DDR import batch
from DDR import config
from DDR import fileio
from DDR import dvcs
from DDR import identifier
from DDR.models import Collection

COLLECTION_ID = 'ddr-testing-123'
VOCABS_URL = config.VOCABS_URL
GIT_USER = 'gjost'
GIT_MAIL = 'gjost@densho.org'
AGENT = 'pytest'
TEST_CSV_DIR = os.path.join(
    os.getcwd(), 'ddr-cmdln/ddr/tests/ddrimport'
)
TEST_FILES_DIR = os.path.join(
    os.getcwd(), 'ddr-cmdln-assets'
)
TEST_FILES_TMP = 'ddrimport'


@pytest.fixture(scope="session")
def test_csv_dir(tmpdir_factory):
    fn = tmpdir_factory.mktemp(TEST_FILES_TMP)
    dest_dir = str(fn)
    #shutil.copytree(TEST_CSV_DIR, dest_dir, dirs_exist_ok=True)
    for f in os.listdir(TEST_CSV_DIR):
        src_file = os.path.join(TEST_CSV_DIR, f)
        shutil.copy(src_file, dest_dir)
    return dest_dir

@pytest.fixture(scope="session")
def test_files_dir(tmpdir_factory):
    fn = tmpdir_factory.mktemp(TEST_FILES_TMP)
    dest_dir = str(fn)
    #shutil.copytree(TEST_FILES_DIR, dest_dir, dirs_exist_ok=True)
    for f in os.listdir(TEST_FILES_DIR):
        src_file = os.path.join(TEST_FILES_DIR, f)
        if not os.path.isfile(src_file):
            continue
        shutil.copy(src_file, dest_dir)
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

def test_import_entities(tmpdir, collection, test_csv_dir, test_files_dir):
    entity_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-entity-new.csv'
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

def test_update_entities(tmpdir, collection, test_csv_dir, test_files_dir):
    entity_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-entity-update.csv'
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

EXPECTED_STAGED = [
    'files/ddr-testing-123-1/changelog',
    'files/ddr-testing-123-1/entity.json',
    'files/ddr-testing-123-1/mets.xml',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce-a.jpg',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce.pdf',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967-a.jpg',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967.tif',
    'files/ddr-testing-123-2/changelog',
    'files/ddr-testing-123-2/entity.json',
    'files/ddr-testing-123-2/mets.xml',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef-a.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce-a.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce.pdf',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-de47cb83a4.htm',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-de47cb83a4.json',
    'files/ddr-testing-123-3/changelog',
    'files/ddr-testing-123-3/entity.json',
    'files/ddr-testing-123-3/mets.xml',
    'files/ddr-testing-123-4/changelog',
    'files/ddr-testing-123-4/entity.json',
    'files/ddr-testing-123-4/mets.xml',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/changelog',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/entity.json',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/mets.xml',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/files/ddr-testing-123-4-1-transcript-de47cb83a4.htm',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/files/ddr-testing-123-4-1-transcript-de47cb83a4.json',
    'files/ddr-testing-123-5/changelog',
    'files/ddr-testing-123-5/entity.json',
    'files/ddr-testing-123-5/mets.xml',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-master-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-mezzanine-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-mezzanine-ea2f8d4f4d.mp3',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce-a.jpg',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce.pdf',
    'files/ddr-testing-123-6/changelog',
    'files/ddr-testing-123-6/entity.json',
    'files/ddr-testing-123-6/mets.xml',
    'files/ddr-testing-123-6/files/ddr-testing-123-6-master-9bd65ab22c.json',
    'files/ddr-testing-123-6/files/ddr-testing-123-6-master-9bd65ab22c.mp4',
]

# TODO confirm that file updates update the parents
def test_import_files(tmpdir, collection, test_csv_dir, test_files_dir):
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-file-new.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-file-new.log'
    )
    out = batch.Importer.import_files(
        file_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT,
        log_path=log_path,
        tmp_dir=test_files_dir,
    )
    repo = dvcs.repository(collection.path_abs)
    staged = dvcs.list_staged(repo)
    print('staged %s' % staged)
    ddiff = DeepDiff(staged, EXPECTED_STAGED, ignore_order=True)
    print('DDIFF')
    print(ddiff)
    assert not ddiff
    
# TODO confirm that file updates update the parents
def test_update_files(tmpdir, collection, test_csv_dir, test_files_dir):
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-file-update.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-file-update.log'
    )
    out = batch.Importer.import_files(
        file_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT,
        log_path=log_path,
        tmp_dir=test_files_dir,
    )
    repo = dvcs.repository(collection.path_abs)
    staged = sorted(dvcs.list_staged(repo))
    print('staged %s' % staged)
    ddiff = DeepDiff(staged, EXPECTED_STAGED, ignore_order=True)
    print('DDIFF')
    print(ddiff)
    assert not ddiff

def rewrite_file_paths(path, test_files_dir):
    """Load the import CSV, prepend pytest tmpdir to basename_orig
    """
    rows = fileio.read_csv(path)
    headers = rows.pop(0)
    
    basename_index = headers.index('basename_orig')
    for row in rows:
        src = os.path.join(
            test_files_dir, row[basename_index]
        )
        row[basename_index] = src
        print(src)
    
    fileio.write_csv(path, headers, rows)
