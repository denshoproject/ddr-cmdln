# -*- coding: utf-8 -*-

from collections import OrderedDict
import os
import shutil

from deepdiff import DeepDiff
import git
import pytest

from DDR import batch
from DDR import config
from DDR import csvfile
from DDR import fileio
from DDR import dvcs
from DDR import identifier
from DDR.models import Collection
from DDR import util

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
    # save and commit
    git_files = []
    for o in out:
        exit,status,updated_files = o.save(
            'pytest', 'pytest@densho.org', 'pytest',
            collection=collection,
            commit=False
        )
        print(o, status)
        git_files += updated_files
    repo = dvcs.repository(collection.path_abs)
    dvcs.stage(repo, git_files)
    commit = repo.index.commit('test_import_entities')

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
    # save and commit
    git_files = []
    for o in out:
        exit,status,updated_files = o.save(
            'pytest', 'pytest@densho.org', 'pytest',
            collection=collection,
            commit=False
        )
        print(o, status)
        git_files += updated_files
    repo = dvcs.repository(collection.path_abs)
    dvcs.stage(repo, git_files)
    commit = repo.index.commit('test_update_entities')

EXPECTED_FILES_IMPORT_EXTERNAL = [
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.json',
]

def test_files_import_external(tmpdir, collection, test_csv_dir, test_files_dir):
    """Test importing *external* files
    """
    print('collection_path %s' % collection.path_abs)
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-files-import-external.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-files-import-external.log'
    )
    out = batch.Importer.import_files(
        file_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT,
        log_path=log_path,
        tmp_dir=test_files_dir,
    )
    # save and commit
    repo = dvcs.repository(collection.path_abs)
    commit = repo.index.commit('test_files_import_external')
    print('commit %s' % commit)
    # test hashes present
    check_file_hashes(collection.path_abs)

def test_files_import_external_emptyhashes_nofile(tmpdir, collection, test_csv_dir, test_files_dir):
    """Test importing *external* files with *empty* hashes and no files - should fail
    """
    print('collection_path %s' % collection.path_abs)
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-files-import-external-emptyhashes-nofile.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-files-import-external-emptyhashes-nofile.log'
    )
    with pytest.raises(Exception):
        out = batch.Importer.import_files(
            file_csv_path,
            collection.identifier,
            VOCABS_URL,
            GIT_USER, GIT_MAIL, AGENT,
            log_path=log_path,
            tmp_dir=test_files_dir,
        )

def test_files_import_external_nohashes_nofile(tmpdir, collection, test_csv_dir, test_files_dir):
    """Test importing *external* files with *no* hash cols and no files - should fail
    """
    print('collection_path %s' % collection.path_abs)
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-files-import-external-nohashes-nofile.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-files-import-external-nohashes-nofile.log'
    )
    with pytest.raises(Exception):
        out = batch.Importer.import_files(
            file_csv_path,
            collection.identifier,
            VOCABS_URL,
            GIT_USER, GIT_MAIL, AGENT,
            log_path=log_path,
            tmp_dir=test_files_dir,
        )

EXPECTED_FILES_IMPORT_INTERNAL = [
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce.pdf',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce-a.jpg',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967.tif',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967-a.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef-a.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce.pdf',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce-a.jpg',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-de47cb83a4.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-de47cb83a4.htm',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/files/ddr-testing-123-4-1-transcript-de47cb83a4.json',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/files/ddr-testing-123-4-1-transcript-de47cb83a4.htm',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-master-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-master-ea2f8d4f4d.mp3',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-mezzanine-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-mezzanine-ea2f8d4f4d.mp3',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce.pdf',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce-a.jpg',
    'files/ddr-testing-123-6/files/ddr-testing-123-6-master-9bd65ab22c.json',
    'files/ddr-testing-123-6/files/ddr-testing-123-6-master-9bd65ab22c.mp4',
]

# TODO confirm that file updates update the parents
def test_files_import_internal(tmpdir, collection, test_csv_dir, test_files_dir):
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-files-import-internal.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-files-import-internal.log'
    )
    out = batch.Importer.import_files(
        file_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT,
        log_path=log_path,
        tmp_dir=test_files_dir,
    )
    # save and commit
    repo = dvcs.repository(collection.path_abs)
    commit = repo.index.commit('test_files_import_internal')
    # test hashes present
    check_file_hashes(collection.path_abs)

EXPECTED_FILES_IMPORT_INTERNAL_NOHASHES = [
    'files/ddr-testing-123-1/files/ddr-testing-123-1-mezzanine-b9773b9aef.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-mezzanine-b9773b9aef.jpg',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-mezzanine-b9773b9aef-a.jpg',
]

def test_files_import_internal_nohashes(tmpdir, collection, test_csv_dir, test_files_dir):
    """test CSV with hash columns (sha1/sha256/md5/size) removed
    """
    file_csv_path = os.path.join(
        test_csv_dir, 'ddrimport-files-import-internal-nohashes.csv'
    )
    rewrite_file_paths(file_csv_path, test_files_dir)
    log_path = os.path.join(
        test_files_dir, 'ddrimport-files-import-internal-nohashes.log'
    )
    out = batch.Importer.import_files(
        file_csv_path,
        collection.identifier,
        VOCABS_URL,
        GIT_USER, GIT_MAIL, AGENT,
        log_path=log_path,
        tmp_dir=test_files_dir,
    )
    # save and commit
    repo = dvcs.repository(collection.path_abs)
    commit = repo.index.commit('test_files_import_internal_nohashes')
    # test hashes present
    check_file_hashes(collection.path_abs)

EXPECTED_UPDATE_FILES = [
    'files/ddr-testing-123-1/changelog',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-administrative-775f8d2cce.json',
    'files/ddr-testing-123-1/files/ddr-testing-123-1-master-684e15e967.json',
    'files/ddr-testing-123-2/changelog',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-master-b9773b9aef.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-775f8d2cce.json',
    'files/ddr-testing-123-2/files/ddr-testing-123-2-mezzanine-de47cb83a4.json',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/changelog',
    'files/ddr-testing-123-4/files/ddr-testing-123-4-1/files/ddr-testing-123-4-1-transcript-de47cb83a4.json',
    'files/ddr-testing-123-5/changelog',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-master-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-mezzanine-ea2f8d4f4d.json',
    'files/ddr-testing-123-5/files/ddr-testing-123-5-transcript-775f8d2cce.json',
    'files/ddr-testing-123-6/changelog',
    'files/ddr-testing-123-6/files/ddr-testing-123-6-master-9bd65ab22c.json',
]

# TODO confirm that file updates update the parents
def test_update_files(tmpdir, collection, test_csv_dir, test_files_dir):
    hashes_before = collect_hashes(collection.path_abs)
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
    # test
    unstaged = []
    for path in EXPECTED_UPDATE_FILES:
        if path not in staged:
            unstaged.append(path)
    unstaged = sorted(unstaged)
    for n,path in enumerate(unstaged):
        print('UNSTAGED %s %s' % (n+1, path))
    print(repo)
    print(log_path)
    assert not unstaged
    # save and commit
    repo = dvcs.repository(collection.path_abs)
    commit = repo.index.commit('test_update_files')
    # test hashes present
    check_file_hashes(collection.path_abs)
    # test hashes not modified
    hashes_after = collect_hashes(collection.path_abs)
    check_hashes(hashes_before, hashes_after)


# helpers

def rewrite_file_paths(path, test_files_dir):
    """Load the import CSV, prepend pytest tmpdir to basename_orig
    """
    headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(path))
    for rowd in rowds:
        src = os.path.join(
            test_files_dir, rowd['basename_orig']
        )
        rowd['basename_orig'] = src
    headers,rows = csvfile.make_rows(rowds)
    fileio.write_csv(path, headers, rows)

def check_file_hashes(collection_path):
    """Check that hashes are present in file JSONs
    """
    paths = util.find_meta_files(
        collection_path, recursive=True, model='file', force_read=True
    )
    for path in paths:
        f = identifier.Identifier(path).object()
        if not f.sha1 and f.sha256 and f.md5 and f.size:
            print('f.sha1   %s' % f.sha1)
            print('f.sha256 %s' % f.sha256)
            print('f.md5    %s' % f.md5)
            print('f.size   %s' % f.size)
            raise Exception('Hash data missing')

def collect_hashes(collection_path):
    """Make dict of existing file hash data
    
    @param collection_path: str
    @returns: dict {file_id: {'sha1':..., 'sha256':..., 'md5':..., 'size':...}
    """
    paths = util.find_meta_files(
        collection_path, recursive=True, model='file', force_read=True
    )
    data = OrderedDict()
    for path in paths:
        o = identifier.Identifier(path).object()
        data[o.id] = OrderedDict()
        data[o.id]['sha1'] = o.sha1
        data[o.id]['sha256'] = o.sha256
        data[o.id]['md5'] = o.md5
        data[o.id]['size'] = o.size
    return data

def check_hashes(before, after):
    """Check if file hashes modified during file-update from CSV
    
    @param before: dict
    @param after: dict
    @returns: bool True if modified
    """
    keys_changed = False
    hashes_changed = False
    if not after.keys() == before.keys():
        keys_changed = True
        print('KEYS CHANGED: %s' % oid)
        print('BEFORE %s' % before.keys())
        print('AFTER  %s' % after.keys())
    assert after.keys() == before.keys()
    for oid in before.keys():
        if not before[oid] == after[oid]:
            hashes_changed = True
            print('HASHES CHANGED: %s' % oid)
            print('BEFORE %s' % before[oid])
            print('AFTER  %s' % after[oid])
    assert not (keys_changed or hashes_changed)
