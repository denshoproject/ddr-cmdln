import os
import shutil
import urllib

from nose.tools import assert_raises

from DDR import config
from DDR import identifier
from DDR import ingest
from DDR.models import Entity, File

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'ingest')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

IMG_URL = 'https://web.archive.org/web/20011221151014im_/http://densho.org/images/logo.jpg'
IMG_PATH = os.path.join(TESTING_BASE_DIR, 'test-imaging.jpg')
# checksums of test file test-imaging.jpg
IMG_MD5    = 'c034f564e3ae1b603270cd332c3e858d'
IMG_SHA1   = 'f7ab5eada2e30f274b0b3166d658fe7f74f22b65'
IMG_SHA256 = 'b27c8443d393392a743c57ee348a29139c61f0f5d5363d6cfb474f35fcba2174'

COLLECTION_ID = 'ddr-testing-123'
ENTITY_ID     = 'ddr-testing-123-4'
FILE_ID       = 'ddr-testing-123-4-master-a1b2c3'
COLLECTION_DIR = os.path.join(TESTING_BASE_DIR, COLLECTION_ID)
ENTITY_IDENTIFIER = identifier.Identifier(
    id=ENTITY_ID, base_path=TESTING_BASE_DIR
)
FILE_IDENTIFIER = identifier.Identifier(
    id=FILE_ID, base_path=TESTING_BASE_DIR
)
LOGPATH = os.path.join(
    TESTING_BASE_DIR, 'addfile', COLLECTION_ID,
    '%s.log' % ENTITY_ID
)

SRC_PATH = os.path.join(TESTING_BASE_DIR, 'testfile.tif')
DEST_PATH = os.path.join(TESTING_BASE_DIR, '%s.tif' % FILE_ID)
TMP_PATH = os.path.join(
    TESTING_BASE_DIR,
    'tmp/file-add/%s/%s/testfile.tif' % (COLLECTION_ID, ENTITY_ID)
)
TMP_PATH_RENAMED = os.path.join(TESTING_BASE_DIR, '%s.tif' % FILE_ID)
ACCESS_PATH = os.path.join(TESTING_BASE_DIR, '%s-a.jpg' % FILE_ID)


class TestAddFileLogger():
    
    def test_repr(self):
        log = ingest.addfile_logger(
            identifier=ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR
        )
        out = log.__repr__()
        expected = "<DDR.ingest.AddFileLogger '%s'>" % LOGPATH
        assert out == expected

    # TODO def test_entry(self):
    # TODO def test_ok(self):
    # TODO def test_not_ok(self):
    # TODO def test_log(self):
    # TODO def test_crash(self):


def test_log_path():
    out = ingest._log_path(ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR)
    assert out == LOGPATH

def test_addfile_logger():
    out0 = ingest.addfile_logger(
        identifier=ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR
    ).logpath
    print(out0)
    assert out0 == LOGPATH
    
    out1 = ingest.addfile_logger(
        log_path=LOGPATH, base_dir=TESTING_BASE_DIR
    ).logpath
    print(out1)
    assert out1 == LOGPATH

def test_check_dir():
    log = ingest.addfile_logger(ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR)
    label = 'testing'
    assert ingest.check_dir('tmp', '/tmp', log)
    assert_raises(
        Exception,
        ingest.check_dir, 'var', '/var', log
    )

def test_checksums():
    md5,sha1,sha256 = ingest.checksums(
        IMG_PATH,
        ingest.addfile_logger(
            identifier=ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR
        )
    )
    assert md5    == IMG_MD5
    assert sha1   == IMG_SHA1
    assert sha256 == IMG_SHA256

def test_destination_path():
    out = ingest.destination_path(SRC_PATH, TESTING_BASE_DIR, FILE_IDENTIFIER)
    print('out      %s' % out)
    print('expected %s' % DEST_PATH)
    assert out == DEST_PATH

def test_temporary_path():
    out = ingest.temporary_path(SRC_PATH, TESTING_BASE_DIR, FILE_IDENTIFIER)
    print('out      %s' % out)
    print('expected %s' % TMP_PATH)
    assert out == TMP_PATH

def test_temporary_path_renamed():
    out = ingest.temporary_path_renamed(SRC_PATH, DEST_PATH)
    print('out      %s' % out)
    print('expected %s' % TMP_PATH_RENAMED)
    assert out == TMP_PATH_RENAMED

def test_access_path():
    out = ingest.access_path(FILE_IDENTIFIER.object_class(), TMP_PATH_RENAMED)
    print('out      %s' % out)
    print('expected %s' % ACCESS_PATH)
    assert out == ACCESS_PATH

def test_copy_to_workdir():
    log = ingest.addfile_logger(ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR)
    # prep
    src_path = os.path.join(TESTING_BASE_DIR, 'src', 'somefile.tif')
    tmp_path = os.path.join(TESTING_BASE_DIR, 'tmp', 'somefile.tif')
    tmp_path_renamed = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123.tif')
    src_dir = os.path.dirname(src_path)
    tmp_dir = os.path.dirname(tmp_path)
    # clean slate
    if os.path.exists(src_dir):
        shutil.rmtree(src_dir, ignore_errors=True)
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(src_dir)
    os.makedirs(tmp_dir)
    with open(src_path, 'w') as f:
        f.write('test_copy_to_workdir')
    # tests
    assert os.path.exists(src_path)
    ingest.copy_to_workdir(src_path, tmp_path, tmp_path_renamed, log)
    assert os.path.exists(src_path)
    assert not os.path.exists(tmp_path)
    assert os.path.exists(tmp_path_renamed)
    # clean up
    if os.path.exists(src_dir):
        shutil.rmtree(src_dir, ignore_errors=True)
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

def test_make_access_file():
    # inputs
    log = ingest.addfile_logger(ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR)
    # prep
    if os.path.exists(ACCESS_PATH):
        os.remove(ACCESS_PATH)
    # no src_path so fails
    missing_file = os.path.join(TESTING_BASE_DIR, 'src', 'somefile.png')
    assert ingest.make_access_file(missing_file, ACCESS_PATH, log) == None
    # get test jpg
    src_path = IMG_PATH
    parent_dir = os.path.dirname(src_path)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    if not os.path.exists(src_path):
        urllib.urlretrieve(IMG_URL, IMG_PATH)
    assert ingest.make_access_file(src_path, ACCESS_PATH, log) == ACCESS_PATH
    # clean up
    if os.path.exists(ACCESS_PATH):
        os.remove(ACCESS_PATH)

# TODO def test_write_object_metadata():

def test_move_files():
    # this seems way too complicated
    # inputs
    files = [
        (os.path.join(TESTING_BASE_DIR, 'src', 'file1.txt'), os.path.join(TESTING_BASE_DIR, 'dest', 'file1.txt')),
        (os.path.join(TESTING_BASE_DIR, 'src', 'file2.txt'), os.path.join(TESTING_BASE_DIR, 'dest', 'file2.txt')),
    ]
    log = ingest.addfile_logger(ENTITY_IDENTIFIER, base_dir=TESTING_BASE_DIR)
    # fresh start
    for tmp,dest in files:
        shutil.rmtree(os.path.dirname(tmp), ignore_errors=True)
        shutil.rmtree(os.path.dirname(dest), ignore_errors=True)
    for tmp,dest in files:
        tmp_dir = os.path.dirname(tmp)
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        dest_dir = os.path.dirname(dest)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        with open(tmp, 'w') as f:
            f.write('test file 1')
    # test
    for tmp,dest in files:
        assert os.path.exists(tmp)
        assert not os.path.exists(dest)
    ingest.move_files(files, log)
    for tmp,dest in files:
        assert not os.path.exists(tmp)
        assert os.path.exists(dest)
    # clean up
    for tmp,dest in files:
        shutil.rmtree(os.path.dirname(tmp), ignore_errors=True)
        shutil.rmtree(os.path.dirname(dest), ignore_errors=True)

def test_reverse_files_list():
    files = [
        ('a', 'b'),
        ('c', 'd'),
    ]
    reverse = [
        ('b', 'a'),
        ('d', 'c'),
    ]
    assert ingest.reverse_files_list(files) == reverse

# TODO def test_move_new_files_back():
# TODO def test_move_existing_files_back():

def test_predict_staged():
    already = ['a', 'b']
    planned = ['a', 'c', 'd']
    expected = ['a', 'b', 'c', 'd']
    assert ingest.predict_staged(already, planned) == expected

# TODO def test_stage_files():
# TODO def test_add_local_file():
# TODO def test_add_external_file():
# TODO def test_add_access():
# TODO def test_add_file_commit():
