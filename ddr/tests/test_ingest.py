import os
import shutil
import urllib

from nose.tools import assert_raises
import pytest

from DDR import identifier
from DDR import ingest
from DDR.models import Entity, File

IMG_URL = 'https://web.archive.org/web/20011221151014im_/http://densho.org/images/logo.jpg'
IMG_FILENAME = 'test-imaging.jpg'
# checksums of test file test-imaging.jpg
IMG_MD5    = 'c034f564e3ae1b603270cd332c3e858d'
IMG_SHA1   = 'f7ab5eada2e30f274b0b3166d658fe7f74f22b65'
IMG_SHA256 = 'b27c8443d393392a743c57ee348a29139c61f0f5d5363d6cfb474f35fcba2174'

COLLECTION_ID = 'ddr-testing-123'
ENTITY_ID     = 'ddr-testing-123-4'
FILE_ID       = 'ddr-testing-123-4-master-a1b2c3'
LOGPATH_REL = os.path.join(
    'addfile',
    COLLECTION_ID,
    '%s.log' % ENTITY_ID
)

TMP_PATH_REL = os.path.join(
    'tmp/file-add/%s/%s/testfile.tif' % (COLLECTION_ID, ENTITY_ID)
)


@pytest.fixture(scope="session")
def test_base_dir(tmpdir_factory):
    return str(tmpdir_factory.mktemp('ingest'))

@pytest.fixture(scope="session")
def test_image(tmpdir_factory):
    base_dir =  str(tmpdir_factory.mktemp('ingest'))
    img_path = os.path.join(base_dir, IMG_FILENAME)
    if not os.path.exists(img_path):
        urllib.request.urlretrieve(
            IMG_URL,
            os.path.join(base_dir, IMG_FILENAME)
        )
    return img_path

@pytest.fixture(scope="session")
def logpath(tmpdir_factory):
    return os.path.join(
        str(tmpdir_factory.mktemp('addfile')), COLLECTION_ID, '%s.log' % ENTITY_ID
    )

@pytest.fixture(scope="session")
def entity_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(ENTITY_ID, str(tmp))

@pytest.fixture(scope="session")
def file_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(FILE_ID, str(tmp))


class TestAddFileLogger():
    
    def test_repr(self, tmpdir, entity_identifier):
        log = ingest.addfile_logger(
            identifier=entity_identifier, base_dir=str(tmpdir)
        )
        out = log.__repr__()
        assert LOGPATH_REL in out

    # TODO def test_entry(self):
    # TODO def test_ok(self):
    # TODO def test_not_ok(self):
    # TODO def test_log(self):
    # TODO def test_crash(self):


def test_file_import_actions():
    rowd00a = {
        'external': False,
    }
    expected00a = ingest.FILE_IMPORT_ACTIONS['bin,local,noattrs']
    out00a = ingest.import_actions(rowd00a, True)
    assert out00a == expected00a
    rowd00b = {
        'external': 0,
        'md5':'', 'sha1':'', 'sha256':'', 'size':'',
    }
    expected00b = ingest.FILE_IMPORT_ACTIONS['bin,local,noattrs']
    out00b = ingest.import_actions(rowd00b, True)
    assert out00b == expected00b
    rowd01 = {
        'external': False,
        'md5':'abc', 'sha1':'abc', 'sha256':'abc', 'size':'abc',
    }
    expected01 = ingest.FILE_IMPORT_ACTIONS['bin,local,attrs']
    out01 = ingest.import_actions(rowd01, True)
    assert out01 == expected01
    rowd10 = {
        'external': True,
    }
    expected10 = ingest.FILE_IMPORT_ACTIONS['bin,external,noattrs']
    out10 = ingest.import_actions(rowd10,True)
    assert out10 == expected10
    rowd11 = {
        'external': True,
        'md5':'abc', 'sha1':'abc', 'sha256':'abc', 'size':'abc',
    }
    expected11 = ingest.FILE_IMPORT_ACTIONS['bin,external,attrs']
    out11 = ingest.import_actions(rowd11, True)
    assert out11 == expected11

def test_log_path(tmpdir, logpath, entity_identifier):
    out = ingest._log_path(entity_identifier, base_dir=str(tmpdir))
    assert LOGPATH_REL in out

def test_addfile_logger(tmpdir, logpath, entity_identifier):
    out0 = ingest.addfile_logger(
        identifier=entity_identifier, base_dir=str(tmpdir)
    ).logpath
    print('out0 %s' % out0)
    assert LOGPATH_REL in out0

    #out1 = ingest.addfile_logger(
    #    log_path=logpath, base_dir=str(tmpdir)
    #).logpath
    #print('out1 %s' % out1)
    #print('LOGPATH_REL %s' % LOGPATH_REL)
    #assert LOGPATH_REL in out1
    #assert False

def test_check_dir(tmpdir, entity_identifier):
    log = ingest.addfile_logger(entity_identifier, base_dir=str(tmpdir))
    label = 'testing'
    assert ingest.check_dir('tmp', '/tmp', log)
    assert_raises(
        Exception,
        ingest.check_dir, 'var', '/var', log
    )

def test_checksums(tmpdir, test_base_dir, entity_identifier, test_image):
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    md5,sha1,sha256 = ingest.checksums(
        test_image,
        log
    )
    assert md5    == IMG_MD5
    assert sha1   == IMG_SHA1
    assert sha256 == IMG_SHA256

def test_destination_path(test_base_dir, file_identifier):
    src_path = os.path.join(test_base_dir, 'testfile.tif')
    dest_filename = '%s%s' % (
        file_identifier.id,
        os.path.splitext(src_path)[1]
    )
    expected = os.path.join(
        test_base_dir,
        dest_filename
    )
    out = ingest.destination_path(src_path, test_base_dir, file_identifier)
    assert out == expected

def test_temporary_path(test_base_dir, file_identifier):
    src_path = os.path.join(test_base_dir, 'testfile.tif')
    out = ingest.temporary_path(src_path, test_base_dir, file_identifier)
    assert TMP_PATH_REL in out

def test_temporary_path_renamed(test_base_dir):
    src_path = os.path.join(test_base_dir, 'testfile.tif')
    dest_path = os.path.join(test_base_dir, '%s.tif' % FILE_ID)
    out = ingest.temporary_path_renamed(src_path, dest_path)
    assert out == dest_path

def test_access_path(test_base_dir, file_identifier):
    expected = os.path.join(test_base_dir, '%s-a.jpg' % FILE_ID)
    out = ingest.access_path(
        file_identifier.object_class(),
        os.path.join(test_base_dir, '%s.tif' % FILE_ID)
    )
    print('out      %s' % out)
    print('expected %s' % expected)
    assert out == expected

def test_copy_to_workdir(test_base_dir, entity_identifier):
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    # prep
    src_path = os.path.join(test_base_dir, 'src', 'somefile.tif')
    tmp_path = os.path.join(test_base_dir, 'tmp', 'somefile.tif')
    tmp_path_renamed = os.path.join(test_base_dir, 'ddr-test-123-456-master-abc123.tif')
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

def test_make_access_file(test_base_dir, entity_identifier, test_image):
    src_path = test_image
    access_path = os.path.join(test_base_dir, '%s-a.jpg' % FILE_ID)
    # inputs
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    # no src_path so fails
    missing_file = os.path.join(test_base_dir, 'src', 'somefile.png')
    assert ingest.make_access_file(missing_file, access_path, log) == None
    # get test jpg
    assert ingest.make_access_file(src_path, access_path, log) == access_path

# TODO def test_write_object_metadata():

def test_move_files(test_base_dir, entity_identifier):
    # this seems way too complicated
    # inputs
    files = [
        (
            os.path.join(test_base_dir, 'src', 'file1.txt'),
            os.path.join(test_base_dir, 'dest', 'file1.txt')
        ),
        (
            os.path.join(test_base_dir, 'src', 'file2.txt'),
            os.path.join(test_base_dir, 'dest', 'file2.txt')
        ),
    ]
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
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
