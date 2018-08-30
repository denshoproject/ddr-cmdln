import os
import shutil
import urllib

from nose.tools import assert_raises

import config
import identifier
import ingest
from models import Entity, File

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'ingest')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

# TODO test_AddFileLogger_entry

#def test_AddFileLogger_ok():
    

# TODO test_AddFileLogger_not_ok
# TODO test_AddFileLogger_log
# TODO test_AddFileLogger_crash

TEST_IMG_URL = 'https://web.archive.org/web/20011221151014im_/http://densho.org/images/logo.jpg'
TEST_IMG_PATH = os.path.join(TESTING_BASE_DIR, 'test-imaging.jpg')

def test_log_path():
    eid = 'ddr-test-123-456'
    expected = os.path.join(TESTING_BASE_DIR, 'addfile/ddr-test-123/ddr-test-123-456.log')
    out = ingest._log_path(identifier.Identifier(eid), TESTING_BASE_DIR)
    assert out == expected

# TODO test_addfile_logger

def test_check_dir():
    eid = 'ddr-test-123-456'
    log = ingest.addfile_logger(identifier.Identifier(eid), base_dir=TESTING_BASE_DIR)
    label = 'testing'
    assert ingest.check_dir('tmp', '/tmp', log)
    assert_raises(
        Exception,
        ingest.check_dir, 'var', '/var', log
    )

# TODO test_checksums

def test_destination_path():
    src_path = os.path.join(TESTING_BASE_DIR, 'somefile.tif')
    fidentifier = identifier.Identifier('ddr-test-123-456-master-abcde12345')
    expected = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abcde12345.tif')
    out = ingest.destination_path(src_path, TESTING_BASE_DIR, fidentifier)
    print('out      %s' % out)
    print('expected %s' % expected)
    assert out == expected

def test_temporary_path():
    src_path = os.path.join(TESTING_BASE_DIR, 'somefile.tif')
    fidentifier = identifier.Identifier('ddr-test-123-456-master-abc123')
    expected = os.path.join(TESTING_BASE_DIR, 'tmp/file-add/ddr-test-123/ddr-test-123-456/somefile.tif')
    out = ingest.temporary_path(src_path, TESTING_BASE_DIR, fidentifier)
    print('out      %s' % out)
    print('expected %s' % expected)
    assert out == expected

def test_temporary_path_renamed():
    tmp_path = os.path.join(TESTING_BASE_DIR, 'somefile.tif')
    dest_path = os.path.join(TESTING_BASE_DIR, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-abc123.tif')
    expected = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123.tif')
    out = ingest.temporary_path_renamed(tmp_path, dest_path)
    print('out      %s' % out)
    print('expected %s' % expected)
    assert out == expected

def test_access_path():
    fidentifier = identifier.Identifier('ddr-test-123-456-master-abc123')
    file_class = fidentifier.object_class()
    tmp_path_renamed = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123.tif')
    expected = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123-a.jpg')
    out = ingest.access_path(file_class, tmp_path_renamed)
    print('out      %s' % out)
    print('expected %s' % expected)
    assert out == expected

def test_copy_to_workdir():
    eid = 'ddr-test-123-456-master-abc123'
    entity = identifier.Identifier(eid)
    # inputs
    src_path = os.path.join(TESTING_BASE_DIR, 'src', 'somefile.tif')
    tmp_path = os.path.join(TESTING_BASE_DIR, 'tmp', 'somefile.tif')
    tmp_path_renamed = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123.tif')
    log = ingest.addfile_logger(entity, base_dir=TESTING_BASE_DIR)
    # prep
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
    src_path = os.path.join(TESTING_BASE_DIR, 'src', 'somefile.png')
    access_dest_path = os.path.join(TESTING_BASE_DIR, 'ddr-test-123-456-master-abc123-a.jpg')
    expected = access_dest_path
    log = ingest.addfile_logger(identifier.Identifier('ddr-test-123-456'), base_dir=TESTING_BASE_DIR)
    # no src_path so fails
    assert ingest.make_access_file(src_path, access_dest_path, log) == None
    # prep
    if os.path.exists(access_dest_path):
        os.remove(access_dest_path)
    # arrange test jpg
    src_path = TEST_IMG_PATH
    parent_dir = os.path.dirname(src_path)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    if not os.path.exists(src_path):
        urllib.urlretrieve(TEST_IMG_URL, TEST_IMG_PATH)
    assert ingest.make_access_file(src_path, access_dest_path, log) == access_dest_path
    # clean up
    if os.path.exists(access_dest_path):
        os.remove(access_dest_path)

# TODO test_write_object_metadata

def test_move_files():
    # this seems way too complicated
    # inputs
    files = [
        (os.path.join(TESTING_BASE_DIR, 'src', 'file1.txt'), os.path.join(TESTING_BASE_DIR, 'dest', 'file1.txt')),
        (os.path.join(TESTING_BASE_DIR, 'src', 'file2.txt'), os.path.join(TESTING_BASE_DIR, 'dest', 'file2.txt')),
    ]
    log = ingest.addfile_logger(identifier.Identifier('ddr-test-123-456'), base_dir=TESTING_BASE_DIR)
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

def reverse_files_list():
    files = [
        ('a', 'b'),
        ('c', 'd'),
    ]
    reverse = [
        ('b', 'a'),
        ('d', 'c'),
    ]
    assert ingest.reverse_files_list(files) == reverse

# TODO test_move_new_files_back
# TODO test_move_existing_files_back

def test_predict_staged():
    already = ['a', 'b']
    planned = ['a', 'c', 'd']
    expected = ['a', 'b', 'c', 'd']
    assert ingest.predict_staged(already, planned) == expected

# TODO test_stage_files
# TODO test_add_file
# TODO test_add_access
# TODO test_add_file_commit
