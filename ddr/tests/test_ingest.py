import os
from pathlib import Path
import shutil
import urllib

import git
from nose.tools import assert_raises
import pytest

from DDR import dvcs
from DDR import identifier
from DDR import ingest
from DDR.models import Entity, File
from DDR import util

IMG_URL = 'https://web.archive.org/web/20011221151014im_/http://densho.org/images/logo.jpg'
IMG_FILENAME = 'test-imaging.jpg'
# checksums of test file test-imaging.jpg
IMG_SIZE   = 12529
IMG_MD5    = 'c034f564e3ae1b603270cd332c3e858d'
IMG_SHA1   = 'f7ab5eada2e30f274b0b3166d658fe7f74f22b65'
IMG_SHA256 = 'b27c8443d393392a743c57ee348a29139c61f0f5d5363d6cfb474f35fcba2174'
IMG_XMP    = None

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

def test_check_dir(tmpdir, entity_identifier):
    log = util.FileLogger(identifier=entity_identifier, base_dir=str(tmpdir))
    label = 'testing'
    assert ingest.check_dir('tmp', '/tmp', log)
    assert_raises(
        Exception,
        ingest.check_dir, 'var', '/var', log
    )

def test_checksums(tmpdir, test_base_dir, entity_identifier, test_image):
    log = util.FileLogger(identifier=entity_identifier, base_dir=test_base_dir)
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
    assert TMP_PATH_REL in str(out)

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
    log = util.FileLogger(identifier=entity_identifier, base_dir=test_base_dir)
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

def test_copy_to_file_path(test_base_dir, entity_identifier):
    file_ = identifier.Identifier('ddr-test-123-456-master-abc123', test_base_dir).object()
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    # prep
    src_path = Path(test_base_dir) / 'src' / 'somefile.tif'
    tmp_path = Path(test_base_dir) / 'tmp' / 'somefile.tif'
    tmp_path_renamed = Path(test_base_dir) / 'ddr-test-123-456-master-abc123.tif'
    src_dir = src_path.parent
    tmp_dir = tmp_path.parent
    # clean slate
    if src_dir.exists():
        shutil.rmtree(src_dir, ignore_errors=True)
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(src_dir)
    os.makedirs(tmp_dir)
    with src_path.open('w') as f:
        f.write('test_copy_to_file_path')
    ingest.copy_to_file_path(file_, str(src_path), log)

def test_make_access_file(test_base_dir, entity_identifier, test_image):
    src_path = test_image
    access_path = os.path.join(test_base_dir, '%s-a.jpg' % FILE_ID)
    # inputs
    log = util.FileLogger(identifier=entity_identifier, base_dir=test_base_dir)
    # no src_path so fails
    missing_file = os.path.join(test_base_dir, 'src', 'somefile.png')
    assert ingest.make_access_file(missing_file, access_path, log) == None
    # get test jpg
    assert ingest.make_access_file(src_path, access_path, log) == access_path

def test_write_object_metadata(test_base_dir, entity_identifier):
    obj = identifier.Identifier('ddr-test-123-456-master-abc123', test_base_dir).object()
    tmp_dir = test_base_dir
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    tmp_json = ingest.write_object_metadata(obj, tmp_dir, log)
    assert Path(tmp_json).exists()

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
    log = util.FileLogger(identifier=entity_identifier, base_dir=test_base_dir)
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

def test_rename_in_place(entity_identifier, test_base_dir, test_image):
    src_path = Path(test_image).parent / 'rename-image.jpg'
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    #
    with src_path.open('w') as f:
        f.write('test_rename_in_place')
    new_path = src_path.parent / 'new.jpg'
    #
    assert src_path.exists() and not new_path.exists()
    ingest.rename_in_place(src_path, new_path, log)
    assert new_path.exists() and not src_path.exists()

def test_copy_in_place(entity_identifier, test_base_dir, test_image):
    src_path = Path(test_image).parent / 'rename-image.jpg'
    file_ = identifier.Identifier('ddr-test-123-456-master-abc123', test_base_dir).object()
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    #
    with src_path.open('w') as f:
        f.write('test_rename_in_place')
    new_path = src_path.parent / f'{file_.id}{src_path.suffix}'
    #
    assert src_path.exists() and not new_path.exists()
    ingest.copy_in_place(src_path, file_, log)
    assert src_path.exists() and new_path.exists()

def test_predict_staged():
    already = ['a', 'b']
    planned = ['a', 'c', 'd']
    expected = ['a', 'b', 'c', 'd']
    assert ingest.predict_staged(already, planned) == expected

def test_stage_files(entity_identifier, test_base_dir, test_image):
    entity = Entity(
        entity_identifier.path_abs(), entity_identifier.id, entity_identifier
    )
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    print(f'{log=}')
    # set up repo
    repo_dir = Path(test_base_dir) / 'test_stage_files_repo'
    repo_dir.mkdir()
    print(f'{repo_dir=}')
    repo = git.Repo.init(repo_dir)
    repo.git.annex('init')
    print(f'{repo=}')
    entity.collection_path = repo_dir
    # prep text files
    file0 = repo_dir / 'file0'
    file1 = repo_dir / 'file1'
    file2 = repo_dir / 'file2'
    file3 = repo_dir / 'file3'
    with file0.open('w') as f:
        f.write('test_repo_status_0')
    with file1.open('w') as f:
        f.write('test_repo_status_1')
    with file2.open('w') as f:
        f.write('test_repo_status_2')
    with file3.open('w') as f:
        f.write('test_repo_status_3')
    print(f'{file0=}')
    print(f'{file0.exists()=}')
    print(f'{file1=}')
    print(f'{file1.exists()=}')
    print(f'{file2=}')
    print(f'{file2.exists()=}')
    print(f'{file3=}')
    print(f'{file3.exists()=}')
    # prep binary file
    test_image = Path(test_image)
    print(f'{test_image=}')
    file4 = file1.parent / f'file4{test_image.suffix}'
    shutil.copyfile(test_image, file4)
    print(f'{file4=}')
    print(f'{file4.exists()=}')
    # stage files0-2, leave file3 untracked
    git_files = [str(file0), str(file1), str(file2)]
    annex_files = [str(file4)]
    print(f'{git_files=}')
    print(f'{annex_files=}')
    repo = ingest.stage_files(
        entity=entity, git_files=git_files, annex_files=annex_files,
        log=log, show_staged=0
    )
    print(f'{repo=}')
    # modify file2
    with file2.open('w') as f:
        f.write('test_repo_status_2_modified')
    # assert
    staged = dvcs.list_staged(repo) == ['file0', 'file1', 'file2', 'file4.jpg']
    modified = dvcs.list_modified(repo) == ['file2']
    untracked = dvcs.list_untracked(repo) == ['file3']

def test_repo_status(entity_identifier, test_base_dir):
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    # set up repo
    repo_dir = Path(test_base_dir) / 'test_repo_status_repo'
    repo_dir.mkdir()
    print(f'{repo_dir=}')
    import git
    repo = git.Repo.init(repo_dir)
    print(f'{repo=}')
    # prep files
    file0 = repo_dir / 'file0'
    file1 = repo_dir / 'file1'
    file2 = repo_dir / 'file2'
    with file0.open('w') as f:
        f.write('test_repo_status_0')
    with file1.open('w') as f:
        f.write('test_repo_status_1')
    with file2.open('w') as f:
        f.write('test_repo_status_2')
    # modify and stage
    repo.git.add(['file0'])
    # modify
    repo.git.add(['file1'])
    with file1.open('w') as f:
        f.write('test_repo_status_1 modified')
    #
    staged, modified, untracked = ingest.repo_status(repo, log)
    assert staged == ['file0', 'file1']
    assert modified == ['file1']
    assert untracked == ['file2']

def test_file_info(entity_identifier, test_base_dir, test_image):
    src_path = Path(test_image)
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    #
    print(f'{src_path=}')
    print(f'{src_path.exists()=}')
    #with src_path.open('w') as f:
    #    f.write('test_file_info')
    #
    size,md5,sha1,sha256,xmp = ingest.file_info(src_path, log)
    assert size == IMG_SIZE
    assert md5 == IMG_MD5
    assert sha1 == IMG_SHA1
    assert sha256 == IMG_SHA256
    assert xmp == IMG_XMP

class FakeEntity():
    def __init__(self, identifier):
        self.identifier = identifier

def test_file_identifier(entity_identifier, test_base_dir):
    entity = FakeEntity(identifier=entity_identifier)
    data = entity.identifier.idparts
    data['role'] = 'master'
    sha1 = IMG_SHA1[:10]
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    #
    fi = ingest.file_identifier(entity, data, sha1, log)
    assert fi
    assert fi.id == 'ddr-testing-123-4-master-f7ab5eada2'

def test_file_object(entity_identifier, file_identifier, test_base_dir, test_image):
    entity = Entity(
        entity_identifier.path_abs(), entity_identifier.id, entity_identifier
    )
    print(f'{entity=}')
    print(f'{entity.path_abs=}')
    data = entity.identifier.idparts
    data['role'] = 'master'
    data['id'] = file_identifier.id
    sha1 = IMG_SHA1[:10]
    src_path = Path(test_image)
    print(f'{src_path=}')
    log = ingest.addfile_logger(entity_identifier, base_dir=test_base_dir)
    #
    file_ = ingest.file_object(
        file_identifier, entity, data, src_path,
        IMG_SIZE, IMG_MD5, IMG_SHA1, IMG_SHA256, IMG_XMP,
        log
    )
    print(f'{file_}')
    assert file_.basename_orig == Path(test_image).name
    fname = f'{file_identifier.id}{src_path.suffix}'
    file_path_abs = Path(entity.path_abs) / 'files' / fname
    print(f'{str(file_path_abs)=}')
    print(f'{file_.path_abs=}')
    file_path_expected = file_path_abs.relative_to(
        file_path_abs.parent.parent.parent.parent.parent
    )
    file_path_out = Path(file_.path_abs).relative_to(
        Path(file_.path_abs).parent.parent.parent.parent.parent
    )
    print('{file_path_expected=}')
    print('{file_path_out=}')
    assert file_path_out == file_path_expected
    #assert file_.mimetype == IMG_MIMETYPE
    assert file_.size == IMG_SIZE
    assert file_.sha1 == IMG_SHA1
    assert file_.md5 == IMG_MD5
    assert file_.sha256 == IMG_SHA256
    assert file_.xmp == IMG_XMP
    assert file_.id == file_identifier.id

# TODO def test_add_local_file():
# TODO def test_add_external_file():
# TODO def test_add_access():
# TODO def test_add_file_commit():
