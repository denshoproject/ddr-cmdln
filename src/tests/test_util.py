from datetime import datetime
import os
import shutil

import pytest

from DDR import util

SAMPLE_DIRS = [
    '.git',
    'files',
    'files/ddr-test-123-1',
    'files/ddr-test-123-2',
    'files/ddr-test-123-2/files',
]
SAMPLE_FILES = [
    'collection.json',
    '.git/config',
    '.gitignore',
    'files/ddr-test-123-1/entity.json',
    'files/ddr-test-123-1/changelog',
    'files/ddr-test-123-2/entity.json',
    'files/ddr-test-123-2/control',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.jpg',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.json',
]
META_ALL = [
    'collection.json',
    'files/ddr-test-123-1/entity.json',
    'files/ddr-test-123-2/entity.json',
    'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.json',
]
META_MODEL = {
    'collection': [
        'collection.json',
    ],
    'entity': [
        'files/ddr-test-123-1/entity.json',
        'files/ddr-test-123-2/entity.json',
    ],
    'file': [
        'files/ddr-test-123-2/files/ddr-test-123-2-master-abc123.json',
    ],
}

def test_find_meta_files(tmpdir):
    basedir = str(tmpdir / 'find-meta-files')
    if os.path.exists(basedir):
        shutil.rmtree(basedir, ignore_errors=1)
    
    # build sample repo
    sampledir = os.path.join(basedir, 'ddr-test-123')
    for d in SAMPLE_DIRS:
        path = os.path.join(sampledir, d)
        os.makedirs(path)
    for fn in SAMPLE_FILES:
        path = os.path.join(sampledir, fn)
        with open(path, 'w') as f:
            f.write('testing')
    
    # cache
    cache_path = os.path.join(basedir, 'cache')
    if os.path.exists(cache_path):
        os.remove(cache_path)
    assert not os.path.exists(cache_path)

    def clean(paths):
        base = '%s/' % sampledir
        cleaned = [path.replace(base, '') for path in paths]
        cleaned.sort()
        return cleaned
    
    paths0 = clean(util.find_meta_files(sampledir, recursive=True, force_read=True))
    assert paths0 == META_ALL

    for model in ['collection', 'entity', 'file']:
        paths2 = clean(util.find_meta_files(sampledir, model=model, recursive=True, force_read=True))
        assert paths2 == META_MODEL[model]
    
    paths3 = clean(util.find_meta_files(sampledir, recursive=False, force_read=True))
    assert paths3 == META_MODEL['collection']
    
    paths4 = clean(util.find_meta_files(sampledir, recursive=True, force_read=True, files_first=True))
    assert paths4 == META_ALL
    
    paths5 = clean(util.find_meta_files(sampledir, recursive=True, force_read=False))
    assert paths5 == META_ALL


def test_natural_sort():
    l = ['11', '1', '12', '2', '13', '3']
    util.natural_sort(l)
    assert l == ['1', '2', '3', '11', '12', '13']

def test_natural_order_string():
    assert util.natural_order_string('ddr-testing-123') == '123'
    assert util.natural_order_string('ddr-testing-123-1') == '1'
    assert util.natural_order_string('ddr-testing-123-15') == '15'

def test_file_hash(tmpdir):
    path = str(tmpdir / 'test-hash')
    text = 'hash'
    sha1 = '2346ad27d7568ba9896f1b7da6b5991251debdf2'
    sha256 = 'd04b98f48e8f8bcc15c6ae5ac050801cd6dcfd428fb5f9e65c4e16e7807340fa'
    md5 = '0800fc577294c34e0b28ad2839435945'
    with open(path, 'w') as f:
        f.write(text)
    assert util.file_hash(path, 'sha1') == sha1
    assert util.file_hash(path, 'sha256') == sha256
    assert util.file_hash(path, 'md5') == md5
    os.remove(path)

def test_normalize_text():
    assert util.normalize_text('  this is a test') == 'this is a test'
    assert util.normalize_text('this is a test  ') == 'this is a test'
    assert util.normalize_text('this\r\nis a test') == 'this\\nis a test'
    assert util.normalize_text('this\ris a test') == 'this\\nis a test'
    assert util.normalize_text('this\\nis a test') == 'this\\nis a test'
    assert util.normalize_text(['this is a test']) == ['this is a test']
    assert util.normalize_text({'this': 'is a test'}) == {'this': 'is a test'}


from DDR import identifier

COLLECTION_ID = 'ddr-testing-123'
ENTITY_ID     = 'ddr-testing-123-4'
LOGPATH_REL = os.path.join('addfile', COLLECTION_ID, f'{ENTITY_ID}.log')

@pytest.fixture(scope="session")
def logpath(tmpdir_factory):
    return os.path.join(
        str(tmpdir_factory.mktemp('addfile')), COLLECTION_ID, '%s.log' % ENTITY_ID
    )

@pytest.fixture(scope="session")
def entity_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(ENTITY_ID, str(tmp))

class TestAddFileLogger():
    
    def test_repr(self, tmpdir, entity_identifier):
        log = util.FileLogger(
            identifier=entity_identifier, base_dir=str(tmpdir)
        )
        out = log.__repr__()
        assert LOGPATH_REL in str(out)

    # TODO def test_entry(self):
    # TODO def test_debug(self):
    # TODO def test_info(self):
    # TODO def test_warning(self):
    # TODO def test_error(self):
    # TODO def test_critical(self):
    # TODO def test_log(self):
    # TODO def test_crash(self):

def test_log_path(tmpdir, logpath, entity_identifier):
    out = util.FileLogger.log_path(identifier=entity_identifier, base_dir=str(tmpdir))
    print(f'{LOGPATH_REL=}')
    print(f'{out=}')
    assert LOGPATH_REL in str(out)

def test_addfile_logger(tmpdir, logpath, entity_identifier):
    out0 = util.FileLogger(
        identifier=entity_identifier, base_dir=str(tmpdir)
    ).path
    print('out0 %s' % out0)
    assert LOGPATH_REL in str(out0)

    #out1 = util.FileLogger(
    #    log_path=logpath, base_dir=str(tmpdir)
    #).path
    #print('out1 %s' % out1)
    #print('LOGPATH_REL %s' % LOGPATH_REL)
    #assert LOGPATH_REL in str(out1)
    #assert False
