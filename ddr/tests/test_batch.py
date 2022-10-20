# -*- coding: utf-8 -*-

import os
from pathlib import Path
import urllib

import git
from nose.tools import assert_raises
import pytest

from DDR import batch
from DDR import config
from DDR import fileio
from DDR import dvcs
from DDR import identifier
from DDR.models import Collection, Entity, File

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
LOGPATH_REL   = Path(f'addfile/{COLLECTION_ID}/{ENTITY_ID}.log')

TMP_PATH_REL  = Path(f'tmp/file-add/{COLLECTION_ID}/{ENTITY_ID}/testfile.tif')


@pytest.fixture(scope="session")
def test_base_dir(tmpdir_factory):
    return tmpdir_factory.mktemp('ingest')

@pytest.fixture(scope="session")
def test_image(tmpdir_factory):
    base_dir = tmpdir_factory.mktemp('ingest')
    img_path = base_dir / IMG_FILENAME
    if not img_path.exists():
        urllib.request.urlretrieve(
            IMG_URL,
            base_dir / IMG_FILENAME
        )
    return img_path

@pytest.fixture(scope="session")
def logpath(tmpdir_factory):
    return tmpdir_factory.mktemp('addfile') / f'{COLLECTION_ID}/{ENTITY_ID}.log'

@pytest.fixture(scope="session")
def collection_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(COLLECTION_ID, str(tmp))

@pytest.fixture(scope="session")
def entity_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(ENTITY_ID, str(tmp))

@pytest.fixture(scope="session")
def file_identifier(tmpdir_factory):
    tmp = tmpdir_factory.mktemp(COLLECTION_ID)
    return identifier.Identifier(FILE_ID, str(tmp))


class TestExporter():
    pass
    # TODO def test_make_tmpdir(self):
    # TODO def test_export(self):


class TestChecker():
    
    def test_check_repository(self, test_base_dir, collection_identifier):
        repo_dir = Path(collection_identifier.path_abs())
        # prep repo
        repo = git.Repo.init(repo_dir)
        print(f'{repo=}')
        # prep text files
        file0 = repo_dir / 'file0'
        with file0.open('w') as f:
            f.write('test_0')
        print(f'{file0=}')
        print(f'{file0.exists()=}')
        # untracked
        results = batch.Checker.check_repository(collection_identifier)
        print(f'{results=}')
        assert results['passed'] == True
        assert results['staged'] == []
        assert results['modified'] == []
        # staged
        repo.git.add([file0])
        results = batch.Checker.check_repository(collection_identifier)
        print(f'{results=}')
        assert results['passed'] == False
        assert results['staged'] == ['file0']
        assert results['modified'] == []
        # modified
        with file0.open('w') as f:
            f.write('test_0_modified')
        results = batch.Checker.check_repository(collection_identifier)
        print(f'{results=}')
        assert results['passed'] == False
        assert results['staged'] == ['file0']
        assert results['modified'] == ['file0']

    #def test_check_csv(self, test_base_dir, collection_identifier):
    #    csv_path = test_base_dir / 'test.csv'
    #    vocabs_url = config.VOCABS_URL
    #    print(f'{csv_path=}')
    #    # prep csv
    #    headers = ['id','title']
    #    rowds = [
    #        {'id':'ddr-densho-123-1', 'title':'just testing'},
    #        {'id':'ddr--123-2', 'title':'moar testing'},
    #    ]
    #    fileio.write_csv(csv_path, headers, rowds)
    #    #
    #    results = batch.Checker.check_csv(csv_path, collection_identifier, vocabs_url)
    #    print(f'{results=}')
    #    assert 0

    # TODO def test_check_eids(self):

    # TODO def test_get_module(self):

    def test_ids_in_local_repo(self, test_base_dir, collection_identifier, entity_identifier, file_identifier):
        repo_dir = Path(collection_identifier.path_abs())
        # prep repo
        repo = git.Repo.init(repo_dir)
        print(f'{repo=}')
        ci = identifier.Identifier(COLLECTION_ID, repo_dir)
        ei = identifier.Identifier(ENTITY_ID, repo_dir)
        fi = identifier.Identifier(FILE_ID, repo_dir)
        c = Collection(ci.path_abs(), ci.id, ci)
        e = Entity(ei.path_abs(), ei.id, ei)
        f = File(fi.path_abs(), fi.id, fi)
        c.write_json()
        e.write_json()
        f.write_json()
        cpath = Path(c.identifier.path_abs('json'))
        epath = Path(e.identifier.path_abs('json'))
        fpath = Path(f.identifier.path_abs('json'))
        #
        rowds = [
            {'id': 'ddr-testing-123-3'}, {'id': 'ddr-testing-123-4'},
        ]
        results = batch.Checker._ids_in_local_repo(rowds, 'entity', repo_dir)
        assert results == ['ddr-testing-123-4']
        #
        rowds = [
            {'id': 'ddr-testing-123-4-master-a1b2c3'},
            {'id': 'ddr-testing-123-4-mezzanine-a1b2c3'},
        ]
        results = batch.Checker._ids_in_local_repo(rowds, 'file', repo_dir)
        assert results == ['ddr-testing-123-4-master-a1b2c3']
    
    def test_prep_valid_values(self):
        json_texts = {
            'status': {'id': 'status', 'terms': [
                {'id': 'inprocess', 'title': 'In Progress'},
                {'id': 'completed', 'title': 'Completed'}
            ]},
            'language': {'id': 'language', 'terms': [
                {'id': 'eng', 'title': 'English'},
                {'id': 'jpn', 'title': 'Japanese'},
            ]}
        }
        expected = {
            'status': ['inprocess', 'completed'],
            'language': ['eng', 'jpn']
        }
        assert batch.Checker._prep_valid_values(json_texts) == expected
    
    # TODO def test_validate_csv_file(self):


class TestImporter():
    
    def test_fidentifier_parent(self):
        ei = identifier.Identifier('ddr-testing-123-4')
        si = identifier.Identifier('ddr-testing-123-4-5')
        efi = identifier.Identifier('ddr-testing-123-4-master-abc123')
        sfi = identifier.Identifier('ddr-testing-123-4-5-master-abc123')
        out0 = batch.Importer._fidentifier_parent(efi)
        out1 = batch.Importer._fidentifier_parent(sfi)
        assert out0.id == ei.id
        assert out1.id == si.id
        
    def test_write_entity_changelog(self, test_base_dir, entity_identifier):
        entity = Entity(
            entity_identifier.path_abs(), entity_identifier.id, entity_identifier
        )
        git_name = 'pytest' 
        git_mail = 'pytest@densho.org' 
        agent = 'pytest'
        #
        changelog_path = Path(entity.changelog_path)
        os.makedirs(str(changelog_path.parent))
        #
        assert not changelog_path.exists()
        batch.Importer._write_entity_changelog(entity, git_name, git_mail, agent)
        assert changelog_path.exists()

    # NOPE def test_write_file_changelogs(self):
    # TODO def test_import_entities(self):

    def test_csv_load(self):
        # prep
        rowds_csv = [
            {'id': 'ddr-testing-123-4', 'title': 'yay testing!'},
            {'id': 'ddr-testing-123-5', 'title': 'moar testing!'},
        ]
        #
        module = batch.Checker._get_module('entity')
        rowds_out = batch.Importer._csv_load(module, rowds_csv)
        print(f'{rowds_out=}')
        assert rowds_out == rowds_csv

    def test_fidentifiers(self):
        ci = identifier.Identifier('ddr-testing-123')
        rowds0 = [
            {'id': 'ddr-testing-123'},
            {'id': 'ddr-testing-123-4'},
            {'id': 'ddr-testing-123-4-master'},
            {'id': 'ddr-testing-123-4-master-abc123'},
            {'id': 'ddr-testing-123-4-5'},
            {'id': 'ddr-testing-123-4-5-master'},
            {'id': 'ddr-testing-123-4-5-master-abc123'},
        ]
        out0 = list(batch.Importer._fidentifiers(rowds0, ci).keys())
        expected_keys = [
            'ddr-testing-123-4-master-abc123', 'ddr-testing-123-4-5-master-abc123',
        ]
        for key in out0:
            assert key in expected_keys
    
    def test_fid_parents(self):
        ci = identifier.Identifier('ddr-testing-123')
        
        # new files
        fids0 = {}
        rowds0 = [
            {'id': 'ddr-testing-123-4'},
            {'id': 'ddr-testing-123-4-master-abc123'},
            {'id': 'ddr-testing-123-4-5'},
            {'id': 'ddr-testing-123-4-5-master-abc123'},
        ]
        out0 = list(batch.Importer._fid_parents(fids0, rowds0, ci).keys())
        expected0 = [
            'ddr-testing-123-4',
            'ddr-testing-123-4-5',
        ]
        for key in out0:
            assert key in expected0
        
        # existing files
        fid10 = identifier.Identifier('ddr-testing-123-4-master-abc123')
        fid11 = identifier.Identifier('ddr-testing-123-4-5-master-abc123')
        fids1 = {
            'ddr-testing-123-4-master-abc123': fid10,
            'ddr-testing-123-4-5-master-abc123': fid11,
        }
        rowds1 = [
            {'id': 'ddr-testing-123-4-master-abc123'},
            {'id': 'ddr-testing-123-4-5-master-abc123'},
        ]
        out1 = list(batch.Importer._fid_parents(fids1, rowds1, ci).keys())
        expected1 = [
            'ddr-testing-123-4-master-abc123',
            'ddr-testing-123-4-5-master-abc123',
        ]
        for key in out1:
            assert key in expected1

    # TODO def test_existing_bad_entities(self):
    # TODO def test_file_objects(self):
    # TODO def test_rowds_new_existing(self):
    
    def test_rowd_is_external(self):
        rowds = [
            {'external': 0},
            {'external': '0'},
            {'external': 1},
            {'external': '1'},
        ]
        expected = [
            False,
            False,
            True,
            True
        ]
        out = [
            batch.Importer._rowd_is_external(rowd)
            for rowd in rowds
        ]
        assert out == expected
    
    # TODO def test_import_files(self):
    # TODO def test_update_existing_files(self):
    # TODO def test_add_new_files(self):
    # TODO def test_register_entity_ids(self):


class TestUpdaterMetrics():
    pass
    # TODO def test_headers(self):
    # TODO def test_row(self):


class TestUpdater():
    pass
    # TODO def test_update_collection(self):
    # TODO def test_update_multi(self):
    # TODO def test_consolidate_paths(self):
    # TODO def test_update_collection_objects(self):
    # TODO def test_prep_todo(self):
    # TODO def test_read_todo(self):
    # TODO def test_write_todo(self):
    # TODO def test_read_this(self):
    # TODO def test_write_this(self):
    # TODO def test_csv_reader(self):
    # TODO def test_csv_writer(self):
    # TODO def test_read_csv(self):
    # TODO def test_write_csv(self):
    # TODO def test_read_done(self):
    # TODO def test_write_done(self):
