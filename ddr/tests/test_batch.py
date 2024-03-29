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

    def test_export_field_csv(self):
        pass

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
        staged,modified = batch.Checker.check_repository(collection_identifier)
        print(f'{staged=}')
        print(f'{modified=}')
        assert staged == []
        assert modified == []
        # staged
        repo.git.add([file0])
        staged,modified = batch.Checker.check_repository(collection_identifier)
        print(f'{staged=}')
        print(f'{modified=}')
        assert staged == ['file0']
        assert modified == []
        # modified
        with file0.open('w') as f:
            f.write('test_0_modified')
        staged,modified = batch.Checker.check_repository(collection_identifier)
        print(f'{staged=}')
        print(f'{modified=}')
        assert staged == ['file0']
        assert modified == ['file0']

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

    def test_validate_csv_identifiers(self):
        # good
        out = batch.Checker.validate_csv_identifiers([{'id':'ddr-densho-123-456'}])
        assert out == []
        # malformed
        out = batch.Checker.validate_csv_identifiers([{'id':'ddr-densho-densho'}])
        assert 'Bad Identifier' in out[0]
        out = batch.Checker.validate_csv_identifiers([{'id':'ddr-densho--123'}])
        assert 'Bad Identifier' in out[0]
        out = batch.Checker.validate_csv_identifiers([{'id':'ddr.densho.123'}])
        assert 'Bad Identifier' in out[0]
    
    # def test_validate_csv_headers  SEE test_csvfile.test_validate_headers
    # def test_validate_csv_rowds  SEE test_csvfile.test_validate_rowds

    def test_validate_csv_files(self, test_base_dir):
        # setup fake imports dir
        imports_dir = Path(test_base_dir) / 'validate_files'
        csv_path = imports_dir / 'import.csv'
        imports_dir.mkdir(parents=True)
        with csv_path.open('w') as f:
            f.write('csv file')
        # metadata update (no import)
        invalid_path = imports_dir / 'invalid_file'
        rowds = [{
            'identifier': identifier.Identifier('ddr-testing-123-456'),
            'basename_orig': invalid_path, 'external': False,
        }]  # first-time import so check file
        out = batch.Checker.validate_csv_files(csv_path, rowds)
        assert 'Missing file' in out[0]  # file missing -> error
        rowds = [{
            'identifier': identifier.Identifier('ddr-testing-123-456-master-abc123'),
            'basename_orig': invalid_path, 'external': False
        }]  # existing file so don't check file
        out = batch.Checker.validate_csv_files(csv_path, rowds)
        assert out == []  # no file but we don't care bc only update
        # external file
        valid_path = imports_dir / 'valid_file'
        rowds = [{'basename_orig': valid_path, 'external': True}]
        assert batch.Checker.validate_csv_files(csv_path, rowds) == []
        # valid, present file
        valid_path = imports_dir / 'valid_file'
        with valid_path.open('w') as f:
            f.write('valid_path')
        rowds = [{'basename_orig': valid_path, 'external': False}]
        assert batch.Checker.validate_csv_files(csv_path, rowds) == []
        rowds = [{'basename_orig': valid_path}]
        assert batch.Checker.validate_csv_files(csv_path, rowds) == []
        # not exists
        missing_path = imports_dir / 'missing_file'
        rowds = [{'basename_orig': missing_path}]
        out = batch.Checker.validate_csv_files(csv_path, rowds)
        assert out and out[0]
        assert 'Missing file' in out[0]
        assert str(missing_path) in out[0]
        # not os.R_OK
        # TODO how to make a file unreadable to test this?


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
