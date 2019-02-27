# -*- coding: utf-8 -*-

import os

import envoy
import git
from nose.tools import assert_raises

from DDR import batch
from DDR import config
from DDR import identifier

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'batch')
#if not os.path.exists(TESTING_BASE_DIR):
#    os.makedirs(TESTING_BASE_DIR)


class TestExporter():
    pass
    # TODO def test_make_tmpdir(self):
    # TODO def test_export(self):


class TestChecker():
    
    # TODO def test_check_repository(self):
    # TODO def test_check_csv(self):
    # TODO def test_check_eids(self):

    def test_guess_model(self):
        # no rows
        rowds0 = []
        expected0 = []
        #out0 = batch.Checker._guess_model(rowds0)
        #assert out0 == expected0
        assert_raises(Exception, batch.Checker._guess_model, rowds0)
        # no identifiers
        rowds1 = [
            {'id':'ddr-testing-123-1'},
            {'id':'ddr-testing-123-1'},
        ]
        expected1 = []
        assert_raises(Exception, batch.Checker._guess_model, rowds1)
        # too many models
        rowds2 = [
            {
                'id':'ddr-testing-123-1',
                'identifier': identifier.Identifier('ddr-testing-123-1'),
            },
            {
                'id':'ddr-testing-123-2-master',
                'identifier': identifier.Identifier('ddr-testing-123-2-master'),
            },
        ]
        expected2 = ('entity', ['More than one model type in imput file!'])
        out2 = batch.Checker._guess_model(rowds2)
        assert out2 == expected2
        # entities
        rowds3 = [
            {
                'id':'ddr-testing-123-1',
                'identifier': identifier.Identifier('ddr-testing-123-1'),
            },
        ]
        expected3 = ('entity',[])
        out3 = batch.Checker._guess_model(rowds3)
        assert out3 == expected3
        # segments
        rowds3 = [
            {
                'id':'ddr-testing-123-4-5',
                'identifier': identifier.Identifier('ddr-testing-123-4-5'),
            },
        ]
        expected3 = ('segment',[])
        out3 = batch.Checker._guess_model(rowds3)
        assert out3 == expected3
        # files
        rowds4 = [
            {
                'id':'ddr-testing-123-2-master-a1b2c3',
                'identifier': identifier.Identifier('ddr-testing-123-2-master-a1b2c3'),
            },
        ]
        expected4 = ('file',[])
        out4 = batch.Checker._guess_model(rowds4)
        assert out4 == expected4
        # file-roles are files
        rowds5 = [
            {
                'id':'ddr-testing-123-2-master',
                'identifier': identifier.Identifier('ddr-testing-123-2-master'),
            },
        ]
        expected5 = ('file',[])
        out5 = batch.Checker._guess_model(rowds5)
        assert out5 == expected5
        # external files
        rowds6 = [
            {
                'id':'ddr-testing-123-1',
                'identifier': identifier.Identifier('ddr-testing-123-1'),
                'basename_orig': 'somefile.jpg',
            },
        ]
        expected6 = ('file',[])
        out6 = batch.Checker._guess_model(rowds6)
        assert out6 == expected6

    # TODO def test_get_module(self):
    # TODO def test_ids_in_local_repo(self):
    
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
        
    # TODO def test_write_entity_changelog(self):
    # TODO def test_write_file_changelogs(self):
    # TODO def test_import_entities(self):
    # TODO def test_csv_load(self):

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
        out0 = batch.Importer._fidentifiers(rowds0, ci)
        print(out0)
        print(out0.keys())
        expected_keys = [
            'ddr-testing-123-4-master-abc123', 'ddr-testing-123-4-5-master-abc123',
        ]
        assert out0.keys() == expected_keys
    
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
        out0 = batch.Importer._fid_parents(fids0, rowds0, ci)
        expected0 = [
            'ddr-testing-123-4',
            'ddr-testing-123-4-5',
        ]
        assert out0.keys() == expected0
        
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
        out1 = batch.Importer._fid_parents(fids1, rowds1, ci)
        expected1 = [
            'ddr-testing-123-4-master-abc123',
            'ddr-testing-123-4-5-master-abc123',
        ]
        assert out1.keys() == expected1
    
    # TODO def test_eidentifiers(self):
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
