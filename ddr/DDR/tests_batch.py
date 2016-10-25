# -*- coding: utf-8 -*-

import os

import envoy
import git
from nose.tools import assert_raises

import batch
import identifier

TMP_DIR = '/tmp/tests-ddr-batch'


# Exporter
# TODO test_make_tmpdir
# TODO test_export

# Checker
# TODO check_repository
# TODO check_csv
# TODO check_eids

def test_guess_model():
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
    assert_raises(Exception, batch.Checker._guess_model, rowds2)
    # entities
    rowds3 = [
        {
            'id':'ddr-testing-123-1',
            'identifier': identifier.Identifier('ddr-testing-123-1'),
        },
    ]
    expected3 = 'entity'
    out3 = batch.Checker._guess_model(rowds3)
    assert out3 == expected3
    # files
    rowds4 = [
        {
            'id':'ddr-testing-123-2-master-a1b2c3',
            'identifier': identifier.Identifier('ddr-testing-123-2-master-a1b2c3'),
        },
    ]
    expected4 = 'file'
    out4 = batch.Checker._guess_model(rowds4)
    assert out4 == expected4
    # file-roles are files
    rowds5 = [
        {
            'id':'ddr-testing-123-2-master',
            'identifier': identifier.Identifier('ddr-testing-123-2-master'),
        },
    ]
    expected5 = 'file'
    out5 = batch.Checker._guess_model(rowds5)
    assert out5 == expected5

# TODO _ids_in_local_repo
# TODO _load_vocab_files
# TODO _vocab_urls
# TODO _http_get_vocabs
# TODO _validate_csv_file

def test_prep_valid_values():
    json_texts = [
        '{"terms": [{"id": "advertisement"}, {"id": "album"}, {"id": "architecture"}], "id": "genre"}',
        '{"terms": [{"id": "eng"}, {"id": "jpn"}, {"id": "chi"}], "id": "language"}',
    ]
    expected = {u'genre': [u'advertisement', u'album', u'architecture'], u'language': [u'eng', u'jpn', u'chi']}
    assert batch.Checker._prep_valid_values(json_texts) == expected

# Importer
# TODO _fidentifier_parent
# TODO _file_is_new
# TODO _write_entity_changelog
# TODO _write_file_changelogs
# TODO import_entities
# TODO import_files
# TODO register_entity_ids
