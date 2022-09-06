# -*- coding: utf-8 -*-

from collections import OrderedDict
from copy import deepcopy

import git
from nose.tools import assert_raises

from DDR import csvfile
from DDR import identifier
from DDR import modules


def test_make_row_dict():
    headers0 = ['id', 'created', 'lastmod', 'title', 'description']
    row0 = ['id', 'then', 'now', 'title', 'descr']
    expected0 = OrderedDict()
    expected0['id'] = 'id'
    expected0['created'] = 'then'
    expected0['lastmod'] = 'now'
    expected0['title'] = 'title'
    expected0['description'] = 'descr'
    out0 = csvfile.make_row_dict(headers0, row0)
    print('expected0 %s' % expected0)
    print('out0      %s' % out0)
    assert out0 == expected0
    # strip whitespace from rows
    headers1 = ['id', 'created', 'lastmod']
    row1 = ['id', ' then', 'now ']
    expected1 = OrderedDict()
    expected1['id'] = 'id'
    expected1['created'] = 'then'
    expected1['lastmod'] = 'now'
    out1 = csvfile.make_row_dict(headers1, row1)
    print('expected1 %s' % expected1)
    print('out1      %s' % out1)
    assert out1 == expected1

def test_make_rowds():
    rows0 = [
        ['id', 'created', 'lastmod', 'title', 'description'],
        ['id0', 'then', 'now', 'title0', 'descr0'],
        ['id1', 'later', 'later', 'title1', 'descr1'],
    ]
    expected = (
        ['id', 'created', 'lastmod', 'title', 'description'],
        [
            OrderedDict([
                ('id', 'id0'), ('created', 'then'), ('lastmod', 'now'),
                ('title', 'title0'), ('description', 'descr0')
            ]),
            OrderedDict([
                ('id', 'id1'), ('created', 'later'), ('lastmod', 'later'),
                ('title', 'title1'), ('description', 'descr1')
            ])
        ],
        []
    )
    out = csvfile.make_rowds(rows0)
    print(out)
    assert out == expected
    # strip whitespace from headers
    rows1 = [
        ['id', ' created', 'lastmod ', '  title', 'description   '],
        ['id0', 'then', 'now', 'title0', 'descr0'],
        ['id1', 'later', 'later', 'title1', 'descr1'],
    ]
    out1 = csvfile.make_rowds(rows1)
    print(out1)
    assert out1 == expected

def test_validate_headers():
    headers0 = ['id', 'title']
    field_names0 = ['id', 'title', 'notused']
    exceptions = ['notused']
    additional = []
    
    # UNIX style silent if everything's OK
    expected0 = {}
    out0 = csvfile.validate_headers(headers0, field_names0, exceptions, additional)
    assert out0 == expected0
    # missing header
    headers1 = ['id']
    field_names1 = ['id', 'title', 'notused']
    expected1 = {
        'Missing headers': ['title'],
    }
    out1 = csvfile.validate_headers(headers1, field_names1, exceptions, additional)
    assert out1 == expected1
    # bad header
    headers2 = ['id', 'title', 'badheader']
    field_names2 = ['id', 'title']
    expected2 = {
        'Bad headers': ['badheader'],
    }
    out2 = csvfile.validate_headers(headers2, field_names2, exceptions, additional)
    assert out2 == expected2
    # note
    headers3 = ['id', 'title', 'note']
    field_names3 = ['id', 'title', 'this is a note']
    additional = ['note']
    expected3 = {}
    out2 = csvfile.validate_headers(headers2, field_names2, exceptions, additional)
    assert out2 == expected2
    # ignored header
    headers4 = ['MEMO', 'id', 'title']
    field_names4 = ['MEMO', 'id', 'title']
    additional = ['note']
    expected4 = {}
    out4 = csvfile.validate_headers(headers4, field_names4, exceptions, additional)
    assert out4 == expected4

def test_account_row():
    required_fields0 = ['id', 'title']
    rowd = {'id': 123, 'title': 'title'}
    out0 = []
    assert csvfile.account_row(required_fields0, rowd) == out0
    required_fields1 = ['id', 'title', 'description']
    out1 = ['description']
    assert csvfile.account_row(required_fields1, rowd) == out1

def test_validate_id():
    in0 = 'ddr-testing-123'
    expected0 = identifier.Identifier('ddr-testing-123')
    assert csvfile.validate_id(in0).id == expected0.id
    in1 = 'not a valid ID'
    expected1 = identifier.Identifier('ddr-testing-123')
    out1 = csvfile.validate_id(in1)
    assert not out1


class TestSchema(object):
    __file__ = None
    FIELDS = [
        {
            'name': 'id',
        },
        {
            'name': 'status',
        }
    ]
    @staticmethod
    def csvvalidate_status( data ):
        """TODO csvvalidate_* should be in DDR.validation"""
        return TestSchema._choice_is_valid('status', data[0], data[1])
    @staticmethod
    def _choice_is_valid(field, valid_values, value):
        """
        @param field: str
        @param valid_values: dict {'field': ['list', 'of', 'valid', 'values']
        @param value: str
        @returns: boolean
        """
        if value in valid_values[field]:
            return True
        return False


def test_check_row_values():
    module = modules.Module(TestSchema())
    headers = ['id', 'status']
    valid_values = {
        'status': ['inprocess', 'complete',]
    }
    rowd0 = {
        'id': 'ddr-test-123',
        'status': 'inprocess',
    }
    expected0 = []
    out0 = csvfile.check_row_values(module, headers, valid_values, rowd0)
    print('out0 %s' % out0)
    assert out0 == expected0
    # invalid ID
    rowd1 = {
        'id': 'not a valid ID',
        'status': 'inprocess',
    }
    expected1 = ['id']
    out1 = csvfile.check_row_values(module, headers, valid_values, rowd1)
    print('out1 %s' % out1)
    assert out1 == expected1
    # invalid value
    rowd2 = {
        'id': 'ddr-testing-123',
        'status': 'inprogress',
    }
    expected2 = ['status']
    out2 = csvfile.check_row_values(module, headers, valid_values, rowd2)
    print('out2 %s' % out2)
    assert out2 == expected2

def test_find_duplicate_ids():
    # OK
    rowds0 = [
        {'id':'ddr-test-123-456', 'status':'inprocess',},
        {'id':'ddr-test-123-457', 'status':'complete',},
    ]
    expected0 = []
    out0 = csvfile.find_duplicate_ids(rowds0)
    assert out0 == expected0
    # error
    rowds1 = [
        {'id':'ddr-test-123-456', 'status':'inprocess',},
        {'id':'ddr-test-123-456', 'status':'complete',},
    ]
    expected1 = [
        'row 1: ddr-test-123-456'
    ]
    out1 = csvfile.find_duplicate_ids(rowds1)
    assert out1 == expected1

def test_find_multiple_cids():
    # OK
    rowds0 = [
        {'id':'ddr-test-123-456', 'status':'inprocess',},
        {'id':'ddr-test-123-457', 'status':'complete',},
    ]
    expected0 = []
    out0 = csvfile.find_duplicate_ids(rowds0)
    assert out0 == expected0
    # error
    rowds1 = [
        {'id':'ddr-test-123-456', 'status':'inprocess',},
        {'id':'ddr-test-124-457', 'status':'complete',},
    ]
    expected1 = [
        'ddr-test-123',
        'ddr-test-124',
    ]
    out1 = csvfile.find_multiple_cids(rowds1)
    assert out1 == expected1

def test_find_missing_required():
    # OK
    required_fields = ['id', 'status']
    rowds0 = [
        {'id':'ddr-test-123', 'status':'inprocess',},
        {'id':'ddr-test-124', 'status':'inprocess',},
    ]
    expected0 = []
    out0 = csvfile.find_missing_required(required_fields, rowds0)
    assert out0 == expected0
    # error
    rowds1 = [
        {'id':'ddr-test-123', 'status':'inprocess',},
        {'id':'ddr-test-124',},
    ]
    expected1 = [
        "row 1: ddr-test-124 ['status']"
    ]
    out1 = csvfile.find_missing_required(required_fields, rowds1)
    assert out1 == expected1

def test_find_invalid_values():
    module = modules.Module(TestSchema())
    headers = ['id', 'status']
    required_fields = ['id', 'status']
    valid_values = {
        'status': ['inprocess', 'complete',]
    }
    # OK
    rowds0 = [
        {'id':'ddr-test-123', 'status':'inprocess',},
        {'id':'ddr-test-124', 'status':'complete',},
    ]
    expected0 = []
    out0 = csvfile.find_invalid_values(module, headers, valid_values, rowds0)
    assert out0 == expected0
    # error
    rowds1 = [
        {'id':'ddr-test-123', 'status':'inprogress',},
        {'id':'ddr-test-124', 'status':'complete',},
    ]
    expected1 = [
        "row 0: ddr-test-123 ['status']"
    ]
    out1 = csvfile.find_invalid_values(module, headers, valid_values, rowds1)
    assert out1 == expected1

# validate_rowds
