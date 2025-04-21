# coding: utf-8

import json
import os
import re

from nose.tools import assert_raises

from DDR import identifier

BASE_PATHS = [
    '/var/www/media/ddr',
    '/media/qnfs/kinkura/gold',
    '/mnt/nfsdrive/ddr',
    '/tmp',
]


# ----------------------------------------------------------------------

def test_definitions_models():
    IDENTIFIERS = [
        {'model': 'collection'},
        {'model': 'entity'},
        {'model': 'file'},
    ]
    expected = ['file', 'entity', 'collection']
    out = identifier.Definitions.models(IDENTIFIERS)
    assert out == expected

def test_definitions_modules():
    IDENTIFIERS = [
        {'model': 'collection'},
        {'model': 'entity'},
        {'model': 'file'},
    ]
    expected = {
        'collection': None,
        'entity': None,
        'file': None,
    }
    out = identifier.Definitions.modules(IDENTIFIERS)
    assert out == expected

def test_definitions_model_classes():
    IDENTIFIERS = [
        {'model': 'collection', 'class':'DDR.models.Collection'},
        {'model': 'entity',     'class':'DDR.models.Entity'},
        {'model': 'file-role',  'class':'DDR.models.Stub'},
        {'model': 'file',       'class':'DDR.models.File'},
    ]
    expected = {
        'collection': {'class': 'Collection', 'module': 'DDR.models'},
        'entity':     {'class': 'Entity',     'module': 'DDR.models'},
        'file-role':  {'class': 'Stub',       'module': 'DDR.models'},
        'file':       {'class': 'File',       'module': 'DDR.models'}
    }
    out = identifier.Definitions.model_classes(IDENTIFIERS)
    assert out == expected

#def test_definitions_models_modules():
#    MODULES = {
#        'repository': None,
#        'organization': None,
#        'collection': <module 'repo_models.collection' from '/etc/ddr/ddr-defs/repo_models/collection.pyc'>,
#        'entity': <module 'repo_models.entity' from '/etc/ddr/ddr-defs/repo_models/entity.pyc'>,
#        'segment': <module 'repo_models.entity' from '/etc/ddr/ddr-defs/repo_models/entity.pyc'>
#        'file-role': None,
#        'file': <module 'repo_models.files' from '/etc/ddr/ddr-defs/repo_models/files.pyc'>,
#    }
#    expected = {
#        'collection': {'as': 'collectionmodule', 'class': 'collection', 'module': 'repo_models.collection'},
#        'entity': {'as': 'entitymodule', 'class': 'entity', 'module': 'repo_models.entity'},
#        'segment': {'as': 'segmentmodule', 'class': 'segment', 'module': 'repo_models.entity'},
#        'file': {'as': 'filemodule', 'class': 'file', 'module': 'repo_models.files'},
#    }
#    out = identifier.Definitions.models_modules(MODULES)
#    assert out == expected

def test_definitions_collection_models():
    IDENTIFIERS = [
        {'level': -2, 'model': 'repository'},
        {'level': -1, 'model': 'organization'},
        {'level': 0, 'model': 'collection'},
        {'level': 1, 'model': 'entity'},
        {'level': 2, 'model': 'file'},
    ]
    expected = ['collection', 'entity', 'file']
    out = identifier.Definitions.collection_models(IDENTIFIERS)
    assert out == expected

def test_definitions_containers():
    IDENTIFIERS = [
        {'model': 'collection', 'children': ['entity'], 'children_all': ['entity']},
        {'model': 'entity',     'children': ['file'],   'children_all': ['file-role','file']},
        {'model': 'file',       'children': [],         'children_all': []},
    ]
    expected = ['entity', 'collection']
    out = identifier.Definitions.containers(IDENTIFIERS)
    assert out == expected

def test_definitions_models_parents():
    IDENTIFIERS = [
        {'model': 'organization', 'class': 'DDR.models.Stub',       'parents': []            },
        {'model': 'collection',   'class': 'DDR.models.Collection', 'parents': []            },
        {'model': 'entity',       'class': 'DDR.models.Entity',     'parents': ['collection']},
        {'model': 'file-role',    'class': 'DDR.models.Stub',       'parents': []            },
        {'model': 'file',         'class': 'DDR.models.File',       'parents': ['entity']    },
    ]
    expected = {
        'file': ['entity'],
        'entity': ['collection'],
    }
    out = identifier.Definitions.models_parents(IDENTIFIERS)
    assert out == expected

def test_definitions_models_parents_all():
    IDENTIFIERS = [
        {'model': 'organization', 'parents_all': []},
        {'model': 'collection',   'parents_all': ['organization']},
        {'model': 'entity',       'parents_all': ['collection']},
        {'model': 'file-role',    'parents_all': ['entity']},
        {'model': 'file',         'parents_all': ['file-fole']},
    ]
    expected = {
        'collection': ['organization'],
        'entity': ['collection'],
        'file-role': ['entity'],
        'file': ['file-fole'],
    }
    out = identifier.Definitions.models_parents_all(IDENTIFIERS)
    assert out == expected

def test_definitions_children():
    PARENTS = {
        'collection': [],
        'entity': ['collection'],
        'segment': ['entity'],
        'file': ['segment', 'entity'],
    }
    expected = {
        'collection': ['entity'],
        'entity': ['segment', 'file'],
        'segment': ['file'],
    }
    out = identifier.Definitions.children(PARENTS)
    # test contents of list regardless of order
    assert set(out) == set(expected)

def test_definitions_id_components():
    IDENTIFIERS = [
        {'model': 'repository',   'component': {'name': 'repo'}},
        {'model': 'organization', 'component': {'name': 'org'}},
        {'model': 'collection',   'component': {'name': 'cid'}},
    ]
    expected = [
        'repo', 'org', 'cid',
    ]
    out = identifier.Definitions.id_components(IDENTIFIERS)
    assert out == expected

def test_definitions_valid_components():
    IDENTIFIERS = [
        {'model': 'repository',   'component': {'name': 'repo', 'valid': ['ddr']}},
        {'model': 'organization', 'component': {'name': 'org',  'valid': ['densho', 'testing']}},
        {'model': 'collection',   'component': {'name': 'cid',  'valid': []}},
    ]
    # TODO These values are set in git@mits.densho.org:ddr.git.
    #      Figure out a way to pass these as function args or we will have to
    #      update tests whenever we update ddr.git
    expected = {
        'repo': 'ddr',
        'org': [
            'densho', 'ajah', 'chi', 'csujad', 'fom', 'hmwf', 'jamsj', 'janm',
            'jcch', 'manz', 'njpa', 'one', 'pc', 'phljacl', 'sbbt', 'sjacl', 'dev',
            'qumulo', 'test', 'testing'
        ],
        'role': [
            'mezzanine', 'master', 'transcript', 'gloss', 'preservation',
            'administrative'
        ]
    }
    out = identifier.Definitions.valid_components(IDENTIFIERS, media_base='/var/www/media/ddr')
    print(f'expected {expected}')
    print(f'out {out}')
    assert out == expected

def test_definitions_nextable_models():
    IDENTIFIERS = [
        {'model': 'repository',   'component': {'name': 'repo', 'type': str}},
        {'model': 'collection',   'component': {'name': 'cid',  'type': int}},
    ]
    expected = [
        'collection'
    ]
    out = identifier.Definitions.nextable_models(IDENTIFIERS)
    assert out == expected

# TODO FILETYPE_MATCH_ANNEX

def test_definitions_id_patterns():
    regexes = {
        'repo': r'^(?P<repo>[\w]+)$',
        'coll': r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)$',
    }
    IDENTIFIERS = [
        {'model': 'repository', 'patterns': {'id': [regexes['repo']]}},
        {'model': 'collection', 'patterns': {'id': [regexes['coll']]}},
    ]
    expected = [
        (re.compile(regexes['coll']), '', 'collection'),
        (re.compile(regexes['repo']), '', 'repository'),
    ]
    out = identifier.Definitions.id_patterns(IDENTIFIERS)
    assert out == expected

def test_definitions_path_patterns():
    regexes = {
        'repo': r'^(?P<repo>[\w]+)$',
        'coll': r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)$',
    }
    IDENTIFIERS = [
        {'model': 'repository', 'patterns': {'path': [regexes['repo']]}},
        {'model': 'collection', 'patterns': {'path': [regexes['coll']]}},
    ]
    expected = [
        (re.compile(regexes['coll']), '', 'collection'),
        (re.compile(regexes['repo']), '', 'repository'),
    ]
    out = identifier.Definitions.path_patterns(IDENTIFIERS)
    assert out == expected

# TODO PATH_PATTERNS_LOOP

def test_definitions_url_patterns():
    regexes = {
        'repo': r'^(?P<repo>[\w]+)$',
        'coll': r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)$',
    }
    IDENTIFIERS = [
        {'model': 'repository', 'patterns': {'url': [regexes['repo']]}},
        {'model': 'collection', 'patterns': {'url': [regexes['coll']]}},
    ]
    expected = [
        (re.compile(regexes['coll']), '', 'collection'),
        (re.compile(regexes['repo']), '', 'repository'),
    ]
    out = identifier.Definitions.url_patterns(IDENTIFIERS)
    assert out == expected

def test_definitions_id_templates():
    IDENTIFIERS = [
        {'model': 'segment',   'templates': {'id': [
            'a',
        ]}},
        {'model': 'file-role', 'templates': {'id': [
            'b', 'c',
        ]}},
    ]
    expected = {
        'segment': ['a'],
        'file-role': ['b', 'c'],
    }
    out = identifier.Definitions.id_templates(IDENTIFIERS)
    assert out == expected

def test_definitions_path_templates():
    IDENTIFIERS = [
        {'model': 'segment',   'templates': {'path': {
            'rel': ['a'],
            'abs': ['A'],
        }}},
        {'model': 'file-role', 'templates': {'path': {
            'rel': ['b', 'c'],
            'abs': ['B', 'C'],
        }}},
    ]
    expected = {
        'segment-rel': ['a'],
        'segment-abs': ['A'],
        'file-role-rel': ['b', 'c'],
        'file-role-abs': ['B', 'C'],
    }
    out = identifier.Definitions.path_templates(IDENTIFIERS)
    assert out == expected

def test_definitions_url_templates():
    IDENTIFIERS = [
        {'model': 'segment',   'templates': {'url': {
            'editor': ['a'],
            'public': ['A'],
        }}},
        {'model': 'file-role', 'templates': {'url': {
            'editor': ['b', 'c'],
            'public': ['B', 'C'],
        }}},
    ]
    expected = {
        'editor': {
            'segment': ['a'],
            'file-role': ['b', 'c']
        },
        'public': {
            'segment': ['A'],
            'file-role': ['B', 'C']
        },
    }
    out = identifier.Definitions.url_templates(IDENTIFIERS)
    print(out)
    assert out == expected

def test_definitions_additional_paths():
    IDENTIFIERS = [
        {'model': 'repository', 'files': {
            'json': 'repository.json',
        }},
        {'model': 'collection', 'files': {
            'annex': '.git/annex',
            'files': 'files',
            'json': 'collection.json',
        }},
        {'model': 'file-role', 'files': {
        }},
    ]
    expected = {
        'repository': {'json': 'repository.json'},
        'collection': {'files': 'files', 'json': 'collection.json', 'annex': '.git/annex'},
        'file-role': {},
    }
    out = identifier.Definitions.additional_paths(IDENTIFIERS)
    assert out == expected


# ----------------------------------------------------------------------

# TODO test_compile_patterns

# TODO test_render_models_digraph

def test_identify_object():
    patterns = (
        (r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)-(?P<eid>[\d]+)$', 'entity-rel', 'entity'),
        (r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)$', 'collection-rel', 'collection'),
    )
    id0 = 'ddr-test-123'
    id1 = 'ddr-test-123-456'
    id2 = 'ddr.test.123.456'
    id0_expected_model = 'collection'
    id1_expected_model = 'entity'
    id2_expected_model = None
    id0_expected_memo = 'collection-rel'
    id1_expected_memo = 'entity-rel'
    id2_expected_memo = None
    id0_expected_gd = {'repo':'ddr', 'org':'test', 'cid':'123'}
    id1_expected_gd = {'repo':'ddr', 'org':'test', 'cid':'123', 'eid':'456'}
    id2_expected_gd = None
    assert identifier.identify_object(id0, patterns) == (id0_expected_model,id0_expected_memo,id0_expected_gd)
    assert identifier.identify_object(id1, patterns) == (id1_expected_model,id1_expected_memo,id1_expected_gd)
    assert identifier.identify_object(id2, patterns) == (id2_expected_model,id2_expected_memo,id2_expected_gd)

def test_validate_idparts():
    VALID_COMPONENTS = {
        'repo': ['ddr'],
        'org': ['densho', 'testing'],
        'role': ['mezzanine', 'master']
    }
    idparts = {
        'model':'file',
        'repo':'ddr', 'org':'densho', 'cid':'1000', 'eid':'485', 'role':'master'
    }
    oid = 'ddr-densho-1000-485-master'
    identifier.validate_idparts('ddr-densho-1000-485-master', idparts, VALID_COMPONENTS)
    idparts['repo'] = 'dd'; oid = 'dd-densho-1000-485-master'
    assert_raises(Exception, identifier.validate_idparts, oid, idparts, VALID_COMPONENTS)
    idparts['repo'] = 'ddr'; idparts['role'] = '0'; oid = 'ddr-densho-1000-485-0'
    assert_raises(Exception, identifier.validate_idparts, oid, idparts, VALID_COMPONENTS)

def test_identify_filepath():
    assert identifier.identify_filepath('something-a.jpg') == 'access'
    assert identifier.identify_filepath('ddr-test-123-456-mezzanine-abc123') == 'mezzanine'
    assert identifier.identify_filepath('ddr-test-123-456-master-abc123') == 'master'
    assert identifier.identify_filepath('ddr-test-123-456-master-012012') == 'master'
    assert identifier.identify_filepath('nothing in particular') == None

def test_set_idparts():
    i = identifier.Identifier('ddr-test-123-456-master-abcde12345', '/tmp')
    assert i.parts['repo'] == 'ddr'
    assert i.parts['org'] == 'test'
    assert i.parts['cid'] == 123
    assert i.parts['eid'] == 456
    assert i.parts['role'] == 'master'
    assert i.parts['sha1'] == 'abcde12345'
    # sha1 should not be an int
    i = identifier.Identifier('ddr-test-123-456-master-012345', '/tmp')
    assert i.parts['repo'] == 'ddr'
    assert i.parts['org'] == 'test'
    assert i.parts['cid'] == 123
    assert i.parts['eid'] == 456
    assert i.parts['role'] == 'master'
    assert i.parts['sha1'] == '012345'

def test_format_id():
    templates = {
        'entity':       ['{repo}-{org}-{cid}-{eid}'],
        'collection':   ['{repo}-{org}-{cid}'],
    }
    i0 = 'ddr-test-123'
    i1 = 'ddr-test-123-456'
    assert identifier.format_id(identifier.Identifier(i0), 'collection', templates) == i0
    assert identifier.format_id(identifier.Identifier(i1), 'entity', templates) == i1
    # child identifiers (e.g. entity) can get ID of parents (e.g. collection)
    assert identifier.format_id(identifier.Identifier(i1), 'collection', templates) == i0
    # but not the other way around
    assert_raises(
        identifier.IdentifierFormatException,
        identifier.format_id,
        identifier.Identifier(i0), 'entity', templates
    )

def test_format_path():
    for base_path in BASE_PATHS:
        templates = {
            'entity-abs':       ['{basepath}/{repo}-{org}-{cid}/files/{repo}-{org}-{cid}-{eid}'],
            'collection-abs':   ['{basepath}/{repo}-{org}-{cid}'],
            'entity-rel':       ['files/{repo}-{org}-{cid}-{eid}'],
        }
        i0 = 'ddr-test-123'
        i1 = 'ddr-test-123-456'
        i0_abs_expected = os.path.join(base_path, 'ddr-test-123')
        i1_abs_expected = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456')
        i1_rel_expected = 'files/ddr-test-123-456'
        # abs, rel
        path0 = identifier.format_path(identifier.Identifier(i0, base_path), 'collection', 'abs', templates)
        path1 = identifier.format_path(identifier.Identifier(i1, base_path), 'entity', 'abs', templates)
        path2 = identifier.format_path(identifier.Identifier(i1, base_path), 'entity', 'rel', templates)
        assert path0 == i0_abs_expected
        assert path1 == i1_abs_expected
        assert path2 == i1_rel_expected
        # missing patterns key
        try:
            path3 = identifier.format_path(identifier.Identifier(i1, base_path), 'entity', 'meta-rel', templates)
        except:
            path3 = None
        assert path3 == None
        # no base_path in identifier
        assert_raises(
            Exception,
            identifier.format_path,
            identifier.Identifier(i0), 'collection', 'abs', templates
        )

def test_format_url():
    templates = {
        'editor': {
            'entity':       '/ui/{repo}-{org}-{cid}-{eid}',
            'collection':   '/ui/{repo}-{org}-{cid}',
        },
        'public': {
            'entity':       '/{repo}/{org}/{cid}/{eid}',
            'collection':   '/{repo}/{org}/{cid}',
        },
    }
    base_path = '/tmp'
    i0 = 'ddr-test-123'
    i1 = 'ddr-test-123-456'
    i0_edt_expected = '/ui/ddr-test-123'
    i1_edt_expected = '/ui/ddr-test-123-456'
    i0_pub_expected = '/ddr/test/123'
    i1_pub_expected = '/ddr/test/123/456'
    # editor
    url0 = identifier.format_url(identifier.Identifier(i0, base_path), 'collection', 'editor', templates)
    url1 = identifier.format_url(identifier.Identifier(i1, base_path), 'entity', 'editor', templates)
    # public
    url2 = identifier.format_url(identifier.Identifier(i0, base_path), 'collection', 'public', templates)
    url3 = identifier.format_url(identifier.Identifier(i1, base_path), 'entity', 'public', templates)

def test_matches_pattern():
    patterns = (
        (r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)-(?P<eid>[\d]+)$', '', 'entity'),
        (r'^(?P<repo>[\w]+)-(?P<org>[\w]+)-(?P<cid>[\d]+)$', '', 'collection'),
    )
    id0 = 'ddr-test-123'
    id1 = 'ddr-test-123-456'
    id2 = '/ddr/test/123'
    id3 = 'ddr-test'
    id0_expected = {'repo': 'ddr', 'org': 'test', 'cid': '123', 'model': 'collection'}
    id1_expected = {'repo': 'ddr', 'org': 'test', 'cid': '123', 'eid': '456', 'model': 'entity'}
    id2_expected = {}
    id3_expected = {}
    assert identifier.matches_pattern(id0, patterns) == id0_expected
    assert identifier.matches_pattern(id1, patterns) == id1_expected
    assert identifier.matches_pattern(id2, patterns) == id2_expected
    assert identifier.matches_pattern(id3, patterns) == id3_expected

# TODO test_is_id
# TODO test_is_path
# TODO test_is_url
# TODO test_is_abspath

def test_parse_args_kwargs():
    keys = identifier.KWARG_KEYS
    args0 = []; kwargs0 = {}
    args1 = []; kwargs1 = {'id':'ddr-bar', 'base_path':'/opt'}
    args2 = ['ddr-foo', '/tmp']; kwargs2 = {}
    args3 = ['ddr-foo', '/tmp']; kwargs3 = {'id':'ddr-bar'}
    args4 = ['ddr-foo', '/tmp']; kwargs4 = {'id':'ddr-bar', 'base_path':'/opt'}
    args5 = ['/ddr-bar-1', '/opt']; kwargs5 = {'url':'/collection/ddr-bar-1', 'base_path':'/opt'}
    expected0 = {'url': None, 'path': None, 'parts': None, 'id': None, 'base_path': None}
    expected1 = {'url': None, 'path': None, 'parts': None, 'id': 'ddr-bar', 'base_path': '/opt'}
    expected2 = {'url': None, 'path': None, 'parts': None, 'id': 'ddr-foo', 'base_path': '/tmp'}
    expected3 = {'url': None, 'path': None, 'parts': None, 'id': 'ddr-bar', 'base_path': '/tmp'}
    expected4 = {'url': None, 'path': None, 'parts': None, 'id': 'ddr-bar', 'base_path': '/opt'}
    expected5a = {'url': None, 'path': '/ddr-bar-1', 'parts': None, 'id': None, 'base_path': '/opt'}
    expected5b = {'url': '/collection/ddr-bar-1', 'path': None, 'parts': None, 'id': None, 'base_path': '/opt'}
    assert identifier._parse_args_kwargs(keys, args0, kwargs0) == expected0
    assert identifier._parse_args_kwargs(keys, args1, kwargs1) == expected1
    assert identifier._parse_args_kwargs(keys, args2, kwargs2) == expected2
    assert identifier._parse_args_kwargs(keys, args3, kwargs3) == expected3
    assert identifier._parse_args_kwargs(keys, args4, kwargs4) == expected4
    assert identifier._parse_args_kwargs(keys, args5, {}) == expected5a
    assert identifier._parse_args_kwargs(keys, [], kwargs5) == expected5b

# TODO test_module_for_name
# TODO test_class_for_name

def test_field_names():
    template = '{a}-{b}-{c}'
    expected = ['a', 'b', 'c']
    assert identifier._field_names(template) == expected


# ----------------------------------------------------------------------

def test_identifier_first_id():
    out0 = identifier.first_id(
        'collection',
        identifier.Identifier('ddr-testing'),
    )
    expected0 = identifier.Identifier('ddr-testing-1')
    assert out0.id == expected0.id
    
    out1 = identifier.first_id(
        'entity',
        identifier.Identifier('ddr-testing-123'),
    )
    expected1 = identifier.Identifier('ddr-testing-123-1')
    assert out1.id == expected1.id

def test_identifier_max_id():
    model0 = 'entity'
    identifiers0 = [
        identifier.Identifier('ddr-testing-123-1'),
        identifier.Identifier('ddr-testing-123-2'),
        identifier.Identifier('ddr-testing-123-3'),
    ]
    out0 = identifier.max_id(model0, identifiers0)
    expected0 = identifier.Identifier('ddr-testing-123-3')
    assert out0.id == expected0.id
    
    model1 = 'entity'
    identifiers1 = [
        identifier.Identifier('ddr-testing-123-3'),
        identifier.Identifier('ddr-testing-123-1'),
        identifier.Identifier('ddr-testing-123-2'),
    ]
    out1 = identifier.max_id(model1, identifiers1)
    expected1 = identifier.Identifier('ddr-testing-123-3')
    assert out1.id == expected1.id

    # different parent
    model2 = 'entity'
    identifiers2 = [
        identifier.Identifier('ddr-testing-124-3'),
        identifier.Identifier('ddr-testing-123-1'),
        identifier.Identifier('ddr-testing-123-2'),
    ]
    assert_raises(
        Exception,
        identifier.max_id, model2, identifiers2
    )

    # different model
    model3 = 'entity'
    identifiers3 = [
        identifier.Identifier('ddr-testing-123-1'),
        identifier.Identifier('ddr-testing-123-2'),
        identifier.Identifier('ddr-testing-124'),
    ]
    assert_raises(
        Exception,
        identifier.max_id, model3, identifiers3
    )

    # empty list
    model4 = 'entity'
    identifiers4 = []
    assert_raises(
        Exception,
        identifier.max_id, model4, identifiers4
    )

ADD_ID_INPUT0 = {
    'num_new': 5,
    'model': 'entity',
    'identifiers': [
        identifier.Identifier(id='ddr-test-123-1'),
        identifier.Identifier(id='ddr-test-123-2'),
        identifier.Identifier(id='ddr-test-123-3'),
    ],
    'startwith': 6,
}
ADD_ID_INPUT1 = {
    'num_new': 5,
    'model': 'entity',
    'identifiers': [
        identifier.Identifier(id='ddr-test-123-1'),
        identifier.Identifier(id='ddr-test-123-2'),
        identifier.Identifier(id='ddr-test-123-3'),
    ],
}
ADD_ID_INPUT2 = {
    'num_new': 5,
    'model': 'segment',
    'identifiers': [
        identifier.Identifier(id='ddr-test-123-1-1'),
        identifier.Identifier(id='ddr-test-123-1-2'),
        identifier.Identifier(id='ddr-test-123-1-3'),
    ],
    'startwith': 6,
}
ADD_ID_EXPECTED0 = {
    'success': True,
    'taken': [],
    'new': [6,7,8,9,10],
    'max_id': 4,
}

def test_identifier_add_ids():
    identifier.add_ids(
        ADD_ID_INPUT0['num_new'],
        ADD_ID_INPUT0['model'],
        ADD_ID_INPUT0['identifiers'],
        ADD_ID_INPUT0['startwith'],
    ) == ADD_ID_EXPECTED0
    identifier.add_ids(
        ADD_ID_INPUT1['num_new'],
        ADD_ID_INPUT1['model'],
        ADD_ID_INPUT1['identifiers'],
    ) == ADD_ID_EXPECTED0

def test_identifier_available():
    a = ['a', 'b', 'c']
    b = ['c', 'd', 'e']
    c = ['x', 'y', 'z']
    a_b = {'success': False, 'overlap': ['c']}
    a_c = {'success': True, 'overlap': []}
    assert identifier.available(a, b) == a_b
    assert identifier.available(a, c) == a_c

def test_identifier_wellformed():
    for base_path in BASE_PATHS:
        REPO_PATH_ABS       = os.path.join(base_path, 'ddr')
        ORG_PATH_ABS        = os.path.join(base_path, 'ddr-test')
        COLLECTION_PATH_ABS = os.path.join(base_path, 'ddr-test-123')
        ENTITY_PATH_ABS     = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456')
        FILE_PATH_ABS       = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5')
        
        assert identifier.Identifier.wellformed('id', REPO_ID)
        assert identifier.Identifier.wellformed('id', ORG_ID)
        assert identifier.Identifier.wellformed('id', COLLECTION_ID)
        assert identifier.Identifier.wellformed('id', ENTITY_ID)
        assert identifier.Identifier.wellformed('id', FILE_ID)
        assert_raises(
            Exception,
            identifier.Identifier.wellformed('id', 'ddr_test_123_456_master_abcde12345')
        )
        assert identifier.Identifier.wellformed('path', REPO_PATH_ABS)
        assert identifier.Identifier.wellformed('path', ORG_PATH_ABS)
        assert identifier.Identifier.wellformed('path', COLLECTION_PATH_ABS)
        assert identifier.Identifier.wellformed('path', ENTITY_PATH_ABS)
        assert identifier.Identifier.wellformed('path', FILE_PATH_ABS)

#def test_identifier_valid():
#    COMPONENTS = {
#        'repo': ['ddr',],
#        'org': ['test', 'testing',],
#    }
#    in0 = {'repo':'ddr', 'org':'test', 'cid':'123'}
#    in1 = {'repo':'ddr', 'org':'blat', 'cid':'123'}
#    expected0 = True
#    expected1 = ['org']
#    assert identifier.Identifier.valid(in0, components=COMPONENTS) == expected0 
#    assert identifier.Identifier.valid(in1, components=COMPONENTS) == expected1 

def test_identifier_nextable():
    assert identifier.Identifier.nextable('repository') == False
    assert identifier.Identifier.nextable('organization') == False
    assert identifier.Identifier.nextable('collection') == True
    assert identifier.Identifier.nextable('entity') == True
    assert identifier.Identifier.nextable('file-role') == False
    assert identifier.Identifier.nextable('file') == False

def test_identifier_next():
    i0 = identifier.Identifier('ddr')
    i1 = identifier.Identifier('ddr-testing')
    i2 = identifier.Identifier('ddr-testing-123')
    i3 = identifier.Identifier('ddr-testing-123-456')
    i4 = identifier.Identifier('ddr-testing-123-456-master')
    i5 = identifier.Identifier('ddr-testing-123-456-master-a1b2c3d4e5')
    expected2 = identifier.Identifier('ddr-testing-124')
    expected3 = identifier.Identifier('ddr-testing-123-457')
    assert_raises(
        Exception,
        i0.next,
    )
    assert_raises(
        Exception,
        i1.next,
    )
    out2 = i2.next()
    out3 = i3.next()
    assert_raises(
        Exception,
        i4.next,
    )
    assert_raises(
        Exception,
        i5.next,
    )
    assert out2.id == expected2.id
    assert out3.id == expected3.id

def test_identifier_components():
    in0 = 'ddr'
    in1 = 'ddr-test'
    in2 = 'ddr-test-123'
    in3 = 'ddr-test-123-456'
    in4 = 'ddr-test-123-456-master'
    in5 = 'ddr-test-123-456-master-abcde12345'
    out0 = ['repository', 'ddr']
    out1 = ['organization', 'ddr', 'test']
    out2 = ['collection', 'ddr', 'test', 123]
    out3 = ['entity', 'ddr', 'test', 123, 456]
    out4 = ['file-role', 'ddr', 'test', 123, 456, 'master']
    out5 = ['file', 'ddr', 'test', 123, 456, 'master', 'abcde12345']
    assert identifier.Identifier(id=in0).components() == out0
    assert identifier.Identifier(id=in1).components() == out1
    assert identifier.Identifier(id=in2).components() == out2
    assert identifier.Identifier(id=in3).components() == out3
    assert identifier.Identifier(id=in4).components() == out4
    assert identifier.Identifier(id=in5).components() == out5

# TODO test_identifier_fields_module
# TODO test_identifier_object_class
# TODO test_identifier_object


REPO_ID = 'ddr'
REPO_MODEL = 'repository'
REPO_PARTS = ['ddr']
REPO_REPR = '<DDR.identifier.Identifier repository:ddr>'

ORG_ID = 'ddr-test'
ORG_MODEL = 'organization'
ORG_PARTS = ['ddr', 'test']
ORG_REPR = '<DDR.identifier.Identifier organization:ddr-test>'

COLLECTION_ID = 'ddr-test-123'
COLLECTION_MODEL = 'collection'
COLLECTION_PARTS = ['ddr', 'test', '123']
COLLECTION_REPR = '<DDR.identifier.Identifier collection:ddr-test-123>'

ENTITY_ID = 'ddr-test-123-456'
ENTITY_MODEL = 'entity'
ENTITY_PARTS = ['ddr', 'test', '123', '456']
ENTITY_REPR = '<DDR.identifier.Identifier entity:ddr-test-123-456>'

# FILE_ROLE

FILE_ID = 'ddr-test-123-456-master-a1b2c3d4e5'
FILE_MODEL = 'file'
FILE_PARTS = ['ddr', 'test', '123', '456', 'master', 'a1b2c3d4e5']
FILE_REPR = '<DDR.identifier.Identifier file:ddr-test-123-456-master-a1b2c3d4e5>'


# from_id --------------------------------------------------------------

def test_repository_from_id():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier('ddr')
        i1 = identifier.Identifier('ddr', base_path)
        assert str(i0)  == str(i1)  == REPO_REPR
        assert i0.id    == i1.id    == REPO_ID
        assert i0.model == i1.model == REPO_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_organization_from_id():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier('ddr-test')
        i1 = identifier.Identifier('ddr-test', base_path)
        assert str(i0)  == str(i1)  == ORG_REPR
        assert i0.id    == i1.id    == ORG_ID
        assert i0.model == i1.model == ORG_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_collection_from_id():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier('ddr-test-123')
        i1 = identifier.Identifier('ddr-test-123', base_path)
        assert str(i0)  == str(i1)  == COLLECTION_REPR
        assert i0.id    == i1.id    == COLLECTION_ID
        assert i0.model == i1.model == COLLECTION_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_entity_from_id():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier('ddr-test-123-456')
        i1 = identifier.Identifier('ddr-test-123-456', base_path)
        assert str(i0)  == str(i1)  == ENTITY_REPR
        assert i0.id    == i1.id    == ENTITY_ID
        assert i0.model == i1.model == ENTITY_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

# TODO test_filerole_from_id

def test_file_from_id():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5')
        i1 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5', base_path)
        assert str(i0)  == str(i1)  == FILE_REPR
        assert i0.id    == i1.id    == FILE_ID
        assert i0.model == i1.model == FILE_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

# from_idparts ---------------------------------------------------------

def test_repository_from_idparts():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            {'model':'repository', 'repo':'ddr',}
        )
        i1 = identifier.Identifier(
            {'model':'repository', 'repo':'ddr',},
            base_path
        )
        assert str(i0)  == str(i1)  == REPO_REPR
        assert i0.id    == i1.id    == REPO_ID
        assert i0.model == i1.model == REPO_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_organization_from_idparts():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            {'model':'organization', 'repo':'ddr', 'org':'test',}
        )
        i1 = identifier.Identifier(
            {'model':'organization', 'repo':'ddr', 'org':'test',},
            base_path
        )
        assert str(i0)  == str(i1)  == ORG_REPR
        assert i0.id    == i1.id    == ORG_ID
        assert i0.model == i1.model == ORG_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_collection_from_idparts():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            {'model':'collection', 'repo':'ddr', 'org':'test', 'cid':123,}
        )
        i1 = identifier.Identifier(
            {'model':'collection', 'repo':'ddr', 'org':'test', 'cid':123,},
            base_path
        )
        assert str(i0)  == str(i1)  == COLLECTION_REPR
        assert i0.id    == i1.id    == COLLECTION_ID
        assert i0.model == i1.model == COLLECTION_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_entity_from_idparts():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            {'model':'entity', 'repo':'ddr', 'org':'test', 'cid':123, 'eid':456,}
        )
        i1 = identifier.Identifier(
            {'model':'entity', 'repo':'ddr', 'org':'test', 'cid':123, 'eid':456,},
            base_path
        )
        assert str(i0)  == str(i1)  == ENTITY_REPR
        assert i0.id    == i1.id    == ENTITY_ID
        assert i0.model == i1.model == ENTITY_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

# TODO test_filerole_from_idparts

def test_file_from_idparts():
    for base_path in BASE_PATHS:
        idparts = {
                'model':'file',
                'repo':'ddr', 'org':'test', 'cid':123,
                'eid':456, 'role':'master', 'sha1':'a1b2c3d4e5',
            }
        i0 = identifier.Identifier(idparts)
        i1 = identifier.Identifier(idparts, base_path)
        assert str(i0)  == str(i1)  == FILE_REPR
        assert i0.id    == i1.id    == FILE_ID
        assert i0.model == i1.model == FILE_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

# from_path ------------------------------------------------------------

def test_repository_from_path():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(path=os.path.join(base_path, 'ddr'))
        assert str(i0) == REPO_REPR
        assert i0.id == REPO_ID
        assert i0.model == REPO_MODEL
        assert i0.basepath == base_path

def test_organization_from_path():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test'))
        i1 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test/'))
        assert str(i0)  == str(i1)  == ORG_REPR
        assert i0.id    == i1.id    == ORG_ID
        assert i0.model == i1.model == ORG_MODEL
        assert i0.basepath == i1.basepath == base_path

def test_collection_from_path():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123'))
        i1 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123/'))
        i2 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123/collection.json'))
        assert str(i0)     == str(i1)     == str(i2)     == COLLECTION_REPR
        assert i0.id       == i1.id       == i2.id       == COLLECTION_ID
        assert i0.model    == i1.model    == i2.model    == COLLECTION_MODEL
        assert i0.basepath == i1.basepath == i2.basepath == base_path

def test_entity_from_path():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456'))
        i1 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/'))
        i2 = identifier.Identifier(path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/entity.json'))
        assert str(i0)     == str(i1)     == str(i2)     == ENTITY_REPR
        assert i0.id       == i1.id       == i2.id       == ENTITY_ID
        assert i0.model    == i1.model    == i2.model    == ENTITY_MODEL
        assert i0.basepath == i1.basepath == i2.basepath == base_path

# TODO test_filerole_from_path

def test_file_from_path():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5')
        )
        i1 = identifier.Identifier(
            path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5/')
        )
        i2 = identifier.Identifier(
            path=os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5.json')
        )
        assert str(i0)     == str(i1)     == str(i2)     == FILE_REPR
        assert i0.id       == i1.id       == i2.id       == FILE_ID
        assert i0.model    == i1.model    == i2.model    == FILE_MODEL
        assert i0.basepath == i1.basepath == i2.basepath == base_path

# from_url -------------------------------------------------------------

def test_repository_from_url():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(url='http://192.168.56.101/ddr')
        i1 = identifier.Identifier(url='http://192.168.56.101/ddr/', base_path=base_path)
        print('i0 %s' % i0)
        assert str(i0)  == str(i1)  == REPO_REPR
        assert i0.id    == i1.id    == REPO_ID
        assert i0.model == i1.model == REPO_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_organization_from_url():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(url='http://192.168.56.101/ddr/test')
        i1 = identifier.Identifier(url='http://192.168.56.101/ddr/test/', base_path=base_path)
        assert str(i0)  == str(i1)  == ORG_REPR
        assert i0.id    == i1.id    == ORG_ID
        assert i0.model == i1.model == ORG_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

def test_collection_from_url():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(url='http://192.168.56.101/ddr/test/123')
        i1 = identifier.Identifier(url='http://192.168.56.101/ddr/test/123/')
        i2 = identifier.Identifier(url='http://192.168.56.101/ddr/test/123/', base_path=base_path)
        assert_raises(
            Exception,
            identifier.Identifier,
            url='http://192.168.56.101/ddr/test/123/',
            base_path='ddr/test/123'
        )
        assert str(i0)  == str(i1)  == COLLECTION_REPR
        assert i0.id    == i1.id    == COLLECTION_ID
        assert i0.model == i1.model == COLLECTION_MODEL
        assert i0.basepath == i1.basepath == None
        assert i2.basepath == base_path

def test_entity_from_url():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(url='http://192.168.56.101/ddr/test/123/456')
        i1 = identifier.Identifier(url='http://192.168.56.101/ddr/test/123/456/', base_path=base_path)
        assert_raises(
            Exception,
            identifier.Identifier,
            url='http://192.168.56.101/ddr/test/123/456/',
            base_path='ddr/test/123/456'
        )
        assert str(i0)  == str(i1)  == ENTITY_REPR
        assert i0.id    == i1.id    == ENTITY_ID
        assert i0.model == i1.model == ENTITY_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path

# TODO test_filerole_from_id

def test_file_from_url():
    for base_path in BASE_PATHS:
        i0 = identifier.Identifier(
            url='http://192.168.56.101/ddr/test/123/456/master/a1b2c3d4e5'
        )
        i1 = identifier.Identifier(
            url='http://192.168.56.101/ddr/test/123/456/master/a1b2c3d4e5/',
            base_path=base_path
        )
        assert_raises(
            Exception,
            identifier.Identifier,
            url='http://192.168.56.101/ddr/test/123/456/master/a1b2c3d4e5/',
            base_path='ddr/test/123/456/master/a1b2c3d4e5'
        )
        assert str(i0)  == str(i1)  == FILE_REPR
        assert i0.id    == i1.id    == FILE_ID
        assert i0.model == i1.model == FILE_MODEL
        assert i0.basepath == None
        assert i1.basepath == base_path


REPO_COLLECTION_ID = None
ORG_COLLECTION_ID = None
COLLECTION_COLLECTION_ID = 'ddr-test-123'
ENTITY_COLLECTION_ID = 'ddr-test-123'
FILE_COLLECTION_ID = 'ddr-test-123'

def test_collection_id():
    i0 = identifier.Identifier('ddr')
    i1 = identifier.Identifier('ddr-test')
    i2 = identifier.Identifier('ddr-test-123')
    i3 = identifier.Identifier('ddr-test-123-456')
    i4 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5')
    assert_raises(Exception, i0, 'collection_id')
    assert_raises(Exception, i1, 'collection_id')
    assert i2.collection_id() == COLLECTION_COLLECTION_ID
    assert i3.collection_id() == ENTITY_COLLECTION_ID
    assert i4.collection_id() == FILE_COLLECTION_ID


def test_collection_path():
    for base_path in BASE_PATHS:
        REPO_COLLECTION_PATH = None
        ORG_COLLECTION_PATH = None
        COLLECTION_COLLECTION_PATH = os.path.join(base_path, 'ddr-test-123')
        ENTITY_COLLECTION_PATH = os.path.join(base_path, 'ddr-test-123')
        FILE_COLLECTION_PATH = os.path.join(base_path, 'ddr-test-123')

        i0 = identifier.Identifier('ddr')
        i1 = identifier.Identifier('ddr-test')
        i2 = identifier.Identifier('ddr-test-123')
        i3 = identifier.Identifier('ddr-test-123-456')
        i4 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5')
        assert_raises(Exception, i0, 'collection_path')
        assert_raises(Exception, i1, 'collection_path')
        assert_raises(Exception, i2, 'collection_path')
        assert_raises(Exception, i3, 'collection_path')
        assert_raises(Exception, i4, 'collection_path')
         
        i0 = identifier.Identifier('ddr', base_path)
        i1 = identifier.Identifier('ddr-test', base_path)
        i2 = identifier.Identifier('ddr-test-123', base_path)
        i3 = identifier.Identifier('ddr-test-123-456', base_path)
        i4 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5', base_path)
        assert_raises(Exception, i0, 'collection_path')
        assert_raises(Exception, i1, 'collection_path')
        assert i2.collection_path() == COLLECTION_COLLECTION_PATH
        assert i3.collection_path() == ENTITY_COLLECTION_PATH
        assert i4.collection_path() == FILE_COLLECTION_PATH

# TODO test_identifier_collection


PARENT_REPO_ID = 'ddr'
PARENT_ORG_ID = 'ddr-test'
PARENT_COLLECTION_ID = 'ddr-test-123'
PARENT_ENTITY_ID = 'ddr-test-123-456'
PARENT_FILEROLE_ID = 'ddr-test-123-456-master'
PARENT_FILE_ID = 'ddr-test-123-456-master-a1b2c3d4e5'

def test_parent_id():
    rep = identifier.Identifier(PARENT_REPO_ID)
    org = identifier.Identifier(PARENT_ORG_ID)
    col = identifier.Identifier(PARENT_COLLECTION_ID)
    ent = identifier.Identifier(PARENT_ENTITY_ID)
    rol = identifier.Identifier(PARENT_FILEROLE_ID)
    fil = identifier.Identifier(PARENT_FILE_ID)
    assert col.parent_id() == ''
    assert ent.parent_id() == PARENT_COLLECTION_ID
    assert fil.parent_id() == PARENT_ENTITY_ID
    assert rep.parent_id(stubs=1) == ''
    assert org.parent_id(stubs=1) == PARENT_REPO_ID
    assert col.parent_id(stubs=1) == PARENT_ORG_ID
    assert ent.parent_id(stubs=1) == PARENT_COLLECTION_ID
    assert rol.parent_id(stubs=1) == PARENT_ENTITY_ID
    assert fil.parent_id(stubs=1) == PARENT_FILEROLE_ID

def test_parent_path():
    basepath='/tmp/ddr-test-123'
    fi = identifier.Identifier(id='ddr-test-123-456-master-abcde12345', base_path=basepath)
    assert fi.parent_path() == '/tmp/ddr-test-123/ddr-test-123/files/ddr-test-123-456'
    ei = identifier.Identifier(id='ddr-test-123-456', base_path=basepath)
    assert ei.parent_path() == '/tmp/ddr-test-123/ddr-test-123'
    ci = identifier.Identifier(id='ddr-test-123', base_path=basepath)
    assert ci.parent_path() == ''

def test_parent():
    i = identifier.Identifier(id='ddr-test-123-456-master-abcde12345')
    assert i.parent().id == 'ddr-test-123-456'
    assert i.parent(stubs=1).id == 'ddr-test-123-456-master'
    assert i.parent().__class__ == i.__class__
    assert i.parent(stubs=1).__class__ == i.__class__

# TODO this test relies on ddr-defs, not test data...
CHILD_MODELS_DATA = [
    ('ddr', False, []),
    ('ddr-test', False, []),
    ('ddr-test-123', False, ['entity']),
    ('ddr-test-123-456', False, ['segment', 'file']),
    ('ddr-test-123-456-master', False, []),
    ('ddr-test-123-456-master-abc123', False, []),
    ('ddr', True, ['organization']),
    ('ddr-test', True, ['collection']),
    ('ddr-test-123', True, ['entity']),
    ('ddr-test-123-456', True, ['file-role', 'segment']),
    ('ddr-test-123-456-master', True, ['file']),
    ('ddr-test-123-456-master-abc123', True, []),
]
def test_child_models():
    for oid,stubs,expected in CHILD_MODELS_DATA:
        out = identifier.Identifier(id=oid).child_models(stubs)
        # test contents of list regardless of order
        assert set(out) == set(expected)

def test_child():
    i = identifier.Identifier(id='ddr-test-123')
    assert i.child('entity', {'eid':'456'}).id == 'ddr-test-123-456'
    assert i.child('entity', {'eid':'456'}).__class__ == i.__class__
    assert_raises(
        Exception,
        i.child,
        'file', {'eid':'456'}
    )

def test_lineage():
    re = identifier.Identifier(id='ddr')
    og = identifier.Identifier(id='ddr-test')
    co = identifier.Identifier(id='ddr-test-123')
    en = identifier.Identifier(id='ddr-test-123-456')
    fr = identifier.Identifier(id='ddr-test-123-456-master')
    fi = identifier.Identifier(id='ddr-test-123-456-master-abcde12345')
    
    def sameclass(i, lineage):
        matches = [x.__class__ for x in lineage if x.__class__ == i.__class__]
        return len(matches) == len(lineage)

    assert sameclass(fi, fi.lineage()) == True
    assert sameclass(fi, fi.lineage(stubs=1)) == True
    assert len(fi.lineage()) == 3
    assert len(fi.lineage(stubs=1)) == 6



def test_path_abs():
    for base_path in BASE_PATHS:
        REPO_PATH_ABS       = os.path.join(base_path, 'ddr')
        ORG_PATH_ABS        = os.path.join(base_path, 'ddr-test')
        COLLECTION_PATH_ABS = os.path.join(base_path, 'ddr-test-123')
        ENTITY_PATH_ABS     = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456')
        FILE_PATH_ABS       = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5')
        REPO_PATH_ABS_JSON       = os.path.join(base_path, 'ddr/repository.json')
        ORG_PATH_ABS_JSON        = os.path.join(base_path, 'ddr-test/organization.json')
        COLLECTION_PATH_ABS_JSON = os.path.join(base_path, 'ddr-test-123/collection.json')
        ENTITY_PATH_ABS_JSON     = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/entity.json')
        FILE_PATH_ABS_JSON       = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5.json')
        FILE_PATH_ABS_ACCESS = os.path.join(base_path, 'ddr-test-123/files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5-a.jpg')
        
        ri0 = identifier.Identifier(
            'ddr'
        )
        assert_raises(Exception, ri0, 'path_abs')
        assert_raises(Exception, ri0, 'path_abs', 'json')
        assert_raises(Exception, ri0, 'path_abs', 'BAD')
        ri1 = identifier.Identifier(
            'ddr',
            base_path
        )
        assert ri1.path_abs()       == REPO_PATH_ABS
        assert ri1.path_abs('json') == REPO_PATH_ABS_JSON
        assert_raises(Exception, ri1, 'path_abs', 'BAD')
        
        oi0 = identifier.Identifier(
            'ddr-test'
        )
        assert_raises(Exception, oi0, 'path_abs')
        assert_raises(Exception, oi0, 'path_abs', 'json')
        assert_raises(Exception, oi0, 'path_abs', 'BAD')
        oi1 = identifier.Identifier(
            'ddr-test',
            base_path
        )
        assert oi1.path_abs()       == ORG_PATH_ABS
        assert oi1.path_abs('json') == ORG_PATH_ABS_JSON
        assert_raises(Exception, oi1, 'path_abs', 'BAD')
        
        ci0 = identifier.Identifier(
            'ddr-test-123'
        )
        assert_raises(Exception, ci0, 'path_abs')
        assert_raises(Exception, ci0, 'path_abs', 'json')
        assert_raises(Exception, ci0, 'path_abs', 'BAD')
        ci1 = identifier.Identifier(
            'ddr-test-123',
            base_path
        )
        assert ci1.path_abs()       == COLLECTION_PATH_ABS
        assert ci1.path_abs('json') == COLLECTION_PATH_ABS_JSON
        assert_raises(Exception, ci1, 'path_abs', 'BAD')
        
        ei0 = identifier.Identifier(
            'ddr-test-123-456'
        )
        assert_raises(Exception, ei0, 'path_abs')
        assert_raises(Exception, ei0, 'path_abs', 'json')
        assert_raises(Exception, ei0, 'path_abs', 'BAD')
        ei1 = identifier.Identifier(
            'ddr-test-123-456',
            base_path
        )
        assert ei1.path_abs()       == ENTITY_PATH_ABS
        assert ei1.path_abs('json') == ENTITY_PATH_ABS_JSON
        assert_raises(Exception, ei1, 'path_abs', 'BAD')
        
        fi0 = identifier.Identifier(
            'ddr-test-123-456-master-a1b2c3d4e5'
        )
        assert_raises(Exception, fi0, 'path_abs')
        assert_raises(Exception, fi0, 'path_abs', 'access')
        assert_raises(Exception, fi0, 'path_abs', 'json')
        assert_raises(Exception, fi0, 'path_abs', 'BAD')
        fi1 = identifier.Identifier(
            'ddr-test-123-456-master-a1b2c3d4e5',
            base_path
        )
        assert fi1.path_abs()         == FILE_PATH_ABS
        assert fi1.path_abs('access') == FILE_PATH_ABS_ACCESS
        assert fi1.path_abs('json')   == FILE_PATH_ABS_JSON
        assert_raises(Exception, fi1, 'path_abs', 'BAD')


REPO_PATH_REL       = ''
ORG_PATH_REL        = ''
COLLECTION_PATH_REL = ''
ENTITY_PATH_REL     = 'files/ddr-test-123-456'
FILE_PATH_REL       = 'files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5'
REPO_PATH_REL_JSON       = 'repository.json'
ORG_PATH_REL_JSON        = 'organization.json'
COLLECTION_PATH_REL_JSON = 'collection.json'
ENTITY_PATH_REL_JSON     = 'files/ddr-test-123-456/entity.json'
FILE_PATH_REL_JSON       = 'files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5.json'    
FILE_PATH_REL_ACCESS     = 'files/ddr-test-123-456/files/ddr-test-123-456-master-a1b2c3d4e5-a.jpg'

def test_path_rel():
    i0 = identifier.Identifier('ddr')
    assert i0.path_rel()         == REPO_PATH_REL
    assert i0.path_rel('json')   == REPO_PATH_REL_JSON
    assert_raises(Exception, i0, 'path_rel', 'BAD')
    
    i1 = identifier.Identifier('ddr-test')
    assert i1.path_rel()         == ORG_PATH_REL
    assert i1.path_rel('json')   == ORG_PATH_REL_JSON
    assert_raises(Exception, i1, 'path_rel', 'BAD')
    
    i2 = identifier.Identifier('ddr-test-123')
    assert i2.path_rel()         == COLLECTION_PATH_REL
    assert i2.path_rel('json')   == COLLECTION_PATH_REL_JSON
    assert_raises(Exception, i2, 'path_rel', 'BAD')
    
    i3 = identifier.Identifier('ddr-test-123-456')
    assert i3.path_rel()         == ENTITY_PATH_REL
    assert i3.path_rel('json')   == ENTITY_PATH_REL_JSON
    assert_raises(Exception, i3, 'path_rel', 'BAD')
    
    i4 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5')
    assert i4.path_rel()         == FILE_PATH_REL
    assert i4.path_rel('access') == FILE_PATH_REL_ACCESS
    assert i4.path_rel('json')   == FILE_PATH_REL_JSON
    assert_raises(Exception, i4, 'path_rel', 'BAD')


REPO_EDITOR_URL       = '/ui/ddr'
ORG_EDITOR_URL        = '/ui/ddr-test'
COLLECTION_EDITOR_URL = '/ui/ddr-test-123'
ENTITY_EDITOR_URL     = '/ui/ddr-test-123-456'
FILE_EDITOR_URL       = '/ui/ddr-test-123-456-master-a1b2c3d4e5'
REPO_PUBLIC_URL       = '/ddr'
ORG_PUBLIC_URL        = '/ddr/test'
COLLECTION_PUBLIC_URL = '/ddr/test/123'
ENTITY_PUBLIC_URL     = '/ddr/test/123/456'
FILE_PUBLIC_URL       = '/ddr/test/123/456/master/a1b2c3d4e5'

def test_urlpath():
    i0 = identifier.Identifier('ddr')
    assert i0.urlpath('editor') == REPO_EDITOR_URL
    assert i0.urlpath('public') == REPO_PUBLIC_URL
    
    i1 = identifier.Identifier('ddr-test')
    assert i1.urlpath('editor') == ORG_EDITOR_URL
    assert i1.urlpath('public') == ORG_PUBLIC_URL
    
    i2 = identifier.Identifier('ddr-test-123')
    assert i2.urlpath('editor') == COLLECTION_EDITOR_URL
    assert i2.urlpath('public') == COLLECTION_PUBLIC_URL
    
    i3 = identifier.Identifier('ddr-test-123-456')
    assert i3.urlpath('editor') == ENTITY_EDITOR_URL
    assert i3.urlpath('public') == ENTITY_PUBLIC_URL
    
    i4 = identifier.Identifier('ddr-test-123-456-master-a1b2c3d4e5')
    assert i4.urlpath('editor') == FILE_EDITOR_URL
    assert i4.urlpath('public') == FILE_PUBLIC_URL


