from datetime import datetime
import json

import models
import modules


def test_Module_path():
    class TestModule(object):
        pass
    
    module = TestModule()
    module.__file__ = '/var/www/media/base/ddr/repo_models/testmodule.pyc'
    assert modules.Module(module).path == '/var/www/media/base/ddr/repo_models/testmodule.py'

def test_Module_is_valid():
    class TestModule0(object):
        __name__ = 'TestModule0'
        __file__ = ''
    
    class TestModule1(object):
        __name__ = 'TestModule1'
        __file__ = 'ddr/repo_models'
    
    class TestModule2(object):
        __name__ = 'TestModule2'
        __file__ = 'ddr/repo_models'
        FIELDS = 'not a list'
    
    class TestModule3(object):
        __name__ = 'TestModule3'
        __file__ = 'ddr/repo_models'
        FIELDS = ['fake fields']
    
    assert modules.Module(TestModule0()).is_valid() == (False,"TestModule0 not in 'ddr' Repository repo.")
    assert modules.Module(TestModule1()).is_valid() == (False,'TestModule1 has no FIELDS variable.')
    assert modules.Module(TestModule2()).is_valid() == (False,'TestModule2.FIELDS is not a list.')
    assert modules.Module(TestModule3()).is_valid() == (True,'ok')

#def test_field_names():
#    # we're using a class not a module but functionally it's the same
#    class TestModule(object):
#        __name__ = 'TestModule'
#        __file__ = 'ddr/repo_models'
#        MODEL = None
#        FIELDS_CSV_EXCLUDED = []
#        FIELDS = []
#    m = TestModule()
#    m.FIELDS = [{'name':'id'}, {'name':'title'}, {'name':'description'}]
#    m.FIELDS_CSV_EXCLUDED = ['description']
#    # test
#    m.MODEL = 'collection'
#    assert modules.Module(TestModule()).field_names(m) == ['id', 'title']
#    m.MODEL = 'entity'
#    assert modules.Module(TestModule()).field_names(m) == ['id', 'title']
#    m.MODEL = 'file'
#    assert modules.Module(TestModule()).field_names(m) == ['file_id', 'id', 'title']
#    m.MODEL = 'entity'

#def test_required_fields():
#    # we're using a class not a module but functionally it's the same
#    fields = [
#        {'name':'id', 'form':{'required':True}},
#        {'name':'title', 'form':{'required':True}},
#        {'name':'description', 'form':{'required':False}},
#        {'name':'formless'},
#        {'name':'files', 'form':{'required':True}},
#    ]
#    exceptions = ['files', 'whatever']
#    expected = ['id', 'title']
#    assert batch_update.get_required_fields(fields, exceptions) == expected

def test_Module_function():
    class TestModule(object):
        def hello(self, text):
            return 'hello %s' % text
    
    module = TestModule()
    module.__file__ = 'ddr/repo_models'
    assert modules.Module(module).function('hello', 'world') == 'hello world'

# TODO Module_xml_function

class TestModule(object):
    __name__ = 'TestModule'
    __file__ = 'ddr/repo_models'
    FIELDS = [
        {
            'name': 'id',
            'model_type': str,
            'form': {
                'label': 'Object ID',
            },
            'default': '',
        },
        {
            'name': 'modified',
            'model_type': datetime,
            'form': {
                'label': 'Last Modified',
            },
            'default': '',
        },
        {
            'name': 'title',
            'model_type': str,
            'form': {
                'label': 'Title',
            },
            'default': '',
        },
    ]

class TestDocument():
    pass

def test_Module_labels_values():
    module = TestModule()
    document = TestDocument()
    data = [
        {'id': 'ddr-test-123'},
        {'modified': '2015-10-20T15:42:26'},
        {'title': 'labels_values'},
    ]
    json_data = models.common.load_json(document, module, json.dumps(data))
    expected = [
        {'value': u'ddr-test-123', 'label': 'Object ID'},
        {'value': u'2015-10-20T15:42:26', 'label': 'Last Modified'},
        {'value': u'labels_values', 'label': 'Title'}
    ]
    assert modules.Module(module).labels_values(document) == expected

def test_Module_parse_commit():
    module = TestModule()
    text = '95a3a0ed3232990ee8fbbc3065a11316bccd0b35  2015-03-26 15:49:58 -0700'
    expected = '95a3a0ed3232990ee8fbbc3065a11316bccd0b35'
    assert modules.Module(module)._parse_commit(text) == expected

def test_Module_document_commit():
    module = TestModule()
    # commit exists
    document = TestDocument()
    document.object_metadata = {
        "models_commit": "20dd4e2096e6f9a9eb7c2db52907b094f41f58de  2015-10-13 17:08:43 -0700",
    }
    expected = '20dd4e2096e6f9a9eb7c2db52907b094f41f58de'
    assert modules.Module(module).document_commit(document) == expected
    # no commit
    document = TestDocument()
    document.object_metadata = {}
    expected = None
    assert modules.Module(module).document_commit(document) == expected

# TODO Module_module_commit

# TODO Module_cmp_model_definition_commits

def test_Module_cmp_model_definition_fields():
    module = TestModule()
    module.FIELDS = [
        {'name': 'id',},
        {'name': 'modified',},
        {'name': 'title',},
    ]
    m = modules.Module(module)
    data = [
        {},  # object_metadata
        {'id': 'ddr-test-123'},
        {'modified': '2015-10-20T15:42:26'},
        {'title': 'labels_values'},
    ]
    
    expected0 = {'removed': [], 'added': []}
    out0 = m.cmp_model_definition_fields(json.dumps(data))
    
    data.append( {'new': 'new field'} )
    expected1 = {'removed': [], 'added': ['new']}
    out1 = m.cmp_model_definition_fields(json.dumps(data))
    
    data.pop()  # rm new
    data.pop()  # rm title
    expected2 = {'removed': ['title'], 'added': []}
    out2 = m.cmp_model_definition_fields(json.dumps(data))
    
    assert out0 == expected0
    assert out1 == expected1
    assert out2 == expected2
