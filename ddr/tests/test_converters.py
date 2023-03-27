"""Functions for converting between text and various data structures.
"""

from datetime import datetime

from DDR import config
from DDR import converters
from repo_models import entity as entity_defs


def test_normalize_string():
    assert converters.normalize_string(None) == u''
    assert converters.normalize_string(1) == 1
    assert converters.normalize_string([]) == []
    assert converters.normalize_string({}) == {}
    assert converters.normalize_string('') == u''
    assert converters.normalize_string('a\r\nb') == u'a\nb'
    assert converters.normalize_string('a\rb') == u'a\nb'
    assert converters.normalize_string(' ab') == u'ab'
    assert converters.normalize_string('\nab') == u'ab'
    assert converters.normalize_string('ab\n') == u'ab'
    assert converters.normalize_string('ab ') == u'ab'


# TODO load_dirty_json


STRIP_LIST_DATA = ['a', '']
STRIP_LIST_EXPECTED = ['a']

def test_strip_list():
    assert converters.strip_list(STRIP_LIST_DATA) == STRIP_LIST_EXPECTED
    assert converters.strip_list(STRIP_LIST_EXPECTED) == STRIP_LIST_EXPECTED


def test_render():
    template = """<a href="{{ data.url }}">{{ data.label }}</a>"""
    data = {'url':'http://densho.org', 'label':'Densho'}
    expected = """<a href="http://densho.org">Densho</a>"""
    assert converters.render(template,data) == expected


COERCE_TEXT_DATA0 = 1
COERCE_TEXT_EXPECTED0 = u'1'
COERCE_TEXT_DATA1 = datetime(2017,4,28, 10,51,27, tzinfo=config.TZ)
COERCE_TEXT_EXPECTED1 = u'2017-04-28T10:51:27PDT-0700'

def test_coerce_text():
    # int
    assert converters.coerce_text(COERCE_TEXT_DATA0) == COERCE_TEXT_EXPECTED0
    assert converters.coerce_text(COERCE_TEXT_EXPECTED0) == COERCE_TEXT_EXPECTED0
    # datetime
    assert converters.coerce_text(COERCE_TEXT_DATA1) == COERCE_TEXT_EXPECTED1
    assert converters.coerce_text(COERCE_TEXT_EXPECTED1) == COERCE_TEXT_EXPECTED1


TEXT_TO_BOOLEAN = [
    (None,    False),
    (False,   False),
    (True,    True),
    (0,       False),
    ('0',     False),
    ('false', False),
    ('False', False),
    (1,       True),
    ('1',     True),
    ('true',  True),
    ('True',  True),
]

def test_text_to_boolean():
    for input_,expected in TEXT_TO_BOOLEAN:
        print('%s (%s) -> %s' % (input_, type(input_), expected))
        assert converters.text_to_boolean(input_) == expected

TEXT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
TEXT_DATETIME_TEXT0 = '2016-08-31T15:42:17'
TEXT_DATETIME_TEXT1 = '2016-08-31T15:42:17-07:00'
TEXT_DATETIME_DATA_NOTZ = datetime(2016,8,31,15,42,17)
TEXT_DATETIME_DATA_TZ = datetime(2016,8,31,15,42,17,tzinfo=config.TZ)

def test_text_to_datetime():
    assert converters.text_to_datetime(TEXT_DATETIME_TEXT0) == TEXT_DATETIME_DATA_NOTZ
    assert converters.text_to_datetime(TEXT_DATETIME_TEXT1) == TEXT_DATETIME_DATA_TZ
    # already in target format
    assert converters.text_to_datetime(TEXT_DATETIME_DATA_NOTZ) == TEXT_DATETIME_DATA_NOTZ

def test_datetime_to_text():
    assert converters.datetime_to_text(TEXT_DATETIME_DATA_NOTZ) == TEXT_DATETIME_TEXT0


TEXTLIST_TEXT = 'thing1; thing2'
TEXTLIST_DATA = ['thing1', 'thing2']

def test_is_listofstrs():
    assert converters._is_listofstrs(TEXTLIST_TEXT) == False
    BAD0 = {'abc':123}
    BAD1 = [{'abc':123}]
    assert converters._is_listofstrs(BAD0) == False
    assert converters._is_listofstrs(BAD1) == False
    assert converters._is_listofstrs(TEXTLIST_DATA) == True

def test_text_to_list():
    assert converters.text_to_list(TEXTLIST_TEXT) == TEXTLIST_DATA
    # already in target format
    assert converters.text_to_list(TEXTLIST_DATA) == TEXTLIST_DATA

def test_list_to_text():
    assert converters.list_to_text(TEXTLIST_DATA) == TEXTLIST_TEXT

TEXT_DICT_TEXT_LABELS    = "term:Hawai'i|id:123"
TEXT_DICT_TEXT_NOLABELS  = "Hawai'i:123"
TEXT_DICT_TEXT_BRACKETID = "Hawai'i [123]"
TEXT_DICT_TEXT_BRACKETID_NL = "Hawai'i\n[123]"
TEXT_DICT_TEXT_BRACKETID_QUOTES = "Hawai'i [123]"
TEXT_DICT_KEYS = ['term', 'id']
TEXT_DICT_SEPARATORS = ':|'
TEXT_DICT_SEPARATOR = ':'
TEXT_DICT_DATA = {'term': u"Hawai'i", 'id': '123'}

TEXT_DICT_KEYS_DATE = ['term', 'startdate']
TEXT_DICT_TEXT_NOLABELS_DATE = "Hawai'i:1970-01-01 00:00:00"
TEXT_DICT_DATA_DATE = {'term': u"Hawai'i", 'startdate': '1970-01-01 00:00:00'}

def test_is_text_labels():
    assert converters._is_text_labels(TEXT_DICT_TEXT_LABELS)
    assert converters._is_text_labels(TEXT_DICT_TEXT_NOLABELS) == False
    assert converters._is_text_labels(TEXT_DICT_TEXT_BRACKETID) == False

def test_textlabels_to_dict():
    assert converters.textlabels_to_dict('', []) == {}
    assert converters.textlabels_to_dict(TEXT_DICT_TEXT_LABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA

def test_dict_to_textlabels():
    assert converters.dict_to_textlabels(TEXT_DICT_DATA, TEXT_DICT_KEYS, TEXT_DICT_SEPARATORS) == TEXT_DICT_TEXT_LABELS

def test_is_text_nolabels():
    assert converters._is_text_nolabels(TEXT_DICT_TEXT_LABELS) == False
    assert converters._is_text_nolabels(TEXT_DICT_TEXT_NOLABELS)
    assert converters._is_text_nolabels(TEXT_DICT_TEXT_BRACKETID) == False

def test_textnolabels_to_dict():
    assert converters.textnolabels_to_dict('', []) == {}
    assert converters.textnolabels_to_dict(TEXT_DICT_TEXT_NOLABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.textnolabels_to_dict(TEXT_DICT_TEXT_NOLABELS_DATE, TEXT_DICT_KEYS_DATE) == TEXT_DICT_DATA_DATE

def test_dict_to_textnolabels():
    assert converters.dict_to_textnolabels(TEXT_DICT_DATA, TEXT_DICT_KEYS, TEXT_DICT_SEPARATOR) == TEXT_DICT_TEXT_NOLABELS

def test_is_text_bracketid():
    assert converters._is_text_bracketid(TEXT_DICT_TEXT_LABELS) == False
    assert converters._is_text_bracketid(TEXT_DICT_TEXT_NOLABELS) == False
    assert converters._is_text_bracketid(TEXT_DICT_TEXT_BRACKETID)

def test_textbracketid_to_dict():
    assert converters.textbracketid_to_dict('', []) == {}
    assert converters.textbracketid_to_dict(TEXT_DICT_TEXT_BRACKETID, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.textbracketid_to_dict(TEXT_DICT_TEXT_BRACKETID_NL, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.textbracketid_to_dict(TEXT_DICT_TEXT_BRACKETID_QUOTES, TEXT_DICT_KEYS) == TEXT_DICT_DATA

def test_dict_to_textbracketid():
    assert converters.dict_to_textbracketid(TEXT_DICT_DATA, TEXT_DICT_KEYS) == TEXT_DICT_TEXT_BRACKETID

def test_text_to_dict():
    assert converters.text_to_dict('', []) == {}
    assert converters.text_to_dict(TEXT_DICT_TEXT_LABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.text_to_dict(TEXT_DICT_TEXT_NOLABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.text_to_dict(TEXT_DICT_TEXT_BRACKETID, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    
    TEXT0 = "Geographic communities: Hawai`i [277]"; DATA0 = {'id':'277', 'term':"Geographic communities: Hawai`i"}
    TEXT1 = "Geographic communities: Hawai'i [277]"; DATA1 = {'id':'277', 'term':"Geographic communities: Hawai'i"}
    TEXT2 = 'Race and racism: "Yellow Peril" [185]'; DATA2 = {'id':'185', 'term':'Race and racism: "Yellow Peril"'}
    TEXT3 = 'Painting: Ink painting (sumi-e) [266]'; DATA3 = {'id':'266', 'term':'Painting: Ink painting (sumi-e)'}
    TEXT4 = 'Reflections: September 11, 2001 [169]'; DATA4 = {'id':'169', 'term':'Reflections: September 11, 2001'}
    TEXT5 = 'Oregon: Gresham/Troutdale [285]';       DATA5 = {'id':'285', 'term':'Oregon: Gresham/Troutdale'}

    def testit(text, keys, data):
        print(text)
        print(data)
        output = converters.text_to_dict(text, keys)
        print(output)
        assert output == data
    
    testit(TEXT0, ['term', 'id'], DATA0)
    testit(TEXT1, ['term', 'id'], DATA1)
    testit(TEXT2, ['term', 'id'], DATA2)
    testit(TEXT3, ['term', 'id'], DATA3)
    testit(TEXT4, ['term', 'id'], DATA4)
    testit(TEXT5, ['term', 'id'], DATA5)

def test_dict_to_text():
    assert converters.dict_to_text({}, []) == ''
    assert converters.dict_to_text(TEXT_DICT_DATA, TEXT_DICT_KEYS, style='labels') == TEXT_DICT_TEXT_LABELS
    assert converters.dict_to_text(TEXT_DICT_DATA, TEXT_DICT_KEYS, style='nolabels') == TEXT_DICT_TEXT_NOLABELS
    assert converters.dict_to_text(TEXT_DICT_DATA, TEXT_DICT_KEYS, style='bracketid') == TEXT_DICT_TEXT_BRACKETID

TEXTKVLIST_TEXT = 'name1:author; name2:photog'
TEXTKVLIST_DATA = [
    {u'name1': u'author'},
    {u'name2': u'photog'}
]

def test_is_kvlist():
    assert converters._is_kvlist('') == False
    assert converters._is_kvlist({}) == False
    BAD0 = ['abc', 'def']
    BAD1 = [['abc'], ['def']]
    assert converters._is_kvlist(BAD0) == False
    assert converters._is_kvlist(BAD1) == False
    assert converters._is_kvlist([])
    assert converters._is_kvlist(TEXTKVLIST_DATA)

def test_text_to_kvlist():
    assert converters.text_to_kvlist('') == []
    assert converters.text_to_kvlist(TEXTKVLIST_TEXT) == TEXTKVLIST_DATA

def test_kvlist_to_text():
    assert converters.kvlist_to_text([]) == ''
    assert converters.kvlist_to_text(TEXTKVLIST_DATA) == TEXTKVLIST_TEXT

TEXTLABELLEDLIST_TEXT0 = 'eng'
TEXTLABELLEDLIST_DATA0 = [u'eng']
TEXTLABELLEDLIST_DOUT0 = 'eng'
TEXTLABELLEDLIST_TEXT1 = 'eng; jpn'
TEXTLABELLEDLIST_DATA1 = [u'eng', u'jpn']
TEXTLABELLEDLIST_DOUT1 = 'eng; jpn'
TEXTLABELLEDLIST_TEXT2 = 'eng:English'
TEXTLABELLEDLIST_DATA2 = [u'eng']
TEXTLABELLEDLIST_DOUT2 = 'eng'
TEXTLABELLEDLIST_TEXT3 = 'eng:English; jpn:Japanese'
TEXTLABELLEDLIST_DATA3 = [u'eng', u'jpn']
TEXTLABELLEDLIST_DOUT3 = 'eng; jpn'

def test_text_to_labelledlist():
    assert converters.text_to_labelledlist('') == []
    assert converters.text_to_labelledlist(TEXTLABELLEDLIST_TEXT0) == TEXTLABELLEDLIST_DATA0
    assert converters.text_to_labelledlist(TEXTLABELLEDLIST_TEXT1) == TEXTLABELLEDLIST_DATA1
    assert converters.text_to_labelledlist(TEXTLABELLEDLIST_TEXT2) == TEXTLABELLEDLIST_DATA2
    assert converters.text_to_labelledlist(TEXTLABELLEDLIST_TEXT3) == TEXTLABELLEDLIST_DATA3

def test_labelledlist_to_text():
    assert converters.labelledlist_to_text([]) == ''
    assert converters.labelledlist_to_text(TEXTLABELLEDLIST_DATA0) == TEXTLABELLEDLIST_DOUT0
    assert converters.labelledlist_to_text(TEXTLABELLEDLIST_DATA1) == TEXTLABELLEDLIST_DOUT1
    assert converters.labelledlist_to_text(TEXTLABELLEDLIST_DATA2) == TEXTLABELLEDLIST_DOUT2
    assert converters.labelledlist_to_text(TEXTLABELLEDLIST_DATA3) == TEXTLABELLEDLIST_DOUT3

LISTOFDICTS_TERMS0 = ['label', 'url']
LISTOFDICTS_TEXT0 = 'label:ABC|url:http://abc.org/'
LISTOFDICTS_DATA0 = [
    {'label': 'ABC', 'url': 'http://abc.org/'}
]
LISTOFDICTS_TERMS1 = ['label', 'url']
LISTOFDICTS_TEXT1 = 'label:ABC|url:http://abc.org/;\nlabel:DEF|url:http://def.org/'
LISTOFDICTS_DATA1 = [
    {'label': 'ABC', 'url': 'http://abc.org/'},
    {'label': 'DEF', 'url': 'http://def.org/'}
]
LISTOFDICTS_TERMS2 = ['label', 'start', 'end']
LISTOFDICTS_TEXT2 = 'label:Pre WWII|end:1941;\nlabel:WWII|start:1941|end:1944;\nlabel:Post WWII|start:1944'
LISTOFDICTS_DATA2 = [
    {u'end': '1941', u'label': u'Pre WWII'},
    {u'start': '1941', u'end': '1944', u'label': u'WWII'},
    {u'start': '1944', u'label': u'Post WWII'}
]

def test_text_to_dicts():
    assert converters.text_to_dicts('', []) == []
    assert converters.text_to_dicts(LISTOFDICTS_TEXT0, LISTOFDICTS_TERMS0) == LISTOFDICTS_DATA0
    assert converters.text_to_dicts(LISTOFDICTS_TEXT1, LISTOFDICTS_TERMS1) == LISTOFDICTS_DATA1
    assert converters.text_to_dicts(LISTOFDICTS_TEXT2, LISTOFDICTS_TERMS2) == LISTOFDICTS_DATA2

def test_text_to_listofdicts():
    assert converters.text_to_listofdicts('', []) == []
    assert converters.text_to_listofdicts(LISTOFDICTS_TEXT0) == LISTOFDICTS_DATA0
    assert converters.text_to_listofdicts(LISTOFDICTS_TEXT1) == LISTOFDICTS_DATA1
    assert converters.text_to_listofdicts(LISTOFDICTS_TEXT2) == LISTOFDICTS_DATA2

def test_listofdicts_to_text():
    assert converters.listofdicts_to_text([], []) == ''
    assert converters.listofdicts_to_text(LISTOFDICTS_DATA0, LISTOFDICTS_TERMS0) == LISTOFDICTS_TEXT0
    assert converters.listofdicts_to_text(LISTOFDICTS_DATA1, LISTOFDICTS_TERMS1) == LISTOFDICTS_TEXT1
    assert converters.listofdicts_to_text(LISTOFDICTS_DATA2, LISTOFDICTS_TERMS2) == LISTOFDICTS_TEXT2


# Text can contain one key-val pair
TEXTNOLABELS_LISTOFDICTS_TEXT0 = "ABC:http://abc.org"
TEXTNOLABELS_LISTOFDICTS_TEXT1 = "ABC:http://abc.org;"
TEXTNOLABELS_LISTOFDICTS_KEYS0 = ['label','url']
TEXTNOLABELS_LISTOFDICTS_DATA0 = [
    {'label': 'ABC', 'url': 'http://abc.org'}
]
# or multiple key-val pairs.
TEXTNOLABELS_LISTOFDICTS_TEXT2a = "ABC:http://abc.org;DEF:http://def.org"
TEXTNOLABELS_LISTOFDICTS_TEXT2b = "ABC:http://abc.org;DEF:http://def.org;"
TEXTNOLABELS_LISTOFDICTS_TEXT2c = "ABC:http://abc.org;\n DEF:http://def.org;"
TEXTNOLABELS_LISTOFDICTS_TEXT2z = "ABC:http://abc.org;\nDEF:http://def.org"
TEXTNOLABELS_LISTOFDICTS_KEYS2 = ['label','url']
TEXTNOLABELS_LISTOFDICTS_DATA2 = [
    {'label': 'ABC', 'url': 'http://abc.org'},
    {'label': 'DEF', 'url': 'http://def.org'}
]
# Old JSON data may be a list of strings rather than dicts.
TEXTNOLABELS_LISTOFDICTS_TEXT3 = "ABC:http://abc.org;\n DEF:http://def.org;"
TEXTNOLABELS_LISTOFDICTS_TEXT3a = "ABC:http://abc.org;DEF:http://def.org"
TEXTNOLABELS_LISTOFDICTS_TEXT3z = "ABC:http://abc.org;\nDEF:http://def.org"
TEXTNOLABELS_LISTOFDICTS_KEYS3 = ['label','url']
TEXTNOLABELS_LISTOFDICTS_DATA3 = [
    'ABC:http://abc.org',
    'DEF:http://def.org'
]
TEXTNOLABELS_LISTOFDICTS_TEXT4 = "term:ABC|id:123; \nterm:DEF|id:456"
TEXTNOLABELS_LISTOFDICTS_TEXT4z = "term:ABC|id:123;\nterm:DEF|id:456"
TEXTNOLABELS_LISTOFDICTS_KEYS4 = ['label','url']
TEXTNOLABELS_LISTOFDICTS_DATA4 = [
    'ABC [123]',
    'DEF [456]'
]

def textnolabels_to_listofdicts():
    assert converters.textnolabels_to_listofdicts(None) == []
    assert converters.textnolabels_to_listofdicts('') == []
    assert converters.textnolabels_to_listofdicts(' ') == []
    
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_DATA0) == TEXTNOLABELS_LISTOFDICTS_DATA0
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT0) == TEXTNOLABELS_LISTOFDICTS_DATA0
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT1) == TEXTNOLABELS_LISTOFDICTS_DATA0

    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_DATA2) == TEXTNOLABELS_LISTOFDICTS_DATA2
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT2a) == TEXTNOLABELS_LISTOFDICTS_DATA2
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT2b) == TEXTNOLABELS_LISTOFDICTS_DATA2
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT2c) == TEXTNOLABELS_LISTOFDICTS_DATA2

    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_DATA3) == TEXTNOLABELS_LISTOFDICTS_DATA3
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT3) == TEXTNOLABELS_LISTOFDICTS_DATA3

    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_DATA4) == TEXTNOLABELS_LISTOFDICTS_DATA4
    assert converters.textnolabels_to_listofdicts(TEXTNOLABELS_LISTOFDICTS_TEXT4) == TEXTNOLABELS_LISTOFDICTS_DATA4


def test_listofdicts_to_textnolabels():
    assert converters.listofdicts_to_textnolabels(TEXTNOLABELS_LISTOFDICTS_DATA0, TEXTNOLABELS_LISTOFDICTS_KEYS0) == TEXTNOLABELS_LISTOFDICTS_TEXT0
    assert converters.listofdicts_to_textnolabels(TEXTNOLABELS_LISTOFDICTS_DATA2, TEXTNOLABELS_LISTOFDICTS_KEYS2) == TEXTNOLABELS_LISTOFDICTS_TEXT2z
    assert converters.listofdicts_to_textnolabels(TEXTNOLABELS_LISTOFDICTS_DATA3, TEXTNOLABELS_LISTOFDICTS_KEYS3) == TEXTNOLABELS_LISTOFDICTS_TEXT3z
    assert converters.listofdicts_to_textnolabels(TEXTNOLABELS_LISTOFDICTS_TEXT4, TEXTNOLABELS_LISTOFDICTS_KEYS4) == TEXTNOLABELS_LISTOFDICTS_TEXT4z


TEXTBRACKETIDS_FIELDS = ['term', 'id']
TEXTBRACKETIDS_MULTI_TEXT = "ABC: DEF [123]; ABC: Hawai'i [456]; ABC: Hawai`i [456]"
TEXTBRACKETIDS_MULTI_LIST = [
    "ABC: DEF [123]",
    "ABC: Hawai'i [456]",
    "ABC: Hawai`i [456]",
]
TEXTBRACKETIDS_MULTI_DATA = [
    {"term": "ABC: DEF", "id": '123'},
    {"term": "ABC: Hawai'i", "id": '456'},
    {"term": "ABC: Hawai`i", "id": '456'},
]

def test_text_to_bracketids():
    assert converters.text_to_bracketids(None) == []
    assert converters.text_to_bracketids('') == []
    print(converters.text_to_bracketids(TEXTBRACKETIDS_MULTI_TEXT, TEXTBRACKETIDS_FIELDS))
    assert converters.text_to_bracketids(TEXTBRACKETIDS_MULTI_TEXT, TEXTBRACKETIDS_FIELDS) == TEXTBRACKETIDS_MULTI_DATA
    assert converters.text_to_bracketids(TEXTBRACKETIDS_MULTI_LIST, TEXTBRACKETIDS_FIELDS) == TEXTBRACKETIDS_MULTI_DATA
    assert converters.text_to_bracketids(TEXTBRACKETIDS_MULTI_DATA, TEXTBRACKETIDS_FIELDS) == TEXTBRACKETIDS_MULTI_DATA


TEXTROLEPEOPLE_NAME_TEXT = "Watanabe, Joe"
TEXTROLEPEOPLE_NAME_DATA = [
    {'namepart': 'Watanabe, Joe',}
]
TEXTROLEPEOPLE_NAME_OUT = 'namepart: Watanabe, Joe'

TEXTROLEPEOPLE_NAME_TEXT_ROLE = "Watanabe, Joe"
TEXTROLEPEOPLE_NAME_DATA_ROLE = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'}
]
TEXTROLEPEOPLE_NAME_OUT_ROLE = 'namepart: Watanabe, Joe | role: author'

TEXTROLEPEOPLE_SINGLE_TEXT = 'namepart: Masuda, Kikuye | role: narrator | id: 42'
TEXTROLEPEOPLE_SINGLE_DATA = [
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_SINGLE_ID_TEXT = 'namepart: Masuda, Kikuye | role: narrator | id: 42'
TEXTROLEPEOPLE_SINGLE_ID_DATA = [
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_SINGLE_NRID_TEXT = 'namepart: Masuda, Kikuye | nr_id: 88922/nr014m435 | role: narrator'
TEXTROLEPEOPLE_SINGLE_NRID_DATA = [
    {'namepart': 'Masuda, Kikuye', 'nr_id': '88922/nr014m435', 'role': 'narrator'},
]

TEXTROLEPEOPLE_SINGLE_NRIDID_TEXT = 'namepart: Masuda, Kikuye | nr_id: 88922/nr014m435 | role: narrator | id: 42'
TEXTROLEPEOPLE_SINGLE_NRIDID_DATA = [
    {'namepart': 'Masuda, Kikuye', 'nr_id': '88922/nr014m435', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_MULTI_TEXT = 'namepart: Watanabe, Joe | role: author; namepart: Masuda, Kikuye | role: narrator | id: 42'
TEXTROLEPEOPLE_MULTI_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_MULTI_NRID_TEXT = 'namepart: Ito, Jo | role: author; namepart: Ban, Shig | nr_id: 88922/nr014m437 | role: arch; namepart: Aso, San | role: narrator | id: 42'
TEXTROLEPEOPLE_MULTI_NRID_DATA = [
    {'namepart': 'Ito, Jo', 'role': 'author'},
    {'namepart': 'Ban, Shig', 'nr_id': '88922/nr014m437', 'role': 'arch'},
    {'namepart': 'Aso, San', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_LISTSTRSNAME_TEXT = [
    'Watanabe, Joe',
]
TEXTROLEPEOPLE_LISTSTRSNAME_DATA = [
    {'namepart': 'Watanabe, Joe',},
]

TEXTROLEPEOPLE_LISTSTRS_TEXT = [
    'Watanabe, Joe:author',
    'Masuda, Kikuye [42]:narrator',
]
TEXTROLEPEOPLE_LISTSTRS_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_MULTI_TEXT = 'namepart: Watanabe, Joe | role: author; namepart: Masuda, Kikuye | role: narrator | id: 42'
TEXTROLEPEOPLE_MULTI_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_MULTI_NRID_TEXT = 'namepart: Ito, Jo | role: author; namepart: Ban, Shig | nr_id: 88922/nr014m437 | matching: match | role: arch; namepart: Aso, San | role: narrator | id: 42'
TEXTROLEPEOPLE_MULTI_NRID_DATA = [
    {'namepart': 'Ito, Jo', 'role': 'author'},
    {'namepart': 'Ban, Shig', 'nr_id': '88922/nr014m437', 'matching': 'match', 'role': 'arch'},
    {'namepart': 'Aso, San', 'role': 'narrator', 'id': 42},
]

TEXTROLEPEOPLE_PIPES_TEXT = 'namepart: Watanabe, Joe | role: author; namepart: Masuda, Kikuye [42] | role: narrator; namepart: Joi Ito | role: techie | id:123'
TEXTROLEPEOPLE_PIPES_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
    {'namepart': 'Joi Ito', 'role': 'techie', 'id': 123},
]

TEXTROLEPEOPLE_NOSPACES_TEXT = 'namepart:Watanabe, Joe|role:author; namepart:Masuda, Kikuye [42]|role:narrator;'
TEXTROLEPEOPLE_NOSPACES_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42},
]

# many legacy files have this pattern
TEXTROLEPEOPLE_MULTIERR_TEXT = [
    {'namepart': '', 'role': 'author'},
]
TEXTROLEPEOPLE_MULTIERR_DATA = []

# output of FORM.cleaned_data['persons']
TEXTROLEPEOPLE_FORM_CLEANED = [
    'namepart: Yasuda, Mitsu;\n', 'namepart: Tanaka, Cherry;\n'
]
TEXTROLEPEOPLE_FORM_DATA = [
    {'namepart': 'Yasuda, Mitsu'}, {'namepart': 'Tanaka, Cherry'},
]

def test_text_to_rolepeople():
    defaults = entity_defs.PERSONS_DEFAULT_DICT
    assert converters.text_to_rolepeople(None, defaults) == []
    assert converters.text_to_rolepeople('',   defaults) == []
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_NAME_TEXT,          defaults) == TEXTROLEPEOPLE_NAME_DATA
    # it'd be neat if the function could consume its own output as an input, but no
    #assert converters.text_to_rolepeople(TEXTROLEPEOPLE_NAME_OUT,           defaults) == TEXTROLEPEOPLE_NAME_DATA
    assert converters.text_to_rolepeople(
        TEXTROLEPEOPLE_NAME_TEXT_ROLE, default={'namepart':'', 'role':'author'}
    ) == TEXTROLEPEOPLE_NAME_DATA_ROLE
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_SINGLE_TEXT,        defaults) == TEXTROLEPEOPLE_SINGLE_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_SINGLE_ID_TEXT,     defaults) == TEXTROLEPEOPLE_SINGLE_ID_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_SINGLE_NRID_TEXT,   defaults) == TEXTROLEPEOPLE_SINGLE_NRID_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_SINGLE_NRIDID_TEXT, defaults) == TEXTROLEPEOPLE_SINGLE_NRIDID_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTI_TEXT,         defaults) == TEXTROLEPEOPLE_MULTI_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTI_NRID_TEXT,    defaults) == TEXTROLEPEOPLE_MULTI_NRID_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_LISTSTRSNAME_TEXT,  defaults) == TEXTROLEPEOPLE_LISTSTRSNAME_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_LISTSTRS_TEXT,      defaults) == TEXTROLEPEOPLE_LISTSTRS_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTI_DATA,         defaults) == TEXTROLEPEOPLE_MULTI_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_PIPES_TEXT,         defaults) == TEXTROLEPEOPLE_PIPES_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_NOSPACES_TEXT,      defaults) == TEXTROLEPEOPLE_NOSPACES_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTIERR_TEXT,      defaults) == TEXTROLEPEOPLE_MULTIERR_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_FORM_CLEANED,       defaults) == TEXTROLEPEOPLE_FORM_DATA

def test_rolepeople_to_text():
    assert converters.rolepeople_to_text([]) == ''
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_NAME_DATA) == TEXTROLEPEOPLE_NAME_OUT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_SINGLE_DATA) == TEXTROLEPEOPLE_SINGLE_TEXT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_MULTI_DATA) == TEXTROLEPEOPLE_MULTI_TEXT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_SINGLE_NRID_DATA) == TEXTROLEPEOPLE_SINGLE_NRID_TEXT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_MULTI_NRID_DATA) == TEXTROLEPEOPLE_MULTI_NRID_TEXT
