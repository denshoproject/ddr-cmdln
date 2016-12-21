"""Functions for converting between text and various data structures.
"""

from datetime import datetime

import converters


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

def test_render():
    template = """<a href="{{ data.url }}">{{ data.label }}</a>"""
    data = {'url':'http://densho.org', 'label':'Densho'}
    expected = """<a href="http://densho.org">Densho</a>"""
    assert converters.render(template,data) == expected


TEXT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
TEXT_DATETIME_TEXT = '2016-08-31T15:42:17'
TEXT_DATETIME_DATA = datetime(2016,8,31,15,42,17)

def test_text_to_datetime():
    assert converters.text_to_datetime(TEXT_DATETIME_TEXT) == TEXT_DATETIME_DATA

def test_datetime_to_text():
    assert converters.datetime_to_text(TEXT_DATETIME_DATA) == TEXT_DATETIME_TEXT


TEXTLIST_TEXT = 'thing1; thing2'
TEXTLIST_DATA = ['thing1', 'thing2']

def test_is_listofstrs():
    assert converters._is_listofstrs(TEXTLIST_TEXT) == True
    assert converters._is_listofstrs(TEXTLIST_DATA) == True
    BAD0 = {'abc':123}
    BAD1 = [{'abc':123}]
    assert converters._is_listofstrs(BAD0) == False
    assert converters._is_listofstrs(BAD1) == False

def test_text_to_list():
    assert converters.text_to_list(TEXTLIST_TEXT) == TEXTLIST_DATA
    assert converters.text_to_list(TEXTLIST_DATA) == TEXTLIST_DATA

def test_list_to_text():
    assert converters.list_to_text(TEXTLIST_DATA) == TEXTLIST_TEXT

TEXT_DICT_TEXT_LABELS    = 'term:ABC|id:123'
TEXT_DICT_TEXT_NOLABELS  = 'ABC:123'
TEXT_DICT_TEXT_BRACKETID = 'ABC [123]'
TEXT_DICT_TEXT_BRACKETID_NL = 'ABC\n[123]'
TEXT_DICT_TEXT_BRACKETID_QUOTES = '"ABC" [123]'
TEXT_DICT_KEYS = ['term', 'id']
TEXT_DICT_SEPARATORS = ':|'
TEXT_DICT_SEPARATOR = ':'
TEXT_DICT_DATA = {'term': u'ABC', 'id': '123'}

TEXT_DICT_KEYS_DATE = ['term', 'startdate']
TEXT_DICT_TEXT_NOLABELS_DATE = 'ABC:1970-01-01 00:00:00'
TEXT_DICT_DATA_DATE = {'term': u'ABC', 'startdate': '1970-01-01 00:00:00'}

def test_textlabels_to_dict():
    assert converters.textlabels_to_dict('', []) == {}
    assert converters.textlabels_to_dict(TEXT_DICT_TEXT_LABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA

def test_dict_to_textlabels():
    assert converters.dict_to_textlabels(TEXT_DICT_DATA, TEXT_DICT_KEYS, TEXT_DICT_SEPARATORS) == TEXT_DICT_TEXT_LABELS

def test_textnolabels_to_dict():
    assert converters.textnolabels_to_dict('', []) == {}
    assert converters.textnolabels_to_dict(TEXT_DICT_TEXT_NOLABELS, TEXT_DICT_KEYS) == TEXT_DICT_DATA
    assert converters.textnolabels_to_dict(TEXT_DICT_TEXT_NOLABELS_DATE, TEXT_DICT_KEYS_DATE) == TEXT_DICT_DATA_DATE
    
def test_dict_to_textnolabels():
    assert converters.dict_to_textnolabels(TEXT_DICT_DATA, TEXT_DICT_KEYS, TEXT_DICT_SEPARATOR) == TEXT_DICT_TEXT_NOLABELS

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

TEXTROLEPEOPLE_NAME_TEXT = "Watanabe, Joe"
# output has role even if input does not
TEXTROLEPEOPLE_NAME_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'}
]
TEXTROLEPEOPLE_NAME_OUT = 'Watanabe, Joe:author'

TEXTROLEPEOPLE_SINGLE_TEXT = 'Masuda, Kikuye:photographer'
TEXTROLEPEOPLE_SINGLE_DATA = [
    {'namepart': 'Masuda, Kikuye', 'role': 'photographer'}
]

TEXTROLEPEOPLE_MULTI_TEXT = 'Watanabe, Joe:author; Masuda, Kikuye:photographer'
TEXTROLEPEOPLE_MULTI_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'photographer'},
]

TEXTROLEPEOPLE_LISTSTRSNAME_TEXT = [
    'Watanabe, Joe',
]
TEXTROLEPEOPLE_LISTSTRSNAME_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
]

TEXTROLEPEOPLE_LISTSTRS_TEXT = [
    'Watanabe, Joe:author',
    'Masuda, Kikuye:photographer',
]
TEXTROLEPEOPLE_LISTSTRS_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'photographer'},
]

TEXTROLEPEOPLE_MULTI_TEXT = 'Watanabe, Joe:author; Masuda, Kikuye [42]:narrator'
TEXTROLEPEOPLE_MULTI_DATA = [
    {'namepart': 'Watanabe, Joe', 'role': 'author'},
    {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id':42},
]

# many legacy files have this pattern
TEXTROLEPEOPLE_MULTIERR_TEXT = [
    {'namepart': '', 'role': 'author'},
]
TEXTROLEPEOPLE_MULTIERR_DATA = []

def test_text_to_rolepeople():
    assert converters.text_to_rolepeople(None) == []
    assert converters.text_to_rolepeople('') == []
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_NAME_TEXT) == TEXTROLEPEOPLE_NAME_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_SINGLE_TEXT) == TEXTROLEPEOPLE_SINGLE_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTI_TEXT) == TEXTROLEPEOPLE_MULTI_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_LISTSTRSNAME_TEXT) == TEXTROLEPEOPLE_LISTSTRSNAME_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_LISTSTRS_TEXT) == TEXTROLEPEOPLE_LISTSTRS_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTI_DATA) == TEXTROLEPEOPLE_MULTI_DATA
    assert converters.text_to_rolepeople(TEXTROLEPEOPLE_MULTIERR_TEXT) == TEXTROLEPEOPLE_MULTIERR_DATA

def test_rolepeople_to_text():
    assert converters.rolepeople_to_text([]) == ''
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_NAME_DATA) == TEXTROLEPEOPLE_NAME_OUT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_SINGLE_DATA) == TEXTROLEPEOPLE_SINGLE_TEXT
    assert converters.rolepeople_to_text(TEXTROLEPEOPLE_MULTI_DATA) == TEXTROLEPEOPLE_MULTI_TEXT
