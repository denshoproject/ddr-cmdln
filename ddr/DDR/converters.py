# Various ways to represent structured data in strings,
# for use in web forms or in CSV files.
#
# Some of these methods are extremely similar to each other, but they
# originated in different places in the app or different points in the
# process.  They are collected here to document the various formats
# used, and hopefully to make it easier to merge or prune them in the
# future.

import copy
from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)
import re
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union

from dateutil import parser
from jinja2 import Template

from DDR import config


def normalize_string(text: str) -> str:
    if text == None:
        return u''
    elif not isinstance(text, str):
        return text
    elif not text:
        return u''
    return str(text).replace('\r\n', '\n').replace('\r', '\n').strip()

def load_dirty_json(text: str):
    # http://grimhacker.com/2016/04/24/loading-dirty-json-with-python/
    regex_replace = [
        (r"([ \{,:\[])(u)?'([^']+)'", r'\1"\3"'),
        (r" False([, \}\]])", r' false\1'),
        (r" True([, \}\]])", r' true\1')
    ]
    for r, s in regex_replace:
        text = re.sub(r, s, text)
    return json.loads(text)

def strip_list(data: List[Any]) -> List[Any]:
    """Remove empty list items (ex: ['not empty, '']
   
    @param data: list
    @returns: list
    """
    return [
        item for item in data
        if item and (not item == 0)
    ]

def render(template: str, data: Dict[str, str]) -> str:
    """Render a Jinja2 template.
    
    @param template: str Jinja2-formatted template
    @param data: dict
    """
    return Template(template).render(data=data)

def coerce_text(data: Union[int,datetime,str]) -> Optional[str]:
    """Ensure types (ints,datetimes) are converted to text
    """
    if isinstance(data, int):
        return str(data)
    elif isinstance(data, datetime):
        return datetime_to_text(data)
    return data


# boolean --------------------------------------------------------------

def text_to_boolean(value: str) -> bool:
    if value:
        if isinstance(value, bool):
            return value
        elif isinstance(value, int) and value:
            return True
        elif isinstance(value, str):
            if value.isdigit() and int(value):
                return True
            if value in ['true', 'True']:
                return True
    return False


# datetime -------------------------------------------------------------
#
# format = '%Y-%m-%dT%H:%M:%S'
# text = '1970-1-1T00:00:00'
# data = datetime.datetime(1970, 1, 1, 0, 0)
# 

ALT_DATETIME_FORMATS = [
    '%Y-%m-%dT%H:%M:%S:%f',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S%Z%z',
    '%Y-%m-%dT%H:%M:%S%Z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%dT%H:%M:%S.%f%Z%z',
    '%Y-%m-%dT%H:%M:%S.%f%Z',
    '%Y-%m-%dT%H:%M:%S.%f%z',
]

def text_to_datetime(text: str,
                     fmt: str=config.DATETIME_FORMAT) -> Optional[datetime]:
    """Load datatime from text in specified format.
    
    TODO timezone!
    TODO use dateparse or something?
    
    @param text: str
    @param fmt: str
    @returns: datetime
    """
    if isinstance(text, datetime):
        return text
    text = normalize_string(text)
    try:
        return parser.parse(text)
    except:
        # try a bunch of old/messed up formats
        for fmt in ALT_DATETIME_FORMATS:
            try:
                return datetime.strptime(text, fmt)
            except:
                pass
    return None

def datetime_to_text(data: datetime, fmt: str=config.DATETIME_FORMAT) -> Optional[str]:
    """Dump datetime to text suitable for a CSV field.
    
    TODO timezone!
    
    @param data: datetime
    @returns: str
    """
    if not data:
        return None
    if isinstance(data, str):
        return data
    if not isinstance(data, datetime):
        raise Exception(f'Cannot strformat "{date}": not a datetime.')
    return datetime.strftime(data, fmt)


# list ----------------------------------------------------------------
#
# text0 = 'thing1; thing2'
# text1 = [
#     'thing1',
#     'thing2',
# ]
# data = ['thing1', 'thing2']
#

LIST_SEPARATOR = ';'
LIST_SEPARATOR_SPACE = '%s ' % LIST_SEPARATOR

def _is_listofstrs(data: List[str]) -> bool:
    if isinstance(data, list):
        num_strs = 0
        for x in data:
            if isinstance(x, str):
                num_strs += 1
        if num_strs == len(data):
            return True
    return False

def text_to_list(text: str, separator: str=LIST_SEPARATOR) -> List[str]:
    """
    @param text: str
    @param separator: str
    @returns: list
    """
    if isinstance(text, list):
        return text
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for item in text.split(separator):
        item = item.strip()
        if item:
            data.append(item)
    return data

def list_to_text(data: List[str], separator: str=LIST_SEPARATOR_SPACE) -> str:
    """
    @param data: list
    @param separator: str
    @returns: str
    """
    return separator.join(data)


# dict -----------------------------------------------------------------
#
# Much DDR data is structured as lists of dicts, one dict per record.
# These functions are intended for recognizing text strings from these records.
# 
# text_bracketid = 'ABC [123]'
# text_nolabels  = 'ABC:123'
# text_labels    = 'term:ABC|id:123'
# data = {'term':'ABC', 'id':'123'}
#

def _is_text_labels(text: str, separators: List[str]=[':','|']) -> bool:
    # both separators
    sepsfound = [s for s in separators if s in text]
    if len(sepsfound) == len(separators):
        return True
    return False

def textlabels_to_dict(text: str,
                       keys: List[str],
                       separators: List[str]=[':','|']) -> Dict[str,str]:
    """
    @param text: str
    @param keys: list
    @param separators: list
    @returns: dict
    """
    if not text:
        return {}
    data = {}
    for item in text.split(separators[1]):
        if item:
            key,val = item.split(separators[0], 1)
            data[key] = val
    return data

def dict_to_textlabels(data: Dict[str,str],
                       keys: List[str],
                       separators: List[str]) -> str:
    return separators[1].join([
        separators[0].join([
            key, data[key]
        ])
        for key in keys
    ])

# text_nolabels  = 'ABC:123'
# data = {'term':'ABC', 'id':123}

def _is_text_nolabels(text: str, separators: List[str]=[':','|']) -> bool:
    # Only first separator present
    if (separators[0] in text) and not (separators[1] in text):
        return True
    return False

def textnolabels_to_dict(text: str,
                         keys: List[str],
                         separator: str=':') -> Dict[str,str]:
    """
    @param text: str
    @param keys: list
    @param separator: str
    @returns: dict
    """
    if not text:
        return {}
    if not separator in text:
        raise Exception('Text does not contain "%s": "%s"' % (separator, text))
    values = text.split(separator, 1)
    if not len(values) == len(keys):
        raise Exception('Text contains more than %s values: "%s".' % (len(keys), text))
    data = {
        key: values[n]
        for n,key in enumerate(keys)
    }
    return data

def dict_to_textnolabels(data: Dict[str,str],
                         keys: List[str],
                         separator: str) -> str:
    return separator.join([
        data[key] for key in keys
    ])

# text_bracketid = 'ABC [123]'
# data = {'term':'ABC', 'id':123}

TEXT_BRACKETID_TEMPLATE = '{term} [{id}]'
TEXT_BRACKETID_REGEX = re.compile(r'(?P<term>[\w\d -:()_,`\'"]+)\s\[(?P<id>\d+)\]')

def _is_text_bracketid(text: str) -> Union[Match[str], bool]:
    if text:
        m = re.search(TEXT_BRACKETID_REGEX, text)
        if m and (len(m.groups()) == 2) and m.groups()[1].isdigit():
            return m
    return False

def textbracketid_to_dict(text, keys=['term', 'id'], pattern=TEXT_BRACKETID_REGEX, match=None):
    """
    @param text: str
    @param keys: list
    @param pattern: re.RegexObject
    @param match: re.MatchObject
    @returns: dict
    """
    text = normalize_string(text).replace('\n',' ')
    if not text:
        return {}
    if match:
        m = match
    elif pattern:
        m = re.search(pattern, text)
    if m:
        if m.groups() and (len(m.groups()) == len(keys)):
            return {
                key: m.groups()[n]
                for n,key in enumerate(keys)
            }
    return {}

def dict_to_textbracketid(data: Dict[str,str], keys: List[str]) -> str:
    if isinstance(data, str):
        return data
    if len(keys) != 2:
        raise Exception('Cannot format "Topic [ID]" data: too many keys. "%s"' % data)
    if not 'id' in list(data.keys()):
        raise Exception('No "id" field in data: "%s".' % data)
    data_ = copy.deepcopy(data)
    d = {'id': data_.pop('id')}
    d['term'] = list(data_.values())[0]
    return TEXT_BRACKETID_TEMPLATE.format(**d)

def text_to_dict(text: str, keys: List[str]) -> Dict[str,str]:
    """Convert various text formats to dict
    
    If text cannot be converted it will be assigned to keys[0] in a dict
    
    @param text: str Normalized text
    @param keys: list
    @returns: dict
    """
    text = normalize_string(text).replace('\n',' ')
    if not text:
        return {}
    match = _is_text_bracketid(text)
    if match:
        data = textbracketid_to_dict(text, match=match)
    elif _is_text_labels(text, separators=[':','|']):
        data = textlabels_to_dict(text, keys, separators=[':','|'])
    elif _is_text_nolabels(text):
        data = textnolabels_to_dict(text, keys, separator=':')
    else:
        raise Exception('text_to_dict could not parse "%s"' % text)
    # strip strings, force int values to int
    d = {}
    for key,val in data.items():
        if isinstance(val, str):
            d[key] = val.strip()
        else:
            d[key] = val
    return d

def dict_to_text(data: Dict[str,str],
                 keys: List[str],
                 style: str='labels',
                 nolabelsep: str=':',
                 labelseps: List[str]=[':','|']) -> str:
    """Renders single dict record to text in specified style.
    
    @param data: dict
    @param keys: list Dictionary keys in order they should be printed.
    @param style: str 'labels', 'nolabels', 'bracketid'
    @param nolabelsep: str
    @param labelseps: list
    @returns: str
    """
    assert style in ['labels', 'nolabels', 'bracketid']
    if style == 'bracketid':
        return dict_to_textbracketid(data, keys)
    elif style == 'nolabels':
        return dict_to_textnolabels(data, keys, nolabelsep)
    else:
        return dict_to_textlabels(data, keys, labelseps)


# kvlist ---------------------------------------------------------------
#
# text = 'name1:author; name2:photog'
# data = [
#     {u'name1': u'author'},
#     {u'name2': u'photog'}
# ]
# 

def _is_kvlist(text: str) -> bool:
    if isinstance(text, list):
        matches = 0
        for item in text:
            if isinstance(item, dict):
                matches += 1
        if matches == len(text):
            return True
    return False

def text_to_kvlist(text: str) -> Union[Any, List[Dict[str,str]]]:
    if _is_kvlist(text):
        return text
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for item in text.split(';'):
        item = item.strip()
        if item:
            if not ':' in item:
                raise Exception('Malformed data: %s' % text)
            key,val = item.strip().split(':')
            data.append({
                key.strip(): val.strip()
            })
    return data

def kvlist_to_text(data: List[Dict[str,str]]) -> str:
    items = []
    for d in data:
        i = [k+':'+v for k,v in d.items()]
        item = '; '.join(i)
        items.append(item)
    text = '; '.join(items)
    return text


# labelledlist ---------------------------------------------------------
#
# Filter list of key:value pairs, keeping just the keys.
# NOTE: This is a one-way conversion.
# 
# text = 'eng'
# data = [u'eng']
# text = 'eng;jpn')
# data = [u'eng', u'jpn']
# text = 'eng:English'
# data = [u'eng']
# text = 'eng:English; jpn:Japanese'
# data = [u'eng', u'jpn']
# 
    
def text_to_labelledlist(text: str) -> List[str]:
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for x in text.split(';'):
        x = x.strip()
        if x:
            if ':' in x:
                # NOTE: we're keeping the KEY, discarding the VALUE
                key,val = x.strip().split(':')
                data.append(key)
            else:
                data.append(x.strip())
    return data

def labelledlist_to_text(data: List[str], separator: str='; ') -> str:
    return separator.join(data)


# listofdicts ----------------------------------------------------------
#
# Converts between labelled fields in text to list-of-dicts.
#
# text0 = 'url:http://abc.org/|label:ABC'
# data0 = [
#     {'label': 'ABC', 'url': 'http://abc.org/'}
# ]
# 
# text1 = 'url:http://abc.org/|label:ABC; url:http://def.org/|label:DEF'
# data1 = [
#     {'label': 'ABC', 'url': 'http://abc.org/'},
#     {'label': 'DEF', 'url': 'http://def.org/'}
# ]
# 
# text2 = 'label:Pre WWII|end:1941; label:WWII|start:1941|end:1944; label:Post WWII|start:1944;'
# data2 = [
#     {'label':'Pre WWII', 'end':'1941'},
#     {'label':'WWII', 'start':'1941', 'end':'1944'},
#     {'label':'Post WWII', 'start':'1944'}
# ]
# 

def _is_listofdicts(data: Union[Any, List[Dict[str,str]]]) -> bool:
    if isinstance(data, list):
        num_dicts = 0
        for x in data:
            if isinstance(x, dict):
                num_dicts += 1
        if num_dicts == len(data):
            return True
    return False

def text_to_dicts(text: str,
                  terms: List[str],
                  separator: str=';') -> Union[Any, List[Dict[str,str]]]:
    if _is_listofdicts(text):
        return text
    text = normalize_string(text)
    if not text:
        return []
    dicts = []
    for line in text.split(separator):
        line = line.strip()
        d = text_to_dict(line, terms)
        if d:
            dicts.append(d)
    return dicts
    
def _setsplitnum(separator: str, split1x: List[str]) -> int:
    if separator in split1x:
        return 1
    return -1

LISTOFDICTS_SEPARATORS = [':', '|', ';']
LISTOFDICTS_SPLIT1X = [':']

def text_to_listofdicts(text: str,
                        separators: List[str]=LISTOFDICTS_SEPARATORS,
                        split1x: List[str]=LISTOFDICTS_SPLIT1X
) -> List[Dict[str,str]]:
    text = normalize_string(text).replace('\\n','').replace('\n','')
    if not text:
        return []
    splitnum1 = _setsplitnum(separators[-1], split1x)
    splitnum2 = _setsplitnum(separators[-2], split1x)
    splitnum3 = _setsplitnum(separators[-3], split1x)
    # parse it up
    dicts = []
    for line in text.split(separators[-1], splitnum1):
        # clean up line, skip if empty
        l = line.strip()
        if l:
            items = l.split(separators[-2], splitnum2)
        else:
            items = []
        d = {}
        for item in items:
            i = item.strip()
            if i:
                key,val = i.split(separators[-3], splitnum3)
                d[key.strip()] = val.strip()
        # don't append empty dicts
        if d:
            dicts.append(d)
    return dicts

def listofdicts_to_text(data: List[Dict[str,str]],
                        terms: List[str]=[],
                        separators: List[str]=LISTOFDICTS_SEPARATORS,
                        newlines: bool=True) -> str:
    if not data:
        return ''
    if isinstance(data, str):
        data = text_to_listofdicts(data)
    lines = []
    for datum in data:
        if terms:
            items = [
                separators[0].join([key, str(datum.get(key,''))])
                for key in terms
                if datum.get(key)
            ]
        else:
            items = [
                separators[0].join(keyval)
                for keyval in datum.items()
            ]
        line = separators[1].join(items)
        lines.append(line)
    if newlines:
        separators2 = '%s\n' % separators[2]
    else:
        separators2 = separators[2]
    return separators2.join(lines)


# textnolabels <> listofdicts ------------------------------------------
# 
# This format is like listofdicts but the text form has no labels
# and records are optionally separated by (semicolons and) newlines.
# Labels must be provided for the encoding step.
# 
# Text can contain one key-val pair
# 
#     text1a = "ABC:http://abc.org"
#     text1b = "ABC:http://abc.org;"
#     data1 = [
#         {'label': 'ABC', 'url': 'http://abc.org'}
#     ]
# 
# or multiple key-val pairs.
# 
#     text2a = "ABC:http://abc.org;DEF:http://def.org"
#     text2b = "ABC:http://abc.org;DEF:http://def.org;"
#     text2c = "ABC:http://abc.org;
#               DEF:http://def.org;"
#     data2 = [
#         {'label': 'ABC', 'url': 'http://abc.org'},
#         {'label': 'DEF', 'url': 'http://def.org'}
#     ]
# 
# Old JSON data may be a list of strings rather than dicts.
# 
#     data3 = [
#         'ABC:http://abc.org',
#         'DEF:http://def.org'
#     ]
#     text3 = "ABC:http://abc.org;
#              DEF:http://def.org;"
# 
#     data4 = [
#         'ABC [123]',
#         'DEF [456]'
#     ]
#     text4 = "term:ABC|id:123;
#              term:DEF|id:456"

TEXTNOLABELS_LISTOFDICTS_SEPARATORS = [':', ';']

def textnolabels_to_listofdicts(
        text: str,
        keys: List[str],
        separators: List[str]=TEXTNOLABELS_LISTOFDICTS_SEPARATORS
) -> List[Dict[str,str]]:
    """
    @param text: str
    @param keys: list
    @param separators: list
    @returns: list of dicts
    """
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for n in text.split(separators[0]):
        values = n.strip().split(separators[1], 1) # only split on first colon
        d = {
            keys[n]: value.strip()
            for n,value in enumerate(values)
        }
        data.append(d)
    return data

def listofdicts_to_textnolabels(
        data: List[Dict[str,str]],
        keys: List[str],
        separators: List[str]=TEXTNOLABELS_LISTOFDICTS_SEPARATORS,
        separator: str=':') -> str:
    """
    @param data: list of dicts
    @param keys: list
    @param separators: list
    @param separator: str
    @returns: str
    """
    # split string into list (see data0)
    if isinstance(data, str) and (separators[1] in data):
        data = data.split(separators[1])
    if not isinstance(data, list):
        raise Exception('Data is not a list "%s".' % data)
    
    items = []
    for n in data:
        # string (see data1)
        if isinstance(n, str):
            values = n.strip().split(separators[0], 1)
            item = separator.join(values)
            items.append(item)
            
        # dict (see data2)
        elif isinstance(n, dict):
            # just the values, no keys
            values = [str(n[key]) for key in keys]
            item = separator.join(values)
            items.append(item)
    
    joiner = '%s\n' % separators[1]
    return joiner.join(items)


# bracketids -----------------------------------------------------------
#
# List of bracketid items.
# 
# text = ''
# data = []
# 
# text = "ABC: DEF [123]; ABC: XYZ [456]"
# text = [
#     "ABC: DEF [123]",
#     "ABC: XYZ [456]",
# ]
# data = [
#     {"term": "ABC: DEF", "id": '123'},
#     {"term": "ABC: XYZ", "id": '456'},
# ]

def text_to_bracketids(text: str,
                       fieldnames: List[str]=[]) -> List[Dict[str,str]]:
    data = []
    # might already be listofdicts or listofstrs
    if text and isinstance(text, list):
        if _is_listofdicts(text):
            data = text
        elif _is_listofstrs(text):
            data = [
                text_to_dict(item, fieldnames)
                for item in text
            ]
    # old-skool string
    elif text and isinstance(text, str):
        data = [
            text_to_dict(item, fieldnames)
            for item in text.split(';')
        ]
    return data


# rolepeople -----------------------------------------------------------
#
# List listofdicts but adds default key:val pairs if missing
# 
# text = ''
# data = []
# 
# text = "Watanabe, Joe"
# data = [
#     {'namepart': 'Watanabe, Joe', 'role': 'author'}
# ]
# 
# text = "Masuda, Kikuye [42]:narrator"
# data = [
#     {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42}
# ]
# 
# text = "Watanabe, Joe:author; Masuda, Kikuye:narrator"
# text = [
#     'Watanabe, Joe: author',
#     'Masuda, Kikuye [42]: narrator'
# ]
# text = [
#     {'namepart': 'Watanabe, Joe', 'role': 'author'}
#     {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42}
# ]
# data = [
#     {'namepart': 'Watanabe, Joe', 'role': 'author'}
#     {'namepart': 'Masuda, Kikuye', 'role': 'narrator', 'id': 42}
# ]
#

def _filter_rolepeople(data: List[Dict[str,str]]) -> List[Dict[str,str]]:
    """filters out items with empty nameparts
    prevents this: [{'namepart': '', 'role': 'author'}]
    """
    return [
        item for item in data
        if item.get('namepart') and item.get('role')
    ]

# TODO add type hints
def _parse_rolepeople_text(texts):
    data = []
    for text in texts:
        txt = text.strip()
        if txt:
            item = {'namepart':None, 'role':'author',}
            
            if ('|' in txt) and (':' in txt):
                # ex: "namepart:Sadako Kashiwagi|role:narrator|id:856"
                for chunk in txt.split('|'):
                    key,val = chunk.split(':')
                    item[key] = val.strip()
                if item.get('name') and not item.get('namepart'):
                    item['namepart'] = item.pop('name')
            
            elif ':' in txt:
                # ex: "Sadako Kashiwagi:narrator"
                name,role = txt.split(':')
                item['namepart'] = name.strip()
                item['role'] = role.strip()
            
            else:
                # ex: "Sadako Kashiwagi"
                item['namepart'] = txt
            
            # extract person ID if present
            match = _is_text_bracketid(item.get('namepart',''))
            if match:
                item['namepart'] = match.groupdict()['term'].strip()
                item['id'] = match.groupdict()['id'].strip()
            if item.get('id') and item['id'].isdigit():
                item['id'] = int(item['id'])
            
            data.append(item)
    return data

def text_to_rolepeople(text: str) -> List[Dict[str,str]]:
    if not text:
        return []
    
    # might already be listofdicts or listofstrs
    if isinstance(text, list):
        if _is_listofdicts(text):
            return _filter_rolepeople(text)
        elif _is_listofstrs(text):
            data = _parse_rolepeople_text(text)
            return _filter_rolepeople(data)
    
    text = normalize_string(text)
    
    # or it might be JSON
    if ('{' in text) or ('[' in text):
        try:
            data = json.loads(text)
        except ValueError:
            try:
                data = load_dirty_json(text)
            except ValueError:
                data = []
        if data:
            return _filter_rolepeople(data)
    
    # looks like it's raw text
    return _filter_rolepeople(
        _parse_rolepeople_text(
            text.split(';')
        )
    )

def rolepeople_to_text(data: List[Dict[str,str]]) -> str:
    """Convert list of dicts to string "KEY:VAL|KEY:VAL|...; KEY:VAL|KEY:VAL|..."
    """
    if isinstance(data, str):
        text = data
    else:
        items = []
        for d in data:
            # strings probably formatted or close enough
            if isinstance(d, str):
                items.append(d)
            elif isinstance(d, dict):
                items.append(
                    '|'.join(
                        [f'{key}:{val}' for key,val in d.items()]
                    )
                )
        text = '; '.join(items)
    return text
