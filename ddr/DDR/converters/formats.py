# Various ways to represent structured data in strings,
# for use in web forms or in CSV files.
#
# Some of these methods are extremely similar to each other, but they
# originated in different places in the app or different points in the
# process.  They are collected here to document the various formats
# used, and hopefully to make it easier to merge or prune them in the
# future.

from datetime import datetime

def normalize_string(text):
    if not text:
        return u''
    return unicode(text).replace('\r\n', '\n').replace('\r', '\n').replace('\n', '\\n').strip()


# datetime -------------------------------------------------------------
#
# format = '%Y-%m-%dT%H:%M:%S'
# text = '1970-1-1T00:00:00'
# data = datetime.datetime(1970, 1, 1, 0, 0)
# 

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

def text_to_datetime(text, fmt=DATETIME_FORMAT):
    """Load datatime from text in specified format.
    
    TODO timezone!
    TODO use dateparse or something?
    
    @param text: str
    @param fmt: str
    @returns: datetime
    """
    text = normalize_string(text)
    if text:
        return datetime.strptime(text, fmt)
    return ''

def datetime_to_text(data, fmt=DATETIME_FORMAT):
    """Dump datetime to text suitable for a CSV field.
    
    TODO timezone!
    
    @param data: datetime
    @returns: str
    """
    if data:
        return datetime.strftime(data, datetime_format)
    return None


# list ----------------------------------------------------------------
#
# text = 'thing1; thing2'
# data = ['thing1', 'thing2']
#

LIST_SEPARATOR = ';'
LIST_SEPARATOR_SPACE = '%s ' % LIST_SEPARATOR

def text_to_list(text, separator=LIST_SEPARATOR):
    """
    @param text: str
    @param separator: str
    @returns: list
    """
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for item in text.split(separator):
        item = item.strip()
        if item:
            data.append(item)
    return data

def list_to_text(data, separator=LIST_SEPARATOR_SPACE):
    """
    @param data: list
    @param separator: str
    @returns: str
    """
    return separator.join(data)


# kvlist ---------------------------------------------------------------
#
# text = 'name1:author; name2:photog'
# data = [
#     {u'name1': u'author'},
#     {u'name2': u'photog'}
# ]
# 

def text_to_kvlist(text):
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
            data.append({key.strip(): val.strip()})
    return data

def kvlist_to_text(data):
    items = []
    for d in data:
        i = [k+':'+v for k,v in d.iteritems()]
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
    
def text_to_labelledlist(text):
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for x in text.split(';'):
        x = x.strip()
        if x:
            if ':' in x:
                data.append(x.strip().split(':')[0])
            else:
                data.append(x.strip())
    return data

def labelledlist_to_text(data, separator=u'; '):
    return separator.join(data)


# rolepeople -----------------------------------------------------------
#
# List listofdicts but adds default key:val pairs if missing
# 
# text = ''
# data = []
# text = "Watanabe, Joe"
# data = [
#     {'namepart': 'Watanabe, Joe', 'role': 'author'}
# ]
# text = "Masuda, Kikuye:author"
# data = [
#     {'namepart': 'Masuda, Kikuye', 'role': 'author'}
# ]
# text = "Boyle, Rob:concept,editor; Cross, Brian:concept,editor"
# data = [
#     {'namepart': 'Boyle, Rob', 'role': 'concept,editor'},
#     {'namepart': 'Cross, Brian', 'role': 'concept,editor'}
# ]
#

def text_to_rolepeople(text):
    text = normalize_string(text)
    if not text:
        return []
    data = []
    for a in text.split(';'):
        b = a.strip()
        if b:
            if ':' in b:
                name,role = b.strip().split(':')
            else:
                name = b; role = 'author'
            c = {'namepart': name.strip(), 'role': role.strip(),}
            data.append(c)
    return data

def rolepeople_to_text(data):
    if isinstance(data, basestring):
        text = data
    else:
        items = []
        for d in data:
            # strings probably formatted or close enough
            if isinstance(d, basestring):
                items.append(d)
            elif isinstance(d, dict) and d.get('namepart',None):
                items.append('%s:%s' % (d['namepart'],d['role']))
        text = '; '.join(items)
    return text


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

def _setsplitnum(separator, split1x):
    if separator in split1x:
        return 1
    return -1

LISTOFDICTS_SEPARATORS = [':', '|', ';']
LISTOFDICTS_SPLIT1X = [':']

def text_to_listofdicts(text, separators=LISTOFDICTS_SEPARATORS, split1x=LISTOFDICTS_SPLIT1X):
    text = normalize_string(text)
    if not text:
        return {}
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
                d[key] = val
        # don't append empty dicts
        if d:
            dicts.append(d)
    return dicts

def listofdicts_to_text(data, separators=LISTOFDICTS_SEPARATORS):
    lines = []
    for datum in data:
        items = [
            separators[0].join(keyval)
            for keyval in datum.iteritems()
        ]
        line = separators[1].join(items)
        lines.append(line)
    return separators[2].join(lines)


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

def textnolabels_to_listofdicts(text, keys, separators=TEXTNOLABELS_LISTOFDICTS_SEPARATORS):
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

def listofdicts_to_textnolabels(data, keys, separators=TEXTNOLABELS_LISTOFDICTS_SEPARATORS, separator=':'):
    """
    @param data: list of dicts
    @param keys: list
    @param separators: list
    @param separator: str
    @returns: str
    """
    # split string into list (see data0)
    if isinstance(data, basestring) and (separators[1] in data):
        data = data.split(separators[1])
    if not isinstance(data, list):
        raise Exception('Data is not a list "%s".' % data)
    
    items = []
    for n in data:
        # string (see data1)
        if isinstance(n, basestring):
            values = n.strip().split(separators[0], 1)
            item = separator.join(values)
            items.append(item)
            
        # dict (see data2)
        elif isinstance(n, dict):
            # just the values, no keys
            values = [n[key] for key in keys]
            item = separator.join(values)
            items.append(item)
    
    joiner = '%s\n' % separators[1]
    return joiner.join(items)
