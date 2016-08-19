from datetime import datetime


# module function ------------------------------------------------------

def choice_is_valid(valid_values, field, value):
    """Indicates whether value is one of valid values for field.
    """
    if value in valid_values[field]:
	return True
    return False

def normalize_string(text):
    if not text:
        return ''
    text = unicode(text)
    text = text.strip()
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = text.replace('\n', '\\n')
    return text


# csvload_* --- import-from-csv functions ------------------------------
#
# These functions take data from a CSV field and convert it to Python
# data for the corresponding Entity field.
#
    
def load_string(text):
    """
    """
    return normalize_string(text)

def load_datetime(text, datetime_format):
    """Loads a datetime
    
    >>>csv.load_datetime('1970-1-1T00:00:00', '%Y-%m-%dT%H:%M:%S')
    datetime.datetime(1970, 1, 1, 0, 0)
    
    """
    text = normalize_string(text)
    if text and text.strip():
        return datetime.strptime(text.strip(), datetime_format)
    return ''

def load_list(text):
    """Loads a simple list.
    
    >>> csv.load_list('thing1; thing2')
    ['thing1', 'thing2']
    """
    if not text:
        return []
    data = []
    for x in normalize_string(text).split(';'):
        x = x.strip()
        if x:
            data.append(x)
    return data

def load_kvlist(text):
    """Loads a list of key-value pairs
    
    >>>csv.load_kvlist('name1:author; name2:photog')
    [{u'name1': u'author'}, {u'name2': u'photog'}]
    """
    if not text:
        return []
    data = []
    for x in normalize_string(text).split(';'):
        x = x.strip()
        if x:
            if not ':' in x:
                raise Exception('Malformed data: %s' % text)
            key,val = x.strip().split(':')
            data.append({key.strip(): val.strip()})
    return data
            
def load_labelledlist(text):
    """language can be 'eng', 'eng;jpn', 'eng:English', 'jpn:Japanese'
    
    >>>csv.load_labelledlist('eng')
    [u'eng']
    >>>csv.load_labelledlist('eng;jpn')
    [u'eng', u'jpn']
    >>>csv.load_labelledlist('eng:English')
    [u'eng']
    >>>csv.load_labelledlist('eng:English; jpn:Japanese')
    [u'eng', u'jpn']
    """
    if not text:
        return []
    data = []
    for x in normalize_string(text).split(';'):
        x = x.strip()
        if x:
            if ':' in x:
                data.append(x.strip().split(':')[0])
            else:
                data.append(x.strip())
    return data

LISTOFDICTS_SEPARATORS = [':', '|', ';']
LISTOFDICTS_SPLIT1X = [':']

def load_listofdicts(text, separators=LISTOFDICTS_SEPARATORS, split1x=LISTOFDICTS_SPLIT1X):
    """
    >>> text0 = 'url:http://abc.org/|label:Label 1'
    >>> load_listofdicts(text0)
    [
        {'label': 'Label 1', 'url': 'http://abc.org/'}
    ]
    >>> text1 = 'url:http://abc.org/|label:Label 1; url:http://def.org/|label:Label 2'
    >>> load_listofdicts(text1)
    [
        {'label': 'Label 1', 'url': 'http://abc.org/'},
        {'label': 'Label 2', 'url': 'http://def.org/'}
    ]
    
    @param text: str
    @param separators: list of separators, from inside out
    @param split1x: list of separators on which to only split once
    @returns: list of dicts
    """
    def setsplitnum(separator, split1x):
        if separator in split1x:
            return 1
        return -1
    splitnum1 = setsplitnum(separators[-1], split1x)
    splitnum2 = setsplitnum(separators[-2], split1x)
    splitnum3 = setsplitnum(separators[-3], split1x)
    # parse it up
    dicts = []
    for line in text.strip().split(separators[-1], splitnum1):
        d = {}
        for item in line.strip().split(separators[-2], splitnum2):
            key,val = item.strip().split(separators[-3], splitnum3)
            d[key] = val
        dicts.append(d)
    return dicts

def load_rolepeople(text):
    """
    >>> data0 = ''
    >>> data1 = "Watanabe, Joe"
    >>> data2 = "Masuda, Kikuye:author"
    >>> data3 = "Boyle, Rob:concept,editor; Cross, Brian:concept,editor"
    >>> formpost_creators(data0)
    []
    >>> formpost_creators(data1)
    [{'namepart': 'Watanabe, Joe', 'role': 'author'}]
    >>> formpost_creators(data2)
    [{'namepart': 'Masuda, Kikuye', 'role': 'author'}]
    >>> formpost_creators(data3)
    [{'namepart': 'Boyle, Rob', 'role': 'concept,editor'}, {'namepart': 'Cross, Brian', 'role': 'concept,editor'}]
    """
    if not text:
        return []
    data = []
    for a in normalize_string(text).split(';'):
        b = a.strip()
        if b:
            if ':' in b:
                name,role = b.strip().split(':')
            else:
                name = b; role = 'author'
            c = {'namepart': name.strip(), 'role': role.strip(),}
            data.append(c)
    return data


# csvdump_* --- export-to-csv functions ------------------------------
#
# These functions take Python data from the corresponding Entity field
# and format it for export in a CSV field.
#

def dump_string(data):
    """Dump stringdata to text suitable for a CSV field.
    
    @param data: str
    @returns: unicode string
    """
    return normalize_string(data)

def dump_datetime(data, datetime_format):
    """Dump datetime to text suitable for a CSV field.
    
    >>> csv.dump_datetime(datetime.datetime(1970, 1, 1, 0, 0), '%Y-%m-%dT%H:%M:%S')
    '1970-1-1T00:00:00'
    
    TODO handle timezone if available
    
    @param data: datetime object
    @returns: unicode string
    """
    if data:
        return datetime.strftime(data, datetime_format)
    return None

def dump_list(data):
    """Dumps a simple list of strings
    
    >>> csv.dump_list(['thing1', 'thing2'])
    'thing1; thing2'
    
    @param data: list
    @returns: unicode string
    """
    return '; '.join(data)

def dump_kvlist(data):
    """Dumps a list of key-value pairs
    
    >>> data = [
        {u'name1': u'author'},
        {u'name2': u'photog'}
    ]
    >>> csv.dump_kvlist(data)
    'thing1; thing2'
    
    @param data: list of dicts
    @returns: unicode string
    """
    items = []
    for d in data:
        i = [k+':'+v for k,v in d.iteritems()]
        item = '; '.join(i)
        items.append(item)
    text = '; '.join(items)
    return text

def dump_labelledlist(data):
    """Dump list of langcode:label items to text suitable for a CSV field.
    
    language can be 'eng', 'eng;jpn', 'eng:English', 'jpn:Japanese'
    
    >>> csv.dump_labelledlist([u'eng'])
    'eng'
    >>> csv.dump_labelledlist([u'eng', u'jpn'])
    'eng; jpn'
    
    @param data: list of strings
    @returns: unicode string
    """
    return u'; '.join(data)

def dump_listofdicts(data, separators=LISTOFDICTS_SEPARATORS):
    """Dumps list-of-dicts data to str
    """
    lines = []
    for datum in data:
        items = [separators[0].join(keyval) for keyval in datum.iteritems()]
        line = separators[1].join(items)
        lines.append(line)
    return separators[2].join(lines)

def dump_rolepeople(data):
    """
    >>> data0 = ''
    >>> data1 = "Watanabe, Joe"
    >>> data2 = "Masuda, Kikuye:author"
    >>> data3 = "Boyle, Rob:concept,editor; Cross, Brian:concept,editor"
    >>> formpost_creators(data0)
    []
    >>> formpost_creators(data1)
    [{'namepart': 'Watanabe, Joe', 'role': 'author'}]
    >>> formpost_creators(data2)
    [{'namepart': 'Masuda, Kikuye', 'role': 'author'}]
    >>> formpost_creators(data3)
    [{'namepart': 'Boyle, Rob', 'role': 'concept,editor'}, {'namepart': 'Cross, Brian', 'role': 'concept,editor'}]
    
    @param data: list of dicts
    @returns: unicode string
    """
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


# csvvalidate_* --------------------------------------------------------
#
# These functions examine data in a CSV field and return True if valid.
#
