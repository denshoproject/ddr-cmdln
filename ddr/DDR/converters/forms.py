# formprep_* --- Form pre-processing functions.--------------------------
#
# These functions take Python data from the corresponding Collection field
# and format it so that it can be used in an HTML form.
#

#def prep_string(data):
#def prep_datetime(data):
#def prep_list(data):
#def prep_kvlist(data):
#def prep_labelledlist(data):

def prep_listofdicts(data, keys, separator=':'):
    """Render list-of-dicts to str, without labels
    
    >>> data0 = 'Label 1:http://abc.org;\nLabel 2:http://def.org'
    >>> formprep_creators(data0, ['label','url'])
    'Label 1:http://abd.org;\nLabel 1:http://def.org'
    >>> data1 = ['Label 1:http://abc.org']
    >>> formprep_creators(data1, ['label','url'])
    'Label 1:http://abc.org'
    >>> data2 = [{'label':'Label 1', 'url':'http://abc.org'}, {'label':'Label 2', 'url':'http://def.org'}]
    >>> formprep_creators(data2, ['label','url'])
    'Label 1:http://abc.org;\nLabel 1:http://def.org'
    
    """
    items = []
    # split string into list (see data0)
    if isinstance(data, basestring) and (';' in data):
        data = data.split(';')
    # prep list of items
    if isinstance(data, list):
        for n in data:
            # string (see data1)
            if isinstance(n, basestring):
                values = n.strip().split(':',1)
                item = separator.join(values)
                items.append(item)
            # dict (see data2)
            elif isinstance(n, dict):
                # just the values, no keys
                values = [n[key] for key in keys]
                item = separator.join(values)
                items.append(item)
            else:
                assert False
    data = ';\n'.join(items)
    return data

#def prep_rolepeople(data):


# formpost_* --- Form post-processing functions ------------------------
#
# These functions take data from the corresponding form field and turn it
# into Python objects that are inserted into the Collection.
#

#def post_string(data):
#def post_datetime(data):
#def post_list(data):
#def post_kvlist(data):
#def post_labelledlist(data):

def post_listofdicts(data, keys, separators=[';', ':']):
    """Splits up data into separate Label:URL pairs.
    
    TODO replace with generic function in DDR.converters.
    
    >>> data1 = "Label 1:http://..."
    >>> formpost_creators(data1)
    [{'label': 'Label 1', 'url': 'http://...'}]
    >>> data2 = "Label 1:http://...;Label 2:http://..."
    >>> formpost_creators(data2)
    [{'label': 'Label 1', 'url': 'http://...'}, {'label': 'Label 2', 'url': 'http://...'}]
    """
    a = []
    if data:
        for n in data.split(separators[0]):
            values = n.strip().split(separators[1], 1) # only split on first colon
            d = {
                keys[n]: value.strip()
                for n,value in enumerate(values)
            }
            a.append(d)
    return a

#def post_rolepeople(data):
