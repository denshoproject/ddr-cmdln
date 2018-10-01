# -*- coding: utf-8 -*-
VERSION = '0.9.4-beta'

from datetime import datetime, timedelta
import logging
logger = logging.getLogger(__name__)

import simplejson as json

from DDR import config


def _json_handler(obj):
    """Function that gets called for objects that can't otherwise be serialized.
    
    Should return a JSON encodable version of the object or raise a TypeError.
    https://docs.python.org/3/library/json.html
    """
    if hasattr(obj, 'isoformat'):
        return obj.strftime(config.DATETIME_FORMAT)
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))
    
def format_json(data, sort_keys=True):
    """Write JSON using consistent formatting and sorting.
    
    For versioning and history to be useful we need data fields to be written
    in a format that is easy to edit by hand and in which values can be compared
    from one commit to the next.  This function prints JSON with nice spacing
    and indentation and with sorted keys, so fields will be in the same relative
    position across commits.
    
    >>> data = {'a':1, 'b':2}
    >>> path = '/tmp/ddrlocal.models.write_json.json'
    >>> write_json(data, path)
    >>> with open(path, 'r') as f:
    ...     print(f.readlines())
    ...
    ['{\n', '    "a": 1,\n', '    "b": 2\n', '}']
    """
    return json.dumps(
        data,
        indent=4, separators=(',', ': '), sort_keys=sort_keys,
        default=_json_handler,
    )
