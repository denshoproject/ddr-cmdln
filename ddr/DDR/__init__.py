VERSION = '0.9.4-beta'

from datetime import datetime, timedelta
import logging
logger = logging.getLogger(__name__)

import simplejson as json

from DDR import config


def _json_handler(obj):
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


class Timer( object ):
    """
    from DDR import Timer
    t = Timer()
    t.mark('start')
    ...YOUR CODE HERE...
    t.mark('did something')
    ...MORE CODE...
    t.mark('did something else')
    ...MORE CODE...
    t.display()
    """
    steps = []
    
    def mark( self, msg ):
        now = datetime.now(config.TZ)
        index = len(self.steps)
        if index:
            delta = now - self.steps[-1]['datetime']
        else:
            delta = timedelta(0)
        step = {'index':index, 'datetime':now, 'delta':delta, 'msg':msg}
        self.steps.append(step)
        logger.debug(msg)
    
    def display( self ):
        """Return list of steps arranged slowest first.
        """
        from operator import itemgetter
        ordered = sorted(self.steps, key=itemgetter('delta'))
        logger.debug('TIMER RESULTS -- SLOWEST-FASTEST -----------------------------------')
        for step in ordered:
            logger.debug('{:>10}: {:<14} | {}'.format(step['index'], step['delta'], step['msg']))
        return self.steps
