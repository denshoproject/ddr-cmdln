from datetime import datetime
import logging
import os

from dateutil import parser

from DDR import config
from DDR import fileio
from DDR import converters


SAMPLE_OLD_CHANGELOG = """* Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg
-- Geoffrey Jost <geoffrey.jost@densho.org>  Tue, 01 Oct 2013 14:33:35 

* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml
-- Geoffrey Jost <geoffrey.jost@densho.org>  Wed, 02 Oct 2013 10:10:45 

* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json
-- Geoffrey Jost <geoffrey.jost@densho.org>  Wed, 02 Oct 2013 10:11:08 
"""

def is_old_entry(txt):
    """Indicate whether this is an old entry.
    
    Sample:
    * Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg
    -- Geoffrey Jost <geoffrey.jost@densho.org>  Tue, 01 Oct 2013 14:33:35 
    """
    try:
        frag = txt.strip().split('\n').pop()[:2]
        if frag == '--':
            return True
    except:
        pass
    return False

def read_old_entry(txt):
    """Read old-style changelog and return entries as data.
    """
    lines = txt.strip().split('\n')
    stamp = lines.pop().replace('-- ', '').split('  ')
    user,mail = stamp[0].replace('>', '').split(' <')
    timestamp = parse_timestamp(stamp[1], mail)
    messages = [l.replace('* ','') for l in lines]
    entry = {'timestamp':timestamp,
             'user':user,
             'mail':mail,
             'messages':messages,}
    return entry

SAMPLE_NEW_CHANGELOG = """2013-10-01T14:33:35 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg

2013-10-02T10:10:45 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml

2013-10-02T10:11:08 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json
"""

def is_new_entry(txt):
    """Indicate whether this is a new entry.
    
    Sample:
    2013-10-01T14:33:35 -- Geoffrey Jost <geoffrey.jost@densho.org>
    * Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg
    """
    try:
        ts = parser.parse(txt.strip().split('\n')[0].split(' -- ')[0])
        return isinstance(ts, datetime)
    except:
        pass
    return False

def read_new_entry(txt):
    """Read new-style changelog and return entries as data.
    """
    lines = txt.strip().split('\n')
    stamp = lines[0].strip().split(' -- ')
    user,mail = stamp[1].replace('>', '').split(' <')
    timestamp = parse_timestamp(stamp[0], mail)
    messages = [l.replace('* ','') for l in lines[1:]]
    entry = {'timestamp':timestamp,
             'user':user,
             'mail':mail,
             'messages':messages,}
    return entry

def parse_timestamp(text, mail):
    # TODO add timezone if absent
    dt = parser.parse(text)
    if dt and (not dt.tzinfo):
        domain = mail.strip().split('@')[-1]
        # Use default timezone unless...
        if domain in config.ALT_TIMEZONES.keys():
            timezone = config.ALT_TIMEZONES[domain]
        else:
            timezone = config.TZ
        dt = timezone.localize(dt)
    return dt

def read_entries(log):
    entries = []
    for e in log.split('\n\n'):
        entry = None
        if is_old_entry(e):
            entry = read_old_entry(e)
        elif is_new_entry(e):
            entry = read_new_entry(e)
        if entry:
            entries.append(entry)
    return entries

def read_changelog(path):
    """
    @param path: Absolute path to changelog file.
    @returns list of entry dicts
    """
    return read_entries(fileio.read_text(path))

def make_entry(messages, user, mail, timestamp=None):
    """Makes a (new-style) changelog entry.
    
    @param messages: List of strings.
    @param user: Person's name.
    @param mail: A valid email address.
    @param timestamp: datetime (optional).
    @returns string
    """
    if not timestamp:
        timestamp = datetime.now(converters.config.TZ)
    stamp = '%s -- %s <%s>' % (converters.datetime_to_text(timestamp), user, mail)
    lines = [stamp] + ['* %s' % m for m in messages]
    return '\n'.join(lines)

def make_changelog(entries):
    cl = [make_entry(e['messages'], e['user'], e['mail'], e['timestamp']) for e in entries]
    return '\n\n'.join(cl)



MODULE_PATH   = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(MODULE_PATH, 'templates')
CHANGELOG_TEMPLATE    = os.path.join(TEMPLATE_PATH, 'changelog.tpl')
CHANGELOG_DATE_FORMAT = os.path.join(TEMPLATE_PATH, 'changelog-date.tpl')

def load_template(filename):
    return fileio.read_text(filename)

def write_changelog_entry(path, messages, user, email, timestamp=None):
    logging.debug('    write_changelog_entry({})'.format(path))
    template = load_template(CHANGELOG_TEMPLATE)
    date_format = load_template(CHANGELOG_DATE_FORMAT)
    # one line per message
    lines = []
    [lines.append('* {}'.format(m)) for m in messages]
    changes = '\n'.join(lines)
    if not timestamp:
        timestamp = datetime.now(converters.config.TZ)
    # render
    entry = template.format(
        changes=changes,
        user=user,
        email=email,
        date=converters.datetime_to_text(timestamp, converters.config.PRETTY_DATETIME_FORMAT)
        )
    fileio.append_text(entry, path)
