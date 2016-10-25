from datetime import datetime
import os

import config
import changelog


def test_is_old_entry():
    """Test that entries are old entries and not new entries.
    """
    for e in SAMPLE_OLD_CHANGELOG.split('\n\n'):
        assert changelog.is_old_entry(e) == True
    for e in SAMPLE_NEW_CHANGELOG.split('\n\n'):
        assert changelog.is_old_entry(e) == False

def test_read_old_entry():
    entries = [changelog.read_old_entry(e) for e in SAMPLE_OLD_CHANGELOG.split('\n\n')]
    assert entries == expected_read_old

def test_is_new_entry():
    """Test that entries are new entries and not old entries.
    """
    for e in SAMPLE_OLD_CHANGELOG.split('\n\n'):
        assert changelog.is_new_entry(e) == False
    for e in SAMPLE_NEW_CHANGELOG.split('\n\n'):
        assert changelog.is_new_entry(e) == True

def test_read_new_entry():
    entries = [changelog.read_new_entry(e) for e in SAMPLE_NEW_CHANGELOG.split('\n\n')]
    assert entries == expected_read_new

def test_read_entries():
    log = '\n\n'.join([SAMPLE_OLD_CHANGELOG, SAMPLE_NEW_CHANGELOG])
    assert changelog.read_entries(log) == expected_read_entries

# read_changelog

def test_make_entry():
    messages = [
        'Updated entity file .../ddr-testing-160-1/entity.json',
        'Updated entity file .../ddr-testing-160-1/mets.xml'
    ]
    user = 'Geoffrey Jost'
    mail = 'geoffrey.jost@densho.org'
    timestamp = datetime(2013,10,2, 10,10,45)
    expected = '2013-10-02 10:10:45 -- Geoffrey Jost <geoffrey.jost@densho.org>\n' \
               '* Updated entity file .../ddr-testing-160-1/entity.json\n' \
               '* Updated entity file .../ddr-testing-160-1/mets.xml'
    assert changelog.make_entry(messages, user, mail, timestamp) == expected

def test_make_changelog():
    assert changelog.make_changelog(expected_read_new) == SAMPLE_NEW_CHANGELOG

def test_load_template():
    expected = '{changes}\n-- {user} <{email}>  {date}\n'
    assert changelog.load_template(changelog.CHANGELOG_TEMPLATE) == expected

def test_write_changelog_entry():
    path = '/tmp/changelog-%s' % datetime.now(config.TZ).strftime('%Y%m%d-%H%M%S')
    user = 'gjost'
    mail = 'gjost@densho.org'
    messages = ['testing', 'testing', '123']
    timestamp = datetime(2014,5,29, 14,38,55)
    expected1 = '* testing\n* testing\n* 123\n' \
                '-- gjost <gjost@densho.org>  Thu, 29 May 2014 14:38:55 \n'
    expected2 = '* testing\n* testing\n* 123\n' \
                '-- gjost <gjost@densho.org>  Thu, 29 May 2014 14:38:55 \n\n' \
                '* testing\n* testing\n* 123\n' \
                '-- gjost <gjost@densho.org>  Thu, 29 May 2014 14:38:55 \n'
    # clean
    if os.path.exists(path):
        os.remove(path)
    # write once
    changelog.write_changelog_entry(path, messages, user, mail, timestamp)
    with open(path, 'r') as f1:
        first = f1.read()
    # write again
    changelog.write_changelog_entry(path, messages, user, mail, timestamp)
    with open(path, 'r') as f2:
        second = f2.read()
    # check
    assert first == expected1
    assert second == expected2


SAMPLE_OLD_CHANGELOG = """* Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg
-- Geoffrey Jost <geoffrey.jost@densho.org>  Tue, 01 Oct 2013 14:33:35 

* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml
-- Geoffrey Jost <geoffrey.jost@densho.org>  Wed, 02 Oct 2013 10:10:45 

* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json
-- Geoffrey Jost <geoffrey.jost@densho.org>  Wed, 02 Oct 2013 10:11:08 """

expected_read_old = [{'timestamp': datetime(2013, 10, 1, 14, 33, 35), 'messages': ['Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 10, 45), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json', 'Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 11, 8), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}]

SAMPLE_NEW_CHANGELOG = """2013-10-01 14:33:35 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg

2013-10-02 10:10:45 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml

2013-10-02 10:11:08 -- Geoffrey Jost <geoffrey.jost@densho.org>
* Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json"""

expected_read_new = [{'timestamp': datetime(2013, 10, 1, 14, 33, 35), 'messages': ['Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 10, 45), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json', 'Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 11, 8), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}]

expected_read_entries = [{'timestamp': datetime(2013, 10, 1, 14, 33, 35), 'messages': ['Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 10, 45), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json', 'Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 11, 8), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 1, 14, 33, 35), 'messages': ['Added entity file files/ddr-testing-160-1-master-c703e5ece1-a.jpg'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 10, 45), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/entity.json', 'Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/mets.xml'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}, {'timestamp': datetime(2013, 10, 2, 10, 11, 8), 'messages': ['Updated entity file /var/www/media/base/ddr-testing-160/files/ddr-testing-160-1/files/ddr-testing-160-1-master-c703e5ece1.json'], 'user': 'Geoffrey Jost', 'mail': 'geoffrey.jost@densho.org'}]
