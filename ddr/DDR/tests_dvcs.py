from datetime import datetime
import os
import re
import shutil

from nose.tools import assert_raises
import git

import dvcs


def make_repo(path, files=[]):
    repo = git.Repo.init(path)
    # create empty files
    for fn in files:
        fpath = os.path.join(path, fn)
        open(fpath, 'wb').close()
    repo.index.add(files)
    repo.index.commit('initial commit')
    return repo

def clone_repo(repo, path):
    return repo.clone(path)
    
def annex_init(repo):
    repo.git.annex('init')

def cleanup_repo(path):
    shutil.rmtree(path, ignore_errors=True)


# TODO git_set_configs
# TODO annex_set_configs

def test_repository():
    """Tests that repository config values are set correctly"""
    # git_set_configs
    # annex_set_configs
    # repository
    path = '/tmp/test_dvcs.repository-%s' % datetime.now().strftime('%Y%m%d-%H%M%S')
    user = 'gjost'
    mail = 'gjost@densho.org'
    repo = git.Repo.init(path)
    dvcs.repository(path=path, user_name=user, user_mail=mail)
    reader = repo.config_reader()
    core_items = {i[0]:i[1] for i in reader.items('core')}
    user_items = {i[0]:i[1] for i in reader.items('user')}
    annex_items = {i[0]:i[1] for i in reader.items('annex')}
    assert core_items.get('fileMode') == 'false'
    assert user_items.get('name') == user
    assert user_items.get('email') == mail
    assert annex_items.get('sshcaching') == 'false'

def test_git_version():
    basedir = '/tmp/test-ddr-dvcs'
    path = os.path.join(basedir, 'testgitversion')
    # rm existing
    if os.path.exists(path):
        shutil.rmtree(path)
    # set up repos
    repo = make_repo(path, ['testing'])
    # test at repo root
    out = dvcs.git_version(repo)
    assert 'git version' in out

def test_annex_version():
    basedir = '/tmp/test-ddr-dvcs'
    path = os.path.join(basedir, 'testgitversion')
    # rm existing
    if os.path.exists(path):
        shutil.rmtree(path)
    # set up repos
    repo = make_repo(path, ['testing'])
    # test at repo root
    out = dvcs.annex_version(repo)
    assert 'git-annex version' in out
    assert 'local repository version' in out

def test_latest_commit():
    basedir = '/tmp/test-ddr-dvcs'
    path = os.path.join(basedir, 'testrepo')
    # rm existing
    if os.path.exists(path):
        shutil.rmtree(path)
    # set up repos
    repo = make_repo(path, ['testing'])
    # test at repo root
    out1 = dvcs.latest_commit(path)
    # test individual file
    path_to_file = os.path.join(path, 'testing')
    out2 = dvcs.latest_commit(path_to_file)
    # analyze
    regex = r'([0123456789abcdef]+)\s+\([a-zA-Z]+, [a-zA-Z-]+\) ([0-9-]+) ([0-9:]+) (-[0-9]+)'
    assert re.match(regex, out1)
    assert re.match(regex, out2)

def test_parse_cmp_commits():
    log = '\n'.join(['e3bde9b', '8adad36', 'c63ec7c', 'eefe033', 'b10b4cd'])
    A = '8adad36'
    B = 'e3bde9b'
    assert dvcs._parse_cmp_commits(log, A,B) == {'a':A, 'b':B, 'op':'lt'}
    assert dvcs._parse_cmp_commits(log, A,A) == {'a':A, 'b':A, 'op':'eq'}
    assert dvcs._parse_cmp_commits(log, B,A) == {'a':B, 'b':A, 'op':'gt'}
    assert_raises(ValueError, dvcs._parse_cmp_commits, log, A,'123')
    assert_raises(ValueError, dvcs._parse_cmp_commits, log, '123',B)

# TODO cmp_commits

def test_compose_commit_message():
    title = 'made-up title'
    body = 'freeform body text\nbody text line 2'
    agent = 'nosetests'
    expected = '%s\n\n%s\n\n@agent: %s' % (title, body, agent)
    msg = dvcs.compose_commit_message(title, body, agent)
    assert msg == expected

SAMPLE_ANNEX_STATUS = {
    "supported backends": "SHA256 SHA1 SHA512 SHA224 SHA384 SHA256E SHA1E SHA51",
    "supported remote types": "git S3 bup directory rsync web hook",
    "trusted repositories": [],
    "semitrusted repositories": [
        {"here":False, "uuid":"00000000-0000-0000-0000-000000000001", "description":"web"},
        {"here":True,  "uuid":"a39a106a-e5c7-11e3-8996-bfa1bcf63a02", "description":"ddrworkstation"},
        {"here":False, "uuid":"a587a176-3dca-11e3-b491-9baacb8840e9", "description":""},
        {"here":False, "uuid":"a5f4d94d-2073-4b59-8c98-9372012a6cbb", "description":"qnfs"},
        {"here":False, "uuid":"c52c412e-467d-11e3-b428-7fb930a6e21c", "description":""},
    ],
    "untrusted repositories": [],
    "dead repositories": [],
    "available local disk space": "128 gigabytes (+1 megabyte reserved)",
    "local annex keys": "0",
    "local annex size": "0 bytes",
    "known annex keys": "60",
    "known annex size": "212 megabytes",
    "bloom filter size": "16 mebibytes (0% full)",
}

def test_annex_parse_description():
    uuid0 = 'a5f4d94d-2073-4b59-8c98-9372012a6cbb'
    uuid1 = 'a39a106a-e5c7-11e3-8996-bfa1bcf63a02'
    assert dvcs._annex_parse_description(SAMPLE_ANNEX_STATUS, uuid0) == None
    assert dvcs._annex_parse_description(SAMPLE_ANNEX_STATUS, uuid1) == 'ddrworkstation'

# TODO annex_get_description

def test_annex_set_description():
    dl = 'WD201405'
    hn = 'ddrworkstation'
    ph = 'testing'
    ml = 'gjost@densho.org'
    # drive label
    assert dvcs.annex_set_description(drive_label=dl, hostname=hn, partner_host=ph, mail=ml) == dl
    # hostname:domainname
    expected1 = 'ddrworkstation:densho.org'
    assert dvcs.annex_set_description(drive_label=None, hostname=hn, partner_host=hn, mail=ml) == expected1
    # hostname
    assert dvcs.annex_set_description(drive_label=None, hostname=hn, partner_host=ph, mail=ml) == hn
    # TODO Test doesn't cover all possibile combinations!!!

def test_annex_set_description():
    path = '/tmp/test-ddr-dvcs/test-repo'
    
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    out0 = dvcs.annex_set_description(repo, annex_status=SAMPLE_ANNEX_STATUS, description='testing')
    expected0 = 'testing'
    cleanup_repo(path)
    
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    out1 = dvcs.annex_set_description(repo, annex_status=SAMPLE_ANNEX_STATUS, drive_label='usb2015')
    expected1 = 'usb2015'
    cleanup_repo(path)
    
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    out2 = dvcs.annex_set_description(repo, annex_status=SAMPLE_ANNEX_STATUS, hostname='machine',)
    expected2 = 'machine'
    cleanup_repo(path)
    
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    out3 = dvcs.annex_set_description(
        repo, annex_status=SAMPLE_ANNEX_STATUS, hostname='pnr',
    )
    expected3 = 'pnr:'
    cleanup_repo(path)
    
    assert out0 == expected0
    assert out1 == expected1
    assert out2 == expected2
    # description may change depending on the machine
    # examples: pnr:densho.org, pnr:mydomain.com
    # so just check that contains "pnr:"
    assert expected3 in out3

# TODO fetch

# git-status --long (sometimes changes)
STATUS_LONG_0 = """# On branch master
nothing to commit (working directory clean)"""
STATUS_LONG_1 = """# On branch master
nothing to commit, working directory clean"""
STATUS_LONG_2 = """On branch master
nothing to commit, working directory clean"""
STATUS_LONG = [
    STATUS_LONG_0,
    STATUS_LONG_1,
    STATUS_LONG_2,
]
# git-status --short (more stable)
STATUS_SHORT_0 = """## master"""
STATUS_SHORT = [
    STATUS_SHORT_0,
]

def test_repo_status():
    path = '/tmp/test-ddr-dvcs/test-repo'
    repo = make_repo(path, ['testing'])
    out0 = dvcs.repo_status(repo, short=False)
    out1 = dvcs.repo_status(repo, short=True)
    cleanup_repo(path)
    assert out0 in STATUS_LONG
    assert out1 in STATUS_SHORT

ANNEX_STATUS = """root: DEBUG: 
supported backends: SHA256 SHA1 SHA512 SHA224 SHA384 SHA256E SHA1E SHA512E SHA224E SHA384E WORM URL
supported remote types: git S3 bup directory rsync web hook
trusted repositories: 0
semitrusted repositories: 2
        00000000-0000-0000-0000-000000000001 -- web
        6a1a6842-7916-11e5-bae8-87adf3053078 -- here
untrusted repositories: 0
dead repositories: 0
available local disk space: 2 gigabytes (+1 megabyte reserved)
local annex keys: 0
local annex size: 0 bytes
known annex keys: 0
known annex size: 0 bytes
bloom filter size: 16 mebibytes (0% full)
backend usage: 
"""

def test_annex_status():
    path = '/tmp/test-ddr-dvcs/test-repo'
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    status = dvcs.annex_status(repo)
    cleanup_repo(path)
    found = False
    for key in status.iterkeys():
        if 'repositories' in key:
            for r in status[key]:
                if r['here']:
                    found = True
    assert found

GITANNEX_WHEREIS = """FATAL: ddr-testing-141.git ddr DENIED

Command ssh ["git@mits.densho.org","git-annex-shell 'configlist' '/~/ddr-testing-141.git'
FATAL: bad git-annex-shell command: git-annex-shell 'configlist' '/~/' at /home/git/gitol

Command ssh ["git@mits.densho.org","git-annex-shell 'configlist' '/~/'"] failed; exit cod
whereis files/ddr-testing-141-1/files/ddr-testing-141-1-master-96c048001e.pdf (2 copies) 
  	643935ea-1cbe-11e3-afb5-3fb5a8f2a937 -- WD5000BMV-2
   	a311a84a-4e48-11e3-ba9f-2fc2ce00326e -- pnr_tmp-ddr
ok
"""
GITANNEX_WHEREIS = """whereis files/ddr-testing-141-1/files/ddr-testing-141-1-master-96c048001e.pdf (2 copies) 
  	643935ea-1cbe-11e3-afb5-3fb5a8f2a937 -- WD5000BMV-2
   	a311a84a-4e48-11e3-ba9f-2fc2ce00326e -- pnr_tmp-ddr
ok
"""
GITANNEX_WHEREIS_EXPECTED = ['WD5000BMV-2', 'pnr_tmp-ddr']

def test_annex_parse_whereis():
    assert dvcs._annex_parse_whereis(GITANNEX_WHEREIS) == GITANNEX_WHEREIS_EXPECTED

# TODO annex_whereis_file

GITOLITE_INFO_OK = """hello ddr, this is git@mits running gitolite3 v3.2-19-gb9bbb78 on git 1.7.2.5

 R W C	ddr-densho-[0-9]+
 R W C	ddr-densho-[0-9]+-[0-9]+
 R W C	ddr-testing-[0-9]+
 R W C	ddr-testing-[0-9]+-[0-9]+
 R W	ddr-densho
 R W	ddr-densho-1
 R W	ddr-testing
 R W	ddr-testing-101
"""
GITOLITE_ORGS_EXPECTED = ['ddr-densho', 'ddr-testing']
GITOLITE_REPOS_EXPECTED = ['ddr-densho', 'ddr-densho-1', 'ddr-testing', 'ddr-testing-101']

def test_gitolite_info_authorized():
    assert dvcs._gitolite_info_authorized(GITOLITE_INFO_OK) == True
    assert dvcs._gitolite_info_authorized('') == False

# TODO gitolite_connect_ok

def test_gitolite_orgs():
    assert dvcs.gitolite_orgs(GITOLITE_INFO_OK) == GITOLITE_ORGS_EXPECTED

def test_gitolite_repos():
    out = dvcs.gitolite_repos(GITOLITE_INFO_OK)
    assert dvcs.gitolite_repos(GITOLITE_INFO_OK) == GITOLITE_REPOS_EXPECTED
    
# TODO gitolite_info

GIT_DIFF_MODIFIED = """collection.json
files/ddr-densho-10-1/entity.json
files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json
"""
GIT_DIFF_MODIFIED_EXPECTED = [
    'collection.json',
    'files/ddr-densho-10-1/entity.json',
    'files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json',
]

def test_parse_list_modified():
    assert dvcs._parse_list_modified(GIT_DIFF_MODIFIED) == GIT_DIFF_MODIFIED_EXPECTED

# TODO list_modified

GIT_DIFF_STAGED = """collection.json
files/ddr-densho-10-1/entity.json
files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json
"""
GIT_DIFF_STAGED_EXPECTED = [
    'collection.json',
    'files/ddr-densho-10-1/entity.json',
    'files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json',
]

def test_parse_list_staged():
    assert dvcs._parse_list_staged(GIT_DIFF_STAGED) == GIT_DIFF_STAGED_EXPECTED

# TODO list_staged

SAMPLE_COMMIT_LOG = """
commit 4df7877f43a10873ced2c484cc9f65605ee4ca68
Author: DDRAdmin <kinkura@hq.densho.org>
Date:   Tue Apr 22 17:44:37 2014 -0700

    Manual fix for rights.

 collection.json                                                         |    2 +-
 files/ddr-densho-10-1/entity.json                                       |    2 +-
 files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json      |    2 +-
"""
SAMPLE_COMMIT_LOG_PARSED = [
    'collection.json',
    'files/ddr-densho-10-1/entity.json',
    'files/ddr-densho-10-1/files/ddr-densho-10-1-master-c85f8d0f91.json',
]

# TODO stage

def test_parse_list_committed():
    assert dvcs._parse_list_committed(SAMPLE_COMMIT_LOG) == SAMPLE_COMMIT_LOG_PARSED

# TODO list_committed
# TODO commit

SAMPLE_CONFLICTED_0 = """
100755 a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2 1\tpath/to/conflicted_file/01
100755 1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2a 2\tpath/to/conflicted_file/02
100755 ab2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b21 3\tpath/to/conflicted_file/03
"""
SAMPLE_CONFLICTED_1 = ""
SAMPLE_CONFLICTED_0_EXPECTED = [
    'path/to/conflicted_file/01',
    'path/to/conflicted_file/02',
    'path/to/conflicted_file/03',
]
SAMPLE_CONFLICTED_1_EXPECTED = []
def test_parse_list_conflicted():
    assert dvcs._parse_list_conflicted(SAMPLE_CONFLICTED_0) == SAMPLE_CONFLICTED_0_EXPECTED
    assert dvcs._parse_list_conflicted(SAMPLE_CONFLICTED_1) == SAMPLE_CONFLICTED_1_EXPECTED

# TODO list_conflicted

CONFLICTED_JSON_TEXT = """{
    {
        "record_created": "2013-09-30T12:43:11"
    },
    {
<<<<<<< HEAD
        "record_lastmod": "2013-10-02T12:59:30"
=======
        "record_lastmod": "2013-10-02T12:59:30"
>>>>>>> 0b9d669da8295fc05e092d7abdce22d4ffb50f45
    },
    {
        "status": "completed"
    },
}"""
CONFLICTED_JSON_EXPECTED = [
    {u'record_created': u'2013-09-30T12:43:11'},
    {u'record_lastmod': {'right': u'2013-10-02T12:59:30', 'left': u'2013-10-02T12:59:30'}},
    {u'status': u'completed'},
]

def test_load_conflicted_json():
    assert dvcs.load_conflicted_json(CONFLICTED_JSON_TEXT) == CONFLICTED_JSON_EXPECTED

AUTOMERGE_TEXT = """
Next level meh sriracha, distillery Tonx actually Etsy sustainable Tumblr.
<<<<<<< HEAD
Art party meggings tote bag drinking vinegar distillery jean shorts, mumblecore
farm-to-table flexitarian. Pug small batch Thundercats mustache. Trust fund XOXO
=======
Polaroid blog Kickstarter. Ennui disrupt tote bag, you probably haven't heard of
them VHS food truck DIY 8-bit swag direct trade fingerstache. Cliche wayfarers
>>>>>>> 0a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d
hashtag pour-over, church-key tousled trust fund Tonx Intelligentsia vinyl
photo booth Vice Brooklyn seitan. Meggings irony Echo Park Pitchfork Thundercats.
"""
AUTOMERGE_LEFT_EXPECTED = """
Next level meh sriracha, distillery Tonx actually Etsy sustainable Tumblr.
Art party meggings tote bag drinking vinegar distillery jean shorts, mumblecore
farm-to-table flexitarian. Pug small batch Thundercats mustache. Trust fund XOXO
hashtag pour-over, church-key tousled trust fund Tonx Intelligentsia vinyl
photo booth Vice Brooklyn seitan. Meggings irony Echo Park Pitchfork Thundercats.
"""
AUTOMERGE_RIGHT_EXPECTED = """
Next level meh sriracha, distillery Tonx actually Etsy sustainable Tumblr.
Polaroid blog Kickstarter. Ennui disrupt tote bag, you probably haven't heard of
them VHS food truck DIY 8-bit swag direct trade fingerstache. Cliche wayfarers
hashtag pour-over, church-key tousled trust fund Tonx Intelligentsia vinyl
photo booth Vice Brooklyn seitan. Meggings irony Echo Park Pitchfork Thundercats.
"""

def test_automerge_conflicted():
    assert dvcs.automerge_conflicted(AUTOMERGE_TEXT) == AUTOMERGE_LEFT_EXPECTED
    assert dvcs.automerge_conflicted(AUTOMERGE_TEXT, which='left') == AUTOMERGE_LEFT_EXPECTED
    assert dvcs.automerge_conflicted(AUTOMERGE_TEXT, which='right') == AUTOMERGE_RIGHT_EXPECTED

# TODO merge_add
# TODO merge_commit
# TODO diverge_commit



GIT_STATUS_MESSAGES = {
    'synced0': '## master',
    'synced1': '## master...origin/master',
    'ahead8':  '## master...origin/master [ahead 8]',
    'behind2': '## master...origin/master [behind 2]',
    'ahead8mod1': '## master...origin/master [ahead 8]\nM collection.json',
    'ahead8mod2': '## master...origin/master [ahead 8]\nM collection.json\ncontrol',
    'behind2mod1': '## master...origin/master [behind 2]\nM files/whatever',
    'ahead4behind2': '## master...origin/master [ahead 4, behind 2]',
    'ahead1behind2conf2': '## master...origin/master [ahead 1, behind 2]\nUU changelog\nUU collection.json',
    'ahead1behind2mod1conf1': '## master...origin/master [ahead 1, behind 2]\nM  changelog\nUU collection.json',
    'ahead1behind2mod2': '## master...origin/master [ahead 1, behind 2]\nM  changelog\nM  collection.json',
}

def test_repo_states():
    results = {
        key: dvcs.repo_states(status)
        for key,status in GIT_STATUS_MESSAGES.iteritems()
    }
    assert results['synced0'] == ['synced']
    assert results['synced1'] == ['synced']
    assert results['ahead8'] == ['ahead']
    assert results['behind2'] == ['behind']
    assert results['ahead8mod1'] == ['ahead', 'modified']
    assert results['ahead8mod2'] == ['ahead', 'modified']
    assert results['behind2mod1'] == ['behind', 'modified']
    assert results['ahead4behind2'] == ['ahead', 'behind']
    assert results['ahead1behind2conf2'] == ['ahead', 'behind', 'conflicted']
    assert results['ahead1behind2mod1conf1'] == ['ahead', 'behind', 'modified', 'conflicted']
    assert results['ahead1behind2mod2'] == ['ahead', 'behind', 'modified']


GIT_STATUS_SYNCED = [
    """## master""",
    """## master\n?? .gitstatus""",
    """## master\n?? .gitstatus\n?? files/ddr-testing-233-1/addfile.log""",
]
GIT_STATUS_AHEAD = [
    """## master...origin/master [ahead 1]""",
    """## master...origin/master [ahead 2]""",
]
GIT_STATUS_BEHIND = [
    """## master...origin/master [behind 1]""",
]
GIT_STATUS_DIVERGED = [
    """## master...origin/master [ahead 1, behind 2]""",
]

def test_synced():
    for status in GIT_STATUS_SYNCED:   assert     dvcs.synced(status) # <<<
    for status in GIT_STATUS_AHEAD:    assert not dvcs.synced(status)
    for status in GIT_STATUS_BEHIND:   assert not dvcs.synced(status)
    for status in GIT_STATUS_DIVERGED: assert not dvcs.synced(status)

def test_ahead():
    for status in GIT_STATUS_SYNCED:   assert not dvcs.ahead(status)
    for status in GIT_STATUS_AHEAD:    assert     dvcs.ahead(status) # <<<
    for status in GIT_STATUS_BEHIND:   assert not dvcs.ahead(status)
    for status in GIT_STATUS_DIVERGED: assert not dvcs.ahead(status)

def test_behind():
    for status in GIT_STATUS_SYNCED:   assert not dvcs.behind(status)
    for status in GIT_STATUS_AHEAD:    assert not dvcs.behind(status)
    for status in GIT_STATUS_BEHIND:   assert     dvcs.behind(status) # <<<
    for status in GIT_STATUS_DIVERGED: assert not dvcs.behind(status)

def test_diverged():
    for status in GIT_STATUS_SYNCED:   assert not dvcs.diverged(status)
    for status in GIT_STATUS_AHEAD:    assert not dvcs.diverged(status)
    for status in GIT_STATUS_BEHIND:   assert not dvcs.diverged(status)
    for status in GIT_STATUS_DIVERGED: assert     dvcs.diverged(status) # <<<

GIT_STATUS_CONFLICTED = [
    '## master...origin/master [ahead 1, behind 2]\nUU changelog\nUU collection.json',
]
GIT_STATUS_PARTIAL_RESOLVED = [
    '## master...origin/master [ahead 1, behind 2]\nM  changelog\nUU collection.json',
]
GIT_STATUS_RESOLVED = [
    '## master...origin/master [ahead 1, behind 2]\nM  changelog\nM  collection.json',
]

def test_conflicted():
    for status in GIT_STATUS_SYNCED:     assert not dvcs.conflicted(status)
    for status in GIT_STATUS_AHEAD:      assert not dvcs.conflicted(status)
    for status in GIT_STATUS_BEHIND:     assert not dvcs.conflicted(status)
    for status in GIT_STATUS_DIVERGED:   assert not dvcs.conflicted(status)
    for status in GIT_STATUS_CONFLICTED: assert     dvcs.conflicted(status) # <<<

# TODO test PARTIAL_RESOLVED
# TODO test RESOLVED

# TODO repos

def test_is_local():
    url0 = ''
    url1 = '/tmp/ddr-testing-141.git'
    url2 = 'git@mits.densho.org:ddr-testing-141.git'
    assert dvcs.is_local(url0) == -1
    assert dvcs.is_local(url1) == 1
    assert dvcs.is_local(url2) == 0

def test_local_exists():
    assert dvcs.local_exists('/tmp') == 1
    assert dvcs.local_exists('/abcde12345') == 0
    
def test_is_clone():
    basedir = '/tmp/test-ddr-dvcs'
    path0 = os.path.join(basedir, 'testrepo0')
    path1 = os.path.join(basedir, 'testrepo1')
    path2 = os.path.join(basedir, 'clone')
    # rm existing
    if os.path.exists(path0):
        shutil.rmtree(path0)
    if os.path.exists(path1):
        shutil.rmtree(path1)
    if os.path.exists(path2):
        shutil.rmtree(path2)
    # set up repos
    repo0 = make_repo(path0, ['test'])
    repo1 = make_repo(path1, ['testing'])
    clone = clone_repo(repo1, path2)
    # test
    assert dvcs.is_clone(path0, path1, 1) == 0
    assert dvcs.is_clone(path1, path0, 1) == 0
    assert dvcs.is_clone(path0, path2, 1) == 0
    assert dvcs.is_clone(path1, path2, 1) == 1
    assert dvcs.is_clone(path1, '/tmp', 1) == -1

def test_remotes():
    basedir = '/tmp/test-ddr-dvcs'
    path_orig = os.path.join(basedir, 'testrepo1')
    path_clon = os.path.join(basedir, 'clone')
    # rm existing
    if os.path.exists(path_orig):
        shutil.rmtree(path_orig)
    if os.path.exists(path_clon):
        shutil.rmtree(path_clon)
    # set up repos
    repo1 = make_repo(path_orig, ['testing'])
    clone = clone_repo(repo1, path_clon)
    # clone lists origin in remotes, origin doesn't know the clone
    expected1 = []
    expected2 = [
        {
            'name': 'origin',
            'url': '/tmp/test-ddr-dvcs/testrepo1/.git',
            'target': '/tmp/test-ddr-dvcs/testrepo1/.git',
            'fetch': '+refs/heads/*:refs/remotes/origin/*',
            'clone': 1,
            'local': 1,
            'local_exists': 1,
        }
    ]
    # test
    out1 = dvcs.remotes(repo1)
    out2 = dvcs.remotes(clone)
    assert out1 == expected1
    assert out2 == expected2

# TODO repos_remotes

def test_annex_file_targets():
    basedir = '/tmp/test-ddr-dvcs'
    path = os.path.join(basedir, 'test-repo')
    repo = make_repo(path, ['testing'])
    annex_init(repo)
    for filename in ['test1', 'test2']:
        fpath = os.path.join(path, filename)
        with open(fpath, 'wb') as f:
            f.write('fsaf;laksjf;lsakjf;aslkfj;aslkfj;salkjf;sadlkfj')
        repo.git.annex('add', filename)
    repo.index.commit('added files')
    targets_abs = dvcs.annex_file_targets(repo, relative=False)
    targets_rel = dvcs.annex_file_targets(repo, relative=True)
    expected_abs = [
        (
            '/tmp/test-ddr-dvcs/test-repo/test1',              # symlink
            '/tmp/test-ddr-dvcs/test-repo/.git/annex/objects'  # target
        ),
        (
            '/tmp/test-ddr-dvcs/test-repo/test2',
            '/tmp/test-ddr-dvcs/test-repo/.git/annex/objects'
        )
    ]
    expected_rel = [
        (
            'test1',
            '.git/annex/objects/'
        ),
        (
            'test2',
            '.git/annex/objects/'
        )
    ]
    # abs and rel symlinks
    assert targets_abs[0][0] == expected_abs[0][0]
    assert targets_rel[0][0] == expected_rel[0][0]
    # git-annex upgrades seem to change the object hashes so we can't
    # match hashes.  Instead we'll confirm that targets are at least
    # under .git/annex/objects/ dir.
    assert expected_abs[0][1] in targets_abs[0][1]
    assert expected_rel[0][1] in targets_rel[0][1]
