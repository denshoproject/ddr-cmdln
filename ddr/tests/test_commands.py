import configparser
from datetime import datetime
import logging
import os
import shutil
import sys
import unittest

import envoy
import git
from nose.tools import nottest
import pytest
import requests

from DDR import commands
from DDR import config
from DDR import dvcs
from DDR import identifier
from DDR import models

DEBUG = True

GIT_USER = 'gjost'
GIT_MAIL = 'gjost@densho.org'
AGENT = 'pytest'
TEST_FILES_DIR = os.path.join(
    os.getcwd(), 'ddr-cmdln-assets'
)

TEST_CID       = 'ddr-testing-123'
TEST_EIDS      = ['{}-{}'.format(TEST_CID, n) for n in [1,2]]
TEST_CID_ALT   = 'ddr-testing-123-alt'

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
TEST_MEDIA_DIR = os.path.join(MODULE_PATH, '..', 'files', 'entity')


@pytest.fixture(scope="session")
def test_paths(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('commands')
    paths = {}
    paths['LOGGING_FILE'] = str(tmpdir / 'log')
    paths['TEST_COLLECTION']      = str(tmpdir / TEST_CID)
    paths['COLLECTION_CHANGELOG'] = str(tmpdir / TEST_CID / 'changelog')
    paths['COLLECTION_EAD']       = str(tmpdir / TEST_CID / 'ead.xml')
    paths['COLLECTION_FILES']     = str(tmpdir / TEST_CID / 'files')
    paths['COLLECTION_GIT']       = str(tmpdir / TEST_CID / '.git')
    paths['COLLECTION_GITIGNORE'] = str(tmpdir / TEST_CID / '.gitignore')
    paths['ALT_COLLECTION'] = str(tmpdir / TEST_CID_ALT)
    paths['ALT_CHANGELOG']  = str(tmpdir / TEST_CID_ALT / 'changelog')
    paths['ALT_CONTROL']    = str(tmpdir / TEST_CID_ALT / 'control')
    paths['ALT_EAD']        = str(tmpdir / TEST_CID_ALT / 'ead.xml')
    paths['ALT_FILES']      = str(tmpdir / TEST_CID_ALT / 'files')
    paths['ALT_GIT']        = str(tmpdir / TEST_CID_ALT / '.git')
    paths['ALT_GITIGNORE']  = str(tmpdir / TEST_CID_ALT / '.gitignore')
    return paths


def last_local_commit(path, branch, debug=False):
    """Gets hash of last LOCAL commit on specified branch.
    
    $ git log <branch> -1
    commit 891c0f2f56a59dcd68ccf04392193be4b075fb2c
    Author: gjost <geoffrey.jost@densho.org>
    Date:   Fri Feb 8 15:29:07 2013 -0700
    """
    h = ''
    os.chdir(path)
    # get last commit
    run = envoy.run('git log {} -1'.format(branch))
    # 'commit 925315a29179c63f0849c0149451f1dd30010c02\nAuthor: gjost <geoffrey.jost@densho.org>\nDate:   Fri Feb 8 15:50:31 2013 -0700\n\n    Initialized entity ddr-testing-3-2\n'
    h = None
    if run.std_out:
        h = run.std_out.split('\n')[0].split(' ')[1]
    return h

def last_remote_commit(path, branch, debug=False):
    """Gets hash of last REMOTE commit on specified branch.
    
    $ git ls-remote --heads
    From git@mits:ddr-testing-3.git
    7174bbd2a628bd2979e05c507c599937de22d2c9        refs/heads/git-annex
    925315a29179c63f0849c0149451f1dd30010c02        refs/heads/master
    d11f9258d0d34c4f0d6bfa3f8a9c7dcb1b64ef53        refs/heads/synced/master
    """
    h = ''
    os.chdir(path)
    ref_head = 'refs/heads/{}'.format(branch)
    run = envoy.run('git ls-remote --heads')
    for line in run.std_out.split('\n'):
        if ref_head in line:
            h = line.split('\t')[0]
    return h    

def file_in_local_commit(path, branch, commit, filename, debug=False):
    """Tells whether specified filename appears in specified commit message.
    
    IMPORTANT: We're not really checking to see if an actual file was in an
    actual commit here.  We're really just checking if a particular string
    (the filename) appears inside another string (the commit message).
    
    $ git log -1 --stat -p f6e877856b3f0536b6df42cafe3a369917950242 master|grep \|
    changelog                       |    2 ++
    files/ddr-testing-3-2/changelog |    2 ++
    """
    logging.debug('file_in_local_commit({}, {}, {}, {})'.format(
        path, branch, commit, filename))
    present = None
    os.chdir(path)
    run = envoy.run('git log -1 --stat -p {} {}|grep \|'.format(commit, branch))
    if run.std_out:
        logging.debug('\n{}'.format(run.std_out))
        for line in run.std_out.split('\n'):
            linefile = line.split('|')[0].strip()
            if linefile == filename:
                present = True
    logging.debug('    present: {}'.format(present))
    return present

def file_in_remote_commit(collection_cid, commit, filename, debug=False):
    """
    Could do HTTP request:
    http://partner.densho.org/gitweb/?a=commitdiff_plain;p={repo}.git;h={hash}
    http://partner.densho.org/gitweb/?a=commitdiff_plain;p=ddr-testing-3.git;h=b0174f500b9235b7adbd799421294865fe374a13
    
    @return True if present, False if not present, or None if could not contact workbench.
    """
    logging.debug('file_in_remote_commit({}, {}, {})'.format(collection_cid, commit, filename))
    # TODO
    url = '{gitweb}/?a=commitdiff_plain;p={repo}.git;h={hash}'.format(
        gitweb=GITWEB_URL, repo=collection_cid, hash=commit)
    logging.debug('    {}'.format(url))
    try:
        r = requests.get(url)
    except:
        return None
    logging.debug(r.status_code)
    if r and r.status_code == 200:
        for line in r.text.split('\n'):
            logging.debug(line)
            match = '+++ b/{}'.format(filename)
            if line == match:
                logging.debug('    OK')
                return True
    logging.debug('    not present')
    return False


def test_00_create(tmpdir, test_paths):
    """Create a collection.
    """
    repo = dvcs.initialize_repository(
        test_paths['TEST_COLLECTION'], GIT_USER, GIT_MAIL
    )
    ci = identifier.Identifier(test_paths['TEST_COLLECTION'])
    collection = models.collection.Collection.new(ci)
    collection.save(GIT_USER, GIT_MAIL, AGENT)
    
    # directories exist
    print("test_paths['TEST_COLLECTION'] %s" % test_paths['TEST_COLLECTION'])
    assert os.path.exists(test_paths['TEST_COLLECTION'])
    assert os.path.exists(test_paths['COLLECTION_CHANGELOG'])
    assert os.path.exists(test_paths['COLLECTION_EAD'])
    # git, git-annex
    git   = os.path.join(test_paths['TEST_COLLECTION'], '.git')
    annex = os.path.join(git, 'annex')
    assert os.path.exists(git)
    assert os.path.exists(annex)

EXPECTED_STATUS = """
On branch master
nothing to commit, working tree clean
"""

def test_020_status(tmpdir, test_paths):
    """Get status info for collection.
    """
    collection = identifier.Identifier(test_paths['TEST_COLLECTION']).object()
    print(collection)
    out = commands.status(collection)
    assert out == EXPECTED_STATUS.strip()

EXPECTED_ANNEX_STATUS = """
"""

#TODO REQUIRES NETWORK ACCESS AND GITOLITE CREDENTIALS
#def test_021_annex_status(tmpdir, test_paths):
#    """Get annex status info for collection.
#    """
#    collection = identifier.Identifier(test_paths['TEST_COLLECTION']).object()
#    print(collection)
#    out = commands.annex_status(collection)
#    assert out == EXPECTED_ANNEX_STATUS.strip()

def test_03_update(tmpdir, test_paths):
    """Register changes to specified file; does not push.
    """
    collection = identifier.Identifier(test_paths['TEST_COLLECTION']).object()
    collection.notes = 'testing testing'
    collection.write_json()
    updated_files = [
        collection.identifier.path_abs('json')
    ]
    exit,status = commands.update(
        GIT_USER, GIT_MAIL,
        collection,
        updated_files,
        agent='pytest', commit=False
    )
    print(exit,status)
    assert exit == 0
    assert status == 'ok'

#TODO REQUIRES NETWORK ACCESS AND GITOLITE CREDENTIALS
#def test_04_sync(tmpdir, test_paths):
#    """git pull/push to workbench server, git-annex sync
#    """
#    logging.debug('test_04_sync ---------------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    cmd = 'ddr sync {} --log {} --user {} --mail {} --collection {}'.format(
#        debug, test_paths['LOGGING_FILE'], GIT_USER, GIT_MAIL, test_paths['TEST_COLLECTION'])
#    logging.debug('{}'.format(cmd))
#    run = envoy.run(cmd, timeout=30)
#    logging.debug(run.std_out)
#    # tests
#    # check that local,remote commits exist and are equal
#    # indicates that local changes made it up to workbench
#    remote_hash_master   = last_remote_commit(test_paths['TEST_COLLECTION'], 'master')
#    remote_hash_gitannex = last_remote_commit(test_paths['TEST_COLLECTION'], 'git-annex')
#    local_hash_master   = last_local_commit(test_paths['TEST_COLLECTION'], 'master')
#    local_hash_gitannex = last_local_commit(test_paths['TEST_COLLECTION'], 'git-annex')
#    assert remote_hash_master
#    assert remote_hash_gitannex
#    assert local_hash_master
#    assert local_hash_gitannex
#    assert remote_hash_master == local_hash_master
#    assert remote_hash_gitannex == local_hash_gitannex
#    # TODO sync is not actually working, but these tests aren't capturing that

def test_10_entity_create(tmpdir, test_paths):
    """Create new entity in the collection

    collection = identifier.Identifier(test_paths['TEST_COLLECTION']).object()
    collection.notes = 'testing testing'
    collection.write_json()
    updated_files = [
        collection.identifier.path_abs('json')
    ]
    """
    collection = identifier.Identifier(test_paths['TEST_COLLECTION']).object()
    print(collection)
    for eid in TEST_EIDS:
        ei = identifier.Identifier(eid, collection.identifier.basepath)
        entity = models.entity.Entity.new(ei)
        print(entity)
        exit,status = commands.entity_create(
            GIT_USER, GIT_MAIL,
            collection,
            entity.identifier,
            [
                collection.json_path_rel,
                collection.ead_path_rel,
            ],
            agent=AGENT
        )
        print(exit,status)

    # confirm entity files exist
    assert os.path.exists(test_paths['COLLECTION_FILES'])
    for eid in TEST_EIDS:
        entity_path = os.path.join(test_paths['COLLECTION_FILES'],eid)
        assert os.path.exists(entity_path)
        assert os.path.exists(os.path.join(entity_path,'changelog'))
        assert os.path.exists(os.path.join(entity_path,'control'))
        assert os.path.exists(os.path.join(entity_path,'mets.xml'))
    # TODO test contents of entity files
    print('entity files exist')
    
    # confirm entities in changelog
    changelog_entries = []
    for eid in TEST_EIDS:
        changelog_entries.append('* Initialized entity {}'.format(eid))
    changelog = None
    with open(test_paths['COLLECTION_CHANGELOG'],'r') as cl:
        changelog = cl.read()
    for entry in changelog_entries:
        assert entry in changelog
    print('entities in changelog')
    
    # confirm entities in ead.xml
    assert os.path.exists(test_paths['COLLECTION_EAD'])
    #ead = None
    #with open(test_paths['COLLECTION_EAD'], 'r') as ec:
    #    ead = ec.read()
    #for eid in TEST_EIDS:
    #    entry = '<unittitle eid="{}">'.format(eid)
    #    assert entry in ead

#def test_11_entity_destroy(tmpdir, test_paths):
#    """Remove entity from the collection
#    """
#    logging.debug('test_11_entity_destroy -----------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    # tests
#    # TODO confirm entity files gone
#    # TODO confirm entity destruction mentioned in changelog
#    # TODO confirm entity no longer in control
#    # TODO confirm entity no longer in ead.xml <dsc>
#    # TODO confirm entity desctruction properly recorded for posterity
#
#def test_12_entity_update(tmpdir, test_paths):
#    """Register changes to specified file; does not push.
#    """
#    logging.debug('test_12_entity_update ------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    # simulate making changes to a file for each entity
#    eid_files = [[TEST_EIDS[0], 'update_control', 'control'],
#                 [TEST_EIDS[1], 'update_mets', 'mets.xml'],]
#    for eid,srcfilename,destfilename in eid_files:
#        entity_path = os.path.join(test_paths['COLLECTION_FILES'],eid)
#        srcfile  = os.path.join(TEST_FILES_DIR, 'entity', srcfilename)
#        destfile = os.path.join(entity_path,              destfilename)
#        shutil.copy(srcfile, destfile)
#        # run update
#        cmd = 'ddr eupdate {} --log {} --user {} --mail {} --collection {} --entity {} --file {}'.format(
#            debug, test_paths['LOGGING_FILE'], GIT_USER, GIT_MAIL, test_paths['TEST_COLLECTION'], eid, destfilename)
#        logging.debug(cmd)
#        run = envoy.run(cmd, timeout=30)
#        logging.debug(run.std_out)
#        # test that modified file appears in local commit
#        commit = last_local_commit(test_paths['TEST_COLLECTION'], 'master')
#        # entity files will appear in local commits as "files/ddr-testing-X-Y/FILENAME"
#        destfilerelpath = os.path.join('files', eid, destfilename)
#        assert file_in_local_commit(
#            test_paths['TEST_COLLECTION'], 'master', commit, destfilerelpath, debug=debug
#        )

def test_13_entity_annex_add(tmpdir, test_paths):
    """git annex add file to entity, push it, and confirm that in remote repo
    """
    collection = identifier.Identifier(
        test_paths['TEST_COLLECTION']).object()
    print(collection)
    print(collection.path_abs)
    basepath = collection.identifier.basepath
    print('basepath %s' % basepath)
    print('TEST_EIDS %s' % TEST_EIDS)
    eid = TEST_EIDS[0]
    print('eid %s' % eid)
    entity = identifier.Identifier(
        eid, basepath).object()
    print(entity)
    print(entity.path_abs)
    
    # place test file
    # ddrimport.jpg
    # sha1   b9773b9aefbbe603127520c58166f4fb94e1ad09
    # sha256 201f855f2bd72745e1353f53a0348f6ef4a0b6e3eefa3c34e2c3fb847f22e8be
    # md5    4e3802dbb78c02524f17a334b6d39312
    # size   1030029
    fid = '%s-master-b9773b9aef' % entity.id
    print(fid)
    file_ = identifier.Identifier(
        fid, collection.identifier.basepath).object()
    print(file_)
    file_.basename_orig = 'ddrimport.jpg'
    file_.sha1   = 'b9773b9aefbbe603127520c58166f4fb94e1ad09'
    file_.sha256 = '201f855f2bd72745e1353f53a0348f6ef4a0b6e3eefa3c34e2c3fb847f22e8be'
    file_.md5    = '4e3802dbb78c02524f17a334b6d39312'
    file_.size   = '1030029'
    file_.label = 'testing testing'
    file_.write_json()
    assert os.path.exists(file_.identifier.path_abs('json'))
    updated_files = [
        file_.identifier.path_rel('json'),
    ]
    
    dest_dir = entity.identifier.path_abs('files')
    print('DEST_DIR %s' % dest_dir)
    file_name = '%s.jpg' % fid
    access_name = '%s-a.jpg' % fid
    dest_file = os.path.join(dest_dir, file_name)
    dest_access = os.path.join(dest_dir, access_name)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    assert os.path.exists(dest_dir)
    shutil.copyfile(
        os.path.join(TEST_FILES_DIR, 'ddrimport.jpg'),
        dest_file
    )
    assert os.path.exists(dest_file)
    shutil.copyfile(
        os.path.join(TEST_FILES_DIR, 'ddrimport.jpg'),
        dest_access
    )
    assert os.path.exists(dest_access)
    new_annex_files = [
        os.path.relpath(dest_file, collection.path_abs),
        os.path.relpath(dest_access, collection.path_abs),
    ]
    
    print('updated_files')
    for path in updated_files:
        print('    %s' % path)
    print('new_annex_files')
    for path in new_annex_files:
        print('    %s' % path)
    
    exit,status = commands.entity_annex_add(
        GIT_USER, GIT_MAIL,
        collection, entity,
        updated_files,
        new_annex_files,
        agent=AGENT
    )
    print(exit,status)
    
    ## test file checksums in mets.xml
    #mets_checksums = [
    #    '<file CHECKSUM="fadfbcd8ceb71b9cfc765b9710db8c2c" CHECKSUMTYPE="md5">',
    #    '<Flocat href="files/6a00e55055.png"/>',
    #    '<file CHECKSUM="42d55eb5ac104c86655b3382213deef1" CHECKSUMTYPE="md5">',
    #    '<Flocat href="files/20121205.jpg"/>',
    #]
    #mets_path = os.path.join(test_paths['COLLECTION_FILES'],eid,'mets.xml')
    #print(mets_path)
    #with open(mets_path, 'r') as mf:
    #    mets = mf.read()
    #    for cs in mets_checksums:
    #        assert cs in mets
    # TODO test 20121205.jpg,6a00e55055.png in local commit
    # TODO test 20121205.jpg,6a00e55055.png in remote commit

#def test_14_sync_again(tmpdir, test_paths):
#    """Sync again, this time to see if 
#    """
#    logging.debug('test_14_sync_again ---------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    cmd = 'ddr sync {} --log {} --user {} --mail {} --collection {}'.format(
#        debug, test_paths['LOGGING_FILE'], GIT_USER, GIT_MAIL, test_paths['TEST_COLLECTION'])
#    logging.debug('{}'.format(cmd))
#    run = envoy.run(cmd, timeout=30)
#    logging.debug(run.std_out)
#    # tests
#    # check that local,remote commits exist and are equal
#    # indicates that local changes made it up to workbench
#    remote_hash_master   = last_remote_commit(test_paths['TEST_COLLECTION'], 'master')
#    remote_hash_gitannex = last_remote_commit(test_paths['TEST_COLLECTION'], 'git-annex')
#    local_hash_master   = last_local_commit(test_paths['TEST_COLLECTION'], 'master')
#    local_hash_gitannex = last_local_commit(test_paths['TEST_COLLECTION'], 'git-annex')
#    assert remote_hash_master
#    assert remote_hash_gitannex
#    assert local_hash_master
#    assert local_hash_gitannex
#    assert remote_hash_master == local_hash_master
#    assert remote_hash_gitannex == local_hash_gitannex
#    # TODO sync is not actually working, but these tests aren't capturing that

#def test_20_push(tmpdir, test_paths):
#    """git annex copy a file to the server; confirm it was actually copied.
#    """
#    logging.debug('test_20_push ---------------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    repo = git.Repo(test_paths['TEST_COLLECTION'])
#    # push all files for first entity
#    eid = TEST_EIDS[0]
#    for f in os.listdir(TEST_MEDIA_DIR):
#        entity_path = os.path.join(test_paths['COLLECTION_FILES'],eid)
#        pushfile_abs = os.path.join(entity_path, 'files', f)
#        pushfile_rel = pushfile_abs.replace('{}/'.format(test_paths['TEST_COLLECTION']), '')
#        assert os.path.exists(pushfile_abs)
#        # run update
#        cmd = 'ddr push {} --log {} --collection {} --file {}'.format(
#            debug, test_paths['LOGGING_FILE'], test_paths['TEST_COLLECTION'], pushfile_rel)
#        logging.debug(cmd)
#        run = envoy.run(cmd, timeout=30)
#        logging.debug(run.std_out)
#        # confirm that GIT_REMOTE_NAME appears in list of remotes the file appears in
#        remotes = dvcs.annex_whereis_file(repo, pushfile_rel)
#        logging.debug('    remotes {}'.format(remotes))
#        assert GIT_REMOTE_NAME in remotes

#def test_30_clone(tmpdir, test_paths):
#    """Clone an existing collection to an alternate location.
#    
#    IMPORTANT: This test cannot be run without running all the previous tests!
#    """
#    logging.debug('test_30_clone --------------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    #if os.path.exists(test_paths['ALT_COLLECTION']):
#    #    shutil.rmtree(test_paths['ALT_COLLECTION'], ignore_errors=True)
#    #
#    cmd = 'ddr clone {} --log {} --user {} --mail {} --cid {} --dest {}'.format(
#        debug, test_paths['LOGGING_FILE'], GIT_USER, GIT_MAIL, TEST_CID, test_paths['ALT_COLLECTION'])
#    logging.debug(cmd)
#    run = envoy.run(cmd, timeout=30)
#    logging.debug(run.std_out)
#    # directories exist
#    assert os.path.exists(test_paths['ALT_COLLECTION'])
#    assert os.path.exists(test_paths['ALT_CHANGELOG'])
#    assert os.path.exists(test_paths['ALT_CONTROL'])
#    assert os.path.exists(test_paths['ALT_EAD'])
#    # git, git-annex
#    git   = os.path.join(test_paths['ALT_COLLECTION'], '.git')
#    annex = os.path.join(git, 'annex')
#    assert os.path.exists(git)
#    assert os.path.exists(annex)

#def test_31_pull(tmpdir, test_paths):
#    """git-annex pull files into collection from test_30_clone.
#    
#    IMPORTANT: This test cannot be run without running all the previous tests!
#    """
#    logging.debug('test_31_pull ---------------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    assert os.path.exists(test_paths['ALT_COLLECTION'])
#    repo = git.Repo(test_paths['ALT_COLLECTION'])
#    # pull all files for first entity
#    eid = TEST_EIDS[0]
#    for f in os.listdir(TEST_MEDIA_DIR):
#        entity_path = os.path.join(test_paths['COLLECTION_FILES'],eid)
#        file_abs = os.path.join(entity_path, 'files', f)
#        file_rel = file_abs.replace(TESTING_BASE_DIR, '').replace(TEST_CID, '', 1)
#        if file_rel.startswith('/'):
#            file_rel = file_rel[1:]
#        logging.debug('entity_path: {}'.format(entity_path))
#        logging.debug('file_abs: {}'.format(file_abs))
#        logging.debug('file_rel: {}'.format(file_rel))
#        # link should exist but file should NOT exist yet
#        #assert os.path.lexists(file_rel)
#        #assert os.path.islink(file_rel)
#        #assert not os.path.exists(file_rel)
#        # run update
#        cmd = 'ddr pull {} --log {} --collection {} --file {}'.format(
#            debug, test_paths['LOGGING_FILE'], test_paths['ALT_COLLECTION'], file_rel)
#        logging.debug(cmd)
#        run = envoy.run(cmd, timeout=30)
#        logging.debug(run.std_out)
#        # file should exist, be a symlink, and point to annex dir
#        assert os.path.exists(file_rel)
#        assert os.path.islink(file_rel)
#        assert '/.git/annex/objects/' in os.readlink(file_rel)

#def test_99_destroy(tmpdir, test_paths):
#    """Destroy a collection.
#    """
#    logging.debug('test_99_destroy ------------------------------------------------------')
#    debug = ''
#    if DEBUG:
#        debug = ' --debug'
#    # tests
