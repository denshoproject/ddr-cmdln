# This module was part of the DR inventory effort back in late 2013,early 2014.
# See discussions on 2013-09-17, 09-27, 11-21, and 2014-02-04.
# This module was intended to work with myrepos, a tool for managing multiple
# repositories at the same time.
# https://myrepos.branchable.com/

import configparser
import logging
logger = logging.getLogger(__name__)
import os
import re

import git

from DDR import fileio
from DDR import util


DRIVE_FILE_FIELDS = 'id,level'

LEVELS = ['meta', 'access', 'all']



def groups( path ):
    """Gets list of groups for which there are CSV files.
    
    >>> from DDR import organization
    >>> p = '/var/www/media/base/ddr-testing'
    >>> organization.groups(p)
    ['TS11TB2013', 'WD5000BMV-2']
    
    @param path: Abs path to org repo
    @returns drives: List of group labels
    """
    pattern = re.compile('.csv$')
    return [os.path.splitext(f)[0] for f in os.listdir(path) if pattern.search(f)]

def group_files( path ):
    """Gets list of paths to group files in the repo.
    
    >>> from DDR import organization
    >>> p = '/var/www/media/base/ddr-testing'
    >>> organization.group_files(p)
    ['/var/www/media/base/ddr-testing/TS11TB2013.csv', '/var/www/media/base/ddr-testing/WD5000BMV-2.csv']
    
    @param path: Abs path to org repo
    """
    pattern = re.compile('.csv$')
    return [f for f in os.listdir(path) if pattern.search(f)]
 
def group_file_path( path ):
    """Gets path to CSV file for the specified group based on orgrepo's location.
    
    This is a holdover from when group files were called "drive files"
    and when the group label was always assumed to match a drive label.
    The label is assumed to be the block of \w following '/media'.
    
    >>> from DDR import organization
    >>> r = '/media/DRIVELABEL/ddr/ORGREPO'
    >>> organization.group_file_path(r)
    '/media/DRIVELABEL/ddr/ORGREPO/DRIVELABEL.csv'
    
    @param path: Abs path to org repo (drive label extracted from this)
    """
    label = orgrepo_path.split(os.sep)[2]
    filename = '%s.csv' % label
    path = os.path.join(orgrepo_path, filename)
    return path

def read_group_file( path ):
    """Reads group file, returns list of repos and their levels
    
    Group file is a CSV file containing a list of DDR collection repo IDs and
    and indicator of which binaries should be present (see LEVELS).
    
    >>> from DDR import organization
    >>> p = '/var/www/media/base/ddr-testing/WD5000BMV-2.csv'
    >>> organization.read_group_file(p)
    [{'id': 'ddr-testing-100', 'level': 'full'}, {'id': 'ddr-testing-101', 'level': 'access'}, ...]
    
    @param path: Absolute path to group file.
    @returns: List of dicts (id, level)
    """
    repos = []
    with open(path, 'rb') as f:
        reader = fileio.csv_reader(f)
        for id,level in reader:
            repos.append({'id':id, 'level':level})
    return repos

def write_group_file( repos, path=None ):
    """
    @param repos: List of dicts (id, level)
    @param path: (optional) Absolute path to group file.
    """
    with open(path, 'wb') as f:
        writer = fileio.csv_writer(f)
        for r in repos:
            writer.writerow(r['id'], r['level'])

def group_repo_level( path, repo_basename ):
    """Get level for the specified repo from group file.
    
    @param path: Absolute path to group file.
    @param repo_basename: Collection repo directory.
    @return level
    """
    level = 'unknown'
    with open(path,'r') as f:
        for line in f.readlines():
            if repo_basename in line:
                level = line.split(',')[1].strip()
    return level

def repo_level( repo_path, level=None ):
    """Gets or sets level for specified repo.
    
    @param path: Absolute path to repo.
    @param level: If present, sets ddr.level to value.
    @returns level
    """
    logging.debug('repo_level(%s, %s)' % (repo_path,level))
    repo = git.Repo(repo_path, search_parent_directories=True)
    if level:
        logging.debug('level -> %s' % level)
        repo.git.config('--local', 'ddr.level', level)
    try:
        level = repo.git.config('--get', 'ddr.level')
    except:
        pass
    return level

def read_mrconfig( path ):
    """Reads .mrconfig file
    
    @param path: Absolute path to .mrconfig file.
    @returns: ConfigParser object
    """
    config = configparser.ConfigParser()
    config.readfp(open(path))
    return config

def make_mrconfig( defaults, repos, server, base_path='' ):
    """Makes a myrepos .mrconfig file.
    
    import organization
    p = '/media/WD5000BMV-2/ddr/ddr-testing/WD5000BMV-2.csv'
    repos = organization.read_drive_file(p)
    defaults = {'ddrstatus': 'ddr status "$@"', 'ddrsync': 'ddr sync "$@"'}
    base_path = '/media/WD5000BMV-2/ddr'
    server = 'git@mits.densho.org'
    mrconfig = organization.mrconfig(defaults, base_path, server, repos)
    organization.write_mrconfig(mrconfig, '/tmp/mrconfig')
 
    @param defaults: dict of settings.
    @param repos: List of dicts (id, level)
    @param server: USERNAME@DOMAIN for Gitolite server.
    @param base_path: Absolute path to the directory in which the repos are located.
    @returns mrconfig: A ConfigParser object
    """
    mrconfig = configparser.ConfigParser(defaults)
    for r in repos:
        section = os.path.join(base_path, r['id'])
        mrconfig.add_section(section)
        mrconfig.set(section, 'checkout', "git clone '%s:%s.git' '%s'" % (server, r['id'], r['id']))
    return mrconfig

def write_mrconfig( mrconfig, path ):
    """Writes an .mrconfig file to the specified path.
    
    @param mrconfig: A ConfigParser object
    @param path: Absolute path to write.
    """
    with open(path, 'wb') as f:
        mrconfig.write(f)

def repo_annex_get(repo_path, level):
    """Runs annex-get commands appropriate to this repo's level.
    
    metadata: does nothing
    access: git-annex-gets files ending with ACCESS_SUFFIX
    all: git annex get .
    """
    logger.debug('repo_annex_get(%s)' % repo_path)
    #level = repo_level(repo_path)
    logger.debug('level: %s' % level)
    repo = git.Repo(repo_path, search_parent_directories=True)
    if level == 'access':
        for accessfile in util.find_access_files(repo_path):
            logger.debug('git annex get %s' % accessfile)
            repo.git.annex('get', accessfile)
    elif level == 'all':
        logger.debug('git annex get .')
        repo.git.annex('get', '.')
    logger.debug('DONE')
