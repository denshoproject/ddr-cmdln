HELP = """
ddr-filter is a wrapper around the git filter-branch command, which steps through
all the commits in a repository's history, running a filter at each step.  This
is a destructive operation!  It results in a new repository that contains only
the public items from the original, and is thus technically a separate
repository.  This new public repository must be kept separate to avoid deleting
private items from the original.

ddr-filter makes a list of items in the original repository that are to be
removed, then makes a clone of the repository and its files.  Once it has made
the clone it removes the clone's git remotes; this prevents accidental pulls and
pushes between the new repository and the original.  The git filter-branch
command is then run and non-public items are removed from the repository.

EXAMPLES

    # List files to be excluded and quit.
    $ ddr-filter -l /PATH/TO/REPOSITORY/ddr-testing-123

    # Run the filter, copying master, mezzanine, and access files.
    $ ddr-filter -Mma -d /DESTINATION/DIR /PATH/TO/REPOSITORY/ddr-testing-123

    # Run the filter, copying master, mezzanine, and access files and keeping
    # the temporary repository.
    $ ddr-filter -k -Mma -d /DESTINATION/DIR /PATH/TO/REPOSITORY/ddr-testing-123

    # Run the filter, copying only the mezzanine and access files.
    $ ddr-filter -ma -d /DESTINATION/DIR /PATH/TO/REPOSITORY/ddr-testing-123

Due to peculiarities of git filter-branch and to time pressures, the operation
is broken up into two phases.  The first phase is this Python command, which
does the initial work and then generates a shell script which finishes the job.
The actual filtering is done in the shell script.  Both scripts write to the log
file.

At the end the following files will be present in the destination directory:

    FILTER_ddr-testing-141      # temporary repository (removed by default)
    FILTER_ddr-testing-141.sh   # phase II shell script
    FILTER_ddr-testing-141.log  # log file for both phases of the operation
    PUBLIC_ddr-testing-141      # final public repository
"""

# References:
# https://xrunhprof.wordpress.com/2012/06/14/changing-git-committers-and-authors-with-python/
# https://jonmccune.wordpress.com/2012/06/17/code-release-history-and-aesthetics-with-git-filter-branch-and-friends/
# http://www.snip2code.com/Snippet/10557/To-remove-files-from-git-history-after-t/
# http://www.bioperl.org/wiki/Using_Git/Advanced#Prepare_list_of_files_to_be_removed


from datetime import datetime
import json
import logging
import os
import re
import subprocess
import sys

import click
import git

from DDR import config
from DDR import fileio
from DDR import identifier
from DDR import models
from DDR import util


@click.group()
@click.option('-l', '--mklist',   is_flag=True, help='Generate exclusion list from source repository.')
@click.option('-k', '--keeptmp',  is_flag=True, help='Keep temporary files.')
@click.option('-M', '--master',   is_flag=True, help='git-annex copy master files.')
@click.option('-m', '--mezzanine',is_flag=True, help='git-annex copy mezzanine files.')
@click.option('-a', '--access',   is_flag=True, help='git-annex copy access files.')
@click.option('-d', '--destdir',  default='',   help='Absolute path to directory in which new repository will be placed.')
@click.argument('source')
def ddrfilter(mklist, keeptmp, master, mezzanine, access, destdir, source):
    """Destructively filter a repository, completely removing items marked non-public or incomplete.
    """
    main(mklist, keeptmp, master, mezzanine, access, destdir, source)


@ddrfilter.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


def dtfmt(dt):
    """Consistent date format.
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

def logprint(filename, msg):
    """Print to log file and console, with timestamp.
    """
    msg = '%s - %s\n' % (dtfmt(datetime.now()), msg)
    fileio.append_text(msg, filename)
    click.echo(msg.strip('\n'))

def logprint_nots(filename, msg):
    """Print to log file and console, no timestamp.
    """
    msg = '%s\n' % msg
    fileio.append_text(msg, filename)
    click.echo(msg.strip('\n'))


# ----------------------------------------------------------------------
# Clone repository, prepending "filtering" to repo directory name
# (e.g. "ddr-densho-123" -> "filtering_ddr-densho-123")

def mk_filter_repo_path( source, destdir, status ):
    """Makes path to filter repository.
    
    @param source: Absolute path to source collection repository.
    @param destdir: Absolute path to filter_repo dir.
    @param status: Current status in the process
    @returns: Absolute path to filter repository dir.
    """
    srccid = os.path.basename(source)
    destcid = '%s_%s' % (status, srccid)
    path = os.path.join(destdir, destcid)
    return path

def clone( source, dest_path ):
    """Clones source repository into destdir.
    
    TODO use DDR.dvcs
    
    @param source: Absolute path to source collection repository.
    @param dest_path
    @returns: A GitPython Repository
    """
    repo = git.Repo.clone_from(source, dest_path)
    # git annex init if not already existing
    if not os.path.exists(os.path.join(dest_path, '.git', 'annex')):
        repo.git.annex('init')
    repo.git.checkout('master')
    return repo

def annex_list( repo_path, level ):
    """Lists annex files of the specified level.
    
    TODO use DDR.dvcs
    
    @param repo_path: Absolute path to repository.
    @param repo: A GitPython Repository
    @param level: String (access, mezzanine, master)
    @returns: list of files, relative to repository directory.
    """
    os.chdir(repo_path)
    r = subprocess.run(
        f'git annex whereis {identifier.FILETYPE_MATCH_ANNEX[level]}',
        check=True, shell=True, capture_output=True, timeout=30
    )
    files = []
    for line in r.stdout.decode("utf-8").strip().split('\n'):
        if 'whereis' in line:
            files.append(line.split(' ')[1])
    return files
    
def annex_get( repo, filename ):
    """git annex gets the specified file
    
    TODO use DDR.dvcs
    
    @param repo: A GitPython Repository
    @param filename: path to file, relative to repository directory
    """
    repo.git.annex('get', filename)

# ----------------------------------------------------------------------
# Remove all git and git-annex remotes.

def rm_remotes( repo ):
    """Removes all git and git-annex remotes from the repository.
    
    TODO use DDR.dvcs
    
    @param repo: A GitPython Repository
    @return: 0 if ok, list of remotes if not
    """
    for remote in repo.remotes:
        repo.delete_remote(remote)
    if repo.remotes:
        return repo.remotes
    return 0


# ----------------------------------------------------------------------
# Walk directory to find all .json files
# For each .json check public/private status.
# If private, add file(s) to exclusion list.
# Exclusion list will be fed to git filter-branch command.

def list_json_files(dirname):
    """Lists absolute paths to .json files in dirname; skips .git directory.
    
    @param path: Absolute path to collection repository directory
    @returns: list of absolute paths
    """
    paths = util.find_meta_files(dirname, recursive=True, force_read=True)
    paths.sort()
    return paths

def is_collection_json(path):
    return re.search(identifier.META_FILENAME_REGEX['collection'], path)

def is_entity_json(path):
    return re.search(identifier.META_FILENAME_REGEX['entity'], path)

def is_file_json(path):
    return re.search(identifier.META_FILENAME_REGEX['file'], path)

def is_publishable( json_path ):
    """Indicates whether the Collection or Entity is public and complete.
    
    Specifically, indicates whether metadata file contains both
    the public=1 value and the status=completed value.
    DDR metadata .json files consist of a list of dicts.
    
    @param path: Absolute path to *.json file.
    @returns: True or False
    """
    data = json.loads(fileio.read_text(json_path))
    if data:
        public = False
        completed = False
        for field in data:
            if ((u'public' in field.keys()) or ('public' in field.keys())):
                if field.get('public') and int(field['public']):
                    public = True
            if ((u'status' in field.keys()) or ('status' in field.keys())):
                if field['status'] == 'completed':
                    completed = True
        if public and completed:
            return True
    return False

def is_publishable_file( json_path ):
    """Indicates whether the File is public.
    
    See is_publishable for details.
    Note: Files don't have "status" field.
    
    @param path: Absolute path to *.json file.
    @returns: True or False
    """
    data = json.loads(fileio.read_text(json_path))
    if data:
        public = False
        for field in data:
            if ((u'public' in field.keys()) or ('public' in field.keys())):
                if int(field['public']):
                    public = True
        if public:
            return True
    return False
    
def nonpublic_json_files( json_files ):
    """Filters list of json files, returning the ones that are non-public.
    
    @param json_files: List of absolute paths
    @returns: list of absolute paths
    """
    paths = []
    for path in json_files:
        if is_file_json(path):
            if not is_publishable_file(path):
                paths.append(path)
        elif is_entity_json(path) or is_collection_json(path):
            if not is_publishable(path):
                paths.append(path)
        else:
            click.echo('UNKNOWN FILE TYPE: %s' % path)
            assert False
    return paths

def nonpublic_collection( path ):
    """Excludes the whole collection directory.
    
    @param path: Absolute path to collection.json
    @returns: directory containing collection
    """
    return '%s/' % os.path.dirname(path)

def nonpublic_entity( path ):
    """Excludes the whole entity directory.
    
    >>> nonpublic_entity('.../ddr-testing-123/files/ddr-testing-123-4/entity.json')
    '.../ddr-testing-123/files/ddr-testing-123-4/'
    
    @param path: Absolute path to entity.json
    @returns: directory containing entity
    """
    return '%s/' % os.path.dirname(path)

def nonpublic_file( path ):
    """Excludes the file's original file, access file, and metadata.
    
    >>> nonpublic_file('.../ddr-testing-123/files/ddr-testing-123-3/files/ddr-testing-123-3-master-1a2b3c4d5e.json')
    '.../ddr-testing-123/files/ddr-testing-123-3/files/ddr-testing-123-3-master-1a2b3c4d5e*'
    
    @param path: Absolute path to file json.
    @returns: path that matches file JSON, original file, access copies.
    """
    return '%s*' % os.path.splitext(path)[0]

def all_nonpublic_files( nonpublic_json_paths ):
    """
    @param nonpublic_json_paths: 
    @returns: list of paths
    """
    paths = []
    for p in nonpublic_json_paths:
        if   is_collection_json(p): paths.append(nonpublic_collection(p))
        elif is_entity_json(p):     paths.append(nonpublic_entity(p))
        elif is_file_json(p):       paths.append(nonpublic_file(p))
    return paths

def make_exclusion_list( path ):
    """Makes list of paths to be excluded from the repository.
    
    Appends 'COLLECTION' to list if collection is marked unpublishable.
    
    @param path
    @returns: List of relative paths to private files/dirs.
    """
    files_json = list_json_files(path)
    nonpublic_json = nonpublic_json_files(files_json)
    exclusion_list = all_nonpublic_files(nonpublic_json)
    exclusion_list.sort()
    # make paths relative
    for n,filepath in enumerate(exclusion_list):
        if filepath[:-1] == path:
            # the collection itself is private or incomplete
            exclusion_list[n] = 'COLLECTION'
        else:
            exclusion_list[n] = filepath.replace(path, '')[1:]
    return exclusion_list

def write_exclusion_list( path, exclusion_list, filename ):
    """Writes exclusion list to the specified directory.
    
    @param path
    @param exclusion_list
    @param filename
    @return: filename if ok, 1 if not
    """
    fname = os.path.join(os.path.dirname(path), filename)
    #cmd = 'git rm -rf %s\n'
    cmd = 'git rm -qrf --cached --ignore-unmatch %s ;\\\n'
    #cmd = 'git rm -rf --cached --ignore-unmatch %s ;\\\n'
    actions = [cmd % path for path in exclusion_list]
    fileio.write_text('\n'.join(actions), fname)
    if os.path.exists(fname):
        return fname
    return 1


# ----------------------------------------------------------------------
# Run git-filter-branch on the exclusion list

def mk_filterbranch_script( tmp_repo_path, exclusion_list_path, public_repo_path, keep_temp_repo=True ):
    """Makes a shell script that runs the git-filter-branch command and cleans up.
    
    Reference:
    https://jonmccune.wordpress.com/2012/06/17/code-release-history-and-aesthetics-with-git-filter-branch-and-friends/
    
    For some reason, the filter-branch command will work in a shell script but not in a subprocess.
    I could not figure out how to garbage-collect the temporary filter repository in-place, so we are using the brute-force approach of cloning an entire new public repository.
    
    @param tmp_repo_path
    @param exclusion_list_path
    @param public_repo_path
    @param filterbranch_ok_path
    @param keep_temp_repo
    @returns: shell script
    """
    tmp_repo_parent_dir = os.path.dirname(tmp_repo_path)
    filterbranch_ok_path = '%s_ok' % tmp_repo_path
    lines = [
        '# %s' % os.path.basename(tmp_repo_path),
        '# This script clones a new repo after running filter-branch as a way to garbage-collect.',
        '# IMPORTANT: Run as sudo-capable user -- sudo is required to delete filtering_* annex files.',
        'cd %s ; ' % tmp_repo_path,
        'echo "`date +%Y-%m-%dT%H:%M:%S:%N` - Running git filter-branch"',
        'git filter-branch -d /dev/shm/git --index-filter "sh %s" --prune-empty HEAD && echo "OK" > %s' % (exclusion_list_path, filterbranch_ok_path),
        'if [ ! -f %s ]; then' % filterbranch_ok_path,
        '    echo "`date +%Y-%m-%dT%H:%M:%S:%N` - filter-branch error: quitting."',
        '    exit 1',
        'fi',
        'cd %s ; ' % tmp_repo_parent_dir,
        'echo "`date +%%Y-%%m-%%dT%%H:%%M:%%S:%%N` - Cloning %s to final: %s"' % (tmp_repo_path, public_repo_path),
        'git clone --no-hardlinks %s %s' % (tmp_repo_path, public_repo_path),
        'cd %s ; ' % public_repo_path,
        'echo "`date +%Y-%m-%dT%H:%M:%S:%N` - Initializing annex"',
        'git annex init',
        'echo "`date +%Y-%m-%dT%H:%M:%S:%N` - Getting public files..."',
        'git annex get .',
        'echo "`date +%%Y-%%m-%%dT%%H:%%M:%%S:%%N` - Removing filter remote from %s"' % public_repo_path,
        'git remote rm origin',
        'echo "`date +%%Y-%%m-%%dT%%H:%%M:%%S:%%N` - Remotes for %s"' % public_repo_path,
        'git remote -v',
        'echo "`date +%Y-%m-%dT%H:%M:%S:%N` - Cleaning up work files"',
        'rm %s' % exclusion_list_path,
        'rm %s' % filterbranch_ok_path,
    ]
    if keep_temp_repo:
        lines = lines + [
            'echo "`date +%%Y-%%m-%%dT%%H:%%M:%%S:%%N` - Keeping temp repo %s"' % tmp_repo_path,
        ]
    else:
        lines = lines + [
            'sudo rm -Rf %s' % tmp_repo_path,
        ]
    lines = lines + [
        'echo "`date +%Y-%m-%dT%H:%M:%S:%N` - DONE"',
        '',
        ]
    cmd = '\n'.join(lines)
    return cmd

def _run_filterbranch( repo_path, exclusion_list_path ):
    """(DEPRECATED) Run git filter-branch in a subprocess
    
    This was part of an attempt to do the filter-branch operation in a Python subprocess.  I kept getting obscure Git errors so I used the shell script instead.  This is kept for historical interest.
    
    @param repo_path: Absolute path to repository.
    @param exclusion_list_path: Absolute path to exclusion list file.
    """
    click.echo(repo_path)
    os.chdir(repo_path)
    cmd = 'cd %s/ ; git filter-branch -d /dev/shm/git --prune-empty --index-filter "sh %s" HEAD' % (repo_path, exclusion_list_path)
    click.echo(cmd)
    r = envoy.run(cmd, timeout=60)
    envoy.run(cmd, timeout=60)
    #click.echo('r.status_code: %s' % r.status_code)
    #click.echo('r.std_out: %s' % r.std_out)
    #click.echo('r.std_err: %s' % r.std_err)
    assert False

def _clone_public( tmp_repo_path, public_repo_path ):
    """(DEPRECATED) Clones TEMP repository into PUBLIC.
    
    Part of the attempt to do the filter-branch operation in a Python subprocess.
    
    @param tmp_repo_path: Absolute path to temporary repo.
    @param public_repo_path: Absolute path to final public repo.
    @returns public_repo: The final public repo (GitPython Repository)
    """
    public_repo = git.Repo.clone_from(tmp_repo_path, public_repo_path)
    # git annex init if not already existing
    if not os.path.exists(os.path.join(dest_path, '.git', 'annex')):
        public_repo.git.annex('init')
    public_repo.git.checkout('master')
    return public_repo


def main(mklist, keeptmp, master, mezzanine, access, destdir, source):
    
    # check args
    if (not destdir) and (not mklist):
        click.echo('ddr-filter: error: -d/--destdir is required except when using -l/--mklist.')
        sys.exit(1)
    if destdir == source:
        click.echo('ddr-filter: error: Source and destdir are the same!')
        sys.exit(1)
    
    # make list of files to exclude
    exclusion_list = make_exclusion_list(source)
    
    if mklist:
        # print list info to console and exit
        if 'COLLECTION' in exclusion_list:
            click.echo('Collection is private or incomplete.')
            sys.exit(1)
        click.echo('The following files will be excluded from the public repository:')
        for p in exclusion_list:
            click.echo('    %s' % p)
        sys.exit(1)
        
    else:
        FILTER_REPO_PATH = mk_filter_repo_path(source, destdir, 'FILTER')
        PUBLIC_REPO_PATH = FILTER_REPO_PATH.replace('FILTER', 'PUBLIC')
        LOG = '%s.log' % FILTER_REPO_PATH
        
        if os.path.exists(FILTER_REPO_PATH):
            click.echo('ddr-filter: error: Destination path already exists! (%s)' % FILTER_REPO_PATH)
            sys.exit(1)
        if os.path.exists(PUBLIC_REPO_PATH):
            click.echo('ddr-filter: error: Destination path already exists! (%s)' % PUBLIC_REPO_PATH)
            sys.exit(1)
        
        # print exclusion list and write to file
        if 'COLLECTION' in exclusion_list:
            logprint(LOG, 'Collection is private or incomplete.')
            sys.exit(1)
        logprint_nots(LOG, '    The following files will be excluded from the public repository:')
        for p in exclusion_list:
            logprint_nots(LOG, '    %s' % p)
        exclusion_list_path = write_exclusion_list(
            FILTER_REPO_PATH, exclusion_list, 'FILTER_%s_exclusions' % os.path.basename(source))
        if os.path.exists(exclusion_list_path):
            logprint(LOG, 'Wrote exclusion list %s' % exclusion_list_path)
        else:
            logprint(LOG, 'ddr-filter: error: Failed to write exclusion list.')
            sys.exit(1)
        
        started = datetime.now()
        logprint(LOG, 'Starting filter')
        
        # clone the temporary filter repo
        logprint(LOG, 'Cloning %s to %s' % (source, FILTER_REPO_PATH))
        repo = clone(source, FILTER_REPO_PATH)
        if not repo:
            logprint(LOG, 'ddr-filter: error: Could not clone repository!')
            sys.exit(1)
        
        # git annex get the files
        file_levels = []
        if access: file_levels.append('access')
        if mezzanine: file_levels.append('mezzanine')
        if master: file_levels.append('master')
        for level in file_levels:
            logprint(LOG, 'Getting %s files...' % (level))
            files = annex_list(FILTER_REPO_PATH, level)
            fstarted = datetime.now()
            for n,fname in enumerate(files):
                annex_get(repo, fname)
                logprint(LOG, '%s/%s %s' % (n+1, len(files), fname))
            ffinished = datetime.now()
            felapsed = ffinished - fstarted
            logprint(LOG, '%s elapsed' % (felapsed))
        
        # remove remotes from temporary filter repo
        logprint(LOG, 'Removing remotes...')
        rm_remotes_status = rm_remotes(repo)
        if rm_remotes_status:
            logprint(LOG, 'ddr-filter: error: Failed to remove one or more remotes: %s' % rm_remotes_status)
            sys.exit(1)
        
        # print git-filter-branch to shell script
        FILTERBRANCH_SH_PATH = os.path.join(os.path.dirname(FILTER_REPO_PATH), '%s.sh' % os.path.basename(FILTER_REPO_PATH))
        sh = mk_filterbranch_script(FILTER_REPO_PATH, exclusion_list_path, PUBLIC_REPO_PATH, keep_temp_repo=keeptmp)
        fileio.write_text(sh, FILTERBRANCH_SH_PATH)
        click.echo('Run this command:\n\n    sh %s | tee -a %s\n' % (FILTERBRANCH_SH_PATH, LOG))
        
        ## run filter-branch in a subprocess
        #run_filterbranch(FILTER_REPO_PATH, exclusion_list_path)
        ## 
        #public_repo = clone_public(FILTER_REPO_PATH, PUBLIC_REPO_PATH)
        
        # clean up
        finished = datetime.now()
        elapsed = finished - started
        logprint(LOG, 'Done with preparations')
        logprint_nots(LOG, '%s elapsed' % elapsed)
        click.echo('')
