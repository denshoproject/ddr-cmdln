# git and git-annex code

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import os
import re
import socket

from dateutil import parser
import envoy
import git
import requests
import simplejson as json

from DDR import config
from DDR import fileio
from DDR import storage

# values are set after defining latest_commit()
APP_COMMITS = {}


def repository(path, user_name=None, user_mail=None):
    """
    @param collection_path: Absolute path to collection repo.
    @param user_name: str
    @param user_mail: str
    @return: GitPython repo object
    """
    repo = git.Repo(path, search_parent_directories=True)
    if user_name and user_mail:
        git_set_configs(repo, user_name, user_mail)
        annex_set_configs(repo, user_name, user_mail)
        return repo
    return repo

def initialize_repository(path, user_name=None, user_mail=None):
    """Runs Git and git-annex init and checks out master
    
    @param collection_path: Absolute path to collection repo.
    @param user_name: str
    @param user_mail: str
    @return: GitPython repo object
    """
    logging.debug('initialize_repository(%s, %s, %s)' % (
        path, user_name, user_mail
    ))
    if not os.path.exists(path):
        os.makedirs(path)
    # initialize
    repo = git.Repo.init(path)
    # create master
    # we have to write a file before we can checkout master, because ???
    # This function just creates an empty file ...
    tmpfile = os.path.join(path, 'README')
    msg = '%s - created %s\n' % (os.path.basename(path), datetime.now())
    with open(tmpfile, 'w') as f:
        f.write(msg)
    repo.index.add([tmpfile])
    repo.index.commit('initial commit')
    repo.git.annex('init')
    #repo.git.checkout('master')
    if user_name and user_mail:
        git_set_configs(repo, user_name, user_mail)
        annex_set_configs(repo, user_name, user_mail)
    return repo
    

# git info -------------------------------------------------------------

def git_version(repo):
    """Returns Git version info.
    
    @param repo: A GitPython Repo object.
    @returns string
    """
    return envoy.run('git --version').std_out.strip()

def repo_status(repo, short=False):
    """Retrieve git status on repository.
    
    @param repo: A GitPython Repo object
    @return: message ('ok' if successful)
    """
    status = 'unknown'
    if short:
        status = repo.git.status(short=True, branch=True)
    else:
        status = repo.git.status()
    #logging.debug('\n{}'.format(status))
    return status

def latest_commit(path):
    """Returns latest commit for the specified repository
    
    TODO pass repo object instead of path
    
    One of several arguments must be provided:
    - Absolute path to a repository.
    - Absolute path to file within a repository. In this case the log
      will be specific to the file.
    
    >>> path = '/path/to/repo'
    >>> latest_commit(path=path)
    'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2 (HEAD, master) 1970-01-01 00:00:00 -0000'
    
    @param path: Absolute path to repo or file within.
    """
    try:
        repo = git.Repo(path, search_parent_directories=True)
    except git.InvalidGitRepositoryError:
        return 'Invalid Git repository: "%s"' % path
    if os.path.isfile(path):
        return repo.git.log('--pretty=format:%H %d %ad', '--date=iso', '-1', path)
    else:
        return repo.git.log('--pretty=format:%H %d %ad', '--date=iso', '-1')
    return None

# Latest commits for ddr-cmdln and ddr-local.
# Include here in settings so only has to be retrieved once,
# and so commits are visible in error pages and in page footers.
APP_COMMITS = {
    'cmd': latest_commit(config.INSTALL_PATH),
    'def': latest_commit(config.REPO_MODELS_PATH),
}

def earliest_commit(path, parsed=False):
    """Returns earliest commit for the specified repository/path
    
    TODO pass repo object instead of path
    
    One of several arguments must be provided:
    - Absolute path to a repository.
    - Absolute path to file within a repository. In this case the log
      will be specific to the file.
    
    >>> path = '/path/to/repo'
    >>> earliest_commit(path=path)
    'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2 (HEAD, master) 1970-01-01 00:00:00 -0000'
    
    @param path: Absolute path to repo or file within.
    @param parsed: boolean
    @return: str or dict {'commit', 'branch', 'ts'}
    """
    if parsed:
        fmt = '{"commit":"%H","branch":"%d","ts":"%ad"}'
    else:
        fmt = "%H %d %ad"
    repo = git.Repo(path, search_parent_directories=True)
    if os.path.isfile(path):
        text = repo.git.log('--pretty=format:%s' % fmt, '--date=iso', path).splitlines()[-1]
    else:
        text = repo.git.log('--pretty=format:%s' % fmt, '--date=iso').splitlines()[-1]
    if parsed:
        data = json.loads(text)
        data['ts'] = parser.parse(data['ts'])
        return data
    return text

def _parse_cmp_commits(gitlog, a, b):
    """
    If abbrev == True:
        git log --pretty=%h
    else:
        git log --pretty=%H
    
    @param gitlog: str
    @param a: str Commit A
    @param b: str Commit B
    @returns: dict See DDR.dvcs.cmp_commits
    """
    result = {
        'a': a,
        'b': b,
        'op': None,
    }
    commits = gitlog.strip().split('\n')
    commits.reverse()
    if   a not in commits: result['op'] = 'a!' # App source was on unmerged
    elif b not in commits: result['op'] = 'b!' # branch when doc committed.
    elif commits.index(a) < commits.index(b): result['op'] = 'lt'
    elif commits.index(a) == commits.index(b): result['op'] = 'eq'
    elif commits.index(a) > commits.index(b): result['op'] = 'gt'
    else: result['op'] = '--'
    return result

def cmp_commits(repo, a, b, abbrev=False):
    """Indicates how two commits are related (newer,older,equal)
    
    Both commits must be in the same branch of the same repo.
    Git log normally lists commits in reverse chronological order.
    This function uses the words "before" and "after" in the normal sense:
    If commit B comes "before" commit A it means that B occurred at an
    earlier datetime than A.
    Raises Exception if can't find both commits.
    
    Returns a dict
    {
        'a': left-hand object,
        'b': right-hand object,
        'op': operation ('lt', 'eq', 'gt', '--')
    }
    
    @param repo: A GitPython Repo object.
    @param a: str A commit hash.
    @param b: str A commit hash.
    @param abbrev: Boolean If True use abbreviated commit hash.
    @returns: dict See above.
    """
    if abbrev:
        fmt = '--pretty=%h'
    else:
        fmt = '--pretty=%H'
    return _parse_cmp_commits(repo.git.log(fmt), a, b)

# git diff

def _parse_list_untracked( text='' ):
    """Parses output of "git status --short".
    """
    return [
        line.split(' ')[1]
        for line in text.strip().split('\n')[1:]
        if ('??' in line)
    ]

def list_untracked(repo):
    """Returns list of untracked files
    
    Works for git-annex files just like for regular files.
    
    @param repo: A Gitpython Repo object
    @return: List of filenames
    """
    stdout = repo_status(repo, short=True)
    return _parse_list_untracked(stdout)

def _parse_list_modified( diff ):
    """Parses output of "git stage --name-only".
    """
    paths = []
    if diff:
        paths = diff.strip().split('\n')
    return paths
    
def list_modified(repo):
    """Returns list of currently modified files
    
    Works for git-annex files just like for regular files.
    
    @param repo: A Gitpython Repo object
    @return: List of filenames
    """
    stdout = repo.git.diff('--name-only')
    return _parse_list_modified(stdout)

def _parse_list_staged( diff ):
    """Parses output of "git stage --name-only --cached".
    """
    staged = []
    if diff:
        staged = diff.strip().split('\n')
    return staged
    
def list_staged(repo):
    """Returns list of currently staged files
    
    Works for git-annex files just like for regular files.
    
    @param repo: A Gitpython Repo object
    @return: List of filenames
    """
    stdout = repo.git.diff('--cached', '--name-only')
    return _parse_list_staged(stdout)

def _parse_list_committed( entry ):
    entrylines = [line for line in entry.split('\n') if '|' in line]
    files = [line.split('|')[0].strip() for line in entrylines]
    return files
    
def list_committed(repo, commit):
    """Returns list of all files in the commit

    $ git log -1 --stat 0a1b2c3d4e...|grep \|

    @param repo: A Gitpython Repo object
    @param commit: A Gitpython Commit object
    @return: list of filenames
    """
    # return just the files from the specific commit's log entry
    entry = repo.git.log('-1', '--stat', commit.hexsha)
    return _parse_list_committed(entry)

def _parse_list_conflicted( ls_unmerged ):
    files = []
    for line in ls_unmerged.strip().split('\n'):
        if line:
            f = line.split('\t')[1]
            if f not in files:
                files.append(f)
    return files
    
def list_conflicted(repo):
    """Returns list of unmerged files in path; presence of files indicates merge conflict.
    
    @param repo: A Gitpython Repo object
    @return: List of filenames
    """
    stdout = repo.git.ls_files('--unmerged')
    return _parse_list_conflicted(stdout)


# git state ------------------------------------------------------------

"""
Indicators for SYNCED,AHEAD,BEHIND,DIVERGED are found in the FIRST LINE
of "git status --short --branch".

SYNCED
    ## master
    
    ## master
    ?? unknown-file.ext
    
    ## master
    ?? .gitstatus
    ?? files/ddr-testing-233-1/addfile.log

AHEAD
    ## master...origin/master [ahead 1]
    
    ## master...origin/master [ahead 2]
    M  collection.json

BEHIND
    ## master...origin/master [behind 1]
    
    ## master...origin/master [behind 2]
    M  collection.json

DIVERGED
    ## master...origin/master [ahead 1, behind 2]
    
    ## master...origin/master [ahead 1, behind 2]
    M  collection.json

Indicators for CONFLICTED,PARTIAL_RESOLVED,RESOLVED are found
AFTER the first line of "git status --short --branch".

CONFLICTED
    ## master...origin/master [ahead 1, behind 2]
    UU changelog
    UU collection.json

PARTIAL_RESOLVED
    ## master...origin/master [ahead 1, behind 2]
    M  changelog
    UU collection.json

RESOLVED
    ## master...origin/master [ahead 1, behind 2]
    M  changelog
    M  collection.json
"""

def _compile_patterns(patterns):
    """Compile regex patterns only once, at import.
    """
    new = []
    for p in patterns:
        pattern = [x for x in p]
        pattern[0] = re.compile(p[0])
        new.append(pattern)
    return new

GIT_STATE_PATTERNS = _compile_patterns((
    (r'^## master',                 'synced'),
    (r'^## master...origin/master', 'synced'),
    (r'(ahead [0-9]+)',              'ahead'),
    (r'(behind [0-9]+)',            'behind'),
    (r'(\nM )',                   'modified'),
    (r'(\nUU )',                'conflicted'),
))

def repo_states(git_status, patterns=GIT_STATE_PATTERNS):
    """Returns list of states the repo may have
    
    @param text: str
    @param patterns: list of (regex, name) tuples
    @returns: list of states
    """
    states = []
    for pattern,name in patterns:
        m = re.search(pattern, git_status)
        if m and (name not in states):
            states.append(name)
    if ('ahead' in states) or ('behind' in states):
        states.remove('synced')
    return states

def synced(status, states=None):
    """Indicates whether repo is synced with remote repo.
    
    @param status: Output of "git status --short --branch"
    @returns: boolean
    """
    if not states:
        states = repo_states(status)
    return ('synced' in states) and ('ahead' not in states) and ('behind' not in states)

def ahead(status, states=None):
    """Indicates whether repo is ahead of remote repos.
    
    @param status: Output of "git status --short --branch"
    @returns: boolean
    """
    if not states:
        states = repo_states(status)
    return ('ahead' in states) and not ('behind' in states)

def behind(status, states=None):
    """Indicates whether repo is behind remote repos.

    @param status: Output of "git status --short --branch"
    @returns: boolean
    """
    if not states:
        states = repo_states(status)
    return ('behind' in states) and not ('ahead' in states)

def diverged(status, states=None):
    """
    @param status: Output of "git status --short --branch"
    @returns: boolean
    """
    if not states:
        states = repo_states(status)
    return ('ahead' in states) and ('behind' in states)

def conflicted(status, states=None):
    """Indicates whether repo has a merge conflict.
    
    NOTE: Use list_conflicted if you have a repo object.
    
    @param status: Output of "git status --short --branch"
    @returns: boolean
    """
    if not states:
        states = repo_states(status)
    return 'conflicted' in states


# git operations -------------------------------------------------------

def git_set_configs(repo, user_name=None, user_mail=None):
    if user_name and user_mail:
        repo.git.config('user.name', user_name)
        repo.git.config('user.email', user_mail)
        # we're not actually using gitweb any more...
        repo.git.config('gitweb.owner', '{} <{}>'.format(user_name, user_mail))
    # ignore file permissions
    repo.git.config('core.fileMode', 'false')
    return repo

def compose_commit_message(title, body='', agent=''):
    """Composes a Git commit message.
    
    TODO wrap body text at 72 chars
    
    @param title: (required) 50 chars or less
    @param body: (optional) Freeform body text.
    @param agent: (optional) Do not include the word 'agent'.
    """
    # force to str
    if not body: body = ''
    if not agent: agent = ''
    # formatting
    if body:  body = '\n\n%s' % body
    if agent: agent = '\n\n@agent: %s' % agent
    return '%s%s%s' % (title, body, agent)

def fetch(repo):
    """run git fetch; fetches from origin.
    
    @param repo: A GitPython Repo object
    @return: message ('ok' if successful)
    """
    return repo.git.fetch()

def stage(repo, git_files=[]):
    """Stage some files; DON'T USE FOR git-annex FILES!
    
    @param repo: A GitPython repository
    @param git_files: list of file paths, relative to repo bas
    """
    repo.git.add([git_files])

def commit(repo, msg, agent):
    """Commit some changes.
    
    @param repo: A GitPython repository
    @param msg: str Commit message
    @param agent: str
    @returns: GitPython commit object
    """
    # TODO cancel commit if list of staged doesn't match list of files added?
    # TODO complain if list of committed files doesn't match list of staged?
    # log staged files
    staged = list_staged(repo)
    staged.sort()
    logging.debug('STAGED {}'.format(staged))
    # do the commit
    commit_message = compose_commit_message(msg, agent=agent)
    commit = repo.index.commit(commit_message)
    logging.debug('COMMIT {}'.format(commit))
    # log committed files
    committed = list_committed(repo, commit)
    committed.sort()
    logging.debug('COMMITTED {}'.format(committed))
    # done
    return commit

def reset(repo):
    """Resets all staged files in repo."""
    return repo.git.reset('HEAD')

def revert(repo):
    """Reverts all modified files in repo."""
    return repo.git.checkout('--', '.')

def remove_untracked(repo):
    """Deletes all untracked files in the repo."""
    out = []
    untracked_paths = [
        os.path.join(repo.working_dir, p)
        for p in list_untracked(repo)
    ]
    for untracked in untracked_paths:
        try:
            os.remove(untracked)
            out.append('OK %s' % untracked)
        except:
            out.append('FAIL %s' % untracked)
    return out


# git merge ------------------------------------------------------------

MERGE_MARKER_START = '<<<<<<<'
MERGE_MARKER_MID   = '======='
MERGE_MARKER_END   = '>>>>>>>'

def load_conflicted_json(text):
    """Reads DDR JSON file, extracts conflicting fields; arranges in left-right pairs.
    
    Takes JSON like this:
        ...
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
        ...

    Outputs like this:
        ...
        {u'record_created': u'2013-09-30T12:43:11'}
        {u'record_lastmod': {'right': u'2013-10-02T12:59:30', 'left': u'2013-10-02T12:59:30'}}
        {u'status': u'completed'}
        ...
    """
    def make_dict(line):
        """
        Sample inputs:
            '    "application": "https://github.com/densho/ddr-local.git",'
            '    "release": "0.20130711"'
        Sample outputs:
            {"application": "https://github.com/densho/ddr-local.git"}
            {"release": "0.20130711"}
        """
        txt = line.strip()
        if txt[-1] == ',':
            txt = txt[:-1]
        txt = '{%s}' % txt
        return json.loads(txt)
    fieldlines = []
    l = ' '; r = ' '
    for line in text.split('\n'):
        KEYVAL_SEP = '": "'  # only keep lines with keyval pairs
        mrk = ' ';  sep = ' '
        if MERGE_MARKER_START in line: mrk='M'; l='L'; r=' ' # <<<<<<<<
        elif MERGE_MARKER_MID in line: mrk='M'; l=' '; r='R' # ========
        elif MERGE_MARKER_END in line: mrk='M'; l=' '; r=' ' # >>>>>>>>
        elif KEYVAL_SEP in line: sep='S'               # normal field
        flags = '%s%s%s%s' % (sep, mrk, l, r)
        fieldlines.append((flags, line))
    fields = []
    for flags,line in fieldlines:
        if   flags == 'S   ': fields.append(make_dict(line)) # normal field
        elif flags == ' ML ': left = []; right = []          # <<<<<<<<
        elif flags == 'S L ': left.append(make_dict(line))   # left
        elif flags == 'S  R': right.append(make_dict(line))  # right
        elif flags == ' M  ':                                # >>>>>>>>
            if len(left) == len(right):
                for n in range(0, len(left)):
                    key = left[n].keys()[0]
                    val = {'left': left[n].values()[0],
                           'right': right[n].values()[0],}
                    fields.append( {key:val} )
    return fields

def automerge_conflicted(text, which='left'):
    """Automatically accept left or right conflicted changes in a file.
    
    Works on any kind of file.
    Does not actually understand the file contents!
    
    Used for files like ead.xml, mets.xml that are autogenerated
    We'll just accept whatever change and then it'll get fixed
    next time the file is edited.
    These really shouldn't be in Git anyway...
    """
    lines = []
    l = 0; r = 0
    for line in text.split('\n'):
        marker = 0
        if MERGE_MARKER_START in line: l = 1; r = 0; marker = 1
        elif MERGE_MARKER_MID in line: l = 0; r = 1; marker = 1
        elif MERGE_MARKER_END in line: l = 0; r = 0; marker = 1
        flags = '%s%s%s' % (l, r, marker)
        add = 0
        if ( flags == '000'): add = 1
        if ((flags == '100') and (which == 'left')): add = 1
        if ((flags == '010') and (which == 'right')): add = 1
        if add:
            lines.append(line)
    return '\n'.join(lines)

def merge_add( repo, file_path_rel ):
    """Adds file unless contains conflict markers
    """
    # check for merge conflict markers
    file_path_abs = os.path.join(repo.working_dir, file_path_rel)
    txt = fileio.read_text(file_path_abs)
    if (MERGE_MARKER_START in txt) or (MERGE_MARKER_MID in txt) or (MERGE_MARKER_END in txt):
        return 'ERROR: file still contains merge conflict markers'
    repo.git.add(file_path_rel)
    return 'ok'

def merge_commit( repo ):
    """Performs the final commit on a merge.
    
    Assumes files have already been added; quits if it finds unmerged files.
    """
    unmerged = list_conflicted(repo)
    if unmerged:
        return 'ERROR: unmerged files exist!'
    commit = repo.git.commit('--message', 'merge conflicts resolved using DDR web UI.')

def diverge_commit( repo ):
    """Performs the final commit on diverged repo.
    
    Assumes files have already been added; quits if it finds unmerged files.
    """
    unmerged = list_conflicted(repo)
    if unmerged:
        return 'ERROR: unmerged files exist!'
    commit = repo.git.commit('--message', 'divergent commits resolved using DDR web UI.')


# git inventory --------------------------------------------------------

def repos(path):
    """Lists all the repositories in the path directory.
    Duplicate of collections list?
    
    >>> from DDR import dvcs
    >>> p = '/media/WD5000BMV-2/ddr'
    >>> dvcs.repos(p)
    ['/media/WD5000BMV-2/ddr/ddr-testing-130', '/media/WD5000BMV-2/ddr/ddr-testing-131', ...]
    """
    repos = []
    for d in os.listdir(path):
        dpath = os.path.join(path, d)
        if os.path.isdir(dpath) and ('.git' in os.listdir(dpath)):
            repos.append(dpath)
    return repos

def is_local(url):
    """Indicates whether or not the git URL is local.
    
    Currently very crude: just checks if there's an ampersand and a colon.
    
    @returns 1 (is local), 0 (not local), or -1 (unknown)
    """
    if url:
        if ('@' in url) and (':' in url):
            return 0 # remote
        return 1     # local
    return -1        # unknown

def local_exists(path):
    """Indicates whether a local remote can be found in the filesystem.
    """
    if os.path.exists(path):
        return 1
    return 0

def is_clone(path1, path2, n=5):
    """Indicates whether two repos at the specified paths are clones of each other.
    
    Compares the first N hashes
    TODO What if repo has less than N commits?
    
    @param path1
    @param path2
    @param n
    @returns 1 (is a clone), 0 (not a clone), or -1 (unknown)
    """
    if is_local(path2):
        def get(path):
            try:
                repo = git.Repo(path, search_parent_directories=True)
            except:
                repo = None
            if repo:
                log = repo.git.log('--reverse', '-%s' % n, pretty="format:'%H'").split('\n')
                if log and (type(log) == type([])):
                    return log
            return None
        log1 = get(path1)
        log2 = get(path2)
        if log1 and log2:
            if (log1 == log2):
                return 1
            else:
                return 0
    return -1

def remotes(repo, paths=None, clone_log_n=1):
    """Lists remotes for the repository at path.
    
    For each remote lists info you'd find in REPO/.git/config plus a bit more:
    - name
    - url
    - annex-uuid
    - fetch
    - push
    - local or remote
    - if local, whether the remote is a clone
    
    $ git remote -v
    memex	gjost@memex:~/music (fetch)
    memex	gjost@memex:~/music (push)
    origin	/media/WD5000BMV-2/music/ (fetch)
    origin	/media/WD5000BMV-2/music/ (push)
    seagate596-2010	gjost@memex:/media/seagate596-2010/Music (fetch)
    seagate596-2010	gjost@memex:/media/seagate596-2010/Music (push)
    serenity	gjost@jostwebwerks.com:~/git/music.git (fetch)
    serenity	gjost@jostwebwerks.com:~/git/music.git (push)
    wd5000bmv-2	/media/WD5000BMV-2/music/ (fetch)
    wd5000bmv-2	/media/WD5000BMV-2/music/ (push)
    
    >>> import git
    >>> repo = git.Repo(path, search_parent_directories=True)
    >>> remotes(repo)
    [<git.Remote "origin">, <git.Remote "serenity">, <git.Remote "wd5000bmv-2">, <git.Remote "memex">, <git.Remote "seagate596-2010">]
    >>> cr = repo.config_reader()
    # normal remote
    >>> cr.items('remote "serenity"')
[('url', 'gjost@jostwebwerks.com:~/git/music.git'), ('fetch', '+refs/heads/*:refs/remotes/serenity/*'), ('annex-uuid', 'e7e4c020-9335-11e2-8184-835f755b29c5')],
    # git-annex special remote
    >>> cr.items('remote "hq-backup-gold-test1"')
[('name', 'hq-backup-gold-test1')), ('annex-uuid', '2c6fc912-3ba2-4df2-a741-8021cc66d918'), ('annex-rsyncurl', '10.0.1.10:/densho_backup/hq-backup-gold/ddr-testing-268')]
    
    @param repo: A GitPython Repo object
    @param paths: 
    @param clone_log_n: 
    @returns: list of remotes
    """
    reader = repo.config_reader()
    reader.sections()  # reader.items() works better if you do this first
    remotes = []
    for remote in repo.remotes:
        section_name = 'remote "%s"' % remote.name
        # next line must be present or this doesn't work - WTF???
        section_exists = section_name in reader.sections()
        section_items = reader.items(section_name)
        r = {key: val for key,val in section_items}
        r['name'] = remote.name
        # handle regular remotes and git-annex special remotes
        if r.get('url'):
            r['target'] = r['url']
        elif r.get('annex-rsyncurl'):
            r['target'] = r['annex-rsyncurl']
        else:
            r['target'] = ''
        #
        r['local'] = is_local(r['target'])
        r['local_exists'] = local_exists(r['target'])
        r['clone'] = is_clone(repo.working_dir, r['target'], clone_log_n)
        remotes.append(r)
    return remotes

def remote_add(repo, url, name=config.GIT_REMOTE_NAME):
    """Add the specified remote name unless it already exists
    
    @param repo: GitPython Repository
    @param url: str Git remote URL
    @param name: str remote name
    """
    if name in [r.name for r in repo.remotes]:
        logging.debug('remote exists: %s %s %s' % (repo, name, url))
    else:
        logging.debug('remote_add(%s, %s %s)' % (repo, name, url))
        repo.create_remote(name, url)
        logging.debug('ok')

def repos_remotes(repo):
    """Gets list of remotes for each repo in path.
    
    @param repo: A GitPython Repo object
    @returns list of dicts {'path':..., 'remotes':[...]}
    """
    return [{'path':p, 'remotes':remotes(p),} for p in repos(repo.working_dir)]


# annex ----------------------------------------------------------------

def annex_set_configs(repo, user_name=None, user_mail=None):
    # earlier versions of git-annex have problems with ssh caching on NTFS
    repo.git.config('annex.sshcaching', 'false')
    return repo

def annex_parse_version(text):
    """Takes output of "git annex version" and returns dict
    
    ANNEX_3_VERSION
    git-annex version: 3.20120629
    local repository version: 3
    default repository version: 3
    supported repository versions: 3
    upgrade supported from repository versions: 0 1 2
     
    ANNEX_5_VERSION
    git-annex version: 5.20141024~bpo70+1
    build flags: Assistant Webapp Pairing S3 Inotify XMPP Feeds Quvi ...
    key/value backends: SHA256E SHA1E SHA512E SHA224E SHA384E SHA256 ...
    remote types: git gcrypt S3 bup directory rsync web tahoe glacier...
    local repository version: 5
    supported repository version: 5
    upgrade supported from repository versions: 0 1 2 4
    
    @param text: str
    @returns: dict
    """
    lines = text.strip().split('\n')
    data = {
        line.split(': ')[0]: line.split(': ')[1]
        for line in lines
    }
    UPDATED_FIELDNAMES = [
        ('supported repository versions', 'supported repository version'),
    ]
    for old,new in UPDATED_FIELDNAMES:
        if old in data.iterkeys():
            data[new] = data.pop(old)
    # add major version
    data['major version'] = data['git-annex version'].split('.')[0]
    return data

def annex_version(repo, verbose=False):
    """Returns git-annex version only, excludes repository version info.
    
    If verbose, returns all info including version of local repo's annex.
    example:
    'git version 1.7.10.4; git-annex version: 3.20120629; local repository version: 3; ' \
    'default repository version: 3; supported repository versions: 3; ' \
    'upgrade supported from repository versions: 0 1 2'
    
    @param repo: A GitPython Repo object.
    @param verbose: boolean
    @returns string
    """
    if verbose:
        return repo.git.annex('version')
    return 'git-annex version: {}'.format(repo.git.annex('version', '--raw'))

def _annex_parse_description(annex_status, uuid):
    for key in annex_status.iterkeys():
        if 'repositories' in key:
            for r in annex_status[key]:
                if (r['uuid'] == uuid) and r['here']:
                    return r['description']
    return None
    
def annex_get_description(repo, annex_status):
    """Get description of the current repo, if any.
    
    @param repo: A GitPython Repo object
    @param annex_status: dict Output of dvcs.annex_status.
    @return String description or None
    """
    # TODO 
    try:
        uuid = repo.config_reader().get('annex','uuid')
    except:
        uuid = 'unknown-annex-uuid'
        logging.error('UNKNOWN ANNEX UUID')
    return _annex_parse_description(annex_status, uuid)

def _annex_make_description( drive_label=None, hostname=None, partner_host=None, mail=None ):
    description = None
    if drive_label:
        description = drive_label
    elif hostname and (hostname == partner_host) and mail:
        description = ':'.join([ hostname, mail.split('@')[1] ])
    elif hostname and (hostname != partner_host):
        description = hostname
    return description

def annex_set_description( repo, annex_status, description=None, drive_label=None, hostname=None, force=False ):
    """Sets repo's git annex description if not already set.

    NOTE: This needs to run git annex status, which takes some time.
     
    New repo: git annex init "REPONAME"
    Existing repo: git annex describe here "REPONAME"
     
    Descriptions should be chosen/generated base on the following heuristic:
    - Input to description argument of function.
    - If on USB device, the drive label of the device.
    - Hostname of machine, unless it is pnr (used by partner VMs).
    - If hostname is pnr, pnr:DOMAIN where DOMAIN is the domain portion of the git config user.email
    
    @param repo: A GitPython Repo object
    @param annex_status: dict Output of dvcs.annex_status.
    @param description: Manually supply a new description.
    @param drive_label: str Required if description is blank!
    @param hostname: str Required if description is blank!
    @param force: Boolean Apply a new description even if one already exists.
    @return String description if new one was created/applied or None
    """
    desc = None
    PARTNER_HOSTNAME = 'pnr'
    annex_description = annex_get_description(repo, annex_status)
    # keep existing description unless forced
    if (not annex_description) or (force == True):
        if description:
            desc = description
        else:
            # gather information
            user_mail = repo.config_reader().get('user','email')
            # generate description
            desc = _annex_make_description(
                drive_label=drive_label,
                hostname=hostname, partner_host=PARTNER_HOSTNAME,
                mail=user_mail)
        if desc:
            # apply description
            logging.debug('git annex describe here %s' % desc)
            repo.git.annex('describe', 'here', desc)
    return desc

def annex_status(repo):
    """Retrieve git annex status on repository.
    
    @param repo: A GitPython Repo object
    @return: dict
    """
    version_data = annex_parse_version(annex_version(repo))
    text = None
    major_version = version_data['major version']
    if major_version:
        if isinstance(major_version, int):
            pass
        elif isinstance(major_version, basestring) and major_version.isdigit():
            major_version = int(major_version)
        else:
            raise Exception(
                'Cannot parse git-annex version: "%s" (%s)' % (major_version, type(major_version))
            )
    if major_version == 3:
        text = repo.git.annex('status', '--json')
    elif major_version >= 5:
        text = repo.git.annex('info', '--json')
    if text:
        data = json.loads(text)
        data['git-annex version'] = version_data['git-annex version']
        return data
    return None

def annex_info(repo):
    """
    """
    data = json.loads(repo.git.annex('info', '--fast', '--json'))
    data['timestamp'] = datetime.now()
    all_repos = data['trusted repositories'] \
                + data['untrusted repositories'] \
                + data['semitrusted repositories']
    # might be more than one 'here'?
    data['here'] = [r['uuid'] for r in all_repos if r['here']]
    return data

def annex_whereis_file(repo, file_path_rel, info=None):
    """Show remotes that the file appears in
    
    $ git annex whereis files/ddr-testing-201303051120-1/files/20121205.jpg
    whereis files/ddr-testing-201303051120-1/files/20121205.jpg (2 copies)
            0bbf5638-85c9-11e2-aefc-3f0e9a230915 -- workbench
            c1b41078-85c9-11e2-bad2-17e365f14d89 -- here
    ok
    
    {
        u'command': u'whereis',
        u'file': u'files/ddr-njpa-4-1/files/ddr-njpa-4-1-master-0c90a8e5c7.tif',
        u'success': True,
        u'note': u'...RAW TEXT OUTPUT...',
        u'whereis': [
            {u'uuid': u'5d026e8a-f0b8-11e3-b2f2-2f3b74f26f08', u'here': False, u'description': u'qnfs'},
            ...
        ],
        u'untrusted': []
    }

    @param repo: A GitPython Repo object
    @param collection_uid: A valid DDR collection UID
    @return: dict
    """
    data = json.loads(repo.git.annex('whereis', '--json', file_path_rel))
    data['timestamp'] = datetime.now()
    # mark this repo
    if not info:
        info = annex_info(repo)
    for r in data['whereis']:
        if r['uuid'] in info['here']: r['this'] = True
        else: r['this'] = False
    for r in data['untrusted']:
        if r['uuid'] in info['here']: r['this'] = True
        else: r['this'] = False
    return data

def annex_missing_files(repo):
    """List git-annex data for binaries absent from repo
    
    @returns: list of dicts, one per missing file
    """
    return [
        json.loads(line)
        for line in repo.git.annex('find','--not','--in=here','--json').splitlines()
    ]

def annex_trim(repo, confirmed=False):
    """Drop full-size binaries from a repository.
    
    @param repo: A GitPython Repo object
    @param confirmed: boolean Yes I really want to do this
    @returns: {keep,drop,dropped} lists of file paths
    """
    logging.debug('annex_trim(%s, confirmed=%s)' % (repo, confirmed))
    # Keep access files, HTML files, and PDFs.
    KEEP_SUFFIXES = ['-a.jpg', '.htm', '.html', '.pdf']
    annex_file_paths = repo.git.annex('find').split('\n')
    keep = []
    drop = []
    for path_rel in annex_file_paths:
        if [True for suffix in KEEP_SUFFIXES if suffix.lower() in path_rel]:
            keep.append(path_rel)
        else:
            drop.append(path_rel)
    dropped = []
    for path_rel in drop:
        logging.debug(path_rel)
        if confirmed:
            p = drop.remove(path_rel)
            repo.git.annex('drop', '--force', p)
            dropped.append(p)
    return {
        'keep':keep,
        'drop':drop,
        'dropped':dropped,
    }

def annex_stage(repo, annex_files=[]):
    """Stage some files with git-annex.
    
    @param repo: A GitPython repository
    @param annex_files: list of annex file paths, relative to repo base
    """
    for path in annex_files:
        repo.git.annex('add', path)

def annex_file_targets(repo, relative=False ):
    """Lists annex file symlinks and their targets in the annex objects dir
    
    @param repo: A GitPython Repo object
    @param relative: Report paths relative to repo_dir
    @returns: list of (symlink,target)
    """
    paths = []
    excludes = ['.git', 'tmp', '*~']
    basedir = os.path.realpath(repo.working_dir)
    for root, dirs, files in os.walk(basedir):
        # don't go down into .git directory
        if '.git' in dirs:
            dirs.remove('.git')
        for f in files:
            path = os.path.join(root, f)
            if os.path.islink(path):
                if relative:
                    relpath = os.path.relpath(path, basedir)
                    reltarget = os.readlink(path)
                    paths.append((relpath, reltarget))
                else:
                    target = os.path.realpath(path)
                    paths.append((path, target))
    return paths


class Cgit():
    url = None
    
    def __init__(self, cgit_url=config.CGIT_URL):
        self.url = cgit_url
    
    def collection_title(self, repo, session, timeout=5):
        """Gets collection title from CGit
        
        Requests plain blob of collection.json, reads 'title' field.
        PROBLEM: requires knowledge of repository internals.
        
        @param repo: str Repository name
        @param session: requests.Session
        @param timeout: int
        @returns: str Repository collection title
        """
        title = '---'
        URL_TEMPLATE = '%s/cgit.cgi/%s/plain/collection.json'
        url = URL_TEMPLATE % (self.url, repo)
        logging.debug(url)
        try:
            r = session.get(url, timeout=timeout)
            logging.debug(str(r.status_code))
        except requests.ConnectionError:
            r = None
            title = '[ConnectionError]'
        data = None
        if r and r.status_code == 200:
            try:
                data = json.loads(r.text)
            except ValueError:
                title = '[no data]'
        if data:
            for field in data:
                if field and field.get('title', None) and field['title']:
                    title = field['title']
        logging.debug('%s: "%s"' % (repo,title))
        return title


class Gitolite(object):
    """Access information about Gitolite server
    
    >>> gitolite = dvcs.Gitolite(config.GITOLITE)
    >>> gitolite.initialize()
    >>> gitolite.connected
    True
    >>> gitolite.authorized
    True
    >>> gitolite.orgs()
    ['ddr-densho', 'ddr-testing']
    >>> gitolite.repos()
    ['ddr-densho-1', 'ddr-densho-2', 'ddr-densho-3']
    >>> gitolite.collection_titles(USERNAME, PASSWORD)
    [('ddr-test-1', 'A Collection'), ('ddr-test-2', 'Another Collection')]
    """
    server = None
    timeout = None
    info = None
    connected = None
    authorized = None
    initialized = None
    
    def __init__(self, server=config.GITOLITE, timeout=60):
        """
        @param server: USERNAME@DOMAIN
        @param timeout: int Maximum seconds to wait for reponse
        """
        self.server = server
        self.timeout = timeout
    
    def __repr__(self):
        status = []
        if self.info: status.append('Init')
        else: status.append('noinit')
        if self.connected: status.append('Conn')
        else: status.append('noconn')
        if self.authorized: status.append('Auth')
        else: status.append('noauth')
        return "<%s.%s %s %s>" % (
            self.__module__, self.__class__.__name__,
            self.server, ','.join(status)
        )
    
    def initialize(self):
        """Connect to Gitolite server.
        """
        cmd = 'ssh {} info'.format(self.server)
        logging.debug('        {}'.format(cmd))
        r = envoy.run(cmd, timeout=int(self.timeout))
        logging.debug('        {}'.format(r.status_code))
        self.status = r.status_code
        if self.status == 0:
            self.info = r.std_out
            self.connected = True
            self.authorized = self._authorized()
        else:
            self.connected = False
        self.initialized = True
    
    def _authorized(self):
        """Parse Gitolite server response, indicate whether user is authorized
        
        http://gitolite.com/gitolite/user.html#info
        "The only command that is always available to every user is the info command
        (run ssh git@host info -h for help), which tells you what version of gitolite
        and git are on the server, and what repositories you have access to. The list
        of repos is very useful if you have doubts about the spelling of some new repo
        that you know was setup."
        Sample output:
            hello gjost, this is git@mits running gitolite3 v3.2-19-gb9bbb78 on git 1.7.2.5
            
             R W C  ddr-densho-[0-9]+
             R W C  ddr-densho-[0-9]+-[0-9]+
             R W C  ddr-dev-[0-9]+
            ...
        
        @returns: boolean
        """
        lines = self.info.split('\n')
        if lines and len(lines) and ('this is git' in lines[0]) and ('running gitolite' in lines[0]):
            logging.debug('        OK ')
            return True
        logging.debug('        NO CONNECTION')
        return False
    
    def orgs(self):
        """Returns list of orgs to which user has access
        
        @returns: list of organization IDs
        """
        repos_orgs = []
        for line in self.info.split('\n'):
            if 'R W C' in line:
                parts = line.replace('R W C', '').strip().split('-')
                repo_org = '-'.join([parts[0], parts[1]])
                if repo_org not in repos_orgs:
                    repos_orgs.append(repo_org)
        return repos_orgs
    
    def repos(self):
        """Returns list of repos to which user has access
        
        @param gitolite_out: raw output of gitolite_info()
        @returns: list of repo names
        """
        repos = []
        for line in self.info.split('\n'):
            if ('R W' in line) and not ('R W C' in line):
                repo = line.strip().split('\t')[1]
                if repo not in repos:
                    repos.append(repo)
        return repos

    def collections(self, org_id=''):
        """List collections, optionally filtering by organization ID
        
        @param something: str
        @returns: list
        """
        if org_id:
            return [
                repo
                for repo in self.repos()
                if org_id in repo
            ]
        return self.repos()

    def collection_titles(self, username, password, timeout=5):
        """Returns IDs:titles dict for all collections to which user has access.
        
        TODO Page through the Cgit index pages (fewer HTTP requests)?
        TODO Set REPO/.git/description to collection title, read via Gitolite?
        
        @param username: str [optional] Cgit server HTTP Auth username
        @param password: str [optional] Cgit server HTTP Auth password
        @param timeout: int Timeout for getting individual collection info
        @returns: list of (repo,title) tuples
        """
        session = requests.Session()
        session.auth = (username,password)
        collections = [
            (repo,Cgit().collection_title(repo,session,timeout))
            for repo in self.repos()
        ]
        return collections
