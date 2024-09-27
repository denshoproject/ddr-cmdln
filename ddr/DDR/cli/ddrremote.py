DESCRIPTION = """ddrremote - Tools for working with DDR git-annex special remotes"""

HELP = """Tools for working with DDR git-annex special remotes.

check - Verifies that all the annex files in the collection are present in
        the specified special remote. Optionally writes to --log.
copy  - Runs git annex copy to the specified special remote, and
        optionally writes to --log.
recap - Reports info about recent collection modifications.

- Works best if the ddr user has passwordless SSH keys for each remote.
"""


from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path
import subprocess
import sys
from time import sleep

import click
from dateutil import parser
import humanize

from DDR import config
from DDR import dvcs

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIMESTAMP_RECAP = '%Y-%m-%d %H:%M:%S'


def dtfmt(dt=None):
    if not dt:
        dt = datetime.now()
    return dt.strftime(TIMESTAMP_FORMAT)

def log(logfile, line):
    if logfile:
        with logfile.open('a') as f:
            f.write(f"{line}\n")
    click.echo(line)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--debug','-d', is_flag=True, default=False)
def ddrremote(debug):
    """ddrremote - Tools for working with DDR git-annex special remotes

    \b
    See "ddrremote help" for examples.
    """
    if debug:
        click.echo('Debug mode is on')


@ddrremote.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


@ddrremote.command()
@click.option('-f','--fast', is_flag=True, default=False, help='Faster check using git annex info.')
@click.option('-l','--logfile', default=None, help='Write output to log file.')
@click.option('-v','--verbose', is_flag=True, default=False, help='Show all files not just missing ones.')
@click.option('-w','--wait', default=0, help='Wait N seconds after checking a collection.')
@click.argument('remote')
@click.argument('collection')
def check(fast, logfile, verbose, wait, remote, collection):
    """Check that annex files in the collection are present in the special remote
    
    Under the hood, uses the git annex checkpresentkey plumbing command,
    which reaches out to the special remote.
    
    Write output to files for use by compare command:
    ddrremote check REMOTE COLLECTIONPATH | tee /PATH/REMOTE/COLLECTION
    """
    if logfile:
        logfile = Path(logfile)
    collection_path = Path(collection).absolute()
    cid = collection_path.name
    prefix = f"{dtfmt()} ddrremote check"
    starttime = datetime.now()
    log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} START")
    if fast:
        size_here,size_remote,diff = annex_info_remote(collection_path, remote)
        endtime = datetime.now(); elapsed = endtime - starttime
        def natural(filesize):
            if filesize == None: return '---'
            if filesize == 0: return '0'
            return humanize.naturalsize(filesize).replace(' ','') if filesize else '---'
        here_nat = natural(size_here)
        remote_nat = natural(size_remote)
        diff_nat = natural(diff)
        log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} DONE {elapsed} {here_nat} here {remote_nat} remote {diff_nat} missing")
    else:
        startdir = os.getcwd()
        annex_files = annex_find(collection_path)
        errors = 0
        missing = 0
        for rel_path in annex_files:
            output = checkpresentkey(collection_path, rel_path, remote)
            if output.split()[0] == 'ok':
                if (verbose):
                    log(logfile, f"{prefix} {output}")
            else:
                errors += 1
                if 'missing' in output:
                    missing += 1
                log(logfile, f"{prefix} {output}")
        endtime = datetime.now(); elapsed = endtime - starttime
        log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} DONE {str(elapsed)} {len(annex_files)} files {errors} errs {missing} missing")
    if wait:
        sleep(int(wait))

def annex_find(collection_path):
    """Gets list of relative file paths using git annex find
    """
    os.chdir(collection_path)
    return [
        Path(relpath)
        for relpath in subprocess.check_output(
                'git annex find', stderr=subprocess.STDOUT, shell=True,
                encoding='utf-8'
        ).strip().splitlines()
    ]

def checkpresentkey(collection_path, rel_path, remote):
    # get file's annex filename AKA key
    os.chdir(collection_path)
    key = None
    rpath = str(rel_path.resolve())
    if rel_path.is_symlink() and '.git/annex/objects' in rpath and 'SHA' in rpath:
        key = rel_path.resolve().name
    if not key:
        return f"ERROR nokey {str(rel_path)}"
    # git annex checkpresentkey
    cmd = f"git annex checkpresentkey {key} {remote}"
    try:
        # checkpresentkey returns 0 if file is present
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, shell=True,
            encoding='utf-8'
        )
        if output == '':
            return f"ok {str(rel_path)}"
    except subprocess.CalledProcessError as err:
        # checkpresentkey returns 1 if file is *absent*
        if err.returncode == 1:
            return f"ERROR missing {remote} {str(rel_path)}"
        # checkpresentkey returns 100 if something went wrong
        return f"ERROR {err}"
    return f"ERROR fellout {str(rel_path)}"

def annex_info_remote(collection_path, remote):
    """Get size diff between here and specified remote
    
    Uses `git annex info .`
    """
    os.chdir(collection_path)
    cmd = 'git annex info . --bytes --json'
    data = json.loads(subprocess.check_output(
        cmd, stderr=subprocess.STDOUT, shell=True,
        encoding='utf-8'
    ))
    here = None
    for r in data['repositories containing these files']:
        if r['here']:
            r['size'] = int(r['size'])
            here = r; continue
    there = None
    for r in data['repositories containing these files']:
        if remote in r['description']:
            r['size'] = int(r['size'])
            there = r; continue
    if there:
        return here['size'], there['size'], here['size'] - there['size']
    if here:
        return here['size'],None,None
    return None,None,None


@ddrremote.command()
@click.option('-l','--logfile', default=None, help='Write output to log file.')
@click.option('-w','--wait', default=0, help='Wait N seconds after checking a collection.')
@click.argument('remote')
@click.argument('collection')
def copy(logfile, wait, remote, collection):
    """git annex copy collection files to the remote and log
    
    Runs `git annex copy -c annex.sshcaching=true . --to=REMOTE`
    and adds info to the output.
    """
    if logfile:
        logfile = Path(logfile)
    collection_path = Path(collection).absolute()
    cid = collection_path.name
    prefix = f"{dtfmt()} ddrremote"
    starttime = datetime.now()
    log(logfile, f"{dtfmt()} ddrremote copy {remote} {collection_path} START")
    files = 0
    copied = 0
    for line in _annex_copy(collection, remote).splitlines():
        if f"(checking {remote}...)" in line:
            log(logfile, f"{prefix} {line}")
            files += 1
        else:
            if 'sending incremental file list' in line:
                copied += 1
            log(logfile, line)
    endtime = datetime.now(); elapsed = endtime - starttime
    log(logfile, f"{dtfmt()} ddrremote copy {remote} {collection_path} DONE {str(elapsed)} {files} files {copied} copied")
    if wait:
        sleep(int(wait))

def _annex_copy(collection_path, remote):
    # TODO yield lines instead of returning one big str
    os.chdir(collection_path)
    cmd = f"git annex copy -c annex.sshcaching=true . --to {remote}"
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True,encoding='utf-8')
    except subprocess.CalledProcessError as err:
        return f"ERROR {str(err)}"


@ddrremote.command()
@click.option('-d','--days', default=7, help='Data for the past N days (default: 7).')
@click.option('-b','--base', default=config.MEDIA_BASE, help=f"Base path containing collection repositories (default: {config.MEDIA_BASE}).")
def recap(days, base):
    """Report recent activity in synced repositories.
    
    Returns notices of recent repository initializations and merges
    and counts of recently modified metadata and annex files.
    
    Note: The last-modification dates come from *Cgit* on mits3.
    Collections will NOT be listed unless they have been synced!
    """
    basepath = Path(base)
    days = int(days)
    username = config.CGIT_USERNAME
    password = config.CGIT_PASSWORD
    if os.environ.get('CGIT_USERNAME'): username = os.environ['CGIT_USERNAME']
    if os.environ.get('CGIT_PASSWORD'): password = os.environ['CGIT_PASSWORD']
    # Cgit data from last N days
    cgit_repositories = _cgit_recently_modified(days, username, password)
    # add GitPython objects if repo is present in local filesystem
    repos_repositories = _gitpython_repositories(cgit_repositories, basepath)
    # sort chronologically
    repos_repositories = sorted(
        repos_repositories, key=lambda rr: rr[0]['timestamp']
    )
    for repo,repository in repos_repositories:
        lastmod = repo['timestamp'].strftime(TIMESTAMP_RECAP)
        # skip repository if not in local filesystem
        if not repository:
            click.echo(f"{lastmod} ({repo['id']})")
            continue
        # look at local repository's recent commits and count significant events
        totals = {'init': [], 'merge': [], 'metadata': [], 'annex': []}
        for commit in _recent_commits(repository, days):
            totals = _commit_modified_files(repo, repository, commit, totals)
        # print out
        events = []
        if totals['init']:     events.append('INITIALIZED')
        if totals['merge']:    events.append(f"{len(totals['merge'])} merge")
        if totals['metadata']: events.append(f"{len(totals['metadata'])} metadata")
        if totals['annex']:    events.append(f"{len(totals['annex'])} annex")
        events = ', '.join(events)
        click.echo(f"{lastmod}  {repo['id']}  {events}")

def _cgit_recently_modified(days, username, password):
    """Scrape Cgit and return info for repositories modified in last N days
    """
    cgit = dvcs.Cgit()
    if username: cgit.username = username
    if password: cgit.password = password
    # TODO get timezone from OS
    now = datetime.now(tz=ZoneInfo('America/Los_Angeles'))
    recent = []
    for repo in cgit.repositories():
        ts = repo.get('timestamp')
        if ts:
            # TODO parse the time zones correctly this is dumb
            ts = ts.replace('-0800','PST').replace('-0700','PDT')
            repo['timestamp'] = parser.parse(ts)
            delta = now - repo['timestamp']
            if delta.days <= days:
                recent.append(repo)
    return recent

def _gitpython_repositories(repos, basepath):
    """Load GitPython repository objects if collection is reachable
    
    Collections may be known to Cgit but be missing from local filesystem
    i.e. in development environment.
    """
    repositories = []
    for repo in repos:
        path = basepath / repo['id']
        if path.exists():
            repositories.append((repo,dvcs.repository(path)))
        else:
            repositories.append((repo,None))
    return repositories

def _recent_commits(repo, days, now=None):
    """Return commits that occured within N days of timestamp
    """
    if now:
        now = parser.parse(f"{now} PST")
    else:
        # TODO get timezone from OS
        now = datetime.now(tz=ZoneInfo('America/Los_Angeles'))
    commits = []
    for commit in repo.iter_commits('master'):
        delta = now - commit.committed_datetime
        if delta.days <= days:
            commits.append(commit)
    return commits

def _commit_modified_files(repo, repository, commit, totals):
    """Update totals with any significant events that occurred in th commit
    """
    modified = [
        line
        for line in repository.git.show(commit.hexsha, name_only=True).split('\n')
        if (line.find('files/') == 0)
        or ('Merge branch' in line)
        or ('Initialized' in line)
    ]
    totals['init']     += [line for line in modified if 'Initialized' in line]
    totals['merge']    += [line for line in modified if 'Merge' in line]
    totals['metadata'] += [line for line in modified if '.json' in line]
    totals['annex']    += []
    for relpath in modified:
        abspath = Path(repository.working_dir) / relpath
        rpath = str(abspath.resolve())
        if abspath.is_symlink() \
        and '.git/annex/objects' in rpath \
        and 'SHA' in rpath:
            totals['annex'].append(relpath)
    return totals


if __name__ == '__main__':
    ddrremote()
