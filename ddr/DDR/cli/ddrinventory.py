HELP = """
ddrinventory - Gather and report inventory data
"""

from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys

import click
from dateutil import parser

from DDR import config
from DDR import dvcs
from DDR import identifier
from DDR import util

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
TIMESTAMP_RECAP = '%Y-%m-%d %H:%M:%S'


@click.group(context_settings=CONTEXT_SETTINGS)
def ddrinventory():
    pass


@ddrinventory.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


@ddrinventory.command()
def conf():
    """Print configuration settings.
    
    More detail since you asked.
    """
    pass


@ddrinventory.command()
@click.option('--username','-U',
              default=config.CGIT_USERNAME, envvar='CGIT_USERNAME',
              help='HTTP Basic auth username.')
@click.option('--password','-P',
              default=config.CGIT_PASSWORD, envvar='CGIT_PASSWORD',
              help='HTTP Basic auth password.')
@click.option('--jsonl','-j', is_flag=True, default=False, help="Output in JSONL.")
@click.option('--testing','-t', is_flag=True, default=False, help='Include ddr-testing-* repos.')
@click.option('--debug','-d', is_flag=True, default=False)
def cgit(username, password, jsonl, testing, debug):
    """Scrapes Cgit and gets basic data for all repositories
    """
    for repo in _repositories_cgit(username, password, testing):
        if repo.get('lastmod'):
            repo['lastmod'] = repo['lastmod'].isoformat()
        if jsonl:
            if repo.get('datetime'):
                repo.pop('datetime')
            click.echo(json.dumps(repo))
        else:
            click.echo(
                f"{repo['lastmod']} {repo['id']} {repo['title']}"
            )

def _repositories_cgit(username, password, testing=False):
    cgit = dvcs.Cgit()
    cgit.username = username
    cgit.password = password
    #if debug:
    #    click.echo(f"{cgit.url=}")
    for pagenum,offset in cgit._pages():
        #if debug:
        #    click.echo(f"{pagenum=} {offset=}")
        for data in cgit._page_repos(offset, testing):
            # only collection repositories
            cid = data['id']
            try:
                ci = identifier.Identifier(cid)
            except identifier.InvalidIdentifierException:
                ci = None
            except identifier.InvalidInputException:
                ci = None
            if (ci == None) or (ci.model != 'collection'):
                continue
            repo = {
                'id': data['id'],
                'lastmod': data['timestamp'],
                'ts': data['timestamp'],
                'href': data['href'],
                'title': data['title'],
            }
            if repo['lastmod']:
                repo['lastmod'] = parser.parse(repo['lastmod'])
            yield repo


@ddrinventory.command()
@click.option('--jsonl','-j', is_flag=True, default=False, help="Format output as JSONL.")
@click.option('--testing','-t', is_flag=True, default=False, help='Include ddr-testing-* repos.')
@click.option('-b','--base', default=config.MEDIA_BASE, help=f"Base path containing collection repositories (default: {config.MEDIA_BASE}).")
def local(jsonl, testing, base):
    """Crawls local filesystem and gets basic data for all repositories
    """
    for repo in _repositories_local(base, testing):
        repo.pop('repo')
        if jsonl:
            if isinstance(repo['lastmod'], datetime):
                repo['lastmod'] = repo['lastmod'].isoformat()
            if isinstance(repo['path'], Path):
                repo['path'] = str(repo['path'])
            click.echo(json.dumps(repo))
        else:
            click.echo(f"{repo['lastmod']} {repo['path']} {repo['title']}")

def _repositories_local(basedir, testing=False):
    if testing:
        paths = [p for p in dvcs.repos(basedir)]
    else:
        paths = [p for p in dvcs.repos(basedir) if not 'testing' in p]
    for path in sorted(paths):
        cpath = Path(path)
        cid = cpath.name
        try:
            ci = identifier.Identifier(cid)
        except identifier.InvalidIdentifierException:
            ci = None
        except identifier.InvalidInputException:
            ci = None
        if (ci == None) or (ci.model != 'collection'):
            continue
        repo = dvcs.repository(path)
        cjson = cpath / 'collection.json'
        # ignore repo and org repositories
        if not cjson.exists():
            continue
        cid = cpath.name
        try:
            latest_commit = dvcs.latest_commit(path)
            timestamp = ' '.join(latest_commit.split()[-3:])
            lastmod = parser.parse(timestamp)
        except Exception as err:
            timestamp = ''
            lastmod = f"{err=}"
        try:
            with cjson.open('r') as f:
                for line in f.readlines():
                    if 'title' in line:
                        title = line.strip()
                        title = title.replace('"title": "', '')
                        if title[-1] == '"':
                            title = title[:-1]
                        continue
        except FileNotFoundError:
            title = ''
        yield {'id':cid, 'lastmod':lastmod, 'ts':timestamp, 'path':cpath, 'title':title, 'repo':repo}


@ddrinventory.command()
@click.option('--username','-U',
              default=config.CGIT_USERNAME, envvar='CGIT_USERNAME',
              help='HTTP Basic auth username.')
@click.option('--password','-P',
              default=config.CGIT_PASSWORD, envvar='CGIT_PASSWORD',
              help='HTTP Basic auth password.')
@click.option('-d','--days', default=7, help='Data for the past N days (default: 7).')
@click.option('-b','--base', default=config.MEDIA_BASE, help=f"Base path containing collection repositories (default: {config.MEDIA_BASE}).")
def recent(username, password, days, base):
    """Report recent activity in synced repositories.
    
    Returns notices of recent repository initializations and merges
    and counts of recently modified metadata and annex files.
    
    Note: The last-modification dates come from *Cgit* on mits3.
    Collections will NOT be listed unless they have been synced!
    
    TODO indicate changes since last copy
    if given a checklog dir,
    - find the logfile for each collection
    - find the most recent ddrremote copy DONE line
    - see if any commits since last ddrremote copy DONE timestamp
    """
    basepath = Path(base)
    days = int(days)
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
    now = datetime.now(tz=config.TZ)
    recent = []
    for repo in _repositories_cgit(username, password, testing=False):
        if repo.get('timestamp'):
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
        now = datetime.now(tz=config.TZ)
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


@ddrinventory.command()
@click.option('--username','-U',
              default=config.CGIT_USERNAME, envvar='CGIT_USERNAME',
              help='HTTP Basic auth username.')
@click.option('--password','-P',
              default=config.CGIT_PASSWORD, envvar='CGIT_PASSWORD',
              help='HTTP Basic auth password.')
@click.option('--base','-b', default=config.MEDIA_BASE, help=f"Base path containing collection repositories (default: {config.MEDIA_BASE}).")
@click.option('--logsdir','-l', default=config.INVENTORY_LOGS_DIR, help=f"Directory containing special-remote copylogs. (default: {config.INVENTORY_LOGS_DIR}).")
@click.option('--remotes','-r', default=config.INVENTORY_REMOTES, help=f"Comma-separated list of remotes. (default: {config.INVENTORY_REMOTES}).")
@click.option('--absentok','-a', is_flag=True, default=False, help="Absent repositories not considered to be an error.")
@click.option('--verbose','-v', is_flag=True, default=False, help='Print ok status info not just bad.')
@click.option('--quiet','-q', is_flag=True, default=False, help='In UNIX fashion, only print bad status info.')
def report(username, password, logsdir, remotes, absentok, verbose, quiet, base):
    """Status of local repos, annex special remotes, actions to be taken
    """
    start = datetime.now(tz=config.TZ)
    if isinstance(remotes, str):
        remotes = remotes.strip().split(',')
    for remote in remotes:
        logdir = Path(logsdir) / remote
        if not logdir.exists():
            click.echo(f"ERROR: Log directory does not exist: {logdir}.")
            sys.exit(1)
    basedir = Path(base)
    if not basedir.exists():
        click.echo(f"ERROR: Repositories base directory does not exist: {basedir}.")
        sys.exit(1)
    repos,num_local,num_cgit = _combine_local_cgit(
        basedir, username, password, logsdir, remotes, quiet
    )
    cidw = _collection_id_width(repos)
    num_total = len(repos.items())
    num_notok = 0
    for collectionid,repo in repos.items():
        cid = collectionid.ljust(cidw)  # pad collection id
        ok,notok = _analyze_repository(repo, remotes, absentok)
        if verbose:
            click.echo(f"{cid} {','.join(ok)} {','.join(notok)}")
        else:
            if notok:
                num_notok += 1
                click.echo(f"{cid} {','.join(notok)}")
            elif not quiet:
                # show *something* for all collections
                click.echo(f"{cid} ok")
    now = datetime.now(tz=config.TZ)
    e = now - start
    now = now.isoformat(timespec='seconds')
    click.echo(f"{now} ({e}) Checked {num_local} of {num_total} collections: {num_notok} issues")

def _combine_local_cgit(basedir, username, password, logsdir, remotes, quiet=False):
    """Combine data from local repos, cgit repos, and remtoe copy logs
    """
    # load repository data
    # local
    if not quiet:
        click.echo(f"Getting repos in {basedir}...")
    repos_local = [repo for repo in _repositories_local(basedir)]
    if not quiet:
        click.echo(f"{len(repos_local)} local repositories")
    # cgit
    if not quiet:
        click.echo(f"Getting repos from cgit...")
    repos_cgit = [repo for repo in _repositories_cgit(username, password)]
    if not quiet:
        click.echo(f"{len(repos_cgit)} cgit repositories")
    ids_local = [repo['id'] for repo in repos_local]
    ids_cgit = [repo['id'] for repo in repos_cgit]
    ids_combined = sorted(set(ids_local + ids_cgit))
    num_local = len(ids_local)
    num_cgit = len(ids_cgit)
    # format data
    repo_prototype = {
        'here': {},
        'cgit': {},
        'remotes': {},
    }
    repositories = {}
    for cid in ids_combined:
        repositories[cid] = deepcopy(repo_prototype)
        for remote in remotes:
            repositories[cid]['remotes'][remote] = {}
    for r in repos_local:
        cid = r['id']
        repositories[cid]['here']['lastmod'] = r['lastmod']
        repositories[cid]['here']['ts'] = r['ts']
        repositories[cid]['here']['path'] = r['path']
        repositories[cid]['here']['title'] = r['title']
        repositories[cid]['here']['gitpython'] = r['repo']
    for r in repos_cgit:
        cid = r['id']
        repositories[cid]['cgit']['lastmod'] = r['lastmod']
        repositories[cid]['cgit']['ts'] = r['ts']
        repositories[cid]['cgit']['href'] = r['href']
        repositories[cid]['cgit']['title'] = r['title']
    # remote logs data
    for remote in remotes:
        logdir = f"{logsdir}/{remote}"
        if not quiet:
            print(f"Reading remote logs {logdir}")
        stats,fails = _copy_done_lines(logdir)
        for cid,data in stats.items():
            if not repositories.get(cid):
                # in case of logs for collections that have been deleted?
                repositories[cid] = deepcopy(repo_prototype)
            repositories[cid]['remotes'][remote] = data
        for cid in ids_combined:
            for line in fails:
                if cid in line:
                    repositories[cid]['remotes'][remote]['FAIL'] = line
    return repositories,num_local,num_cgit

def _collection_id_width(repos):
    """Return width of widest collection id"""
    width = 0
    for cid in repos.keys():
        if len(cid) > width:
           width = len(cid)
    return width

#from concurrent.futures import ThreadPoolExecutor
# 
#def run_io_tasks_in_parallel(tasks):
#    with ThreadPoolExecutor() as executor:
#        running_tasks = [executor.submit(task) for task in tasks]
#        for running_task in running_tasks:
#            running_task.result()
# 
#run_io_tasks_in_parallel([
#    lambda: print('IO task 1 running!'),
#    lambda: print('IO task 2 running!'),
#])

def _analyze_repository(repo, remotes, absentok=False):
    """Answer questions about individual repositories
    """
    # TODO break this into sub-functions, it's too complicated
    ok = []     # things that are okay
    notok = []  # something's not right
    # is repo in cgit?
    # (should always be in cgit see clone procedures)
    if repo.get('cgit'):
        ok.append('cgit')
    else:
        notok.append('NOT_CGIT')
    # is repo present in local filesystem?
    if repo.get('here'):
        ok.append('here')
    else:
        notok.append('NOT_HERE')
    # does local repo have changes since last sync?
    if repo['here'].get('lastmod') and repo['cgit'].get('lastmod'):
        if repo['here']['lastmod'] <= repo['cgit']['lastmod']:
            ok.append('sync_ok')
        elif repo['here']['lastmod'] > repo['cgit']['lastmod']:
            notok.append('SYNC_CGIT')
    # remotes
    for remote in remotes:
        # does repo have remote X?
        r = repo['remotes'].get(remote)
        if r:
            ok.append(f"{remote}_ok")
        else:
            notok.append(f"{remote}_ABSENT")
            continue
        # repo FAIL
        if r.get('FAIL'):
            if 'has no remote' in r['FAIL']:
                notok.append(f"{remote}_ABSENT")
            else:
                notok.append(f"FAIL {r['FAIL']}")
            continue
        # is remote current with here?
        # remote timestamp ahead of lastmod == OK
        # remote timestamp behind lastmod == UPDATE
        rts = repo['remotes'][remote].get('timestamp')
        if repo['here'].get('lastmod'):
            if rts >= repo['here']['lastmod']:
                ok.append(f"{remote}_current")
            elif rts < repo['here']['lastmod']:
                notok.append(f"{remote}_BEHIND")
        else:
            # there is a remote but no local repo to compare to
            notok.append(f"{remote}_ORPHAN")
        # does ok+copied == files?
        rfiles  = repo['remotes'][remote].get('files')
        rok     = repo['remotes'][remote].get('ok')
        rcopied = repo['remotes'][remote].get('copied')
        rerrs   = repo['remotes'][remote].get('errs')
        # account for old copylog format which is missing ok,errs data
        if (rok > -1) and (rerrs > -1):
            if rok + rcopied == rfiles:
                ok.append(f"{remote}_count_ok")
            else:
                notok.append(f"{remote}_COUNT_BAD")
            if rerrs:
                notok.append(f"{remote}_ERRS")
    if absentok:
        notok = [x for x in notok if x != 'NOT_HERE']
    return ok,notok


@ddrinventory.command()
@click.option('-s','--sort', default='collectionid', help='Sort order. See --help for options.')
@click.option('-j','--jsonl', is_flag=True, default=False, help='Output JSONL aka list of JSONs.')
@click.option('--logsdir','-l', default=config.INVENTORY_LOGS_DIR, help=f"Directory containing special-remote copylogs. (default: {config.INVENTORY_LOGS_DIR}).")
def copylogs(sort, jsonl, logsdir):
    """Report recent activity in synced repositories.
    
    """
    stats_by_collection,fails = _copy_done_lines(logsdir)
    if '-' in sort:
        reverse = True
        sort = sort.replace('-','')
    else:
        reverse = False
    for val in sorted(
            stats_by_collection.values(),
            key=lambda d: d[sort],
            reverse=reverse
    ):
        if jsonl:
            val.pop('ts')
            val.pop('delta')
            click.echo(json.dumps(val))
        else:
            timestamp = val['ts']  # print the str not the datetime
            remote = val['remote']; collectionpath = val['collectionpath']
            elapsed = val['el']    # print the str not the timedelta
            files = val['files']; ok = val['ok']; copied = val['copied']; errs = val['errs']
            click.echo(f"{timestamp} ddrremote copy {remote} {collectionpath} DONE {elapsed} files:{files} ok:{ok} copied:{copied} errs:{errs}")

def _copy_done_lines(logsdir):
    os.chdir(logsdir)
    # Get only the last line of each `ddrremote copy` run,
    # sorted in ascending order by timestamp
    cmd = 'ack "ddrremote copy" -h --nobreak | grep DONE | sort'
    out = subprocess.check_output(
        cmd, stderr=subprocess.STDOUT, shell=True, encoding='utf-8'
    )
    stats = {}
    fails = []
    for line in [line.strip() for line in out.splitlines()]:
        data,bad = _parse_copy_done_line(line)
        if data and data.get('collectionid'):
            stats[data['collectionid']] = data
        elif bad:
            fails.append(bad)
        else:
            print(f"FAIL {data=} {bad=}")
    return stats,fails

LOG_V2_SAMPLE = '2024-09-24T16:45:24 ddrremote copy hq-backup-montblanc /media/qnfs/kinkura/gold/ddr-ajah-8 DONE 0:00:06.409915 36 files 0 copied'
LOG_V2_PATTERN = re.compile(
    "(?P<timestamp>[\d-]+T[\d:]+) "
    "(?P<command>ddrremote copy) "
    "(?P<remote>[\w\d-]+) "
    "(?P<collectionpath>[\w\d/-]+) "
    "DONE "
    "(?P<elapsed>[\d]+:[\d]+:[\d]+.[\d]+) "
    "(?P<files>[\d]+) files (?P<copied>[\d]+) copied"
)

LOG_V3_SAMPLE = '2024-10-03T14:57:40 ddrremote copy b2 /media/qnfs/kinkura/gold/ddr-njpa-11 DONE 0:01:50.543381 files:9661 ok:9661 copied:0 errs:0'
LOG_V3_PATTERN = re.compile(
    "(?P<timestamp>[\d-]+T[\d:)]+) "  # isoformat without timezone
    "(?P<command>ddrremote copy) "
    "(?P<remote>[\w\d-]+) "
    "(?P<collectionpath>[\w\d/-]+) "
    "DONE "
    "(?P<elapsed>[\d]+:[\d]+:[\d]+.[\d]+) "
    "files:(?P<files>[\d]+) ok:(?P<ok>[\d]+) copied:(?P<copied>[\d]+) errs:(?P<errs>[\d]+)"
)

LOG_V4_SAMPLE = '2024-10-10T12:14:26-07:00 ddrremote copy hq-backup-montblanc /media/qnfs/kinkura/gold/ddr-phljacl-2 DONE 0:00:00.122655 files:0 ok:0 copied:0 errs:0'
LOG_V4_PATTERN = re.compile(
    "(?P<timestamp>[\d-]+T[\d:)]+-[\d:]+) "  # isoformat with timezone
    "(?P<command>ddrremote copy) "
    "(?P<remote>[\w\d-]+) "
    "(?P<collectionpath>[\w\d/-]+) "
    "DONE "
    "(?P<elapsed>[\d]+:[\d]+:[\d]+.[\d]+) "
    "files:(?P<files>[\d]+) ok:(?P<ok>[\d]+) copied:(?P<copied>[\d]+) errs:(?P<errs>[\d]+)"
)

LOG_STATS = ['files','ok','copied','errs']

def _parse_copy_done_line(line):
    data = {'collectionid':None,'remote':None,'timestamp':None,'elapsed':None}
    m = LOG_V4_PATTERN.match(line)
    if m:
        for key,val in m.groupdict().items():
            data[key] = val
    else:
        m = LOG_V3_PATTERN.match(line)
        if m:
            for key,val in m.groupdict().items():
                data[key] = val
        else:
            # old format
            m = LOG_V2_PATTERN.match(line)
            if m:
                for key,val in m.groupdict().items():
                    data[key] = val
            else:
                # couldn't figure it out so punt
                return None,f"unknown_line_format"
    # type conversions
    if data.get('collectionpath'):
        # TODO check for legal collection path?
        # TODO check for legal collection id?
        path = Path(data['collectionpath'])
        data['collectionid'] = path.name
    if data.get('timestamp'):
        data['ts'] = data['timestamp']
        ts = datetime.fromisoformat(data['timestamp'])
        if not ts.tzinfo:
            ts = ts.astimezone(config.TZ)
        data['timestamp'] = ts
    if data.get('elapsed'):
        data['el'] = data['elapsed']
        h,m,ss = data['elapsed'].split(':')
        s,ms = ss.split('.')
        data['elapsed'] = timedelta(
            hours=int(h), minutes=int(m), seconds=int(s), milliseconds=int(ms)
        )
    for key in LOG_STATS:
        if data.get(key):
            data[key] = int(data[key])
        else:
            data[key] = -1
    return data,None
