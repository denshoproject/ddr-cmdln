HELP = """
ddrinventory - Gather and report inventory data
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import json
import logging
import os
from pathlib import Path
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
@click.option('--format','-f', default='text', help="Output format (text,jsonl).")
@click.option('--testing','-t', is_flag=True, default=False, help='Include ddr-testing-* repos.')
@click.option('--debug','-d', is_flag=True, default=False)
def cgit(username, password, format, testing, debug):
    """Scrapes Cgit and gets basic data for all repositories
    """
    assert format in ['text', 'json', 'jsonl']
    cgit = dvcs.Cgit()
    cgit.username = username
    cgit.password = password
    if debug:
        click.echo(f"{cgit.url=}")
    for pagenum,offset in cgit._pages():
        if debug:
            click.echo(f"{pagenum=} {offset=}")
        for repo in cgit._page_repos(offset, testing):
            if 'json' in format:
                if repo.get('datetime'):
                    repo.pop('datetime')
                click.echo(json.dumps(repo))
            else:
                click.echo(
                    f"{repo.get('timestamp','')}  {repo['id']}  {repo.get('url','')}  {repo['title']}"
                )


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
