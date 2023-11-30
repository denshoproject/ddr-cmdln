HELP = """
ddrinventory - Gather and report inventory data
"""

import json
import logging
from pathlib import Path
import sys

import click

from DDR import config
from DDR import dvcs
from DDR import identifier
from DDR import util

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


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
