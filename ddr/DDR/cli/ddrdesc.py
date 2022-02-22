#!/usr/bin/env python3

import json
import logging
import os
from pathlib import Path
import random
import shutil
import sys

import click
import git


@click.command()
@click.argument('srcdir')
@click.option('--random','-R',  is_flag=True, help='Pick a random repository in SRCDIR.')
@click.option('--tmpdir','-t',  default='/tmp', help='Temporary directory')
@click.option('--verbosity','-v', count=True, help='Verbosity')
def ddrdesc(srcdir, random, tmpdir, verbosity):
    """Sets repository .git/description to value of collection.title
    
    This way you see a nice list of collection titles in Cgit instead of a
    bunch of "[no description]" placeholders.  Use the --random flag to pick
    a random repository from a directory.
    """
    if verbosity > 1:
        loglevel = logging.DEBUG
    elif verbosity:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARN
    logging.basicConfig(
        level=loglevel,
        format='%(levelname)-8s %(message)s',
        stream=sys.stdout,
    )
    if random:
        srcdir = choose_random(Path(srcdir))
    orig_path,dest_path = mkpaths(Path(srcdir), Path(tmpdir))
    cleanup_tmp(dest_path)
    repo = clone(orig_path, dest_path)
    title = get_title(dest_path)
    set_description(orig_path, title)
    cleanup_tmp(dest_path)

def choose_random(srcdir):
    if srcdir.exists() and srcdir.is_dir():
        logging.debug(f'Looking around in {srcdir}...')
        return random.choice([d for d in srcdir.iterdir()])
    logging.error(f'Not a directory: {srcdir}')

def mkpaths(orig_path, tmp_dir):
    if not orig_path.exists():
        logging.error('Path does not exist %s' % orig_path)
        sys.exit(1)
    cid = orig_path.name.replace('.git', '')
    return orig_path, tmp_dir / cid

def cleanup_tmp(dest_path):
    if dest_path.exists():
        logging.debug(f'Cleaning up {dest_path}') 
        shutil.rmtree(dest_path)

def clone(orig_path, dest_path):
    logging.info(f'Cloning {orig_path} {dest_path}')
    try:
        repo = git.Repo(orig_path)
    except git.exc.InvalidGitRepositoryError as err:
        logging.error(f'Invalid Git repository: {orig_path}')
        sys.exit(1)
    try:
        repo.clone(path=dest_path)
    except FileNotFoundError as err:
        logging.error(err)
        sys.exit(1)
    return git.Repo(dest_path)

def get_title(collection_dir):
    json_path = collection_dir / 'collection.json'
    try:
        with json_path.open(mode='r') as f:
            jsonlines = json.loads(f.read())
    except FileNotFoundError as err:
        logging.error(err)
        sys.exit(1)
    for fielddict in jsonlines:
        if 'title' in fielddict.keys():
            title = fielddict['title']
            if title:
                return title
    logging.info('No title!')
    return ''

def set_description(orig_path, title):
    repo = git.Repo(orig_path)
    if title:
        logging.info(f'Setting description for {repo.working_dir}: "{title}"')
        repo.description = title
        logging.debug('ok')
    else:
        logging.debug('No title')

if __name__ == '__main__':
    ddrdesc()
