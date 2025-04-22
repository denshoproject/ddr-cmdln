from datetime import datetime
import fnmatch
import logging
import os
import sys

import click

from DDR import config
from DDR import commands
from DDR import dvcs
from DDR import identifier
from DDR import vocab
from DDR import util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)


@click.command()
@click.argument('collection')
@click.option('--filter', '-f',
              help='Only touch objects w matching ID (wildcard)')
@click.option('--models', '-M',
              help='Only touch specified models (comma-separated list)')
@click.option('--topics', '-t', is_flag=True,
              help='Fix damaged topics data.')
@click.option('--created', '-R', is_flag=True,
              help='Replace entity.record_created w ts of first commit.')
@click.option('--commit', '-C', is_flag=True, help='Commit changes.')
@click.option('--user','-u', help='(required for commit) User name')
@click.option('--mail','-m', help='(required for commit) User email')
def ddrtransform(collection, filter, models, topics, created, commit, user, mail):
    """ddrtransform - Just loads objects and saves them again.

    This has the effect of updating objects to the latest file format.
    Someday this command could be used to run function from script file
    on each .json file in a repository.
    """
    transform(collection, filter, models, topics, created, commit, user, mail)


def transform(collection, filter='', models='', topics=None, created=None, commit=None, user=None, mail=None):
    
    if commit and ((not user) or (not mail)):
        logging.error('You must specify a user and email address! >:-0')
        sys.exit(1)
    else:
        logging.info('Not committing changes')
    
    start = datetime.now()
    
    if filter:
        logging.info('FILTER: "%s"' % filter)
    ONLY_THESE = []
    if models:
        logging.info('MODELS: "%s"' % models)
        ONLY_THESE = models.split(',')
    
    logging.info('Loading collection')
    cidentifier = identifier.Identifier(os.path.normpath(collection))
    collection = cidentifier.object()
    logging.info(collection)
    
    logging.info('Finding metadata files')
    paths = util.find_meta_files(
        collection.identifier.path_abs(),
        recursive=True,
        force_read=True
    )
    logging.info('%s paths' % len(paths))
    
    TOPICS = vocab.get_vocabs(config.VOCABS_URL)['topics']
    # filter out paths
    these_paths = []
    for path in paths:
        oi = identifier.Identifier(path)
        if filter and (not fnmatch.fnmatch(oi.id, filter)):
            continue
        if models and (oi.model not in ONLY_THESE):
            continue
        these_paths.append(path)
    if len(these_paths) != len(paths):
        logging.info('%s after filters' % len(these_paths))
    
    logging.info('Writing')
    num = len(these_paths)
    for n,path in enumerate(these_paths):
        logging.info('%s/%s %s' % (n, num, path))
        o = identifier.Identifier(path).object()
        if filter and (not fnmatch.fnmatch(o.id, filter)):
            continue
        if models and (o.identifier.model not in ONLY_THESE):
            continue
        
        if o.identifier.model in ['entity', 'segment']:
            o.children(force_read=True)
        
        if topics and o.identifier.model in ['entity', 'segment']:
            before = o.topics
            after = vocab.repair_topicdata(o.topics, TOPICS)
            o.topics = after
        
        if created and hasattr(o, 'record_created'):
            record_created_before = o.record_created
            commit = dvcs.earliest_commit(path, parsed=True)
            o.record_created = commit['ts']
        
        o.write_json()
    
    if commit:
        logging.info('Committing changes')
        status,msg = commands.update(
            user, mail,
            collection,
            paths,
            agent='ddr-transform'
        )
        logging.info('ok')
    else:
        logging.info('Changes not committed')
    
    end = datetime.now()
    elapsed = end - start
    per = elapsed / num
    logging.info('DONE (%s elapsed, %s per object)' % (elapsed, per))
