import logging
import sys

import click

from DDR import config
from DDR import identifier
from DDR import signatures
from DDR import util

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)


@click.command()
@click.argument('collection')
@click.option('--nowrite', '-W', is_flag=True, help='Do not write changes.')
@click.option('--nocommit', '-C', is_flag=True, help='Do not commit changes.')
@click.option('--user', '-u', is_flag=True, help='(required for commit) User name')
@click.option('--mail', '-m', is_flag=True, help='(required for commit) User email')
def ddrsignatures(collection, nowrite, nocommit, user, mail):
    """ddrsignatures - Picks signature files for each collection object.
    """
    if not nocommit:
        if not (user and mail):
            logging.debug('You must specify a user and email address! >:-0')
            sys.exit(1)

    logging.debug('-----------------------------------------------')
    logging.debug('Loading collection')
    collection = identifier.Identifier(path=collection).object()
    logging.debug(collection)

    # Read data files, gather *published* Identifiers, map parents->nodes
    # assign signatures, write files
    updates = signatures.find_updates(
        signatures.choose(
            util.find_meta_files(collection.path, recursive=True, force_read=True)
        )
    )

    if nowrite:
        logging.debug('Not writing changes')
        files_written = []
    else:
        files_written = signatures.write_updates(updates)

    if nocommit:
        logging.debug('Not committing changes')
    elif files_written:
        if (not user) or (not mail):
            logging.debug('You must specify a user and email address! >:-0')
            sys.exit(1)
        status,msg = signatures.commit_updates(
            collection,
            files_written,
            user, mail, agent='ddr-signature',
            commit=True
        )
    logging.debug('DONE')
