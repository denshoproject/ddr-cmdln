from datetime import datetime
import logging
import sys

import click
import simplejson

from DDR import dvcs
from DDR import identifier
from DDR import util

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)


@click.command()
@click.argument('collection')
@click.option('--json','-j',  is_flag=True, help='Output as JSON')
def ddrinfo(collection, json):
    """ddrinfo - Prints info about a repository.
    
    \b
    Example:
        ddr-info /PATH/TO/REPO
    """
    start = datetime.now()
    
    repo = dvcs.repository(collection)
    #logging.debug(repo)

    data = {}
    #logging.debug('Getting file info')
    data.update(file_info(repo))
    
    #logging.debug('Getting annex info')
    data.update(annex_info(repo))

    if json:
        print(simplejson.dumps(data))
    else:
        output(data)
    
    finish = datetime.now()
    elapsed = finish - start
    #logging.info('DONE - (%s)' % (elapsed))


def file_info(repo):
    """Info from looking at set of metadata files
    
    @param repo: GitPython.Repository object
    @returns: dict
    """
    data = {}
    paths = util.find_meta_files(
        repo.working_dir,
        recursive=True,
        force_read=True
    )
    identifiers = [
        identifier.Identifier(path)
        for path in paths
    ]
    data['total objects'] = len(identifiers)
    # model totals
    for i in identifiers:
        key = '%s objects' % i.model
        if not data.get(key):
            data[key] = 0
        data[key] = data[key] + 1
    # role totals
    roles = identifier.VALID_COMPONENTS['role']
    for role in roles:
        key = '%s files' % role
        data[key] = 0
    for i in identifiers:
        if i.model == 'file':
            for role in roles:
                if role in i.path_abs():
                    key = '%s files' % role
                    data[key] = data[key] + 1
    return data

AINFO_FIELDS = {
    'local annex keys': 'local annex keys',
    'local annex size': 'local annex size',
    'annexed files in working tree': 'working tree files',
    'size of annexed files in working tree': 'working tree size',
}

def annex_info(repo):
    """Info from git-annex info command
    
    @param repo: GitPython.Repository object
    @returns: dict
    """
    WHITELIST = [f for f in AINFO_FIELDS.keys()]
    ainfo = {
        AINFO_FIELDS[key]: val
        for key,val in dvcs.annex_status(repo).items()
        if key in WHITELIST
    }
    return ainfo

def output(data):
    for key,val in data.items():
        logging.info('%s: %s' % (key,val))
