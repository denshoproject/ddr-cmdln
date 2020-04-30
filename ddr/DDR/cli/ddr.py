import configparser
import json
import logging
import os
import sys
logger = logging.getLogger(__name__)

import click

from DDR import commands
from DDR import config
from DDR import fileio
from DDR import identifier
from DDR import idservice
from DDR import models

config_parser = configparser.ConfigParser()
configs_read = config_parser.read(config.CONFIG_FILES)
if not configs_read:
    raise config.NoConfigError('No config file!')

DEBUG = config_parser.get('debug','debug')
MEDIA_BASE = config_parser.get('cmdln','media_base')

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOGGING_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOGGING_FILE = config_parser.get('local','log_file')
if config_parser.get('debug','log_level') == 'debug':
    LOGGING_LEVEL = logging.DEBUG
else:
    LOGGING_LEVEL = logging.ERROR
#logging.basicConfig(format=LOGGING_FORMAT, datefmt=LOGGING_DATEFMT, level=LOGGING_LEVEL, filename=LOGGING_FILE)
logging.basicConfig(format=LOGGING_FORMAT, datefmt=LOGGING_DATEFMT, level=logging.DEBUG, filename=LOGGING_FILE)

AGENT = 'ddr-cmdln'


@click.group()
def ddr():
    """DOCSTRING HELP
    
    \b
    "ddr help" for usage examples.
    "ddr COMMAND --help" to see options for each command.
    """
    pass

HELP = """

# Create a new collection repository.
$ ddr2 create -u USER -m MAIL /var/www/media/ddr/ddr-testing-123

# Clones existing collection repository from hub server.
$ ddr2 clone -u USER -m MAIL /var/www/media/ddr/ddr-testing-123

# Check ID server for specified object ID.
$ ddr2 exists -u USERNAME -p PASSWORD ddr-testing-123-1

# Check ID server for next object ID in series.
$ ddr2 next -u USERNAME -p PASSWORD ddr-testing collection
(returns 'ddr-testing-124')
$ ddr2 next -u USERNAME -p PASSWORD ddr-testing-123 entity
(returns 'ddr-testing-123-1')
$ ddr2 next -u USERNAME -p PASSWORD ddr-testing-123-1 segment
(returns 'ddr-testing-123-1-1')

# Register object ID with ID server.
$ ddr2 register -u USERNAME -p PASSWORD ddr-testing-123
$ ddr2 register -u USERNAME -p PASSWORD ddr-testing-123-1

# Open an editor and edit specified file (does not commit)
$ ddr2 edit /var/www/media/ddr/ddr-testing-123
$ ddr2 edit /var/www/media/ddr/ddr-testing-123-1
$ ddr2 edit /var/www/media/ddr/ddr-testing-123-1-master-a1b2c3

# Check JSON file for errors
$ ddr2 check /var/www/media/ddr/ddr-testing-123-1

# Add file to entity (does not commit).
$ ddr2 add -u USER -m MAIL master /tmp/addthese/file.jpg /var/www/media/ddr/ddr-testing-123-1

# Update and commit modified files for the specified object(s).
$ ddr2 save -u USER -m MAIL /var/www/media/ddr/ddr-testing-123-1

# Delete specified file and commit.
$ ddr2 delete -u USER -m MAIL /var/www/media/ddr/ddr-testing-123-1

# Reports git status of collection repository.
$ ddr2 status /var/www/media/ddr/ddr-testing-123

# Reports git annex status of collection repository.
$ ddr2 astatus  Reports git annex status of collection...

$ ddr2 sync     Syncs collection repository with hub server

---"""

#$ ddr2 pull     Pulls a git-annex file from hub server.
#$ ddr2 push     Pushes a git-annex file to hub server.

@ddr.command()
def help():
    """Detailed help and usage examples.
    """
    click.echo(HELP)

@ddr.command()
@click.argument('collection')
@click.option('--user','-u', help='User name.')
@click.option('--mail','-m', help='User e-mail address.')
@click.option('--log','-l', help='Log file path.')
def create(collection, user, mail, log):
    """Create a new collection repository.
    
    Technically, this clones a blank collection object from the hub server,
    adds initial collection files, and commits.
    """
    set_logging(log)
    exit,msg = models.Collection.create(
        identifier.Identifier(collection), user, mail, agent=AGENT
    )
    click.echo(msg); sys.exit(exit)

@ddr.command()
@click.argument('collection')
@click.option('--user','-u', help='User name.')
@click.option('--mail','-m', help='User e-mail address.')
@click.option('--log','-l', help='Log file path.')
def clone(collection, user, mail, log):
    """Clone existing collection repository from hub server.
    """
    set_logging(log)
    exit,msg = commands.clone(
        user, mail, identifier.Identifier(collection), collection
    )
    click.echo(msg); sys.exit(exit)

@ddr.command()
@click.argument('objectid')
@click.option('--username','-u', help='ID Service username.')
@click.option('--password','-p', help='ID Service password.')
def exists(objectid, username, password):
    """Check ID server for specified object ID.
    """
    ic = _idservice_login(username, password)
    data = ic.check_object_id(objectid)
    click.echo('%s: %s' % (objectid, data['registered']))

@ddr.command()
@click.argument('objectid')
@click.argument('model')
@click.option('--username','-u', help='ID Service username.')
@click.option('--password','-p', help='ID Service password.')
def next(objectid, model, username, password):
    """Check ID server for next object ID in series.
    """
    ic = _idservice_login(username, password)
    http_status,reason,new_entity_id = ic.next_object_id(
        identifier.Identifier(objectid), model,
        register=False
    )
    if http_status != 200:
        click.echo('Error: %s %s' % (http_status, reason))
        sys.exit(1)
    click.echo('%s %s: %s' % (http_status, reason, new_entity_id))
    
@ddr.command()
@click.argument('objectid')
@click.option('--username','-u', help='ID Service username.')
@click.option('--password','-p', help='ID Service password.')
def register(objectid, username, password):
    """Register object ID with ID server.
    """
    oi = identifier.Identifier(objectid)
    ic = _idservice_login(username, password)
    http_status,reason,added_ids = ic.register_eids(
        cidentifier=oi.collection(),
        entity_ids=[objectid]
    )
    if http_status not in [200, 201]:
        click.echo('Error: %s %s' % (http_status, reason))
        sys.exit(1)
    click.echo('%s %s: %s' % (http_status, reason, ','.join(added_ids)))

def _idservice_login(username, password):
    ic = idservice.IDServiceClient()
    http_status,reason = ic.login(username, password)
    if http_status != 200:
        click.echo('Login error: %s %s' % (http_status, reason))
        sys.exit(1)
    return ic

@ddr.command()
@click.argument('path')
def edit(path):
    """Edit object metadata.
    """
    oi = identifier.Identifier(path)
    
    # write initial file if not already present
    if not os.path.exists(oi.path_abs('json')):
        
        if not os.path.exists( oi.path_abs() ):
            print('makedir %s' % oi.path_abs())
            os.makedirs( oi.path_abs() )
        
        # TODO hard-coded stuff!
        if oi.model == 'collection':
            models.Collection.new(oi).write_json()
        elif oi.model == 'entity':
            models.Entity.new(oi).write_json()
        elif oi.model == 'file':
            models.File.new(oi).write_json()
    
    click.edit(filename=oi.path_abs('json'))

@ddr.command()
@click.argument('role')
@click.argument('src')
@click.argument('entity')
@click.option('--user','-u', help='User name.')
@click.option('--mail','-m', help='User e-mail address.')
@click.option('--log','-l', help='Log file path.')
def add(role, src, entity, user, mail, log):
    """Add file to entity.
    """
    entity = identifier.Identifier(entity).object()
    file_,repo,log = entity.add_local_file(
        src_path=src,
        role=role,
        data={},
        git_name=user, git_mail=mail, agent=AGENT
    )

@ddr.command()
@click.argument('path')
def check(path):
    """Validate JSON correctness of specified object.
    
    TODO better error reporting
    TODO Let user specify multiple files in path e.g. ddr-testing-123-(1,2,4)
    """
    oi = identifier.Identifier(path)
    json.loads(oi.path_abs('json'))

@ddr.command()
@click.argument('path')
@click.option('--user','-u', help='User name.')
@click.option('--mail','-m', help='User e-mail address.')
@click.option('--log','-l', help='Log file path.')
def save(path, user, mail, log):
    """Stage and commit specified object(s).
    
    NOTE: Only adds .json files - does not add children!
    TODO Let user specify multiple files in path e.g. ddr-testing-123-(1,2,4)
    """
    set_logging(log)
    oi = identifier.Identifier(path)
    # TODO hard-coded models!!
    if oi.model == 'collection':
        exit,status,updated_files = oi.object().save(
            user, mail, agent=AGENT, commit=True
        )
    elif oi.model == 'entity':
        exit,status,updated_files = oi.object().save(
            user, mail, agent=AGENT,
            collection=oi.collection().object(),
            commit=True
        )
    elif oi.model == 'file':
        exit,status,updated_files = oi.object().save(
            user, mail, agent=AGENT,
            collection=oi.collection().object(),
            parent=oi.parent().object(),
            commit=True
        )
    click.echo(status)
    sys.exit(exit)

@ddr.command()
@click.argument('path')
def delete(path):
    """Delete specified object.
    """
    click.echo('NOT IMPLEMENTED YET')

@ddr.command()
@click.argument('collection')
@click.option('--log','-l', help='Log file path.')
def status(collection, log):
    """Report git status of collection repository.
    """
    set_logging(log)
    msg = commands.status(
        identifier.Identifier(collection).object()
    )
    exit = 0
    click.echo(msg)

@ddr.command()
@click.argument('collection')
@click.option('--log','-l', help='Log file path.')
def astatus(collection, log):
    """Report git annex status of collection repository.
    """
    set_logging(log)
    msg = commands.annex_status(
        identifier.Identifier(collection).object()
    )
    exit = 0
    click.echo(msg)

#@ddr.command()
#@click.argument('collection')
#@click.option('--file','-f', help='Relative path to file.')
#@click.argument('file')
#@click.option('--log','-l', help='Log file path.')
#def push(collection, file, log):
#    """Push a git-annex file to hub server.
#    """
#    set_logging(log)
#    exit,msg = commands.annex_push(
#        identifier.Identifier(collection).object(), file
#    )
#    click.echo(msg); sys.exit(exit)

#@ddr.command()
#@click.argument('collection')
#@click.option('--file','-f', help='Relative path to file.')
#@click.option('--log','-l', help='Log file path.')
#def pull(collection, file, log):
#    """Pull a git-annex file from hub server.
#    """
#    set_logging(log)
#    exit,msg = commands.annex_pull(
#        identifier.Identifier(collection).object(), file
#    )
#    click.echo(msg); sys.exit(exit)

@ddr.command()
@click.argument('collection')
@click.option('--user','-u', help='User name.')
@click.option('--mail','-m', help='User e-mail address.')
@click.option('--log','-l', help='Log file path.')
def sync(collection, user, mail, log):
    """Sync collection repository with hub server
    """
    set_logging(log)
    exit,msg = commands.sync(
        user, mail, identifier.Identifier(collection).object()
    )
    click.echo(msg); sys.exit(exit)

#@ddr.command()
#@click.option('--groupfile','-g', help='Absolute path to group file.')
#@click.option('--locbase','-b', help='Absolute path to local base dir, in which repos will be stored.')
#@click.option('--locname','-n', help='Local name.')
#@click.option('--rembase','-B', help='Absolute path to dir containing remote repos from POV of local base dir.')
#@click.option('--remname','-N', help='Remote name.')
#@click.option('--log','-l', help='Log file path.')
#def syncgrp(groupfile, locbase, locname, rembase, remname, log):
#    """Sync group
#    """
#    set_logging(log)
#    exit,msg = commands.sync_group(
#        groupfile, locbase, locname, rembase, remname
#    )
#    click.echo(msg); sys.exit(exit)


def set_logging(log):
    if log and (
            os.path.exists(log) or os.path.exists(os.path.basename(log))
    ):
        logging.basicConfig(
            format=LOGGING_FORMAT,
            datefmt=LOGGING_DATEFMT,
            level=logging.DEBUG,
            filename=log
        )
