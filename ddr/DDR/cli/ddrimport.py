HELP = """
Run "ddrimport COMMAND --help" for a listing of each subcommand's
arguments and options.

Verify CSV file, do various integrity checks.
    $ ddrimport check /tmp/ddr-test-123-entity.csv /PATH/TO/ddr/ddr-test-123/

Import entity records.
    $ ddrimport entity /tmp/ddr-test-123-entity.csv /PATH/TO/ddr/ddr-test-123/

Import file records.  Note that slightly different fields are required
for "external" files than for normal ones for which binaries will be
imported.
    $ ddrimport file /tmp/ddr-test-123-file.csv /PATH/TO/ddr/ddr-test-123/

Register entity IDs with the ID service API:
    $ ddrimport register /tmp/ddr-test-123-entity.csv /PATH/TO/ddr/ddr-test-123

Most commands require authentication with the ID service. You can specify
username and/or password at the commandline to skip the prompts:
    $ ddrimport check -U gjost -P REDACTED ...

You can send all add-file log entries to the same file:
    $ ddrimport file -L /tmp/mylogfile.log ...

ID service username and password can be exported to environment variables:
    $ export DDRID_USER='gjost'
    $ export DDRID_PASS='REDACTED'

Please see "ddrexport help" for information on exporting CSV files.
---"""

from datetime import datetime
import getpass
import logging
logger = logging.getLogger(__name__)
import os
import sys
import traceback

import click

from DDR import config
from DDR import batch
from DDR import dvcs
from DDR import identifier
from DDR import idservice

IDSERVICE_ENVIRONMENT_USERNAME = 'DDRID_USER'
IDSERVICE_ENVIRONMENT_PASSWORD = 'DDRID_PASS'
AGENT = 'ddr-import'

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)


def logprint(level, msg, ts=True):
    try:
        if ts:
            click.echo('%s %s' % (datetime.now(), msg))
        else:
            click.echo(msg)
    except UnicodeEncodeError:
        click.echo('ERROR: UnicodeEncodeError')
    if   level == 'debug': logging.debug(msg)
    elif level == 'info': logging.info(msg)
    elif level == 'error': logging.error(msg)


@click.group()
def ddrimport():
    """Imports new entities or files with data from CSV files.
    
    Type "ddrimport COMMAND --help" for more information on each command.
    """
    pass


@ddrimport.command()
def help():
    """Detailed help and usage examples.
    """
    click.echo(HELP)


@ddrimport.command()
@click.argument('model')
@click.argument('csv')
@click.argument('collection')
@click.option('--username','-U', help='ID service username. Use flag to avoid being prompted.')
@click.option('--password','-P', help='ID service password. Use flag to avoid being prompted. Passwords args will remain in ~/.bash_history.')
@click.option('--idservice','-i', help='Override URL of ID service in configs.')
def check(model, csv, collection, username, password, idservice):
    """Validates CSV file, performs integrity checks.
    """
    start = datetime.now()
    
    csv_path,collection_path = make_paths(csv, collection)
    ci = identifier.Identifier(collection_path)
    logging.debug(ci)
    run_checks(
        model, csv_path, ci, config.VOCABS_URL,
        idservice_api_login(username, password, idservice)
    )
    
    finish = datetime.now()
    elapsed = finish - start
    logging.info('DONE - %s elapsed' % elapsed)


@ddrimport.command()
@click.argument('csv')
@click.argument('collection')
@click.option('--username','-U', help='ID service username. Use flag to avoid being prompted.')
@click.option('--password','-P', help='ID service password. Use flag to avoid being prompted. Passwords args will remain in ~/.bash_history.')
@click.option('--idservice','-i', help='Override URL of ID service in configs.')
@click.option('--dryrun','-d', help="Simulated run-through; don't modify files.")
def register(csv, collection, username, password, idservice, dryrun):
    """Registers entities in CSV with ID service.
    """
    start = datetime.now()
    
    csv_path,collection_path = make_paths(csv, collection)
    ci = identifier.Identifier(collection_path)
    logging.debug(ci)
    idservice_client = idservice_api_login(username, password, idservice)
    batch.Importer.register_entity_ids(
        csv_path, ci, idservice_client, dryrun
    )
    
    finish = datetime.now()
    elapsed = finish - start
    logging.info('DONE - %s elapsed' % elapsed)


@ddrimport.command()
@click.argument('csv')
@click.argument('collection')
@click.option('--user','-u', help='(required for commit) Git user name.')
@click.option('--mail','-m', help='(required for commit) Git user e-mail address.')
@click.option('--username','-U', help='ID service username. Use flag to avoid being prompted.')
@click.option('--password','-P', help='ID service password. Use flag to avoid being prompted. Passwords args will remain in ~/.bash_history.')
@click.option('--idservice','-i', help='Override URL of ID service in configs.')
@click.option('--nocheck','-N', is_flag=True, help="Disable checking/validation (may take time on large collections).")
@click.option('--dryrun','-d', help="Simulated run-through; don't modify files.")
# TODO @click.option('--fromto', '-F', help="Only import specified rows. Use Python list syntax e.g. '523:711' or ':200' or '100:'.")
# TODO @click.option('--log','-l', help='Log addfile to this path')
def entity(csv, collection, user, mail, username, password, idservice, nocheck, dryrun):
    """Import entity/object records from CSV.
    """
    start = datetime.now()
    
    csv_path,collection_path = make_paths(csv, collection)
    ci = identifier.Identifier(collection_path)
    logging.debug(ci)
    if not nocheck:
        idservice_client = idservice_api_login(username, password, idservice)
        run_checks(
            'entity', csv_path, ci, config.VOCABS_URL, idservice_client
        )
    #row_start,row_end = rows_start_end(fromto)
    imported = batch.Importer.import_entities(
        csv_path=csv_path,
        cidentifier=ci,
        vocabs_url=config.VOCABS_URL,
        git_name=user,
        git_mail=mail,
        agent=AGENT,
        #log_path=log,
        dryrun=dryrun,
        #row_start=row_start,
        #row_end=row_end,
    )
    
    finish = datetime.now()
    elapsed = finish - start
    logging.info('DONE - %s elapsed' % elapsed)


@ddrimport.command()
@click.argument('csv')
@click.argument('collection')
@click.option('--user','-u', help='(required for commit) Git user name.')
@click.option('--mail','-m', help='(required for commit) Git user e-mail address.')
@click.option('--nocheck','-N', is_flag=True, help="Disable checking/validation (may take time on large collections).")
@click.option('--dryrun','-d', help="Simulated run-through; don't modify files.")
@click.option('--fromto', '-F', help="Only import specified rows. Use Python list syntax e.g. '523:711' or ':200' or '100:'.")
@click.option('--log','-l', help='(optional) Log addfile to this path')
# TODO @click.option('--nocheck','-N', help="Disable checking/validation (may take time on large collections).")
def file(csv, collection, user, mail, nocheck, dryrun, fromto, log):
    """Import file records from CSV.
    """
    start = datetime.now()
    
    csv_path,collection_path = make_paths(csv, collection)
    ci = identifier.Identifier(collection_path)
    logging.debug(ci)
    if not nocheck:
        run_checks(
            'file', csv_path, ci, config.VOCABS_URL, idservice_client=None
        )
    row_start,row_end = rows_start_end(fromto)
    imported = batch.Importer.import_files(
        csv_path=csv_path,
        cidentifier=ci,
        vocabs_url=config.VOCABS_URL,
        git_name=user,
        git_mail=mail,
        agent=AGENT,
        log_path=log,
        dryrun=dryrun,
        row_start=row_start,
        row_end=row_end,
    )
    
    finish = datetime.now()
    elapsed = finish - start
    logging.info('DONE - %s elapsed' % elapsed)


@ddrimport.command()
@click.argument('collection')
@click.option('--remove', '-R', is_flag=True, help="Remove untracked files.")
def clean(collection, remove):
    """TODO ddrimport cleanup subcommand docs
    """
    start = datetime.now()
    
    # ensure we have absolute paths (CWD+relpath)
    collection_path = os.path.abspath(os.path.normpath(collection))
    # Check args
    if not (os.path.isdir(collection_path)):
        print('ddrimport: collection path must be a directory.')
        sys.exit(1)
    if not os.path.exists(collection_path):
        print('ddrimport: Collection does not exist.')
        sys.exit(1)
    
    repo = dvcs.repository(collection)
    logging.debug('Resetting staged files')
    dvcs.reset(repo)
    logging.debug('Reverting modified files')
    dvcs.revert(repo)
    if remove:
        logging.debug('Removing untracked files')
        dvcs.remove_untracked(repo)
    status = dvcs.repo_status(repo)
    logging.debug('status\n%s' % status)
    
    finish = datetime.now()
    elapsed = finish - start
    logging.info('DONE - %s elapsed' % elapsed)


def make_paths(csv, collection):
    """Massage path args, die if paths missing.
    """
    # ensure we have absolute paths (CWD+relpath)
    csv_path = os.path.abspath(os.path.normpath(csv))
    collection_path = os.path.abspath(os.path.normpath(collection))
    # Check args
    if not os.path.exists(csv_path):
        print('ddrimport: CSV file does not exist.')
        sys.exit(1)
    if not os.path.exists(collection_path):
        print('ddrimport: Collection does not exist.')
        sys.exit(1)
    if not (os.path.isfile(csv_path) and os.path.isdir(collection_path)):
        print('ddrimport: CSV filename comes before collection.')
        sys.exit(1)
    return csv_path,collection_path

def run_checks(model, csv_path, ci, vocabs_url, idservice_client):
    """run the actual checks on the CSV doc,repo
    """
    try:
        chkcsv = batch.Checker.check_csv(model, csv_path, ci, vocabs_url)
        chkrepo = batch.Checker.check_repository(ci)
        if (chkcsv.get('model') == 'entity') and idservice_client:
            chkeids = batch.Checker.check_eids(chkcsv['rowds'], ci, idservice_client)
    except batch.InvalidCSVException as err:
        logging.error(err)
        sys.exit(1)

def rows_start_end(fromto):
    """
    @param fromto: str "NUM0:NUM1"
    """
    row_start = 0
    row_end = 9999999
    if fromto:
        rowstart,rowend = fromto.split(':')
        if rowstart:
            row_start = int(rowstart)
        if rowend:
            row_end = int(rowend)
    return row_start,row_end

def log_error(err, debug=False):
    """Print Exception message to log, or traceback to console
    
    @param err: Exception
    @param debug: boolean If True, print traceback to console
    """
    if debug:
        print('************************************************************************')
        traceback.print_exc()
        print('************************************************************************')
    else:
        logging.error('************************************************************************')
        logging.error(err)
        logging.error('************************************************************************')

def idservice_api_login(username=None, password=None, url=None):
    """Login to ID service, return auth token and user info; prompt if necessary
    
    @param username: str
    @param password: str
    @param url: str
    @returns: idservice.IDServiceClient
    """
    
    if username:
        logging.debug('Username: %s' % username)
    elif os.environ.get(IDSERVICE_ENVIRONMENT_USERNAME):
        username = os.environ.get(IDSERVICE_ENVIRONMENT_USERNAME)
        logging.debug('Username: %s (environ)' % username)
    else:
        username = input('Username: ')
    if not username:
        logging.error('No username!')
        sys.exit(1)
    
    if password:
        redacted = ''.join(['*' for n in password])
        logging.debug('Password: %s' % redacted)
    elif os.environ.get(IDSERVICE_ENVIRONMENT_PASSWORD):
        password = os.environ.get(IDSERVICE_ENVIRONMENT_PASSWORD)
        redacted = ''.join(['*' for n in password])
        logging.debug('Password: %s (environ)' % redacted)
    else:
        password = getpass.getpass(prompt='Password: ')
    if not password:
        logging.error('No password!')
        sys.exit(1)
    
    ic = idservice.IDServiceClient()
    status1,reason1 = ic.login(username, password, url)
    if status1 != 200:
        logging.error('Login failed[1]: %s %s' % (status1,reason1))
        sys.exit(1)
    return ic
