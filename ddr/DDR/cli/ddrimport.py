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
from DDR import csvfile
from DDR import dvcs
from DDR import fileio
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
    headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv_path))
    run_checks(
        model, ci, csv_path, headers, rowds, csv_errs,
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
@click.option('--dryrun','-d', is_flag=True, help="Simulated run-through; don't modify files.")
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
@click.option('--dryrun','-d', is_flag=True, help="Simulated run-through; don't modify files.")
# TODO @click.option('--fromto', '-F', help="Only import specified rows. Use Python list syntax e.g. '523:711' or ':200' or '100:'.")
# TODO @click.option('--log','-l', help='Log addfile to this path')
def entity(csv, collection, user, mail, username, password, idservice, nocheck, dryrun):
    """Import entity/object records from CSV.
    """
    start = datetime.now()
    
    csv_path,collection_path = make_paths(csv, collection)
    ci = identifier.Identifier(collection_path)
    logging.debug(ci)
    headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv_path))
    if not nocheck:
        run_checks(
            'entity', ci, csv_path, headers, rowds, csv_errs,
            idservice_api_login(username, password, idservice),
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
@click.option('--dryrun','-d', is_flag=True, help="Simulated run-through; don't modify files.")
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
    headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv_path))
    if not nocheck:
        run_checks(
            'file', ci, csv_path, headers, rowds, csv_errs,
            log_path=log,
        )
    row_start,row_end = rows_start_end(fromto)
    imported = batch.Importer.import_files(
        csv_path=csv_path,
        rowds=rowds,
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

def run_checks(model, ci, csv_path, headers, rowds, csv_errs, log_path=None, idservice_client=None):
    """run checks on the CSV doc,repo and quit if errors
    """
    logging.info('Validating headers')
    header_errs = batch.Checker.validate_csv_headers(model, headers, rowds)
    logging.info('Validating rows')
    rowds_errs = batch.Checker.validate_csv_rowds(model, headers, rowds)
    logging.info('Validating object IDs')
    id_errs = batch.Checker.validate_csv_identifiers(rowds)
    file_errs = []
    if model == 'file':
        logging.info('Validating files')
        file_errs = batch.Checker.validate_csv_files(csv_path, rowds)
    for err in csv_errs:
        logging.error(f'CSV: {err}')
    for key,val in header_errs.items():
        logging.error(f"CSV header: {key}: {','.join(val)}")
    for err in id_errs:
        logging.error(f"- {err}")
    for err,items in rowds_errs.items():
        logging.error(err)
        for item in items:
            logging.error(f"- {item}")
    for err in file_errs:
        logging.error(f"- {err}")
    # repository
    staged,modified = batch.Checker.check_repository(ci)
    for f in staged: logging.error(f'staged: {f}')
    for f in modified: logging.error(f'modified: {f}')
    if csv_errs or id_errs or header_errs or rowds_errs or file_errs or staged or modified:
        logging.error(f'Quitting--see log for error(s).')
        sys.exit(1)
    if model in ['file']:
        # files with missing parent entities
        entities,missing_entities = batch.Importer._existing_bad_entities(
            batch.Importer._eidentifiers(
                batch.Importer._fid_parents(
                    batch.Importer._fidentifiers(rowds, ci), rowds, ci)))
        if missing_entities:
            for e in missing_entities:
                logging.error(f'Entity {e} missing')
            raise Exception(
                f'{len(bad_entities)} entities could not be loaded! - IMPORT CANCELLED!'
            )
    if (model == 'entity') and idservice_client:
        chkeids = batch.Checker.check_eids(rowds, ci, idservice_client)
        for err in chkeids:
            logging.error(f'entity ID?: {err}')
        if chkeids:
            sys.exit(1)

def rows_start_end(fromto):
    """Returns start/end rows, or the first/last rows
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
