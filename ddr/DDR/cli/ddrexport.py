HELP = """
Run "ddrexport COMMAND --help" for a listing of each subcommand's
arguments and options.

Examples:
    $ ddrexport entity /PATH/TO/ddr/ddr-testing-123 /tmp/ddr-test-123-entity.csv
    $ ddrexport file /PATH/TO/ddr/ddr-testing-123 /tmp/some-files-YYYYMMDD.csv

You can include and/or exclude objects using the --include/--exclude options.
You can use both options at the same time; --include will be applied first.
These options use Python regex syntax.
    --include=ddr-test-123            # All entities in a collection
    --include=ddr-test-123-1          # All files in an entity
    --include=ddr-test-123            # All files in a collection
You may have to escape characters in the pattern or use quotes if they have
special meaning in the bash shell, such as "(" and ")".
    --include="ddr-test-123-([1,5])"  # ddr-test-123-1 and ddr-test-123-5
    --include="ddr-test-123-([3-6])"  # ddr-test-123-3 THRU ddr-test-123-6

You can also print out blank CSV files with all fields:
    $ ddrexport -b file ...

And blank with only required fields:
    $ ddrexport -br entity ...

Please see "ddrimport help" for information on importing CSV files.
---"""

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import os
import re
import sys

import click

from DDR import config
from DDR import batch
from DDR import dvcs
from DDR import fileio
from DDR import identifier
from DDR import util

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)


@click.group()
def ddrexport():
    """Exports a DDR collection's entities or files to CSV.
    
    \b
    "ddrexport help" for usage examples.
    "ddrexport COMMAND --help" to see options for each command.
    """
    pass


@ddrexport.command()
def help():
    """Detailed help and usage examples.
    """
    click.echo(HELP)


@ddrexport.command()
@click.argument('collection')
@click.argument('destination')
@click.option('--blank','-b',  is_flag=True, help='Blank CSV, just headers.')
@click.option('--required','-r', is_flag=True, help='Required fields only.')
@click.option('--idfile','-I', help='File containing list of IDs, one per line.')
@click.option('--include','-i', help='ID(s) to include (see help for formatting).')
@click.option('--exclude','-e', help='ID(s) to exclude (see help for formatting).')
@click.option('--dryrun','-d', is_flag=True, help="Print paths but don't export anything.")
def entity(collection, destination, blank, required, idfile, include, exclude, dryrun):
    """Export entity/object records to CSV.
    """
    export(
        'entity',
        collection, destination,
        blank, required, idfile, include, exclude, dryrun
    )


@ddrexport.command()
@click.argument('collection')
@click.argument('destination')
@click.option('--blank','-b',  is_flag=True, help='Blank CSV, just headers.')
@click.option('--required','-r', is_flag=True, help='Required fields only.')
@click.option('--idfile','-I', help='File containing list of IDs, one per line.')
@click.option('--include','-i', help='ID(s) to include (see help for formatting).')
@click.option('--exclude','-e', help='ID(s) to exclude (see help for formatting).')
@click.option('--dryrun','-d', is_flag=True, help="Print paths but don't export anything.")
def file(collection, destination, blank, required, idfile, include, exclude, dryrun):
    """Export file records to CSV.
    """
    export(
        'file',
        collection, destination,
        blank, required, idfile, include, exclude, dryrun
    )


@ddrexport.command()
@click.argument('model')
@click.argument('fieldname')
@click.argument('collection')
def fieldcsv(model, fieldname, collection):
    """Export value of specified field for all model objects in collections
    
    @param model str: 
    @param fieldname str: 
    @param collection str: 
    """
    for item in batch.Exporter.export_field_csv(
            json_paths=all_paths(collection, model),
            model=model,
            fieldname=fieldname,
    ):
        click.echo(item)


def export(model, collection, destination, blank, required, idfile, include, exclude, dryrun):
    # ensure we have absolute paths (CWD+relpath)
    collection_path = os.path.abspath(collection)
    destination_path = os.path.abspath(destination)
    
    if not os.path.exists(collection_path):
        raise Exception('Collection does not exist: %s' % collection_path)
    if not os.path.exists(os.path.dirname(destination_path)):
        raise Exception('Destination directory does not exist: %s' % destination_path)
    if idfile and not os.path.exists(idfile):
        raise Exception('IDs file does not exist: %s' % idfile)

    if os.path.basename(destination_path):
        filename = destination_path
    elif os.path.isdir(destination_path):
        filename = make_path(destination_path, collection_path, model)
    if not os.access(os.path.dirname(filename), os.W_OK):
        raise Exception('Cannot write to %s.' % filename)
    logging.info('Writing to %s' % filename)
    
    start = datetime.now()
    
    paths = []
    if blank:
        logging.info('Blank: no paths')
    elif idfile:
        logging.info('Looking for paths in %s' % idfile)
        paths = make_paths(collection_path, read_id_file(idfile))
    else:
        logging.info('All paths in %s' % collection_path)
        paths = all_paths(collection_path, model)
    logging.info('found %s paths' % len(paths))

    if include:
        logging.info('Including paths: "%s"' % include)
        paths = filter_paths(paths, include)
    
    if exclude:
        logging.info('Excluding paths: "%s"' % exclude)
        before = len(paths)
        paths = filter_paths(paths, exclude, exclude=True)
        num_excluded = len(paths) - before
        logging.info('excluded %s' % abs(num_excluded))
    
    if not paths and not (blank):
        raise Exception('ERROR: Could not find metadata paths.')
    logging.info('Exporting %s paths' % len(paths))
    
    if dryrun:
        logging.info("Dry run -- no output!")
        for n,path in enumerate(paths):
            logging.info('%s/%s %s' % (n+1, len(paths), path))
    else:
        batch.Exporter.export(paths, model, filename, required_only=required)
    
    finish = datetime.now()
    elapsed = finish - start
    if dryrun:
        logging.info('DONE - (%s elapsed) (DRY RUN)' % (elapsed))
    else:
        logging.info('DONE - (%s elapsed) - %s' % (elapsed, filename))


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

def read_id_file(path):
    """Read file and return list of IDs
    
    @param path: str Absolute path to file.
    @returns: list of IDs
    """
    text = fileio.read_text(path)
    ids = [line.strip() for line in text.strip().split('\n')]
    return ids

def make_paths(collection_path, ids):
    """
    """
    basedir = os.path.dirname(collection_path)
    paths = [
        identifier.Identifier(object_id, basedir).path_abs('json')
        for object_id in ids
    ]
    return paths

def make_path(destdir, collection_path, model):
    """Assemble path for CSV file.
    
    @param destdir: str Absolute path to destination dir
    @param collection_path: str Absolute path to collection repository
    @param model: str One of ['collection', 'entity', 'file']
    """
    filename = '%s-%s-%s.csv' % (
        identifier.Identifier(collection_path).id,
        model,
        datetime.now().strftime('%Y%m%d%H%M')
    )
    return os.path.join(destdir, filename)

def filter_paths(paths, pattern, exclude=False):
    """Get metadata paths containing a regex.
    
    @param paths: list
    @param pattern: str A regular expression
    @param exclude: boolean If true, exclude paths matching pattern.
    @returns: list of absolute paths
    """
    prog = re.compile(pattern)
    if exclude:
        return [path for path in paths if not prog.search(path)]
    else:
        return [path for path in paths if prog.search(path)]

def all_paths(collection_path, model):
    """Get all .json paths for specified model.
    
    @param collection_path: str Absolute path to collection repo
    @param model: str One of ['collection', 'entity', 'file']
    """
    return util.find_meta_files(
        basedir=collection_path, model=model, recursive=1, force_read=1
    )
