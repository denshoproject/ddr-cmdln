HELP = """
ddrnames - Export names from repositories to be matched with NamesDB records
\b
Dump creators and persons names from entire DDR collections to CSV format.
Examples:
    ddrnames dump creators /var/www/media/ddr/ddr-csujad-30 > /tmp/ddr-csujad-30-creators.csv
    ddrnames dump persons /var/www/media/ddr/ddr-densho-10 > /tmp/ddr-densho-10-persons.csv
\b
Feed output of this command to `namesdb searchmulti` for match recommendations.
Examples:
    namesdb searchmulti --elastic /tmp/ddr-densho-10-persons.csv > /tmp/ddr-densho-10-persons-searchmulti.csv
\b
When you have matched some names, load the output of `namesdb searchmulti` back
into your collection.  `ddrnames load` only cares about these fields:
"objectid", "namepart", "nr_id", "matching"
Examples: 
    ddrnames load persons /tmp/ddr-densho-10-persons-searchmulti.csv /var/www/media/ddr/ddr-densho-10
"""

import logging
from pathlib import Path
import sys

import click

from DDR import config
from DDR import csvfile
from DDR import fileio
from DDR.identifier import Identifier
from DDR.models.common import load_json_lite
from DDR import util


@click.group()
def ddrnames():
    pass


@ddrnames.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


@ddrnames.command()
def conf():
    """Print configuration settings.
    
    More detail since you asked.
    """
    pass


PERSONS_FIELDNAMES = ['creators','persons']

@ddrnames.command()
#@click.option('--datasette','-d', default=config.DATASETTE, help='Datasette HOST:IP.')
@click.argument('fieldname')
@click.argument('collection')
def dump(fieldname, collection):
    """Returns creators/person/etc names from all records in a collection
    """
    assert fieldname in PERSONS_FIELDNAMES
    # all the .jsons in collection
    # for each one, extract id and field
    headers = ['id', 'fieldname', 'name']
    click.echo(fileio.write_csv_str(headers))
    for oid,name in _read_collection_files(collection, fieldname):
        row = [oid, fieldname, name]
        click.echo(fileio.write_csv_str(row))

def _read_collection_files(collection_path, fieldname):
    """Returns an OID and name for each creator or person in collection
    """
    # collection
    ci = Identifier(collection_path)
    oid,names = _extract_field_values(ci.path_abs('json'), fieldname)
    for name in names:
        yield (oid, name)
    # objects
    for path in util.natural_sort([
        path for path in util.find_meta_files(
            basedir=collection_path, model='entity', recursive=1, force_read=1
        )
    ]):
        oid,names = _extract_field_values(path, fieldname)
        for name in names:
            yield (oid, name)

def _extract_field_values(path, fieldname):
    """Extracts individual creators,persons values from JSON document
    """
    oi = Identifier(path)
    jsonlines = load_json_lite(path, oi.model, oi.id)
    jsonlines.pop(0)
    values = []
    for data in jsonlines:
        for fname,value in data.items():
            if not value:
                break
            if fname == fieldname:
                for item in value:
                    # creators
                    if isinstance(item, dict):
                        values.append(item['namepart'])
                    # persons
                    elif item:
                        values.append(item)
                break
    return oi.id,values


@ddrnames.command()
@click.argument('fieldname')
@click.argument('csv')
@click.argument('collection')
@click.option('--user','-u', help='(required for commit) Git user name.')
@click.option('--mail','-m', help='(required for commit) Git user e-mail address.')
@click.option('--save','-s', is_flag=True, help="Save changes.")
@click.option('--commit','-c', is_flag=True, help="Commit changes.")
def load(fieldname, csv, collection, user, mail, save, commit):
    """Read CSV and update person/creators fields, matching nr_ids
    
    Reads output of the `ddrnames export` command
    """
    if not (fieldname in PERSONS_FIELDNAMES):
        click.echo(f'ERROR: "{fieldname}" is not a valid field name.')
        sys.exit(1)
    if (save or commit) and not (user and mail):
        click.echo(f'ERROR: --user and --mail required to save or commit changes.')
        sys.exit(1)
    AGENT = 'ddrnames load'
    ci = Identifier(collection)
    click.echo(f'Collection {ci.path_abs()}')
    # load data from CSV
    click.echo(f'Loading data from {csv}')
    headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csv))
    num_rowds = len(rowds)
    click.echo(f'{num_rowds} rows')
    # group CSV data by objectid and namepart
    click.echo(f'Grouping data...')
    objects_by_id = {}
    while(rowds):
        rowd = rowds.pop()
        # skip rows that don't have a match value
        if not rowd['matching']:
            continue
        # remove sample field
        if rowd.get('sample'):
            rowd.pop('sample')
        oid = rowd['objectid']
        namepart = rowd['namepart']
        if not objects_by_id.get(oid):
            objects_by_id[oid] = {}
        objects_by_id[oid][namepart] = rowd
    # update existing persons/creators data
    click.echo(f'Updating objects...')
    collection_parent_dir = str(Path(collection).parent)
    for oid in sorted(objects_by_id.keys()):
        o = Identifier(oid, collection_parent_dir).object()
        for n,person in enumerate(getattr(o, fieldname)):
            n += 1
            namepart = person['namepart']
            if objects_by_id.get(oid) and objects_by_id[oid].get(namepart):
                data = objects_by_id[oid][namepart]
                # do not remove any data - only add nr_id and matching
                person['nr_id'] = data['nr_id']
                person['matching'] = data['matching']
                click.echo(f"up {o.path_abs} {n} {person}")
            else:
                click.echo(f"   {o.path_abs} {n} {person}")
        if save:
            result = o.save(git_name=user, git_mail=mail, agent=AGENT, commit=commit)
            logging.debug(result)
    # load object
    # parse the specified field and update persons that are updated
    # identifiy by oid:namepart
    # Q: how to *remove* nr_id if we discover they're *not* a match?
