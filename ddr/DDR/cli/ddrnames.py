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

import json
import logging
from pathlib import Path
import sys

import click

from DDR import config
from DDR import csvfile
from DDR import fileio
from DDR import format_json
from DDR.identifier import Identifier
from DDR.models.common import load_json_lite
from DDR import util

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
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


@ddrnames.command()
@click.argument('operation')
@click.argument('jsonpath')
@click.argument('csvpath')
@click.option('--verbose','-v', is_flag=True, help="Print more output.")
def narrators(operation, jsonpath, csvpath, verbose):
    """Transform densho-vocabs/narrators.json to CSV in ddrnames dump - or back
    
    PATH is either the .json or the .csv file. Depending on which you choose
    it converts to the other.
    Examples:
    ddrnames narrators dump /opt/densho-vocab/api/0.2/narrators.json /tmp/narrators.csv
    ddrnames narrators load /opt/densho-vocab/api/0.2/narrators.json /tmp/narrators-matched.csv

    "id","fieldname","name"
    "ddr-densho-10-1","persons","Yasuda, Mitsu"
    "ddr-densho-10-1","persons","Tanaka, Cherry"
    "ddr-densho-10-2","persons","Matsumoto, Takako"
    "ddr-densho-10-2","persons","Sata, Elsie"
    
    """
    jsonpath = Path(jsonpath)
    if operation not in ['dump','load']:
        click.echo('ERROR: OPERATION must be either "dump" or "load".')
        sys.exit(1)
    
    elif operation == 'dump':
        # dump narrators.json to csv
        with jsonpath.open('r') as f1:
            data = json.loads(f1.read())
        # all the .jsons in collection
        # for each one, extract id and field
        headers = ['id', 'fieldname', 'name']
        click.echo(fileio.write_csv_str(headers))
        rows = []
        for narrator in data['narrators']:
            id = narrator['id']
            name = ' '.join([
                n for n in [
                    narrator['last_name'],
                    narrator['first_name'],
                    narrator['middle_name'],
                    narrator['nickname']
                ] if n
            ])
            row = [id, 'person', name]
            click.echo(fileio.write_csv_str(row))
            rows.append(row)
        fileio.write_csv(csvpath, headers, rows)
    
    elif operation == 'load':
        # load data from CSV
        with jsonpath.open('r') as f1:
            data = json.loads(f1.read())
        click.echo(f'Loading data from {csvpath}')
        headers,rowds,csv_errs = csvfile.make_rowds(fileio.read_csv(csvpath))
        # group CSV data by objectid and namepart
        click.echo(f'Grouping data...')
        objects_by_id = {}
        while(rowds):
            rowd = rowds.pop()
            # skip rows that don't have a match value
            if (not rowd['matching']) and not (rowd['matching'] == 'match'):
                continue
            # remove sample field
            if rowd.get('sample'):
                rowd.pop('sample')
            oid = rowd['objectid']
            namepart = rowd['namepart']
            if not objects_by_id.get(oid):
                objects_by_id[oid] = {}
            objects_by_id[oid] = rowd
        # update narrators data
        click.echo(f'Updating narrators...')
        matched_narrator_ids = objects_by_id.keys()
        for narrator in data['narrators']:
            if narrator['id'] in matched_narrator_ids:
                nr_id = objects_by_id[narrator['id']]['nr_id']
                narrator['nr_id'] = nr_id
                print(narrator['nr_id'], narrator['id'], narrator['display_name'])
            elif verbose:
                print('               ', narrator['id'], narrator['display_name'])
        # write file
        try:
            with jsonpath.open('w') as f:
                f.write(format_json(data, sort_keys=False))
            click.echo('')
            click.echo('File updated.')
            click.echo(f'Now log in as a sudo-capable user and restore permissions:')
            owner = jsonpath.parent.owner()
            group = jsonpath.parent.group()
            click.echo(f'    sudo chown {owner}.{group} {jsonpath}')
            click.echo('Check file differences using `git diff -w`.')
        except PermissionError:
            # complain if narrators.json not writable
            click.echo(f'ERROR: {jsonpath} is not writable by the ddr user.')
            click.echo(f'Log in as a sudo-capable user and fix it thusly:')
            click.echo(f'    sudo chown ddr.ddr {jsonpath}')
