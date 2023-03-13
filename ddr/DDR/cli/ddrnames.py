HELP = """
ddrnames - Export names from repositories to be matched with NamesDB records
\b
Export creators and persons names from entire DDR collections in CSV format.
Examples:
    ddrnames export creators /var/www/media/ddr/ddr-csujad-30
    ddrnames export persons /var/www/media/ddr/ddr-densho-10
\b
Feed output of this command to `namesdb searchmulti` for match recommendations.
"""

import click

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


@ddrnames.command()
#@click.option('--datasette','-d', default=config.DATASETTE, help='Datasette HOST:IP.')
@click.argument('fieldname')
@click.argument('collection')
def export(fieldname, collection):
    """Returns creators/person/etc names from all records in a collection
    """
    assert fieldname in ['creators','persons']
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
