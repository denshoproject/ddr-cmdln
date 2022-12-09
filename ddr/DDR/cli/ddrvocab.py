HELP = """
ddrvocab - Convert densho-vocab files between JSON and CSV

Vocabularies are stored in densho-vocab in a particular JSON format, but
spreadsheets are the preferred editing format.  This command converts
between the two formats.
\b
IMPORTANT: `ddrvocab csvtojson` depends on having a particular set of
columns available.  Here is a sample document:
    "id","topics"
    "title","Topics"
    "description","'DDR Topics'"
    "id","_title","title","parent_id","weight","created","modified","encyc_urls","description"
    "120","Activism and involvement [120]","Activism and involvement","0","0","2014-02-18 17:06:20-08:00","2014-02-18 17:06:20-08:00","/S.I.%20Hayakawa/",""
"""

import click

from DDR import vocab


@click.group()
def ddrvocab():
    """Convert densho-vocab files between JSON and CSV
    """
    pass

@ddrvocab.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)

@ddrvocab.command()
#@click.option('--datasette','-d', default=config.DATASETTE, help='Datasette HOST:IP.')
@click.argument('jsonpath')
def topicstocsv(jsonpath):
    """Export JSON vocabulary data to CSV
    
    \b
    Load JSON, convert to CSV, and print to console:
        ddrvocab jsontocsv /opt/densho-vocab/api/0.2/topics.json
    Load JSON, convert to CSV, and write to file:
        ddrvocab jsontocsv /opt/densho-vocab/api/0.2/topics.json > /tmp/topics-20221101.csv
    """
    index = vocab.Index()
    index.read(jsonpath)
    click.echo(index.dump_csv())

@ddrvocab.command()
#@click.option('--datasette','-d', default=config.DATASETTE, help='Datasette HOST:IP.')
@click.argument('csvpath')
def topicstojson(csvpath):
    """Convert CSV vocabulary data to JSON
    
    \b
    Load CSV, convert to JSON, and print to console:
        ddrvocab csvtojson /tmp/topics-20221101.csv
    Load CSV, convert to JSON, and write to file:
        ddrvocab csvtojson /tmp/topics-20221101.csv > /tmp/topics-20221101.json
    """
    index = vocab.Index()
    index.read(csvpath)
    click.echo(index.dump_json())
