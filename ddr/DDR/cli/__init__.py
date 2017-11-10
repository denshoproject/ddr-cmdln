MAN = """
ddrindex - publish DDR content to Elasticsearch; debug Elasticsearch

Index Management: create, destroy, alias, mappings, status, reindex
Publishing:       vocabs, post, postjson, index
Debugging:        config, get, exists, search

By default the command uses DOCSTORE_HOSTS and DOCSTORE_INDEX from the config
file.  Use the --hosts and --index options to override these values.

EXAMPLES

Initialize index (creates index and adds mappings)
  $ ddrindex create
Create a bare index (no mappings)
  $ ddrindex create --bare --index INDEXNAME
Add/update mappings
  $ ddrindex mappings

Publish vocabularies
  $ ddrindex vocabs /opt/ddr-vocab/api/0.2

Set or remove an alias
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev --delete --confirm

Post repository and organization:
  $ ddrindex post /var/www/media/ddr/REPO/
  $ ddrindex post /var/www/media/ddr/REPO-ORG/

Post arbitrary JSON documents:
  $ ddrindex postjson narrators DOCUMENTID /etc/ddr/ddr-defs/vocab/narrators.json

Post a collection to public site (only public and completed)
  $ ddrindex index /var/www/media/ddr/ddr-testing-123

Post a collection locally (all documents)
  $ ddrindex index /var/www/media/ddr/ddr-testing-123 --all

Check status
  $ ddrindex status

Get document
  $ ddrindex get collection ddr-testing-123
See if document exists
  $ ddrindex exists collection ddr-testing-123

Search
  $ ddrindex search collection,entity Minidoka

Remove document
  $ ddrindex delete DOCTYPE DOCUMENTID

Delete existing index
  $ ddrindex destroy --index documents --confirm

Reindex
  $ ddrindex reindex --index source --target dest
"""

from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)

import click
import elasticsearch

from DDR import config
from DDR import docstore
from DDR import identifier


def logprint(level, msg):
    try:
        click.echo('%s %s' % (datetime.now(), msg))
    except UnicodeEncodeError:
        click.echo('ERROR: UnicodeEncodeError')
    if   level == 'debug': logging.debug(msg)
    elif level == 'info': logging.info(msg)
    elif level == 'error': logging.error(msg)

def _json_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.strftime(config.DATETIME_FORMAT)
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))
    
def format_json(data, pretty=False):
    if pretty:
        return json.dumps(
            data,
            indent=4,
            separators=(',', ': '),
            default=_json_handler,
        )
    return json.dumps(data, default=_json_handler)


@click.group()
@click.option('--debug','-d', is_flag=True, default=False)
def ddrindex(debug):
    """ddrindex - publish DDR content to Elasticsearch; debug Elasticsearch
    
    \b
    By default the command uses DOCSTORE_HOSTS and DOCSTORE_INDEX from the config
    file.  Use the --hosts and --index options to override these values.
    """
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))


@ddrindex.command()
def help():
    """Print detailed help and usage examples
    """
    click.echo(MAN)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
def conf(hosts, index):
    """Print configuration settings.
    
    More detail since you asked.
    """
    docstore.Docstore(hosts, index).print_configs()


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
def create(hosts, index):
    """Create new index.
    """
    try:
        docstore.Docstore(hosts, index).create_index(index)
    except Exception as err:
        logprint('error', err)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--confirm', is_flag=True,
              help='Yes I really want to delete this index.')
def destroy(hosts, index, confirm):
    """Delete index (requires --confirm).
    
    \b
    It's meant to sound serious. Also to not clash with 'delete', which
    is for individual documents.
    """
    if confirm:
        try:
            docstore.Docstore(hosts, index).delete_index()
        except Exception as err:
            logprint('error', err)
    else:
        click.echo("Add '--confirm' if you're sure you want to do this.")


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--alias','-a', help='Alias to create.')
@click.option('--delete','-D', is_flag=True, help='Delete specified alias.')
def alias(hosts, index, alias, delete):
    """Manage aliases.
    """
    if not alias:
        click.echo("Error: no alias specified.")
        return
    if delete:
        try:
            docstore.Docstore(hosts, index).delete_alias(index=index, alias=alias)
        except Exception as err:
            logprint('error', err)
    else:
        try:
            docstore.Docstore(hosts, index).create_alias(index=index, alias=alias)
        except Exception as err:
            logprint('error', err)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
def mappings(hosts, index):
    """Push mappings to the specified index.
    """
    docstore.Docstore(hosts, index).init_mappings()


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
#@click.option('--report', is_flag=True,
#              help='Report number of records existing, to be indexed/updated.')
#@click.option('--dryrun', is_flag=True,
#              help='perform a trial run with no changes made')
#@click.option('--force', is_flag=True,
#              help='Forcibly update records whether they need it or not.')
@click.argument('path')
def vocabs(hosts, index, path):
    """Index DDR vocabulary facets and terms.
    
    \b
    Example:
      $ ddrindex vocabs /opt/ddr-local/ddr-vocab/api/0.2/
    """
    docstore.Docstore(hosts, index).post_vocabs(path=path)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--all','-a', is_flag=True, help='Include nonpublic documents (private,inprogress).')
@click.argument('path')
def post(hosts, index, all, path):
    """Post the document to Elasticsearch
    """
    oi = identifier.Identifier(path)
    document = oi.object()
    status = docstore.Docstore(hosts, index).post(document, private_ok=all)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('doctype')
@click.argument('object_id')
@click.argument('path')
def postjson(hosts, index, doctype, object_id, path):
    """Post raw JSON file to Elasticsearch (YMMV)
    """
    with open(path, 'r') as f:
        text = f.read()
    status = docstore.Docstore(hosts, index).post_json(doctype, object_id, text)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--all','-a', is_flag=True, help='Include nonpublic documents (private,inprogress).')
@click.argument('path')
def index(hosts, index, all, path):
    """Post the document and its children to Elasticsearch
    """
    if all:
        public = False
    else:
        public = True
    status = docstore.Docstore(hosts, index).index(path, recursive=True, public=public)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--recurse', is_flag=True, help='Delete documents under this one.')
@click.option('--confirm', is_flag=True, help='Yes I really want to delete these objects.')
@click.argument('doctype')
@click.argument('object_id')
def delete(hosts, index, recurse, confirm, doctype, object_id):
    """Delete the specified document from Elasticsearch
    """
    if confirm:
        click.echo(docstore.Docstore(hosts, index).delete(object_id, recursive=recurse))
    else:
        click.echo("Add '--confirm' if you're sure you want to do this.")


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('doctype')
@click.argument('object_id')
def exists(hosts, index, doctype, object_id):
    """Indicate whether the specified document exists
    """
    ds = docstore.Docstore(hosts, index)
    click.echo(ds.exists(doctype, object_id))


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--json','-j', is_flag=True, help='Print as JSON')
@click.option('--pretty','-p', is_flag=True, help='Nicely formated JSON')
@click.argument('doctype')
@click.argument('object_id')
def get(hosts, index, json, pretty, doctype, object_id):
    """Pretty-print a single document
    """
    document = docstore.Docstore(hosts, index).get(doctype, object_id)
    if json:
        click.echo(format_json(document.to_dict(), pretty=pretty))
    else:
        click.echo(document)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
def status(hosts, index):
    """Print status info.
    
    More detail since you asked.
    """
    ds = docstore.Docstore(hosts, index)
    s = ds.status()
    
    logprint('debug', '------------------------------------------------------------------------')
    logprint('debug', 'Elasticsearch')
    # config file
    logprint('debug', 'DOCSTORE_HOST  (default): %s' % config.DOCSTORE_HOST)
    logprint('debug', 'DOCSTORE_INDEX (default): %s' % config.DOCSTORE_INDEX)
    # overrides
    if hosts != config.DOCSTORE_HOST:
        logprint('debug', 'docstore_hosts: %s' % hosts)
    if index != config.DOCSTORE_INDEX:
        logprint('debug', 'docstore_index: %s' % index)
    
    try:
        pingable = ds.es.ping()
        if not pingable:
            logprint('error', "Can't ping the cluster!")
            return
    except elasticsearch.exceptions.ConnectionError:
        logprint('error', "Connection error when trying to ping the cluster!")
        return
    logprint('debug', 'ping ok')
    
    logprint('debug', 'Indexes')
    index_names = ds.es.indices.stats()['indices'].keys()
    for i in index_names:
        if i == index:
            logprint('debug', '* %s *' % i)
        else:
            logprint('debug', '- %s' % i)
    
    logprint('debug', 'Aliases')
    aliases = ds.aliases()
    if aliases:
        for index,alias in aliases:
            logprint('debug', '- %s -> %s' % (alias, index))
    else:
        logprint('debug', 'No aliases')
    
    if ds.es.indices.exists(index=index):
        logprint('debug', 'Index %s present' % index)
    else:
        logprint('error', "Index '%s' doesn't exist!" % index)
        return

    logprint('debug', 'Models')
    for doctype_class in identifier.ELASTICSEARCH_CLASSES['all']:
        #status = doctype_class['class'].init(index=self.indexname, using=self.es)
        #statuses.append( {'doctype':doctype_class['doctype'], 'status':status} )
        num = 0  #len(Page.pages())
        logprint('debug', '- {:>6} {}'.format(num, doctype_class['doctype']))


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.option('--doctypes','-t', help='One or more doctypes (comma-separated).')
@click.option('--query','-q', help='Query string, in double or single quotes.')
@click.option('--must','-m', help='AND arg(s) (e.g. "language:eng,jpn!creators.role:author").')
@click.option('--should','-s',  help ='OR arg(s) (e.g. "language:eng,jpn!creators.role:author").')
@click.option('--mustnot','-n', help='NOT arg(s) (e.g. "language:eng,jpn!creators.role:author").')
@click.option('--raw','-r', is_flag=True, help='Print raw Elasticsearch DSL and output.')
def search(hosts, index, doctypes, text, must, should, mustnot, raw):
    """
    """
    click.echo(search_results(
        docstore.Docstore(hosts, index),
        doctypes,
        text,
        must=must,
        should=should,
        mustnot=mustnot,
        raw=raw
    ))

def search_results(d, doctype, text, must=None, should=None, mustnot=None, raw=False):
    if doctype in ['*', 'all', 'all_', '_all']:
        doctypes = []
    else:
        doctypes = doctype.strip().split(',')
    
    def make_terms(arg):
        # "language:eng,jpn!creators.role:author"
        terms = []
        if arg:
            for term in arg.strip().split('!'):
                fieldname,value = term.strip().split(':')
                values = value.strip().split(',')
                terms.append(
                    {'terms': {fieldname: values}}
                )
        return terms
    
    q = docstore.search_query(
        text=text,
        must=make_terms(must),
        should=make_terms(should),
        mustnot=make_terms(mustnot),
    )
    if raw:
        click.echo(format_json(q, sort_keys=False))
    
    data = d.search(
        doctypes=doctypes,
        query=q,
        fields=['id','title'],
    )
    if raw:
        click.echo(format_json(data, sort_keys=False))
    else:
        try:
            for item in data['hits']['hits']:
                chunks = (
                    item['_id'],
                    item['_type'],
                    item['_source'].get('title', ''),
                )
                click.echo(chunks)
        except:
            click.echo(format_json(data, sort_keys=False))
