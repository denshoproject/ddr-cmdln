HELP = """
ddrindex - publish DDR content to Elasticsearch; debug Elasticsearch

Index Management: create, destroy, alias, mappings, status, reindex
Publishing:       vocabs, post, postjson, index
Debugging:        config, get, exists, search

By default the command uses DOCSTORE_HOSTS and DOCSTORE_INDEX from the
config file.  Override these values using the --hosts and --index options
or with environment variables:
  $ export DOCSTORE_HOSTS=192.168.56.1:9200
  $ export DOCSTORE_INDEX=ddrlocal-YYYYMMDDa

CORE COMMANDS

Initialize index (creates index and adds mappings)
  $ ddrindex create
Create a bare index (no mappings)
  $ ddrindex create --bare --index INDEXNAME

Publish vocabularies (used for topics, facility fields)
  $ ddrindex vocabs /opt/ddr-vocab/api/0.2

Post repository and organization:
  $ ddrindex postjson repository REPO /var/www/media/ddr/REPO/repository.json
  $ ddrindex postjson organization REPO-ORG /var/www/media/ddr/REPO-ORG/organization.json

Post an object. Optionally, publish its child objects and/or ignore publication status.
  $ ddrindex publish [--recurse] [--force] /var/www/media/ddr/ddr-testing-123

Post narrators:
  $ ddrindex narrators /opt/ddr-local/ddr-defs/narrators.json

Post arbitrary JSON files:
  $ ddrindex postjson DOCTYPE DOCUMENTID /PATH/TO/FILE.json

MANAGEMENT COMMANDS

View current settings
  $ ddrindex conf

Check status
  $ ddrindex status

Set or remove an index alias
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev --delete --confirm

Post arbitrary JSON documents:
  $ ddrindex postjson DOCTYPE DOCUMENTID /PATH/TO/DOCUMENT.json

Search
  $ ddrindex search collection,entity Minidoka

See if document exists
  $ ddrindex exists collection ddr-testing-123
Get document
  $ ddrindex get collection ddr-testing-123

Remove document
  $ ddrindex delete DOCTYPE DOCUMENTID

Delete existing index
  $ ddrindex destroy --index documents --confirm

Reindex
  $ ddrindex reindex --index source --target dest

Update mappings (or add to a bare index).
  $ ddrindex mappings
"""

from datetime import datetime
import logging
logger = logging.getLogger(__name__)

import click
import elasticsearch
import simplejson as json

from DDR import config
from DDR import docstore
from DDR import identifier


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
    By default the command uses DOCSTORE_HOSTS and DOCSTORE_INDEX from the
    config file.  Override these values using the --hosts and --index options
    or with environment variables:
      $ export DOCSTORE_HOSTS=192.168.56.1:9200
      $ export DOCSTORE_INDEX=ddrlocal-YYYYMMDDa
    """
    if debug:
        click.echo('Debug mode is on')


@ddrindex.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


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
    """Post DDR vocabulary facets and terms.
    
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
@click.argument('doctype')
@click.argument('object_id')
@click.argument('path')
def postjson(hosts, index, doctype, object_id, path):
    """Post raw JSON file to Elasticsearch (YMMV)
    
    This command is for posting raw JSON files.  If the file you wish to post
    is a DDR object, please use "ddrindex post".
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
@click.option('--recurse','-r', is_flag=True, help='Publish documents under this one.')
@click.option('--force','-f', is_flag=True, help='Publish regardless of status.')
@click.argument('path')
def publish(hosts, index, recurse, force, path):
    """Post the document and its children to Elasticsearch
    """
    status = docstore.Docstore(hosts, index).post_multi(path, recursive=recurse, force=force)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('path')
def narrators(hosts, index, path):
    """Post the DDR narrators file to Elasticsearch
    """
    status = docstore.Docstore(hosts, index).narrators(path)
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
    
    logprint('debug', '------------------------------------------------------------------------',0)
    logprint('debug', 'Elasticsearch',0)
    # config file
    logprint('debug', 'DOCSTORE_HOST  (default): %s' % config.DOCSTORE_HOST, 0)
    logprint('debug', 'DOCSTORE_INDEX (default): %s' % config.DOCSTORE_INDEX, 0)
    # overrides
    if hosts != config.DOCSTORE_HOST:
        logprint('debug', 'docstore_hosts: %s' % hosts, 0)
    if index != config.DOCSTORE_INDEX:
        logprint('debug', 'docstore_index: %s' % index, 0)
    
    try:
        pingable = ds.es.ping()
        if not pingable:
            logprint('error', "Can't ping the cluster!", 0)
            return
    except elasticsearch.exceptions.ConnectionError:
        logprint('error', "Connection error when trying to ping the cluster!", 0)
        return
    logprint('debug', 'ping ok', 0)
    
    logprint('debug', 'Indexes', 0)
    index_names = ds.es.indices.stats()['indices'].keys()
    for i in index_names:
        if i == index:
            logprint('debug', '* %s *' % i, 0)
        else:
            logprint('debug', '- %s' % i, 0)
    
    logprint('debug', 'Aliases', 0)
    aliases = ds.aliases()
    if aliases:
        for index,alias in aliases:
            logprint('debug', '- %s -> %s' % (alias, index), 0)
    else:
        logprint('debug', 'No aliases', 0)
    
    if ds.es.indices.exists(index=index):
        logprint('debug', 'Index %s present' % index, 0)
    else:
        logprint('error', "Index '%s' doesn't exist!" % index, 0)
        return

    # TODO get ddrindex status model counts to work
    logprint('debug', '(Object counts are currently unavailable)', 0)
    #logprint('debug', 'Models', 0)
    #for doctype_class in identifier.ELASTICSEARCH_CLASSES['all']:
    #    #status = doctype_class['class'].init(index=self.indexname, using=self.es)
    #    #statuses.append( {'doctype':doctype_class['doctype'], 'status':status} )
    #    num = 0  #len(Page.pages())
    #    logprint('debug', '- {:>6} {}'.format(num, doctype_class['doctype']), 0)


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
def search(hosts, index, doctypes, query, must, should, mustnot, raw):
    """
    """
    click.echo(search_results(
        d=docstore.Docstore(hosts, index),
        doctype=doctypes,
        text=query,
        must=must,
        should=should,
        mustnot=mustnot,
        raw=raw
    ))

def search_results(d, doctype, text, must=None, should=None, mustnot=None, raw=False):
    if doctype:
        if doctype in ['*', 'all', 'all_', '_all']:
            doctypes = []
        else:
            doctypes = doctype.strip().split(',')
    else:
        doctypes = []
    
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
        click.echo(format_json(q))
    
    data = d.search(
        doctypes=doctypes,
        query=q,
        fields=['id','title'],
    )
    if raw:
        click.echo(format_json(data))
    else:
        try:
            # find longest IDs and types
            len_id = 0
            for item in data['hits']['hits']:
                if len(item['_id']) > len_id:
                    len_id = len(item['_id'])
            len_type = 0
            for item in data['hits']['hits']:
                if len(item['_type']) > len_type:
                    len_type = len(item['_type'])
            
            for item in data['hits']['hits']:
                # format nicely, with padding
                _id   = item['_id']   + ' ' * ((len_id   - len(item['_id']  )) + 2)
                _type = item['_type'] + ' ' * ((len_type - len(item['_type'])) + 2)
                # TODO all objects should have title
                click.echo(_id + _type + item['_source']['title'])
        except:
            click.echo(format_json(data))
