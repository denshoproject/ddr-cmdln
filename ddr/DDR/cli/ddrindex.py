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
  $ ddrindex repo REPO /var/www/media/ddr/REPO/repository.json
  $ ddrindex org REPO-ORG /var/www/media/ddr/REPO-ORG/organization.json

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
from DDR import fileio
from DDR import identifier
from DDR import models
from DDR import search as search_


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
@click.option('--debug', '-d', is_flag=True, help='Display current mappings.')
def mappings(hosts, index, debug):
    """Push mappings to the specified index or display.
    """
    if debug:
        data = docstore.Docstore(hosts, index).get_mappings(raw=1)
        text = json.dumps(data)
        click.echo(text)
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
    status = docstore.Docstore(hosts, index).post_json(
        doctype,
        object_id,
        fileio.read_text(path)
    )
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
def repo(hosts, index, path):
    """Post the repository record to Elasticsearch
    """
    status = docstore.Docstore(hosts, index).repo(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('path')
def org(hosts, index, path):
    """Post the organization record to Elasticsearch
    """
    status = docstore.Docstore(hosts, index).org(path)
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
@click.option('--doctypes','-t',
              default='collection,entity,segment,file',
              help='One or more doctypes (comma-separated).')
@click.option('--parent','-P',
              default='',
              help='ID of parent object.')
@click.option('--filters','-f',
              default='', multiple=True,
              help='Filter on certain fields (FIELD:VALUE,VALUE,...).')
@click.option('--limit','-l',
              default=config.RESULTS_PER_PAGE,
              help='Results page size.')
@click.option('--offset','-o',
              default=0,
              help='Number of initial results to skip (use with limit).')
@click.option('--page','-p',
              default=0,
              help='Which page of results to show.')
@click.option('--aggregations','-a',
              is_flag=True, default=False,
              help='Show filter aggregations for result set.')
@click.option('--raw','-r',
              is_flag=True, default=False,
              help='Raw Elasticsearch output.')
@click.argument('fulltext')
def search(hosts, index, doctypes, parent, filters, limit, offset, page, aggregations, raw, fulltext):
    """Fulltext search using Elasticsearch query_string syntax.
    
    \b
    Examples:
        $ ddrindex search seattle
        $ ddrindex search "fusa OR teruo"
        $ ddrindex search "fusa AND teruo"
        $ ddrindex search "+fusa -teruo"
        $ ddrindex search "title:seattle"
    
    Note: Quoting inside strings is not (yet?) supported in the
    command-line version.
    
    \b
    Specify parent object and doctype/model:
        $ ddrindex search seattle --parent=ddr-densho-12
        $ ddrindex search seattle --doctypes=entity,segment
    
    \b
    Filter on certain fields (filters may repeat):
        $ ddrindex search seattle --filter=topics:373,27
        $ ddrindex search seattle -f=topics:373 -f facility=12
    
    Use the --aggregations/-a flag to display filter aggregations,
    with document counts, filter keys, and labels.
    """
    if filters:
        data = {}
        for f in filters:
            field,v = f.split(':')
            values = v.split(',')
            data[field] = values
        filters = data
    else:
        filters = {}
        
    if page and offset:
        click.echo("Error: Specify either offset OR page, not both.")
        return
    if page:
        thispage = int(page)
        offset = search_.es_offset(limit, thispage)
    
    searcher = search_.Searcher(
        mappings=identifier.ELASTICSEARCH_CLASSES_BY_MODEL,
        fields=identifier.ELASTICSEARCH_LIST_FIELDS,
    )
    searcher.prepare(
        fulltext=fulltext,
        models=doctypes.split(','),
        parent=parent,
        filters=filters,
    )
    results = searcher.execute(limit, offset)
    
    # print raw results
    if raw:
        click.echo(results.to_dict(
            list_function=search_.format_object,
        ))
        return
    
    # print formatted results
    # find longest ID
    longest_url = ''
    for result in results.objects:
        url = '/'.join([
            result.meta.index,
            result.meta.doc_type,
            result.meta.id,
        ])
        if len(url) > len(longest_url):
            longest_url = url
    num = len(results.objects)
    for n,result in enumerate(results.objects):
        url = '/'.join([
            result.meta.index,
            result.meta.doc_type,
            result.meta.id,
        ])
        TEMPLATE = '{n}/{num}  {url}{pad}  {title}'
        out = TEMPLATE.format(
            n=n+1,
            num=num,
            url=url,
            pad=' ' * (len(longest_url) - len(url)),
            title=getattr(result, 'title', ''),
        )
        click.echo(out)
    
    if aggregations:
        click.echo('Aggregations: (doc count, key, label)')
        longest_agg = ''
        for key,val in results.aggregations.items():
            if val:
                if len(key) > len(longest_agg):
                    longest_agg = key
        TEMPLATE = '{key}  {value}'
        for key,val in results.aggregations.items():
            if val:
                values = [
                    '(%s) %s "%s"' % (v['doc_count'], v['key'], v['label'])
                    for v in val
                ]
                for n,v in enumerate(values):
                    if n == 0:
                        k = key + ' ' * (len(longest_agg) - len(key))
                    else:
                        k = ' ' * len(longest_agg)
                    out = TEMPLATE.format(
                        key=k,
                        value=v
                    )
                    click.echo(out)

    return
