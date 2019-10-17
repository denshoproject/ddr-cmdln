HELP = """
ddrindex - publish DDR content to Elasticsearch; debug Elasticsearch

Index Management: create, destroy, alias, mappings, status, reindex
Publishing:       vocabs, post, postjson, index
Debugging:        config, get, exists, search

By default the command uses DOCSTORE_HOSTS from the config file.  Override
these values using the --hosts option or with an environment variable:
  $ export DOCSTORE_HOSTS=192.168.56.1:9200

CORE COMMANDS

Initialize indices (creates indices with mappings)
  $ ddrindex create

Post repository and organization:
  $ ddrindex repo /var/www/media/ddr/REPO/repository.json
  $ ddrindex org /var/www/media/ddr/REPO-ORG/organization.json

Post an object. Optionally, publish its child objects and/or ignore publication status.
  $ ddrindex publish [--recurse] [--force] /var/www/media/ddr/ddr-testing-123

Post vocabularies (used for topics, facility fields)
  $ ddrindex vocabs /opt/ddr-vocab/api/0.2

Post narrators:
  $ ddrindex narrators /opt/ddr-local/ddr-defs/narrators.json

MANAGEMENT COMMANDS

View current settings
  $ ddrindex conf

Check status
  $ ddrindex status

See if document exists
  $ ddrindex exists collection ddr-testing-123

Get document
  $ ddrindex get collection ddr-testing-123
Get document as JSON
  $ ddrindex get collection ddr-testing-123 --json
Get document as JSON (formatted)
  $ ddrindex get collection ddr-testing-123 --json | jq

Remove document
  $ ddrindex delete DOCTYPE DOCUMENTID

Search
  $ ddrindex search collection,entity Minidoka

Delete existing indices
  $ ddrindex destroy --confirm

Reindex
  $ ddrindex reindex --index source --target dest

Set or remove an index alias
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev
  $ ddrindex alias --index ddrpublic-20171108c --alias ddrpublic-dev --delete --confirm

Post arbitrary JSON documents:
  $ ddrindex postjson DOCTYPE DOCUMENTID /PATH/TO/DOCUMENT.json
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
    By default the command uses DOCSTORE_HOSTS from the config file.  Override
    these values using the --hosts option or with an environment variable:
        $ export DOCSTORE_HOSTS=192.168.56.1:9200
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
def conf(hosts):
    """Print configuration settings.
    
    More detail since you asked.
    """
    docstore.Docstore(hosts).print_configs()


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
def create(hosts):
    """Create new indices.
    """
    try:
        docstore.Docstore(hosts).create_indices()
    except Exception as err:
        logprint('error', err)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('indices')
@click.argument('snapshot')
def backup(hosts, index, indices, snapshot):
    """Make a snapshot backup of specified indices.
    
    """
    indices = [i.strip() for i in indices.split(',')]
    try:
        r = docstore.Docstore(hosts, index).backup(snapshot, indices)
    except Exception as err:
        logprint('error', err)
        r = {}
        click.echo('Checklist:')
        click.echo(
            '- Check value of [public] docstore_path_repo in ddrlocal.cfg ({})'.format(
                config.ELASTICSEARCH_PATH_REPO
        ))
        click.echo('- path.repo must be set in elasticsearch.yml on each node of cluster.')
        click.echo('- path.repo must be writable on each node of cluster.')
    if r:
        click.echo('repository: {}'.format(r['repository']))
        # snapshot feedback
        if r['snapshot'].get('accepted') and r['snapshot']['accepted']:
            # snapshot started
            click.echo('Backup started. Reissue command for status updates.')
        elif r['snapshot'].get('accepted') and not r['snapshot']['accepted']:
            # problem
            click.echo('Error: problem with backup!')
        elif r['snapshot'].get('snapshots'):
            # in progress or SUCCESS
            for s in r['snapshot']['snapshots']:
                click.echo('{}  {} {}'.format(
                    s['start_time'],
                    s['snapshot'],
                    ','.join(s['indices']),
                ))
                click.echo('{}  {}'.format(
                    s.get('end_time'),
                    s['state'],
                ))
                if s['state'] != 'SUCCESS':
                    click.echo(s)
        else:
            click.echo('snapshot:   {}'.format(r['snapshot']))


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--index','-i',
              default=config.DOCSTORE_INDEX, envvar='DOCSTORE_INDEX',
              help='Elasticsearch index.')
@click.argument('indices')
@click.argument('snapshot')
def restore(hosts, index, indices, snapshot):
    """Restore a snapshot backup.
    """
    indices = [i.strip() for i in indices.split(',')]
    r = docstore.Docstore(hosts, index).restore_snapshot(snapshot, indices)
    click.echo(r)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--confirm', is_flag=True,
              help='Yes I really want to delete this index.')
def destroy(hosts, confirm):
    """Delete indices (requires --confirm).
    
    \b
    It's meant to sound serious. Also to not clash with 'delete', which
    is for individual documents.
    """
    if confirm:
        try:
            docstore.Docstore(hosts).delete_indices()
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
@click.option('--indices','-i', help='Comma-separated list of indices to display.')
def mappings(hosts, indices):
    """Display mappings for the specified index/indices.
    """
    data = docstore.Docstore(hosts).get_mappings()
    text = json.dumps(data)
    click.echo(text)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
#@click.option('--report', is_flag=True,
#              help='Report number of records existing, to be indexed/updated.')
#@click.option('--dryrun', is_flag=True,
#              help='perform a trial run with no changes made')
#@click.option('--force', is_flag=True,
#              help='Forcibly update records whether they need it or not.')
@click.argument('path')
def vocabs(hosts, path):
    """Post DDR vocabulary facets and terms.
    
    \b
    Example:
      $ ddrindex vocabs /opt/ddr-local/ddr-vocab/api/0.2/
    """
    docstore.Docstore(hosts).post_vocabs(path=path)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('doctype')
@click.argument('object_id')
@click.argument('path')
def postjson(hosts, doctype, object_id, path):
    """TODO Post raw JSON file to Elasticsearch (YMMV)
    
    This command is for posting raw JSON files.  If the file you wish to post
    is a DDR object, please use "ddrindex post".
    """
    status = docstore.Docstore(hosts).post_json(
        doctype,
        object_id,
        fileio.read_text(path)
    )
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--recurse','-r', is_flag=True, help='Publish documents under this one.')
@click.option('--force','-f', is_flag=True, help='Publish regardless of status.')
@click.argument('path')
def publish(hosts, recurse, force, path):
    """Post the document and its children to Elasticsearch
    """
    status = docstore.Docstore(hosts).post_multi(
        path, recursive=recurse, force=force
    )
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('path')
def repo(hosts, path):
    """Post the repository record to Elasticsearch
    """
    status = docstore.Docstore(hosts).repo(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('path')
def org(hosts, path):
    """Post the organization record to Elasticsearch
    """
    status = docstore.Docstore(hosts).org(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('path')
def narrators(hosts, path):
    """Post the DDR narrators file to Elasticsearch
    """
    status = docstore.Docstore(hosts).narrators(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--recurse','-r', is_flag=True, help='Delete documents under this one.')
@click.option('--confirm', is_flag=True, help='Yes I really want to delete these objects.')
@click.argument('object_id')
def delete(hosts, recurse, confirm, object_id):
    """Delete the specified document from Elasticsearch
    """
    if confirm:
        click.echo(docstore.Docstore(hosts).delete(object_id, recursive=recurse))
    else:
        click.echo("Add '--confirm' if you're sure you want to do this.")


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('doctype')
@click.argument('object_id')
def exists(hosts, doctype, object_id):
    """Indicate whether the specified document exists
    """
    ds = docstore.Docstore(hosts)
    click.echo(ds.exists(doctype, object_id))


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--json','-j', is_flag=True, help='Print as JSON')
@click.argument('doctype')
@click.argument('object_id')
def get(hosts, json, doctype, object_id):
    """Print a single document
    """
    document = docstore.Docstore(hosts).get(doctype, object_id)
    if json:
        click.echo(format_json(document.to_dict()))
    else:
        click.echo(document)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
def status(hosts):
    """Print status info.
    
    More detail since you asked.
    """
    ds = docstore.Docstore(hosts)
    s = ds.status()
    
    logprint('debug', '------------------------------------------------------------------------',0)
    logprint('debug', 'Elasticsearch',0)
    # config file
    logprint('debug', 'DOCSTORE_HOST  (default): %s' % config.DOCSTORE_HOST, 0)
    # overrides
    if hosts != config.DOCSTORE_HOST:
        logprint('debug', 'docstore_hosts: %s' % hosts, 0)
    
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
        logprint('debug', '- %s' % i, 0)
    
    logprint('debug', 'Aliases', 0)
    aliases = ds.aliases()
    if aliases:
        for index,alias in aliases:
            logprint('debug', '- %s -> %s' % (alias, index), 0)
    else:
        logprint('debug', 'No aliases', 0)
    
    #if ds.es.indices.exists(index=index):
    #    logprint('debug', 'Index %s present' % index, 0)
    #else:
    #    logprint('error', "Index '%s' doesn't exist!" % index, 0)
    #    return

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
@click.option('--models','-m',
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
def search(hosts, models, parent, filters, limit, offset, page, aggregations, raw, fulltext):
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
        $ ddrindex search seattle --models=entity,segment
    
    \b
    Filter on certain fields (filters may repeat):
        $ ddrindex search seattle --filter=topics:373,27
        $ ddrindex search seattle -f=topics:373 -f facility=12
    
    Use the --aggregations/-a flag to display filter aggregations,
    with document counts, filter keys, and labels.
    """
    models = models.split(',')
    results = search_.search(
        hosts,
        models=models,
        parent=parent,
        filters=filters,
        fulltext=fulltext,
        limit=limit, offset=offset, page=page,
        aggregations=aggregations
    )
    
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
