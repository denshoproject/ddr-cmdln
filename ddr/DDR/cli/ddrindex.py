HELP = """
ddrindex - publish DDR content to Elasticsearch; debug Elasticsearch

Index Management: create, destroy, mappings, status, reindex
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
  $ ddrindex narrators /opt/ddr-local/densho-vocab/api/0.2/narrators.json

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

Post arbitrary JSON documents:
  $ ddrindex postjson DOCTYPE DOCUMENTID /PATH/TO/DOCUMENT.json
"""

from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)
import os
import sys
import time

import click
import elasticsearch

from elastictools.docstore import cluster as docstore_cluster

from DDR import config
from DDR import docstore
from DDR import fileio
from DDR import identifier
from DDR import models
from DDR.models import SEARCH_PARAM_WHITELIST, SEARCH_MODELS
from DDR.models import SEARCH_INCLUDE_FIELDS, SEARCH_NESTED_FIELDS
from DDR.models import SEARCH_AGG_FIELDS
from elastictools.search import Searcher
from DDR import storage


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
        raise TypeError(
            'Object of type %s with value of %s is not JSON serializable' % (
                type(obj), repr(obj)
            )
        )
    
def format_json(data, pretty=False):
    if pretty:
        return json.dumps(
            data,
            indent=4,
            separators=(',', ': '),
            default=_json_handler,
        )
    return json.dumps(data, default=_json_handler)


class FakeSettings():
    def __init__(self, host):
        self.DOCSTORE_HOST = host
        self.DOCSTORE_SSL_CERTFILE = config.DOCSTORE_SSL_CERTFILE
        self.DOCSTORE_USERNAME = config.DOCSTORE_USERNAME
        self.DOCSTORE_PASSWORD = config.DOCSTORE_PASSWORD

def get_docstore(host=config.DOCSTORE_HOST):
    ds = docstore.DocstoreManager(
        docstore.INDEX_PREFIX, host, FakeSettings(host)
    )
    try:
        ds.es.info()
    except Exception as err:
        print(err)
        sys.exit(1)
    return ds


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
    ds = get_docstore(hosts)
    ds.print_configs(hosts)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
def create(hosts):
    """Create new indices.
    """
    ds = get_docstore(hosts)
    try:
        ds.create_indices()
    except Exception as err:
        logprint('error', err)


#@ddrindex.command()
#@click.option('--hosts','-h',
#              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
#              help='Elasticsearch hosts.')
#@click.argument('indices')
#@click.argument('snapshot')
#def backup(hosts, indices, snapshot):
#    """Make a snapshot backup of specified indices.
#    
#    """
#    ds = get_docstore(hosts)
#    indices = [i.strip() for i in indices.split(',')]
#    try:
#        r = ds.backup(snapshot, indices)
#    except Exception as err:
#        logprint('error', err)
#        r = {}
#        click.echo('Checklist:')
#        click.echo(
#            '- Check value of [public] docstore_path_repo in ddrlocal.cfg ({})'.format(
#                config.ELASTICSEARCH_PATH_REPO
#        ))
#        click.echo('- path.repo must be set in elasticsearch.yml on each node of cluster.')
#        click.echo('- path.repo must be writable on each node of cluster.')
#    if r:
#        click.echo('repository: {}'.format(r['repository']))
#        # snapshot feedback
#        if r['snapshot'].get('accepted') and r['snapshot']['accepted']:
#            # snapshot started
#            click.echo('Backup started. Reissue command for status updates.')
#        elif r['snapshot'].get('accepted') and not r['snapshot']['accepted']:
#            # problem
#            click.echo('Error: problem with backup!')
#        elif r['snapshot'].get('snapshots'):
#            # in progress or SUCCESS
#            for s in r['snapshot']['snapshots']:
#                click.echo('{}  {} {}'.format(
#                    s['start_time'],
#                    s['snapshot'],
#                    ','.join(s['indices']),
#                ))
#                click.echo('{}  {}'.format(
#                    s.get('end_time'),
#                    s['state'],
#                ))
#                if s['state'] != 'SUCCESS':
#                    click.echo(s)
#        else:
#            click.echo('snapshot:   {}'.format(r['snapshot']))
#
#
#@ddrindex.command()
#@click.option('--hosts','-h',
#              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
#              help='Elasticsearch hosts.')
#@click.argument('indices')
#@click.argument('snapshot')
#def restore(hosts, indices, snapshot):
#    """Restore a snapshot backup.
#    """
#    ds = get_docstore(hosts)
#    indices = [i.strip() for i in indices.split(',')]
#    r = ds.restore_snapshot(snapshot, indices)
#    click.echo(r)


@ddrindex.command()
@click.option('--confirm', is_flag=True, help='Yes I really want to destroy this database.')
@click.argument('host')
def destroy(confirm, host):
    """Delete indices (requires --confirm).
    
    \b
    It's meant to sound serious. Also to not clash with 'delete', which
    is for individual documents.
    """
    ds = get_docstore(host)
    cluster = docstore_cluster(config.DOCSTORE_CLUSTERS, ds.host)
    if confirm:
        click.echo(
            f"The {cluster} cluster ({ds.host}) with the following indices "
            + "will be DESTROYED!"
        )
        for index in identifier.ELASTICSEARCH_CLASSES['all']:
            click.echo(f"- {index['doc_type']}")
    else:
        click.echo(
            f"Add '--confirm' to destroy the {cluster} cluster ({ds.host})."
        )
        sys.exit(0)
    response = click.prompt(
        'Do you want to continue? [yes/no]',
        default='no', show_default=False
    )
    if response == 'yes':
        click.echo(f"Deleting indices from {ds.host} ({cluster}).")
        time.sleep(3)
        try:
            ds.delete_indices()
        except Exception as err:
            logprint('error', err)
    else:
        click.echo("Cancelled.")


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.option('--indices','-i', help='Comma-separated list of indices to display.')
def mappings(hosts, indices):
    """Display mappings for the specified index/indices.
    """
    ds = get_docstore(hosts)
    data = ds.get_mappings()
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
    ds = get_docstore(hosts)
    ds.post_vocabs(path=path)


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
    ds = get_docstore(hosts)
    status = ds.post_json(
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
@click.option('--b2','-b', is_flag=True, help='Mark files uploaded to Backblaze.')
@click.argument('path')
def publish(hosts, recurse, force, b2, path):
    """Post the document and its children to Elasticsearch
    """
    ds = get_docstore(hosts)
    B2KEYID = os.environ.get('B2KEYID')
    B2APPKEY = os.environ.get('B2APPKEY')
    B2BUCKET = os.environ.get('B2BUCKET')
    backblaze = None
    if b2:
        if not (B2KEYID and B2APPKEY and B2BUCKET):
            click.echo(
                'ERROR: --b2sync requires environment variables ' \
                'B2KEYID, B2APPKEY, B2BUCKET'
            )
            sys.exit(1)
        click.echo('Backblaze: authenticating')
        click.echo(f'  B2BUCKET {B2BUCKET}')
        click.echo(f'  B2APPKEY ...{B2APPKEY[-6:]}')
        click.echo(f'  B2KEYID  ...{B2KEYID[-6:]}')
        try:
            backblaze = storage.Backblaze(B2KEYID, B2APPKEY, B2BUCKET)
        except Exception as err:
            click.echo(f'ERROR: {err}')
            sys.exit(1)
    status = ds.post_multi(
        path, recursive=recurse, force=force, backblaze=backblaze
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
    ds = get_docstore(hosts)
    status = ds.repo(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('path')
def org(hosts, path):
    """Post the organization record to Elasticsearch
    """
    ds = get_docstore(hosts)
    status = ds.org(path)
    click.echo(status)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('path')
def narrators(hosts, path):
    """Post the DDR narrators file to Elasticsearch
    """
    ds = get_docstore(hosts)
    status = ds.narrators(path)
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
    ds = get_docstore(hosts)
    if confirm:
        click.echo(ds.delete(object_id, recursive=recurse))
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
    ds = get_docstore(hosts)
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
    ds = get_docstore(hosts)
    es_class = identifier.ELASTICSEARCH_CLASSES_BY_MODEL[doctype]
    document = ds.get(doctype, es_class, object_id)
    if json:
        click.echo(format_json(document.to_dict()))
    else:
        click.echo(document)


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
@click.argument('doctype')
@click.argument('object_id')
def url(hosts, doctype, object_id):
    """Get Elasticsearch URL for document
    """
    ds = get_docstore(hosts)
    click.echo(ds.url(doctype, object_id))


@ddrindex.command()
@click.option('--hosts','-h',
              default=config.DOCSTORE_HOST, envvar='DOCSTORE_HOST',
              help='Elasticsearch hosts.')
def status(hosts):
    """Print status info.
    
    More detail since you asked.
    """
    logprint('debug', '------------------------------------------------------------------------',0)
    if hosts != config.DOCSTORE_HOST:
        logprint('debug', f'Elasticsearch {hosts} (default {config.DOCSTORE_HOST})', 0)
    else:
        logprint('debug', f'Elasticsearch {hosts}', 0)
    ds = get_docstore(hosts)
    cluster = docstore_cluster(config.DOCSTORE_CLUSTERS, ds.host)
    click.echo(f"{ds} ({cluster})")
    s = ds.status()
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
    index_names = list(ds.es.indices.stats()['indices'].keys())
    for i in index_names:
        logprint('debug', '- %s' % i, 0)
    
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
# DISABLED: elastictools.search.Searcher.prepare handles this poorly
#@click.option('--models','-m',
#              default='collection,entity,segment,file',
#              help='One or more doctypes (comma-separated).')
@click.option('--parent','-P',
              default='',
              help='ID of parent object.')
@click.option('--filters','-f',
              default='', #multiple=True,
              help='Filter on certain fields (FIELD:VALUE,VALUE,...).')
@click.option('--limit','-l',
              default=config.RESULTS_PER_PAGE,
              help='Number of results to display.')
@click.option('--offset','-o',
              default=0,
              help='Starting point in results (use with limit).')
@click.option('--aggregations','-a',
              is_flag=True, default=False,
              help='Show filter aggregations for result set.')
@click.option('--raw','-r',
              is_flag=True, default=False,
              help='Raw Elasticsearch output.')
@click.argument('fulltext')
def search(hosts, parent, filters, limit, offset, aggregations, raw, fulltext):
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
    Specify parent object:
        $ ddrindex search seattle --parent=ddr-densho-12
    
    \b
    Filter on certain fields (filters may repeat):
        $ ddrindex search seattle --filter=topics:373,27
        $ ddrindex search seattle -f=topics:373 -f facility=12
    
    Use the --aggregations/-a flag to display filter aggregations,
    with document counts, filter keys, and labels.
    """
    # DISABLED: elastictools.search.Searcher.prepare handles this poorly
    #Specify doctype/model:
    #    $ ddrindex search seattle --models=entity,segment
    if parent:
        try:
            oi = identifier.Identifier(parent)
        except Exception as err:
            click.echo(f'ERROR: Problem with --parent\n{err}')
            sys.exit(1)
    searcher = Searcher(get_docstore(hosts))
    searcher.prepare(
        params={
            'fulltext': fulltext, 'parent': parent, 'filters': filters,
        },
        params_whitelist=SEARCH_PARAM_WHITELIST,
        search_models=SEARCH_MODELS,
        sort=[],
        fields=SEARCH_INCLUDE_FIELDS,
        fields_nested=SEARCH_NESTED_FIELDS,
        fields_agg=SEARCH_AGG_FIELDS,
        wildcards=False,
    )
    results = searcher.execute(int(limit), int(offset))
    
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
        click.echo('Aggregations: (count key [label])')
        longest_agg = ''
        for key,val in list(results.aggregations.items()):
            if val:
                if len(key) > len(longest_agg):
                    longest_agg = key
        TEMPLATE = '{key}  {value}'
        for key,val in list(results.aggregations.items()):
            if val:
                values = []
                for v in val:
                    try:
                        x = f'({v["doc_count"]}) {v["key"]} "{v["label"]}"'
                    except:
                        x = f'({v["doc_count"]}) {v["key"]}'
                    values.append(x)
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
