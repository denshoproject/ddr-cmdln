"""
TODO pass ES connection object to docstore.* instead of HOSTS

example walkthrough:
------------------------------------------------------------------------

from DDR import docstore
d = docstore.Docstore()

d.delete_indices()
d.create_indices()

# Repository, organization metadata
d.repo(path='/var/www/media/ddr/ddr/repository.json')
d.org(path='/var/www/media/ddr/ddr-densho/organization.json')

# Post an object and its child objects.
d.post_multi('/var/www/media/ddr/ddr-densho-10', recursive=True)

# Post vocabularies (used for topics, facility fields)
d.post_vocabs(docstore.VOCABS_URL)

# Narrators metadata
d.narrators('/opt/ddr-local/densho-vocab/api/0.2/narrators.json')

# Delete a collection
d.delete(os.path.basename(PATH), recursive=True)

------------------------------------------------------------------------
"""
from __future__ import print_function
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
import json
import logging
logger = logging.getLogger(__name__)
import os
from pathlib import Path

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.client import SnapshotClient
import elasticsearch_dsl
import requests

from DDR import config
from DDR import converters
from DDR import fileio
from DDR.identifier import Identifier
from DDR.identifier import ELASTICSEARCH_CLASSES
from DDR.identifier import ELASTICSEARCH_CLASSES_BY_MODEL
from DDR.identifier import ID_COMPONENTS, InvalidInputException
from DDR.identifier import MODEL_REPO_MODELS
from DDR.identifier import MODULES, module_for_name
from DDR import modules
from DDR import storage
from DDR import util
from DDR import vocab

INDEX_PREFIX = 'ddr'

MAX_SIZE = 10000
DEFAULT_PAGE_SIZE = 20

SUCCESS_STATUSES = [200, 201]
STATUS_OK = ['completed']
PUBLIC_OK = [1,'1']

"""
ddr-local

elasticsearch.add_document(settings.ELASTICSEARCH_HOST_PORT, 'ddr', 'collection', os.path.join(collection_path, 'collection.json'))
elasticsearch.index(settings.MEDIA_BASE, settings.ELASTICSEARCH_HOST_PORT, 'ddr')
elasticsearch.status(settings.ELASTICSEARCH_HOST_PORT)
elasticsearch.delete_index(settings.ELASTICSEARCH_HOST_PORT, 'ddr')

ddr-public

modelfields = elasticsearch._model_fields(MODELS_DIR, MODELS)
cached = elasticsearch.query(host=host, index=index, model=model,
raw = elasticsearch.get(HOST, model=Repository.model, id=id)
document = elasticsearch.get(settings.ELASTICSEARCH_HOST_PORT,
elasticsearch.list_facets():
results = elasticsearch.facet_terms(settings.ELASTICSEARCH_HOST_PORT,
"""

def load_json(path):
    try:
        data = json.loads(fileio.read_text(path))
    except json.JSONDecodeError:
        raise Exception('json.errors.JSONDecodeError reading %s' % path)
    return data

class Docstore():

    def __init__(self, hosts=config.DOCSTORE_HOST, connection=None):
        self.hosts = hosts
        if connection:
            self.es = connection
        else:
            self.es = Elasticsearch(hosts, timeout=config.DOCSTORE_TIMEOUT)
    
    def index_name(self, model):
        return '{}{}'.format(INDEX_PREFIX, model)
    
    def __repr__(self):
        return "<%s.%s %s:%s*>" % (
            self.__module__, self.__class__.__name__, self.hosts, INDEX_PREFIX
        )
    
    def print_configs(self):
        print('CONFIG_FILES:           %s' % config.CONFIG_FILES)
        print('')
        print('DOCSTORE_HOST:          %s' % config.DOCSTORE_HOST)
        print('')
    
    def health(self):
        return self.es.cluster.health()
    
    def start_test(self):
        try:
            self.es.cluster.health()
        except TransportError:
            logger.critical('Elasticsearch cluster unavailable')
            print('CRITICAL: Elasticsearch cluster unavailable')
            sys.exit(1)
    
    def index_exists(self, indexname):
        """
        """
        return self.es.indices.exists(index=indexname)
    
    def status(self):
        """Returns status information from the Elasticsearch cluster.
        
        >>> docstore.Docstore().status()
        {
            u'indices': {
                u'ddrpublic-dev': {
                    u'total': {
                        u'store': {
                            u'size_in_bytes': 4438191,
                            u'throttle_time_in_millis': 0
                        },
                        u'docs': {
                            u'max_doc': 2664,
                            u'num_docs': 2504,
                            u'deleted_docs': 160
                        },
                        ...
                    },
                    ...
                }
            },
            ...
        }
        """
        return self.es.indices.stats()
    
    def index_names(self):
        """Returns list of index names
        """
        return [name for name in list(self.status()['indices'].keys())]
    
    def create_indices(self):
        """Create indices for each model defined in ddr-defs/repo_models/elastic.py
        """
        statuses = []
        for i in ELASTICSEARCH_CLASSES['all']:
            status = self.create_index(
                self.index_name(i['doctype']),
                i['class']
            )
            statuses.append(status)
        return statuses
    
    def create_index(self, indexname, dsl_class):
        """Creates the specified index if it does not already exist.
        
        Uses elasticsearch-dsl classes defined in ddr-defs/repo_models/elastic.py
        
        @param indexname: str
        @param dsl_class: elasticsearch_dsl.Document class
        @returns: JSON dict with status codes and responses
        """
        logger.debug('creating index {}'.format(indexname))
        if self.index_exists(indexname):
            status = '{"status":400, "message":"Index exists"}'
            logger.debug('Index exists')
            #print('Index exists')
        else:
            index = elasticsearch_dsl.Index(indexname)
            #print('index {}'.format(index))
            index.aliases(default={})
            #print('registering')
            out = index.document(dsl_class).init(index=indexname, using=self.es)
            if out:
                status = out
            elif self.index_exists(indexname):
                status = {
                    "name": indexname,
                    "present": True,
                }
            #print(status)
            #print('creating index')
        return status
    
    def delete_indices(self):
        """Delete indices for each model defined in ddr-defs/repo_models/elastic.py
        """
        statuses = []
        for i in ELASTICSEARCH_CLASSES['all']:
            status = self.delete_index(
                self.index_name(i['doctype'])
            )
            statuses.append(status)
        return statuses
    
    def delete_index(self, indexname):
        """Delete the specified index.
        
        @returns: JSON dict with status code and response
        """
        logger.debug('deleting index: %s' % indexname)
        if self.index_exists(indexname):
            status = self.es.indices.delete(index=indexname)
        else:
            status = {
                "name": indexname,
                "status": 500,
                "message": "Index does not exist",
            }
        logger.debug(status)
        return status
    
    def model_fields_lists(self):
        """
        Lists of class-specific fields for each class, in order,
        so documents may be emitted as OrderedDicts with fields in order.
        HOSTS:PORT/INDEX/modelfields/collection/
        HOSTS:PORT/INDEX/modelfields/entity/
        HOSTS:PORT/INDEX/modelfields/segment/
        HOSTS:PORT/INDEX/modelfields/file/
        
        identifier.MODEL_REPO_MODELS
        Identifier.fields_module
        """
        DOCTYPE = 'esobjectfields'
        EXCLUDED = [
            'id', 'title', 'description',
        ]
        for model in list(MODEL_REPO_MODELS.keys()):
            module = module_for_name(MODEL_REPO_MODELS[model]['module']
            )
            fields = [
                f['name'] for f in module.FIELDS
                if f['elasticsearch']['public'] and (f['name'] not in EXCLUDED)
            ]
            data = {
                'model': model,
                'fields': fields,
            }
            self.post_json(
                doc_type=DOCTYPE,
                document_id=model,
                json_text=json.dumps(data),
            )
    
    def get_mappings(self):
        """Get mappings for ESObjects
        
        @returns: str JSON
        """
        return self.es.indices.get_mapping()
    
    def post_vocabs(self, path=config.VOCABS_URL):
        """Posts ddr-vocab facets,terms to ES.
        
        curl -XPUT 'http://localhost:9200/meta/facet/format' -d '{ ... }'
        >>> elasticsearch.post_facets(
            '192.168.56.120:9200', 'meta',
            '/opt/ddr-local/ddr-vocab'
            )
        
        @param path: Absolute path to dir containing facet files.
        @returns: JSON dict with status code and response
        """
        logger.debug('index_facets(%s)' % (path))
        vocabs = vocab.get_vocabs(path)
        
        # get classes from ddr-defs
        # TODO we should hard-code indexnames...
        facet_doctype = 'facet'
        facetterm_doctype = 'facetterm'
        Facet = ELASTICSEARCH_CLASSES_BY_MODEL[facet_doctype]
        FacetTerm = ELASTICSEARCH_CLASSES_BY_MODEL[facetterm_doctype]
        
        # push facet data
        statuses = []
        for v in list(vocabs.keys()):
            fid = vocabs[v]['id']
            facet = Facet()
            facet.meta.id = fid
            facet.id = fid
            facet.model = 'facet'
            facet.links_html = fid
            facet.links_json = fid
            facet.links_children = fid
            facet.title = vocabs[v]['title']
            facet.description = vocabs[v]['description']
            logging.debug(facet)
            status = facet.save(
                index=self.index_name(facet_doctype), using=self.es
            )
            statuses.append(status)
            
            for t in vocabs[v]['terms']:
                tid = t.get('id')
                facetterm_id = '-'.join([
                    str(fid),
                    str(tid),
                ])
                term = FacetTerm()
                term.meta.id = facetterm_id
                term.facet = fid
                term.term_id = tid
                term.links_html = facetterm_id
                term.links_json = facetterm_id
                # TODO doesn't handle location_geopoint
                for field in list(FacetTerm._doc_type.mapping.to_dict()['properties'].keys()):
                    if t.get(field):
                        setattr(term, field, t[field])
                term.id = facetterm_id  # overwrite term.id from original
                logging.debug(term)
                print(term)
                status = term.save(
                    index=self.index_name(facetterm_doctype), using=self.es
                )
                statuses.append(status)
        
        forms_choices = {
            'topics-choices': vocab.topics_choices(
                vocab.get_vocabs(config.VOCABS_URL)['topics'],
                ELASTICSEARCH_CLASSES_BY_MODEL['facetterm']
            ),
            'facility-choices': vocab.form_vocab_choices(
                vocab.get_vocabs(config.VOCABS_URL)['facility'],
                'facility'
            ),
            'format-choices': vocab.form_vocab_choices(
                vocab.get_vocabs(config.VOCABS_URL)['format'],
                'format'
            ),
            'genre-choices': vocab.form_vocab_choices(
                vocab.get_vocabs(config.VOCABS_URL)['genre'],
                'genre'
            ),
            'rights-choices': vocab.form_vocab_choices(
                vocab.get_vocabs(config.VOCABS_URL)['rights'],
                'rights'
            ),
        }
        self.post_json('forms', 'forms-choices', forms_choices)
        return statuses
    
    def facet_terms(self, facet, order='term', all_terms=True, model=None):
        """Gets list of terms for the facet.
        
        $ curl -XGET 'http://192.168.56.101:9200/ddr/entity/_search?format=yaml' -d '{
          "fields": ["id"],
          "query": { "match_all": {} },
          "facets": {
            "genre_facet_result": {
              "terms": {
                "order": "count",
                "field": "genre"
              }
            }
          }
        }'
        Sample results:
            {
              u'_type': u'terms',
              u'missing': 203,
              u'total': 49,
              u'other': 6,
              u'terms': [
                {u'term': u'photograph', u'count': 14},
                {u'term': u'ephemera', u'count': 6},
                {u'term': u'advertisement', u'count': 6},
                {u'term': u'book', u'count': 5},
                {u'term': u'architecture', u'count': 3},
                {u'term': u'illustration', u'count': 2},
                {u'term': u'fieldnotes', u'count': 2},
                {u'term': u'cityscape', u'count': 2},
                {u'term': u'blank_form', u'count': 2},
                {u'term': u'portrait, u'count': 1'}
              ]
            }
        
        @param facet: Name of field
        @param order: term, count, reverse_term, reverse_count
        @param model: (optional) Type of object ('collection', 'entity', 'file')
        @returns raw output of facet query
        """
        payload = {
            "fields": ["id"],
            "query": { "match_all": {} },
            "facets": {
                "results": {
                    "terms": {
                        "size": MAX_SIZE,
                        "order": order,
                        "all_terms": all_terms,
                        "field": facet
                    }
                }
            }
        }
        results = self.es.search(index=self.indexname, doc_type=model, body=payload)
        return results['facets']['results']

    def _repo_org(self, path, doctype, remove=False):
        """
        seealso DDR.models.common.DDRObject.to_esobject
        """
        # get and validate file
        data = load_json(path)
        if (not (data.get('id') and data.get('repo'))):
            raise Exception('Data file is not well-formed.')
        oi = Identifier(id=data['id'])

        ES_Class = ELASTICSEARCH_CLASSES_BY_MODEL[doctype]
        d = ES_Class(id=oi.id)
        d.meta.id = oi.id
        d.model = oi.model
        d.parent_id = oi.parent_id(stubs=1)
        # links
        d.links_html = oi.id
        d.links_json = oi.id
        d.links_img = '%s/logo.png' % oi.id
        d.links_thumb = '%s/logo.png' % oi.id
        d.links_parent = oi.parent_id(stubs=1)
        d.links_children = oi.id
        # title,description
        d.title = data['title']
        d.description = data['description']
        d.url = data['url']
        # ID components (repo, org, cid, ...) as separate fields
        idparts = deepcopy(oi.idparts)
        idparts.pop('model')
        for key,val in idparts.items():
            setattr(d, key, val)
        # add/update
        if remove and self.exists(doctype, oi):
            results = d.delete(index=self.index_name(doctype), using=self.es)
        else:
            results = d.save(index=self.index_name(doctype), using=self.es)
        return results
    
    def repo(self, path, remove=False):
        """Add/update or remove base repository metadata.
        
        @param path: str Absolute path to repository.json
        @param remove: bool Remove record from ES
        @returns: dict
        """
        return self._repo_org(path, 'repository', remove)
    
    def org(self, path, remove=False):
        """Add/update or remove base organization metadata.
        
        @param path: str Absolute path to organization.json
        @param remove: bool Remove record from ES
        @returns: dict
        """
        return self._repo_org(path, 'organization', remove)
    
    def narrators(self, path):
        """Add/update or remove narrators metadata.
        
        @param path: str Absolute path to narrators.json
        @returns: dict
        """
        # TODO we should not be hard-coding indexnames
        doctype = 'narrator'
        ES_Class = ELASTICSEARCH_CLASSES_BY_MODEL[doctype]
        indexname = self.index_name(doctype)
        data = load_json(path)
        num = len(data['narrators'])
        for n,document in enumerate(data['narrators']):
            d = ES_Class(id=document['id'])
            # set all values first before exceptions
            for key,val in document.items():
                setattr(d, key, val)
            # make sure certain important fields are set properly
            d.meta.id = document['id']
            d.title = document['display_name']
            d.description = document['bio']
            d.model = doctype
            # publish
            has_published = document.get('has_published', '')
            if has_published.isdigit():
                has_published = int(has_published)
            if has_published:
                logging.debug('{}/{} {}'.format(n, num, d))
                print('{}/{} {}'.format(n, num, d))
                result = d.save(index=self.index_name(doctype), using=self.es)
            else:
                logging.debug('%s not published' % d.id)
                if self.get(doctype, d.id, fields=[]):
                    self.delete(doctype, d.id)
    
    def post_json(self, indexname, document_id, json_text):
        """POST the specified JSON document as-is.
        
        @param indexname: str
        @param document_id: str
        @param json_text: str JSON-formatted string
        @returns: dict Status info.
        """
        logger.debug('post_json(%s, %s)' % (indexname, document_id))
        return self.es.index(index=indexname, id=document_id, body=json_text)

    def post(
            self,
            document,
            public_fields=[],
            additional_fields={},
            parents={},
            b2=False,
            force=False
    ):
        """Add a new document to an index or update an existing one.
        
        This function can produce ElasticSearch documents in two formats:
        - old-style list-of-dicts used in the DDR JSON files.
        - normal dicts used by ddr-public.
        
        DDR metadata JSON files are structured as a list of fieldname:value dicts.
        This is done so that the fields are always in the same order, making it
        possible to easily see the difference between versions of a file.
        [IMPORTANT: documents MUST contain an 'id' field!]
        
        In ElasticSearch, documents are structured in a normal dict so that faceting
        works properly.
        
        curl -XPUT 'http://localhost:9200/ddr/collection/ddr-testing-141' -d '{ ... }'
        
        @param document: Collection,Entity,File The object to post.
        @param public_fields: list
        @param additional_fields: dict
        @param parents: dict Basic metadata for parent documents.
        @param b2: boolean File uploaded to Backblaze bucket
        @param force: boolean Bypass status and public checks.
        @returns: JSON dict with status code and response
        """
        logger.debug('post(%s, %s)' % (
            document, force
        ))

        if force:
            can_publish = True
            public = False
        else:
            if not parents:
                parents = {
                    oid: oi.object()
                    for oid,oi in _all_parents([document.identifier]).items()
                }
            can_publish = publishable([document.identifier], parents)
            public = True
        if not can_publish:
            return {'status':403, 'response':'object not publishable'}
        
        d = document.to_esobject(
            public_fields=public_fields, public=public, b2=b2
        )
        logger.debug('saving')
        results = d.save(
            index=self.index_name(document.identifier.model),
            using=self.es
        )
        logger.debug(str(results))
        return results
    
    def post_multi(self, path, recursive=False, force=False, backblaze=None):
        """Publish (index) specified document and (optionally) its children.
        
        After receiving a list of metadata files, index() iterates through the
        list several times.  The first pass weeds out paths to objects that can
        not be published (e.g. object or its parent is unpublished).
        
        In the final pass, a list of public/publishable fields is chosen based
        on the model.  Additional fields not in the model (e.g. parent ID, parent
        organization/collection/entity ID) are packaged.  Then everything is sent
        off to post().
        
        @param path: Absolute path to directory containing object metadata files.
        @param recursive: Whether or not to recurse into subdirectories.
        @param force: boolean Just publish the damn collection already.
        @param backblaze: storage.Backblaze object Look in b2sync tmpdir and mark
                   files uploaded to Backblaze.
        @returns: number successful,list of paths that didn't work out
        """
        logger.debug(f'post_multi({path}, {recursive}, {force}, {backblaze})')
        # Check that path
        try:
            ci = Identifier(path).collection()
        except:
            raise Exception(
                'Docstore.post_multi path must point to a collection or subdirectory.'
            )
        ci_path = Path(ci.id)
        
        publicfields = _public_fields()
        
        # process a single file if requested
        if os.path.isfile(path):
            paths = [path]
        else:
            # files listed first, then entities, then collections
            logger.debug(f'Finding files in {path}')
            paths = util.find_meta_files(path, recursive, files_first=1)
        
        # Determine if paths are publishable or not
        logger.debug(f'Checking for publishability')
        identifiers = [Identifier(path) for path in paths]
        parents = {
            oid: oi.object()
            for oid,oi in _all_parents(identifiers).items()
        }
        paths = publishable(
            identifiers,
            parents,
            force=force
        )

        # list files in b2 bucket
        # TODO do this in parallel with util.find_meta_files?
        b2_files = []
        if backblaze:
            logger.debug(
                f'Checking Backblaze for uploaded files ({backblaze.bucketname})'
            )
            b2_files = backblaze.list_files(folder=ci.id)
            logger.debug(f'{len(b2_files)} files')
        
        skipped = 0
        successful = 0
        bad_paths = []
        
        num = len(paths)
        for n,path in enumerate(paths):
            oi = path.get('identifier')
            if not oi:
                path['note'] = 'No identifier'
                bad_paths.append(path)
                continue
            try:
                document = oi.object()
            except Exception as err:
                path['note'] = f'Could not instantiate: {err}'
                bad_paths.append(path)
                continue
            if not document:
                path['note'] = 'No document'
                bad_paths.append(path)
                continue
            
            # see if file uploaded to Backblaze
            b2_synced = False; b2str = ''
            if (oi.model == 'file') and b2_files:
                dir_filename = str(ci_path / Path(document.path).name)
                if dir_filename in b2_files:
                    b2_synced = True; b2str = '(b2)'
                    b2_files.remove(dir_filename)
            
            # TODO write logs instead of print
            now = datetime.now(config.TZ)
            action = path['action']
            path_note = path['note'].strip()
            print(f'{now} | {n+1}/{num} {action} {oi.id} {path_note}{b2str}')
            
            # see if document exists
            existing_v = None
            d = self.get(oi.model, oi.id)
            if d:
                existing_v = d.meta.version
            
            # post document
            if path['action'] == 'POST':
                created = self.post(
                    document, parents=parents, b2=b2_synced, force=True
                )
                # force=True bypasses publishable in post() function
            # delete previously published items now marked incomplete/private
            elif existing_v and (path['action'] == 'SKIP'):
                print('%s | %s/%s DELETE' % (datetime.now(config.TZ), n+1, num))
                self.delete(oi.id)
            
            if path['action'] == 'SKIP':
                skipped += 1
                continue
            
            # version is incremented with each updated
            posted_v = None
            # for e.g. segment the ES doc_type will be 'entity' but oi.model is 'segment'
            d = self.get(oi.model, oi.id)
            if d:
                posted_v = d.meta.version

            # success: created, or version number incremented
            status = 'ERROR - unspecified'
            if posted_v and not existing_v:
                status = 'CREATED'
                successful += 1
            elif (existing_v and posted_v) and (existing_v < posted_v):
                status = 'UPDATED'
                successful += 1
            elif not posted_v:
                status = 'ERROR: not created'
                bad_paths.append(path)
                print(status)
            
        logger.debug('INDEXING COMPLETED')
        return {'total':len(paths), 'skipped':skipped, 'successful':successful, 'bad':bad_paths}
     
    def exists(self, model, document_id):
        """
        @param model:
        @param document_id:
        """
        return self.es.exists(
            index=self.index_name(model),
            id=document_id
        )
    
    def url(self, model, document_id):
        """
        @param model:
        @param document_id:
        """
        return f'http://{config.DOCSTORE_HOST}/ddr{model}/_doc/{document_id}'
    
    def get(self, model, document_id, fields=None):
        """Get a single document by its id.
        
        @param model:
        @param document_id:
        @param fields: boolean Only return these fields
        @returns: repo_models.elastic.ESObject or None
        """
        ES_Class = ELASTICSEARCH_CLASSES_BY_MODEL[model]
        return ES_Class.get(
            id=document_id,
            index=self.index_name(model),
            using=self.es,
            ignore=404,
        )

    def count(self, doctypes=[], query={}):
        """Executes a query and returns number of hits.
        
        The "query" arg must be a dict that conforms to the Elasticsearch query DSL.
        See docstore.search_query for more info.
        
        @param doctypes: list Type of object ('collection', 'entity', 'file')
        @param query: dict The search definition using Elasticsearch Query DSL
        @returns raw ElasticSearch query output
        """
        logger.debug('count(doctypes=%s, query=%s' % (doctypes, query))
        if not query:
            raise Exception(
                "Can't do an empty search. Give me something to work with here."
            )
        
        indices = ','.join(
            ['{}{}'.format(INDEX_PREFIX, m) for m in doctypes]
        )
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        
        return self.es.count(
            index=indices,
            body=query,
        )
    
    def delete(self, document_id, recursive=False):
        """Delete a document and optionally its children.
        
        TODO refactor after upgrading Elasticsearch past 2.4.
        delete_by_query was removed sometime during elasticsearch-py 2.*
        I think it was added back in a later version so the code stays for now.
        
        For now, instead of deleting based on document_id, we start with
        document_id, find all paths beneath it in the filesystem,
        and curl DELETE url each individual document from Elasticsearch.
        
        @param document_id:
        @param recursive: True or False
        """
        logger.debug('delete(%s, %s)' % (document_id, recursive))
        oi = Identifier(document_id, config.MEDIA_BASE)
        if recursive:
            paths = util.find_meta_files(
                oi.path_abs(), recursive=recursive, files_first=1
            )
        else:
            paths = [oi.path_abs()]
        identifiers = [Identifier(path) for path in paths]
        num = len(identifiers)
        for n,oi in enumerate(identifiers):
            # TODO hard-coded models here!
            if oi.model == 'segment':
                model = 'entity'
            else:
                model = oi.model
            url = 'http://{}/{}/_doc/{}/'.format(
                self.hosts,
                self.index_name(model),
                oi.id
            )
            r = requests.request('DELETE', url)
            print('{}/{} DELETE {} {} -> {} {}'.format(
                n, num,
                self.index_name(model),
                oi.id,
                r.status_code, r.reason
            ))

    def search(self, doctypes=[], query={}, sort=[], fields=[], from_=0, size=MAX_SIZE):
        """Executes a query, get a list of zero or more hits.
        
        The "query" arg must be a dict that conforms to the Elasticsearch query DSL.
        See docstore.search_query for more info.
        
        @param doctypes: list Type of object ('collection', 'entity', 'file')
        @param query: dict The search definition using Elasticsearch Query DSL
        @param sort: list of (fieldname,direction) tuples
        @param fields: str
        @param from_: int Index of document from which to start results
        @param size: int Number of results to return
        @returns raw ElasticSearch query output
        """
        logger.debug(
            'search(doctypes=%s, query=%s, sort=%s, fields=%s, from_=%s, size=%s' % (
                doctypes, query, sort, fields, from_, size
        ))
        if not query:
            raise Exception(
                "Can't do an empty search. Give me something to work with here."
            )
        
        indices = ','.join(
            ['{}{}'.format(INDEX_PREFIX, m) for m in doctypes]
        )
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        _clean_dict(sort)
        sort_cleaned = _clean_sort(sort)
        fields = ','.join(fields)

        results = self.es.search(
            index=indices,
            body=query,
            #sort=sort_cleaned,  # TODO figure out sorting
            from_=from_,
            size=size,
            #_source_include=fields,  # TODO figure out fields
        )
        return results
    
    def reindex(self, source, dest):
        """Copy documents from one index to another.
        
        @param source: str Name of source index.
        @param dest: str Name of destination index.
        @returns: number successful,list of paths that didn't work out
        """
        logger.debug('reindex(%s, %s)' % (source, dest))
        
        if self.index_exists(source):
            logger.info('Source index exists: %s' % source)
        else:
            return '{"status":500, "message":"Source index does not exist"}'
        
        if self.index_exists(dest):
            logger.info('Destination index exists: %s' % dest)
        else:
            return '{"status":500, "message":"Destination index does not exist"}'
        
        version = self.es.info()['version']['number']
        logger.debug('Elasticsearch version %s' % version)
        
        if version >= '2.3':
            logger.debug('new API')
            body = {
                "source": {"index": source},
                "dest": {"index": dest}
            }
            results = self.es.reindex(
                body=json.dumps(body),
                refresh=None,
                requests_per_second=0,
                timeout='1m',
                wait_for_active_shards=1,
                wait_for_completion=False,
            )
        else:
            logger.debug('pre-2.3 legacy API')
            from elasticsearch import helpers
            results = helpers.reindex(
                self.es, source, dest,
                #query=None,
                #target_client=None,
                #chunk_size=500,
                #scroll=5m,
                #scan_kwargs={},
                #bulk_kwargs={}
            )
        return results
    
    def backup(self, snapshot, indices=[]):
        """Make a snapshot backup of one or more Elasticsearch indices.
        
        repository = 'dev20190827'
        snapshot = 'dev-20190828-1007'
        indices = ['ddrpublic-dev', 'encyc-dev']
        agent = 'gjost'
        memo = 'backup before upgrading'
        from DDR import docstore
        ds = docstore.Docstore()
        ds.backup(repository, snapshot, indices, agent, memo)
        
        @param repository: str
        @param snapshot: str
        @param indices: list
        @returns: dict {"repository":..., "snapshot":...}
        """
        repository = os.path.basename(config.ELASTICSEARCH_PATH_REPO)
        client = SnapshotClient(self.es.cluster.client)
        # Get existing repository or make new one
        try:
            repo = client.get_repository(repository=repository)
        except TransportError:
            repo = client.create_repository(
                repository=repository,
                body={
                    "type": "fs",
                    "settings": {
                        "location": config.ELASTICSEARCH_PATH_REPO
                    }
                }
            )
        # Get snapshot info or initiate new one
        try:
            snapshot = client.get(repository=repository, snapshot=snapshot)
        except TransportError:
            body = {
                "indices": indices,
                "metadata": {},
            }
            snapshot = client.create(
                repository=repository, snapshot=snapshot, body=body
            )
        return {
            "repository": repo,
            "snapshot": snapshot,
        }

    def restore_snapshot(self, snapshot, indices=[]):
        """Restore a snapshot
        """
        repository = os.path.basename(config.ELASTICSEARCH_PATH_REPO)
        client = SnapshotClient(self.es.cluster.client)
        repo = client.get_repository(repository=repository)
        result = client.restore(
            repository=config.ELASTICSEARCH_PATH_REPO,
            snapshot=snapshot,
            body={'indices': indices},
        )
        return result


def make_index_name(text):
    """Takes input text and generates a legal Elasticsearch index name.
    
    I can't find documentation of what constitutes a legal ES index name,
    but index names must work in URLs so we'll say alnum plus _, ., and -.
    
    @param text
    @returns name
    """
    LEGAL_NONALNUM_CHARS = ['-', '_', '.']
    SEPARATORS = ['/', '\\',]
    name = []
    if text:
        text = os.path.normpath(text)
        for n,char in enumerate(text):
            if char in SEPARATORS:
                char = '-'
            if n and (char.isalnum() or (char in LEGAL_NONALNUM_CHARS)):
                name.append(char.lower())
            elif char.isalnum():
                name.append(char.lower())
    return ''.join(name)

def doctype_fields(es_class):
    """List content fields in DocType subclass (i.e. appear in _source).
    
    TODO move to ddr-cmdln
    """
    return list(es_class._doc_type.mapping.to_dict()['properties'].keys())

def _clean_dict(data):
    """Remove null or empty fields; ElasticSearch chokes on them.
    
    >>> d = {'a': 'abc', 'b': 'bcd', 'x':'' }
    >>> _clean_dict(d)
    >>> d
    {'a': 'abc', 'b': 'bcd'}
    
    @param data: Standard DDR list-of-dicts data structure.
    """
    if data and isinstance(data, dict):
        for key in list(data.keys()):
            if not data[key]:
                del(data[key])

def _clean_sort( sort ):
    """Take list of [a,b] lists, return comma-separated list of a:b pairs
    
    >>> _clean_sort( 'whatever' )
    >>> _clean_sort( [['a', 'asc'], ['b', 'asc'], 'whatever'] )
    >>> _clean_sort( [['a', 'asc'], ['b', 'asc']] )
    'a:asc,b:asc'
    """
    cleaned = ''
    if sort and isinstance(sort,list):
        all_lists = [1 if isinstance(x, list) else 0 for x in sort]
        if not 0 in all_lists:
            cleaned = ','.join([':'.join(x) for x in sort])
    return cleaned

def _public_fields(modules=MODULES):
    """Lists public fields for each model
    
    IMPORTANT: Adds certain dynamically-created fields
    
    @returns: Dict
    """
    public_fields = {}
    for model,module in modules.items():
        if module:
            mfields = [
                field['name']
                for field in module.FIELDS
                if field.get('elasticsearch',None) \
                and field['elasticsearch'].get('public',None)
            ]
            public_fields[model] = mfields
    # add dynamically created fields
    public_fields['file'].append('path_rel')
    public_fields['file'].append('id')
    return public_fields

def _all_parents(identifiers, excluded_models=['file']):
    """Given a list of identifiers, finds all the parents
    @param identifiers list: List of Identifiers
    @param excluded_models list: List of model names
    @returns: list of Identifiers
    """
    parents = {}
    for oi in identifiers:
        for n,pi in enumerate(oi.lineage()):
            if (pi.model not in excluded_models) and not parents.get(pi.id):
                parents[pi.id] = pi
    return parents
        
def publishable(identifiers, parents, force=False):
    """Determines which paths represent publishable paths and which do not.
    
    @param identifiers list
    @param parents dict: Parent objects by object ID
    @param force: boolean Just publish the damn collection already.
    @returns list of dicts, e.g. [{'path':'/PATH/TO/OBJECT', 'action':'publish'}]
    """
    def object_is_publishable(o):
        """Determines if individual item is publishable."""
        # TODO Hard-coded - use identifier
        if o.identifier.model == 'file':
            if o.public in PUBLIC_OK:
                return True
        else:
            if (o.public and o.status) \
            and (o.public in PUBLIC_OK) \
            and (o.status in STATUS_OK):
                return True
        return False
    
    path_dicts = []
    for oi in identifiers:
        d = {
            'path': oi.path_abs(),
            'identifier': oi,
            'action': 'UNSPECIFIED',
            'note': '',
        }
        # --force
        if force:
            d['action'] = 'POST'
            path_dicts.append(d)
            continue
        # check this object
        # (don't bother checking parents if object is unpublishable)
        canpublish = object_is_publishable(oi.object())
        if not canpublish:
            d['action'] = 'SKIP'
            d['note'] = 'unpublishable'
            path_dicts.append(d)
            continue
        # check parents
        # object is unpublishable if parents are unpublishable
        UNPUBLISHABLE = []
        for n,pi in enumerate(oi.lineage()[1:]):
            if pi != oi:
                canp = object_is_publishable(parents[pi.id])
                if not object_is_publishable(parents[pi.id]):
                    UNPUBLISHABLE.append(pi.id)
        if UNPUBLISHABLE:
            d['action'] = 'SKIP'
            d['note'] = 'parent unpublishable'
            path_dicts.append(d)
            continue
        # passed all the tests
        if canpublish:
            d['action'] = 'POST'
            path_dicts.append(d)
            continue
        # otherwise...
        d['action'] = 'SKIP'
        path_dicts.append(d)
    return path_dicts

def aggs_dict(aggregations):
    """Simplify aggregations data in search results
    
    input
    {
        u'format': {
            u'buckets': [{u'doc_count': 2, u'key': u'ds'}],
            u'doc_count_error_upper_bound': 0,
            u'sum_other_doc_count': 0
        },
        u'rights': {
            u'buckets': [{u'doc_count': 3, u'key': u'cc'}],
            u'doc_count_error_upper_bound': 0, u'sum_other_doc_count': 0
        },
    }
    output
    {
        u'format': {u'ds': 2},
        u'rights': {u'cc': 3},
    }
    """
    return {
        fieldname: {
            bucket['key']: bucket['doc_count']
            for bucket in data['buckets']
        }
        for fieldname,data in list(aggregations.items())
    }

def search_query(text='', must=[], should=[], mustnot=[], aggs={}):
    """Assembles a dict conforming to the Elasticsearch query DSL.
    
    Elasticsearch query dicts
    See https://www.elastic.co/guide/en/elasticsearch/guide/current/_most_important_queries.html
    - {"match": {"fieldname": "value"}}
    - {"multi_match": {
        "query": "full text search",
        "fields": ["fieldname1", "fieldname2"]
      }}
    - {"terms": {"fieldname": ["value1","value2"]}},
    - {"range": {"fieldname.subfield": {"gt":20, "lte":31}}},
    - {"exists": {"fieldname": "title"}}
    - {"missing": {"fieldname": "title"}}
    
    Elasticsearch aggregations
    See https://www.elastic.co/guide/en/elasticsearch/guide/current/aggregations.html
    aggs = {
        'formats': {'terms': {'field': 'format'}},
        'topics': {'terms': {'field': 'topics'}},
    }
    
    >>> from DDR import docstore,format_json
    >>> t = 'posthuman'
    >>> a = [
        {'terms':{'language':['eng','chi']}},
        {'terms':{'creators.role':['distraction']}}
    ]
    >>> q = docstore.search_query(text=t, must=a)
    >>> print(format_json(q))
    >>> d = ['entity','segment']
    >>> f = ['id','title']
    >>> results = docstore.Docstore().search(doctypes=d, query=q, fields=f)
    >>> for x in results['hits']['hits']:
    ...     print x['_source']
    
    @param text: str Free-text search.
    @param must: list of Elasticsearch query dicts (see above)
    @param should:  list of Elasticsearch query dicts (see above)
    @param mustnot: list of Elasticsearch query dicts (see above)
    @param aggs: dict Elasticsearch aggregations subquery (see above)
    @returns: dict
    """
    body = {
        "query": {
            "bool": {
                "must": must,
                "should": should,
                "must_not": mustnot,
            }
        }
    }
    if text:
        body['query']['bool']['must'].append(
            {
                "match": {
                    "_all": text
                }
            }
        )
    if aggs:
        body['aggregations'] = aggs
    return body
