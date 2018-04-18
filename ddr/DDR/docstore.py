"""
TODO pass ES connection object to docstore.* instead of HOSTS,INDEX

example walkthrough:
------------------------------------------------------------------------

HOSTS = [{'host':'192.168.56.101', 'port':9200}]
INDEX = 'documents0'
PATH = '/var/www/media/base/ddr-testing-141'
PATH = '/var/www/media/base/ddr-densho-2'
PATH = '/var/www/media/base/ddr-densho-10'

HOSTS = [{'host':'192.168.56.120', 'port':9200}]
INDEX = 'dev'
PATH = '/var/www/media/ddr'

from DDR import docstore

d = docstore.Docstore(HOSTS, INDEX)

d.delete_index()

d.create_index()

d.init_mappings(INDEX)
d.post_facets(docstore.VOCABS_PATH)

# Delete a collection
d.delete(os.path.basename(PATH), recursive=True)

# Repository, organization metadata
d.repo(path='%s/ddr/repository.json' % PATH, remove=False)
# Do this once per organization.
d.org(path='%s/REPO-ORG/organization.json' % PATH, remove=False)

# Narrators metadata
d.narrators(NARRATORS_PATH)

d.publish(PATH, recursive=True, public=True )

------------------------------------------------------------------------
"""
from __future__ import print_function
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import os

from elasticsearch import Elasticsearch, TransportError
import elasticsearch_dsl
import simplejson as json

from DDR import config
from DDR import converters
from DDR.identifier import Identifier
from DDR.identifier import ELASTICSEARCH_CLASSES
from DDR.identifier import ELASTICSEARCH_CLASSES_BY_MODEL
from DDR.identifier import ID_COMPONENTS, InvalidInputException
from DDR.identifier import MODEL_REPO_MODELS
from DDR.identifier import MODULES, module_for_name
from DDR import modules
from DDR import util
from DDR import vocab

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
raw = elasticsearch.get(HOST, index=settings.DOCUMENT_INDEX, model=Repository.model, id=id)
document = elasticsearch.get(settings.ELASTICSEARCH_HOST_PORT, settings.DOCUMENT_INDEX,
elasticsearch.list_facets():
results = elasticsearch.facet_terms(settings.ELASTICSEARCH_HOST_PORT,
"""


REPOSITORY_LIST_FIELDS = ['id', 'title', 'description', 'url',]
ORGANIZATION_LIST_FIELDS = ['id', 'title', 'description', 'url',]
COLLECTION_LIST_FIELDS = ['id', 'title', 'description',]
ENTITY_LIST_FIELDS = ['id', 'title', 'description',]
FILE_LIST_FIELDS = ['id', 'basename_orig', 'label', 'access_rel','sort',]

REPOSITORY_LIST_SORT = [
    {'repo':'asc'},
]
ORGANIZATION_LIST_SORT = [
    {'repo':'asc'},
    {'org':'asc'},
]
COLLECTION_LIST_SORT = [
    {'repo':'asc'},
    {'org':'asc'},
    {'cid':'asc'},
    {'id':'asc'},
]
ENTITY_LIST_SORT = [
    {'repo':'asc'},
    {'org':'asc'},
    {'cid':'asc'},
    {'eid':'asc'},
    {'id':'asc'},
]
FILE_LIST_SORT = [
    {'repo':'asc'},
    {'org':'asc'},
    {'cid':'asc'},
    {'eid':'asc'},
    {'sort':'asc'},
    {'role':'desc'},
    {'id':'asc'},
]

def all_list_fields():
    LIST_FIELDS = []
    for mf in [REPOSITORY_LIST_FIELDS, ORGANIZATION_LIST_FIELDS,
               COLLECTION_LIST_FIELDS, ENTITY_LIST_FIELDS, FILE_LIST_FIELDS]:
        for f in mf:
            if f not in LIST_FIELDS:
                LIST_FIELDS.append(f)
    return LIST_FIELDS

class InvalidPage(Exception):
    pass
class PageNotAnInteger(InvalidPage):
    pass
class EmptyPage(InvalidPage):
    pass


class Docstore():
    hosts = None
    indexname = None
    facets = None
    es = None

    def __init__(self, hosts=config.DOCSTORE_HOST, index=config.DOCSTORE_INDEX, connection=None):
        self.hosts = hosts
        self.indexname = index
        if connection:
            self.es = connection
        else:
            self.es = Elasticsearch(hosts, timeout=config.DOCSTORE_TIMEOUT)
    
    def __repr__(self):
        return "<%s.%s %s:%s>" % (
            self.__module__, self.__class__.__name__, self.hosts, self.indexname
        )
    
    def print_configs(self):
        print('CONFIG_FILES:           %s' % config.CONFIG_FILES)
        print('')
        print('DOCSTORE_HOST:          %s' % config.DOCSTORE_HOST)
        print('DOCSTORE_INDEX:         %s' % config.DOCSTORE_INDEX)
        print('')
    
    def health(self):
        return self.es.cluster.health()
    
    def index_exists(self, index):
        """
        """
        return self.es.indices.exists(index=index)
    
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
        return [name for name in self.status()['indices'].keys()]
     
    def aliases(self):
        """
        @param hosts: list of dicts containing host information.
        """
        return _parse_cataliases(
            self.es.cat.aliases(h=['index','alias'])
        )
    
    def delete_alias(self, alias, index):
        """Remove specified alias.
        
        @param alias: Name of the alias
        @param index: Name of the alias' target index.
        """
        logger.debug('deleting alias %s -> %s' % (alias, index))
        alias = make_index_name(alias)
        index = make_index_name(index)
        if alias not in [alias for index,alias in self.aliases()]:
            logger.error('Alias does not exist: "%s".' % alias)
            return
        result = self.es.indices.delete_alias(index=index, name=alias)
        logger.debug(result)
        logger.debug('DONE')
        return result
    
    def create_alias(self, alias, index):
        """Point alias at specified index; create index if doesn't exist.
        
        IMPORTANT: There should only ever be ONE alias per index.
        Existing aliases are deleted before specified one is created.
        
        @param alias: Name of the alias
        @param index: Name of the alias' target index.
        """
        logger.debug('creating alias %s -> %s' % (alias, index))
        alias = make_index_name(alias)
        index = make_index_name(index)
        # delete existing alias
        for i,a in self.aliases():
            removed = ''
            if a == alias:
                self.es.indices.delete_alias(
                    # NOTE: "i" is probably not the arg "index".  That's what
                    #       we want. We only want the arg "index".
                    index=i,
                    name=alias
                )
                removed = ' (removed)'
            print('%s -> %s%s' % (a,i,removed))
        result = self.es.indices.put_alias(index=index, name=alias, body='')
        logger.debug(result)
        logger.debug('DONE')
        return result
     
    def target_index(self, alias):
        """Get the name of the index to which the alias points
        
        >>> es.cat.aliases(h=['alias','index'])
        u'documents0 wd5000bmv-2 \n'
        
        @param alias: Name of the alias
        @returns: name of target index
        """
        alias = make_index_name(alias)
        target = []
        for i,a in _parse_cataliases(self.es.cat.aliases(h=['index','alias'])):
            if a == alias:
                target = i
        return target
     
    def create_index(self, index=None):
        """Creates the specified index if it does not already exist.
        
        @returns: JSON dict with status codes and responses
        """
        if not index:
            index = self.indexname
        logger.debug('creating new index: %s' % index)
        body = {
            'settings': {},
            'mappings': {}
            }
        status = self.es.indices.create(index=index, body=body)
        logger.debug(status)
        statuses = self.init_mappings()
        self.model_fields_lists()
        logger.debug('DONE')
     
    def delete_index(self, index=None):
        """Delete the specified index.
        
        @returns: JSON dict with status code and response
        """
        if not index:
            index = self.indexname
        logger.debug('deleting index: %s' % index)
        if self.index_exists(index):
            status = self.es.indices.delete(index=index)
        else:
            status = '{"status":500, "message":"Index does not exist"}'
        logger.debug(status)
        return status
    
    def init_mappings(self):
        """Initializes mappings for Elasticsearch objects
        
        Mappings for objects in (ddr-defs)repo_models.elastic.ELASTICSEARCH_CLASSES
                
        @returns: JSON dict with status code and response
        """
        logger.debug('registering doc types')
        statuses = []
        for class_ in ELASTICSEARCH_CLASSES['all']:
            logger.debug('- %s' % class_['doctype'])
            print('- %s' % class_)
            status = class_['class'].init(index=self.indexname, using=self.es)
            statuses.append( {'doctype':class_['doctype'], 'status':status} )
        return statuses

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
        for model in MODEL_REPO_MODELS.keys():
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
    
    def get_mappings(self, raw=False):
        """Get mappings for ESObjects
        
        @param raw: boolean Use lower-level function to get all mappings
        @returns: str JSON
        """
        if raw:
            return self.es.indices.get_mapping(self.indexname)
        return {
            class_['doctype']: elasticsearch_dsl.Mapping.from_es(
                index=self.indexname,
                doc_type=class_['doctype'],
                using=self.es,
            ).to_dict()
            for class_ in ELASTICSEARCH_CLASSES['all']
        }
    
    def post_vocabs(self, path=config.VOCABS_PATH):
        """Posts ddr-vocab facets,terms to ES.
        
        curl -XPUT 'http://localhost:9200/meta/facet/format' -d '{ ... }'
        >>> elasticsearch.post_facets(
            '192.168.56.120:9200', 'meta',
            '/opt/ddr-local/ddr-vocab'
            )
        
        @param path: Absolute path to dir containing facet files.
        @returns: JSON dict with status code and response
        """
        logger.debug('index_facets(%s, %s)' % (self.indexname, path))
        vocabs = vocab.get_vocabs_all(path)
        
        # get classes from ddr-defs
        Facet = ELASTICSEARCH_CLASSES_BY_MODEL['facet']
        FacetTerm = ELASTICSEARCH_CLASSES_BY_MODEL['facetterm']
        
        # push facet data
        statuses = []
        for v in vocabs.keys():
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
            status = facet.save(using=self.es, index=self.indexname)
            statuses.append(status)
            
            for t in vocabs[v]['terms']:
                tid = t.pop('id')
                facetterm_id = '-'.join([
                    str(fid),
                    str(tid),
                ])
                term = FacetTerm()
                term.meta.id = facetterm_id
                term.id = facetterm_id
                term.facet = fid
                term.term_id = tid
                term.links_html = facetterm_id
                term.links_json = facetterm_id
                # TODO doesn't handle location_geopoint
                for field in FacetTerm._doc_type.mapping.to_dict()[
                        FacetTerm._doc_type.name]['properties'].keys():
                    if t.get(field):
                        setattr(term, field, t[field])
                logging.debug(term)
                status = term.save(using=self.es, index=self.indexname)
                statuses.append(status)
                
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
        with open(path, 'r') as f:
            data = json.loads(f.read())
        if (not (data.get('id') and data.get('repo'))):
            raise Exception('Data file is not well-formed.')
        oi = Identifier(id=data['id'])
        d = OrderedDict()
        d['id'] = oi.id
        d['model'] = oi.model
        d['parent_id'] = oi.parent_id(stubs=1)
        # links
        d['links_html'] = oi.id
        d['links_json'] = oi.id
        d['links_img'] = '%s/logo.png' % oi.id
        d['links_thumb'] = '%s/logo.png' % oi.id
        d['links_parent'] = oi.parent_id(stubs=1)
        d['links_children'] = oi.id
        # title,description
        d['title'] = data['title']
        d['description'] = data['description']
        d['url'] = data['url']
        # ID components (repo, org, cid, ...) as separate fields
        idparts = deepcopy(oi.idparts)
        idparts.pop('model')
        for k in ID_COMPONENTS:
            d[k] = '' # ensure all fields present
        for k,v in idparts.iteritems():
            d[k] = v
        # add/update
        if remove and self.exists(doctype, oi):
            results = self.es.delete(
                index=self.indexname, doc_type=doctype, id=oi.id
            )
        else:
            results = self.es.index(
                index=self.indexname, doc_type=doctype, id=oi.id, body=d
            )
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
        DOC_TYPE = 'narrator'
        with open(path, 'r') as f:
            data = json.loads(f.read())
        for document in data['narrators']:
            has_published = document.get('has_published', '')
            if has_published.isdigit():
                has_published = int(has_published)
            if has_published:
                result = self.post_json(DOC_TYPE, document['id'], json.dumps(document))
                logging.debug(document['id'], result)
            else:
                logging.debug('%s not published' % document['id'])
                if self.get(DOC_TYPE, document['id'], fields=[]):
                    self.delete(document['id'])
    
    def post_json(self, doc_type, document_id, json_text):
        """POST the specified JSON document as-is.
        
        @param doc_type: str
        @param document_id: str
        @param json_text: str JSON-formatted string
        @returns: dict Status info.
        """
        logger.debug('post_json(%s, %s, %s)' % (
            self.indexname, doc_type, document_id
        ))
        return self.es.index(
            index=self.indexname, doc_type=doc_type, id=document_id, body=json_text
        )

    def post(self, document, public_fields=[], additional_fields={}, parents={}, force=False):
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
        @param force: boolean Bypass status and public checks.
        @returns: JSON dict with status code and response
        """
        logger.debug('post(%s, %s, %s)' % (
            self.indexname, document, force
        ))

        if force:
            publishable = True
            public = False
        else:
            if not parents:
                parents = _parents_status([document.identifier.path_abs()])
            publishable = _publishable([document.identifier.path_abs()], parents)
            public = True
        if not publishable:
            return {'status':403, 'response':'object not publishable'}

        d = document.to_esobject(public_fields=public_fields, public=public)
        logger.debug('saving')
        status = d.save(using=self.es, index=self.indexname)
        logger.debug(str(status))
        return status
    
    def post_multi(self, path, recursive=False, force=False):
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
        @returns: number successful,list of paths that didn't work out
        """
        logger.debug('index(%s, %s, %s, %s)' % (self.indexname, path, recursive, force))
        
        publicfields = _public_fields()
        
        # process a single file if requested
        if os.path.isfile(path):
            paths = [path]
        else:
            # files listed first, then entities, then collections
            paths = util.find_meta_files(path, recursive, files_first=1)
        
        # Store value of public,status for each collection,entity.
        # Values will be used by entities and files to inherit these values
        # from their parent.
        parents = _parents_status(paths)
        
        # Determine if paths are publishable or not
        paths = _publishable(paths, parents, force=force)
        
        skipped = 0
        successful = 0
        bad_paths = []
        
        num = len(paths)
        for n,path in enumerate(paths):
            oi = path.get('identifier')
            # TODO write logs instead of print
            print('%s | %s/%s %s %s %s' % (
                datetime.now(config.TZ), n+1, num, path['action'], oi.id, path['note'])
            )
            
            if not oi:
                path['note'] = 'No identifier'
                bad_paths.append(path)
                continue
            document = oi.object()
            if not document:
                path['note'] = 'No document'
                bad_paths.append(path)
                continue
            
            # see if document exists
            existing_v = None
            d = self.get(oi.model, oi.id)
            if d:
                existing_v = d.meta.version
            
            # post document
            if path['action'] == 'POST':
                created = self.post(document, parents=parents, force=force)
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
            es_model = ELASTICSEARCH_CLASSES_BY_MODEL[oi.model]._doc_type.name
            d = self.get(es_model, oi.id)
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
        return self.es.exists(index=self.indexname, doc_type=model, id=document_id)
     
    def get(self, model, document_id, fields=None):
        """
        @param model:
        @param document_id:
        @param fields: boolean Only return these fields
        """
        if self.exists(model, document_id):
            ES_Class = ELASTICSEARCH_CLASSES_BY_MODEL[model]
            return ES_Class.get(document_id, using=self.es, index=self.indexname)
        return None

    def count(self, doctypes=[], query={}):
        """Executes a query and returns number of hits.
        
        The "query" arg must be a dict that conforms to the Elasticsearch query DSL.
        See docstore.search_query for more info.
        
        @param doctypes: list Type of object ('collection', 'entity', 'file')
        @param query: dict The search definition using Elasticsearch Query DSL
        @returns raw ElasticSearch query output
        """
        logger.debug('count(index=%s, doctypes=%s, query=%s' % (
            self.indexname, doctypes, query
        ))
        if not query:
            raise Exception("Can't do an empty search. Give me something to work with here.")
        
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        
        return self.es.count(
            index=self.indexname,
            doc_type=doctypes,
            body=query,
        )
    
    def delete(self, document_id, recursive=False):
        """Delete a document and optionally its children.
        
        @param document_id:
        @param recursive: True or False
        """
        identifier = Identifier(id=document_id)
        if recursive:
            if identifier.model == 'collection': doc_type = 'collection,entity,file'
            elif identifier.model == 'entity': doc_type = 'entity,file'
            elif identifier.model == 'file': doc_type = 'file'
            query = 'id:"%s"' % identifier.id
            try:
                return self.es.delete_by_query(
                    index=self.indexname, doc_type=doc_type, q=query
                )
            except TransportError:
                pass
        else:
            try:
                return self.es.delete(
                    index=self.indexname, doc_type=identifier.model, id=identifier.id
                )
            except TransportError:
                pass

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
        logger.debug('search(index=%s, doctypes=%s, query=%s, sort=%s, fields=%s, from_=%s, size=%s' % (
            self.indexname, doctypes, query, sort, fields, from_, size
        ))
        if not query:
            raise Exception("Can't do an empty search. Give me something to work with here.")
        
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        _clean_dict(sort)
        sort_cleaned = _clean_sort(sort)
        fields = ','.join(fields)
        
        results = self.es.search(
            index=self.indexname,
            doc_type=doctypes,
            body=query,
            sort=sort_cleaned,
            from_=from_,
            size=size,
            _source_include=fields,
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

def _parse_cataliases( cataliases ):
    """
    Sample input:
    u'ddrworkstation documents0 \nwd5000bmv-2 documents0 \n'
 
    @param cataliases: Raw output of es.cat.aliases(h=['index','alias'])
    @returns: list of (index,alias) tuples
    """
    indices_aliases = []
    for line in cataliases.strip().split('\n'):
        # cat.aliases arranges data in columns so rm extra spaces
        while '  ' in line:
            line = line.replace('  ', ' ')
        if line:
            i,a = line.strip().split(' ')
            indices_aliases.append( (i,a) )
    return indices_aliases

def doctype_fields(es_class):
    """List content fields in DocType subclass (i.e. appear in _source).
    
    TODO move to ddr-cmdln
    """
    return es_class._doc_type.mapping.to_dict()[es_class._doc_type.name]['properties'].keys()

def _filter_payload(data, public_fields):
    """If requested, removes non-public fields from document before sending to ElasticSearch.
    
    >>> data = [{'id': 'ddr-testing-123-1'}, {'title': 'Title'}, {'secret': 'this is a secret'}]
    >>> public_fields = ['id', 'title']
    >>> _filter_payload(data, public_fields)
    removed secret
    >>> data
    [{'id': 'ddr-testing-123-1'}, {'title': 'Title'}]
    
    @param data: Standard DDR list-of-dicts data structure.
    @param public_fields: List of field names; if present, fields not in list will be removed.
    """
    if public_fields and data and isinstance(data, list):
        for field in data[1:]:
            fieldname = field.keys()[0]
            if fieldname not in public_fields:
                data.remove(field)
                logging.debug('removed %s' % fieldname)

def _clean_controlled_vocab(data):
    """Extract topics IDs from textual control-vocab texts.

    >>> _clean_controlled_vocab('Topics [123]')
    ['123']
    >>> _clean_controlled_vocab(['Topics [123]'])
    ['123']
    >>> _clean_controlled_vocab(['123'])
    ['123']
    >>> _clean_controlled_vocab([123])
    ['123']
    >>> _clean_controlled_vocab('123')
    ['123']
    >>> _clean_controlled_vocab(123)
    ['123']
    
    @param data: contents of data field
    @returns: list of ID strings
    """
    if isinstance(data, int):
        data = str(data)
    if isinstance(data, basestring):
        data = [data]
    cleaned = []
    for x in data:
        if not isinstance(x, basestring):
            x = str(x)
        if ('[' in x) and (']' in x):
            y = x.split('[')[1].split(']')[0] 
        else:
            y = x
        cleaned.append(y)
    return cleaned

def _clean_dict(data):
    """Remove null or empty fields; ElasticSearch chokes on them.
    
    >>> d = {'a': 'abc', 'b': 'bcd', 'x':'' }
    >>> _clean_dict(d)
    >>> d
    {'a': 'abc', 'b': 'bcd'}
    
    @param data: Standard DDR list-of-dicts data structure.
    """
    if data and isinstance(data, dict):
        for key in data.keys():
            if not data[key]:
                del(data[key])

def _clean_payload(data):
    """Remove null or empty fields; ElasticSearch chokes on them.
    """
    # remove info about DDR release, git-annex version, etc
    if data and isinstance(data, list):
        # skip the initial metadata field
        data = data[1:]
        # remove empty fields
        for field in data:
            # rm null or empty fields
            _clean_dict(field)

def _format_datetimes(data):
    """force datetimes into ES mapping format
    # TODO refactor this once we get it working
    """
    DATETIME_FIELDS = [
        'record_created',
        'record_lastmod',
    ]
    for field,value in data.iteritems():
        if field in DATETIME_FIELDS:
            dt = converters.text_to_datetime(value)
            # Use default timezone unless...
            if data['org'] in config.ALT_TIMEZONES.keys():
                timezone = config.ALT_TIMEZONES[data['org']]
            else:
                timezone = config.TZ
            if not dt.tzinfo:
                timezone.localize(dt)
            data[field] = converters.datetime_to_text(
                dt, config.ELASTICSEARCH_DATETIME_FORMAT
            )

def _filter_fields(i, data):
    """Run index_* functions on data
    
    @param i: Identifier
    @param data: dict
    @returns: dict data
    """
    module = i.fields_module()
    for field in module.FIELDS:
        fieldname = field['name']
        # run index_* functions on field data if present
        data[fieldname] = modules.Module(module).function(
            'index_%s' % fieldname,
            data[fieldname]
        )
    return data

def _validate_number(number, num_pages):
        """Validates the given 1-based page number.
        see django.core.pagination.Paginator.validate_number
        """
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise PageNotAnInteger('That page number is not an integer')
        if number < 1:
            raise EmptyPage('That page number is less than 1')
        if number > num_pages:
            if number == 1:
                pass
            else:
                raise EmptyPage('That page contains no results')
        return number

def _page_bottom_top(total, index, page_size):
        """
        Returns a Page object for the given 1-based page number.
        """
        num_pages = total / page_size
        if total % page_size:
            num_pages = num_pages + 1
        number = _validate_number(index, num_pages)
        bottom = (number - 1) * page_size
        top = bottom + page_size
        return bottom,top,num_pages

def massage_query_results(results, thispage, page_size):
    """Takes ES query, makes facsimile of original object; pads results for paginator.
    
    Problem: Django Paginator only displays current page but needs entire result set.
    Actually, it just needs a list that is the same size as the actual result set.
    
    GOOD:
    Do an ElasticSearch search, without ES paging.
    Loop through ES results, building new list, process only the current page's hits
    hits outside current page added as placeholders
    
    BETTER:
    Do an ElasticSearch search, *with* ES paging.
    Loop through ES results, building new list, processing all the hits
    Pad list with empty objects fore and aft.
    
    @param results: ElasticSearch result set (non-empty, no errors)
    @param thispage: Value of GET['page'] or 1
    @param page_size: Number of objects per page
    @returns: list of hit dicts, with empty "hits" fore and aft of current page
    """
    def unlistify(o, fieldname):
        if o.get(fieldname, None):
            if isinstance(o[fieldname], list):
                o[fieldname] = o[fieldname][0]
    
    objects = []
    if results and results['hits']:
        total = results['hits']['total']
        bottom,top,num_pages = _page_bottom_top(total, thispage, page_size)
        # only process this page
        for n,hit in enumerate(results['hits']['hits']):
            o = {'n':n,
                 'id': hit['_id'],
                 'placeholder': True}
            if (n >= bottom) and (n < top):
                # if we tell ES only return certain fields, object is in 'fields'
                if hit.get('fields', None):
                    o = hit['fields']
                elif hit.get('_source', None):
                    o = hit['_source']
                # copy ES results info to individual object source
                o['index'] = hit['_index']
                o['type'] = hit['_type']
                o['model'] = hit['_type']
                o['id'] = hit['_id']
                # ElasticSearch wraps field values in lists
                # when you use a 'fields' array in a query
                for fieldname in all_list_fields():
                    unlistify(o, fieldname)
            objects.append(o)
    return objects

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
    for model,module in modules.iteritems():
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

def _parents_status( paths ):
    """Stores value of public,status for each collection,entity so entities,files can inherit.
    
    @param paths
    @returns: dict
    """
    parents = {}
    def _make_coll_ent(path):
        """Store values of id,public,status for a collection or entity.
        """
        p = {'id':None,
             'public':None,
             'status':None,}
        with open(path, 'r') as f:
            data = json.loads(f.read())
        for field in data:
            fname = field.keys()[0]
            if fname in p.keys():
                p[fname] = field[fname]
        return p
    for path in paths:
        if ('collection.json' in path) or ('entity.json' in path):
            o = _make_coll_ent(path)
            parents[o.pop('id')] = o
    return parents

def _file_parent_ids(identifier):
    """Calculate the parent IDs of an entity or file from the filename.
    
    TODO not specific to elasticsearch - move this function so other modules can use
    
    >>> _file_parent_ids('collection', '.../ddr-testing-123/collection.json')
    []
    >>> _file_parent_ids('entity', '.../ddr-testing-123-1/entity.json')
    ['ddr-testing-123']
    >>> _file_parent_ids('file', '.../ddr-testing-123-1-master-a1b2c3d4e5.json')
    ['ddr-testing-123', 'ddr-testing-123-1']
    
    @param identifier: Identifier
    @returns: parent_ids
    """
    return [
        identifier.parent_id(),
        identifier.collection_id(),
    ]

def _publishable(paths, parents, force=False):
    """Determines which paths represent publishable paths and which do not.
    
    @param paths
    @param parents
    @param force: boolean Just publish the damn collection already.
    @returns list of dicts, e.g. [{'path':'/PATH/TO/OBJECT', 'action':'publish'}]
    """
    path_dicts = []
    for path in paths:
        d = {
            'path': path,
            'identifier': Identifier(path=path),
            'action': 'UNSPECIFIED',
            'note': '',
        }
        
        if force:
            d['action'] = 'POST'
            path_dicts.append(d)
            continue
        
        # see if item incomplete or nonpublic
        
        # see if item's parents are incomplete or nonpublic
        # TODO Bad! Bad! Generalize this...
        UNPUBLISHABLE = []
        for parent_id in _file_parent_ids(d['identifier']):
            parent = parents.get(parent_id, {})
            for x in parent.itervalues():
                if (x not in STATUS_OK) and (x not in PUBLIC_OK):
                    if parent_id not in UNPUBLISHABLE:
                        UNPUBLISHABLE.append(parent_id)
        if UNPUBLISHABLE:
            d['action'] = 'SKIP'
            d['note'] = 'parent unpublishable'
            path_dicts.append(d)
            continue
        
        if path and d['identifier'].model:
            d['action'] = 'POST'
        path_dicts.append(d)
    
    return path_dicts

def _has_access_file( identifier ):
    """Determines whether the path has a corresponding access file.
    
    @param path: Absolute or relative path to JSON file.
    @param suffix: Suffix that is applied to File ID to get access file.
    @returns: True,False
    """
    access_abs = identifier.path_abs('access')
    if os.path.exists(access_abs) or os.path.islink(access_abs):
        return True
    return False

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
    >>> a = [{'terms':{'language':['eng','chi']}}, {'terms':{'creators.role':['distraction']}}]
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
