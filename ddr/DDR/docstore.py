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
from ssl import create_default_context
import traceback

from elastictools import docstore
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


class Docstore(docstore.Docstore):

    def __init__(self, index_prefix, host, settings):
        super(Docstore, self).__init__(index_prefix, host, settings)


class DocstoreManager(docstore.DocstoreManager):

    def __init__(self, index_prefix, host, settings):
        super(DocstoreManager, self).__init__(index_prefix, host, settings)

    def print_configs(self, host=config.DOCSTORE_HOST):
        print('CONFIG_FILES:           %s' % config.CONFIG_FILES)
        print('')
        print('DOCSTORE_HOST:          %s' % host)
        print('')

    def create_indices(self):
        return super(DocstoreManager,self).create_indices(ELASTICSEARCH_CLASSES['all'])

    def delete_indices(self):
        return super(DocstoreManager,self).delete_indices(ELASTICSEARCH_CLASSES['all'])
    
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
                if self.get(
                        doctype, ELASTICSEARCH_CLASSES_BY_MODEL[doctype],
                        d.id, fields=[]
                ):
                    self.delete(doctype, d.id)

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
            d = self.get(
                model=oi.model,
                es_class=ELASTICSEARCH_CLASSES_BY_MODEL[oi.model],
                document_id=oi.id
            )
            if d:
                existing_v = d.meta.version
            
            # post document
            if path['action'] == 'POST':
                try:
                    created = self.post(
                        document, parents=parents, b2=b2_synced, force=True
                    )
                except Exception as err:
                    traceback.print_exc()
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
            d = self.get(
                model=oi.model,
                es_class=ELASTICSEARCH_CLASSES_BY_MODEL[oi.model],
                document_id=oi.id
            )
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
            try:
                result = self.es.delete(index=self.index_name(model), id=oi.id)
                print(f'{n}/{num} DELETE {self.index_name(model)} {oi.id} -> {result["result"]}')
            except docstore.NotFoundError as err:
                print(f'{n}/{num} DELETE {self.index_name(model)} {oi.id} -> 404 Not Found')


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
