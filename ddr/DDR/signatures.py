"""
signatures - functions for choosing signatures of entity and collection objects

Entity and Collection objects have a "signature_id" field.

EXAMPLE

collection_path = '/var/www/media/ddr/ddr-testing-333'
basepath = '/var/www/media/ddr'
from DDR import signatures
from DDR import util
paths = util.find_meta_files(collection_path, recursive=True, files_first=True, force_read=True)
identifiers = signatures.signatures(paths, basepath)

"""

from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import os

import simplejson as json

from DDR import config
from DDR import commands
from DDR import fileio
from DDR import identifier
from DDR import models
from DDR import util


JSON_FIELDS = {
    'public': -1,
    'status': '',
    'signature_id': '',
    'sort': 1,
}

# Valid component values from ddr-defs/repo_models/identifier.py
# file-role['component']['valid']
ROLE_NUMBERS = {
    role: n
    for n,role in enumerate(identifier.VALID_COMPONENTS['role'])
}

class SigIdentifier(identifier.Identifier):
    """Subclass of Identifier used for finding/assigning object signature files
    
    NOTE: Reads object JSON file during construction.
    """
    model = None
    public = None
    status = None
    sort = 999999
    signature = None    # immediate signature, next in chain
    signature_id = None # ID of ultimate signature (should be a file)
    sort_key = None
    
    def __repr__(self):
        return '<%s.%s %s:%s sort=%s,sid=%s>' % (
            self.__module__, self.__class__.__name__,
            self.model, self.id,
            self.sort, self.signature_id
        )
    
    def __lt__(self, other):
        """Enables Pythonic sorting"""
        return self.sort_key < other.sort_key
    
    def __init__(self, *args, **kwargs):
        """Load Identifier, read .json and add extra fields
        
        Extra fields are used for sorting and tracking signature_id
        """
        super(SigIdentifier, self).__init__(*args, **kwargs)
        
        # read from .json file
        data = self._read_fields(self.path_abs('json'))
        for key,val in data.items():
            if val:
                setattr(self, key, val)
        
        # prep sorting key
        # TODO refactor - knows too much about model definitions!
        sort_key = self.parts
        if self.model == 'file':
            # insert file sort before sha1
            sort_key['role'] = ROLE_NUMBERS[self.parts['role']]
            sha1 = sort_key.pop('sha1')
            sort_key['sort'] = self.sort
            sort_key['sha1'] = sha1
        elif self.model == 'entity':
            # insert entity sort before eid
            eid = sort_key.pop('eid')
            sort_key['sort'] = self.sort
            sort_key['eid'] = eid
            
        self.sort_key = list(sort_key.values())
    
    def _read_fields(self, path):
        """Extracts specified fields from JSON
        """
        data = {}
        for d in json.loads(fileio.read_text(path)):
            key = list(d.keys())[0]
            if key in list(JSON_FIELDS.keys()):
                # coerces to int
                if d.get(key) and isinstance(JSON_FIELDS[key], int):
                    data[key] = int(d[key])
                else:
                    data[key] = d[key]
        return data

    def publishable(self):
        """Determine if publishable based on .public and .status
        """
        # TODO refactor this - it knows too much about model definitions
        # duplicates ddr-filter.is_publishable
        # duplicates ddr-filter.is_publishable_file
        #print('%s model:%s public:%s status:%s' % (
        #    self.id, self.model, self.public, self.status
        #))

        ## 2019-05-15 geoff.froh decides .public and .status no longer required
        #if self.model in identifier.NODES:
        #    if self.public:
        #        return True
        #else:
        #    if self.public and (self.status == 'completed'):
        #        return True
        #return False
        return True
    
    def _signature_id(self):
        """Follow chain of object signatures to end, return last object.id
        """
        if not self.signature:
            return self.id
        return self.signature._signature_id()


def choose(paths):
    """Reads data files, gathers *published* Identifiers, sorts and maps parents->nodes
    
    Outside function is responsible for reading object JSON files
    and extracting value of signature_id
    
    @param paths: list of object file paths from util.find_meta_files
    @returns: list of parent Identifiers
    """
    nodes = []
    parents = []
    for path in paths:
        i = SigIdentifier(path=path)
        if i.publishable():
            if i.model in identifier.NODES:
                nodes.append(i)
            else:
                parents.append(i)
    
    nodes.sort()
    parents.sort()

    # for each node, and for each parent
    # signature is the first parent ID that is in the node ID
    # NOTE: Most collections have more nodes than parents,
    # so go node:parent to keep number of iterations down
    for pi in parents:
        for ni in nodes:
            if '%s-' % pi.id in str(ni.id):
                pi.signature_id = ni.id
                break
    
    return parents

def find_updates(identifiers):
    """Identifies files to be updated
    
    @param identifiers: list of parent Identifiers, with .signature_id attrs
    @returns: list of objects (Collections, Entities, etc)
    """
    start = datetime.now(config.TZ)
    updates = []
    for n,i in enumerate(identifiers):
        if not i.model in identifier.NODES:
            o = i.object()
            # normalize
            if not o.signature_id: o.signature_id = ''
            if not i.signature_id: i.signature_id = ''
            # only write file if changed
            orig_value = o.signature_id
            status = ''
            if o.signature_id != i.signature_id:
                status = 'updated (%s -> %s)' % (o.signature_id, i.signature_id)
                o.signature_id = i.signature_id
                updates.append(o)
            logging.debug('| %s/%s %s %s' % (n+1, len(identifiers), i.id, status))
    finish = datetime.now(config.TZ)
    elapsed = finish - start
    logging.debug('ok (%s elapsed)' % elapsed)
    return updates

def write_updates(updates):
    """Write metadata of updated objects to disk.
    
    @param updates: list of objects (Collections, Entities, Files, etc)
    @returns: list of updated files (relative paths)
    """
    start = datetime.now(config.TZ)
    logging.debug('Writing changes')
    written = []
    for n,o in enumerate(updates):
        o.write_json()
        written.append(o.identifier.path_abs('json'))
        logging.debug('| %s/%s %s' % (n+1, len(updates), o.id))
    finish = datetime.now(config.TZ)
    elapsed = finish - start
    logging.debug('ok (%s elapsed)' % elapsed)
    logging.debug('NOTE: METADATA FILES ARE NOT YET COMMITTED!')
    return written

def commit_updates(collection, files_written, git_name, git_mail, agent, commit=False):
    """Commit written files to git repository.
    
    @param files_written: list
    @returns: (int,str) status,message (from commands.update)
    """
    if files_written:
        logger.debug('Committing changes')
        return commands.update(
            git_name, git_mail,
            collection,
            files_written,
            agent,
            commit
        )
    return 0,'no files to write'
