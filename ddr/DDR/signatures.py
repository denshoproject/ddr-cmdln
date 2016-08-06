"""
TODO signature chooser
- make list of all the metadata file
- sort the list
- for each non-file,
- enumerate files list from LAST (first time, this is 0)
- if file is child of ancestor, see if publishable
- if child of ancestor and publishable, assign file_id to sig id, set LAST to n

could probably short-circuit this by keeping track of the index in files list at which last sig found.  start there on next entity

l = 'abcdefghijklmnopqrstuvqxyz'
last = 0
for n,x in enumerate(l[last:]):
    print n+last,x

last = 5
for n,x in enumerate(l[last:]):
    print n+last,x

TODO update file __lt__ function to assign number to roles so that mezz comes before master

EXAMPLE

from DDR import signatures
paths = signatures.metadata_paths('/var/www/media/ddr/ddr-testing-333')
identifiers = signatures.load_identifiers(paths, '/var/www/media/ddr')
signatures.choose_signatures(identifiers)
signatures.print_identifiers(identifiers)

"""

import json
import os

from DDR import config
from DDR import identifier
from DDR import models
from DDR import util


# - MAKE LIST OF ALL THE METADATA FILE

def metadata_paths(collection_path):
    return util.find_meta_files(collection_path, recursive=True, files_first=True, force_read=True)


JSON_FIELDS = {
    'sort': 1,
    'signature': '',
}

ROLE_NUMBERS = {
    'mezzanine': 0,
    'master': 1,
    'transcript': 2,
}

class SigIdentifier(identifier.Identifier):
    sort = 999999
    signature = None
    sort_key = None
    
    def __init__(self, *args, **kwargs):
        # Load Identifier, read .json and add sort/signature to self
        # These are used for sorting
        super(SigIdentifier, self).__init__(*args, **kwargs)
        
        # read from .json file
        data = self._read_fields(self.path_abs('json'))
        for key,val in data.iteritems():
            if val:
                setattr(self, key, val)
        
        # prep sorting key
        # *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
        # TODO signature don't consider file unless has access_rel
        # ENTITIES ARE STILL SORTING BY ID ! ! !
        # *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
        sort_key = self.parts
        if self.model == 'file':
            sort_key['role'] = ROLE_NUMBERS[self.parts['role']]
            sha1 = sort_key.pop('sha1')
            sort_key['sort'] = self.sort
            sort_key['sha1'] = sha1
        elif self.model == 'entity':
            eid = sort_key.pop('eid')
            sort_key['sort'] = self.sort
            sort_key['eid'] = eid
            
        self.sort_key = sort_key.values()
    
    def _read_fields(self, path):
        # extracts only specified fields from JSON
        data = {}
        with open(path, 'r') as f:
            for d in json.loads(f.read()):
                key = d.keys()[0]
                if key in JSON_FIELDS.keys():
                    # coerces to int
                    if d.get(key) and isinstance(JSON_FIELDS[key], int):
                        data[key] = int(d[key])
                    else:
                        data[key] = d[key]
        return data
    
    def __repr__(self):
        return '<%s.%s %s:%s sort=%s,sig=%s>' % (
            self.__module__, self.__class__.__name__, self.model, self.id,
            self.sort, self.signature
        )
    
    def __lt__(self, other):
        """Enables Pythonic sorting"""
        return self.sort_key < other.sort_key

def load_identifiers(paths, basepath):
    """Loads and sorts list
    """
    identifiers_unsorted = {}
    for path in paths:
        if util.path_matches_model(path, 'file'):
            # this works on file paths but not on entities/collections
            basename_noext = os.path.splitext(os.path.basename(path))[0]
            i = SigIdentifier(basename_noext, base_path=basepath)
        else:
            # entities, segments, collections
            oid = os.path.basename(os.path.dirname(path))
            i = SigIdentifier(oid, base_path=basepath)
        if not identifiers_unsorted.get(i.model):
            identifiers_unsorted[i.model] = []
        identifiers_unsorted[i.model].append(i)
        
    # - SORT THE LISTS
    identifiers = {}
    for model,ids in identifiers_unsorted.iteritems():
        identifiers[model] = sorted(ids)
    return identifiers

def models_parent_child():
    """List pairs of parent-child models, bottom-up
    
    @returns: list [(parent,child), ...]
    """
    repo_models = identifier.MODEL_REPO_MODELS.keys()
    pairs = []
    for i in identifier.IDENTIFIERS:
        pmodel = i['model']
        if pmodel in repo_models:
            for cmodel in i['children']:
                pairs.append((pmodel,cmodel))
    pairs.reverse()
    return pairs

def choose_signatures(identifiers):
    # - FOR EACH NON-FILE,
    # - ENUMERATE FILES LIST FROM LAST (FIRST TIME, THIS IS 0)
    # - IF FILE IS CHILD OF ANCESTOR, SEE IF PUBLISHABLE
    # - IF CHILD OF ANCESTOR AND PUBLISHABLE, ASSIGN FILE_ID TO SIG ID, SET LAST TO n
    model_pairs = models_parent_child()
    for parent_model,child_model in model_pairs:
        last = 0
        for pi in identifiers.get(parent_model, []):
            # loop through list of children, starting with the last
            for n,ci in enumerate(identifiers.get(child_model, [])[last:]):
                if pi.id in ci.id:
                    pi.signature = ci
                    last = n + last
                    break
    # At this point, collection.signature and possibly some entity.signature,
    # will not be Files.
    # Go back through and replace these with files
    parent_models = identifier.CHILDREN.keys()
    # don't waste time looping on files
    for model in parent_models:
        for i in identifiers[model]:
            if i.signature:
                if i.signature.model is not 'file':


def ultimate_sig(identifier):
    if identifier.signature and (identifier.signature.model == 'file'):
        return identifier.signature
    return ultimate_sig(identifier.signature)
    


MODELS_DOWN = [
    'collection',
    'entity',
    'segment',
    'file',
]

def print_identifiers(identifiers, models=MODELS_DOWN):
    for model in models:
        if model in identifiers.keys():
            for oid in identifiers[model]:
                print oid
