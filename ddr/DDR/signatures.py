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
sigs = signatures.choose_signatures(identifiers, 'entity', 'file')

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
    'public': 1,
    'sort': 1,
    'signature_file': '',
}

ROLE_NUMBERS = {
    'mezzanine': 0,
    'master': 1,
    'transcript': 2,
}

class SigIdentifier(identifier.Identifier):
    public = 0
    sort = 999999
    signature_file = None
    sort_key = None
    
    def __init__(self, *args, **kwargs):
        # Load Identifier, read .json and add public/sort/signature to self
        # These are used for sorting
        super(SigIdentifier, self).__init__(*args, **kwargs)
        
        # read from .json file
        data = self._read_fields(self.path_abs('json'))
        for key,val in data.iteritems():
            self.key = val
        
        # prep sorting key
        sort_key = self.parts
        if self.model == 'file':
            sort_key['role'] = ROLE_NUMBERS[self.parts['role']]
            sha1 = sort_key.pop('sha1')
            sort_key['sort'] = self.sort
            sort_key['sha1'] = sha1
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
        return "<%s.%s %s:%s>" % (self.__module__, self.__class__.__name__, self.model, self.id)
    
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

def choose_signatures(identifiers, parent_model, child_model='file'):
    # - FOR EACH NON-FILE,
    # - ENUMERATE FILES LIST FROM LAST (FIRST TIME, THIS IS 0)
    # - IF FILE IS CHILD OF ANCESTOR, SEE IF PUBLISHABLE
    # - IF CHILD OF ANCESTOR AND PUBLISHABLE, ASSIGN FILE_ID TO SIG ID, SET LAST TO n
    sigs = {}
    last = 0
    for pi in identifiers[parent_model]:
        for n,ci in enumerate(identifiers[child_model][last:]):
            if pi.id in ci.id:
                sigs[pi.id] = ci.id
                last = n + last
                break
    return sigs
