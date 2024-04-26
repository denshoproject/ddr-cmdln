import json
from pathlib import Path
import re

from DDR.models.collection import Collection
from DDR import identifier
from DDR import util


class Organization():

    @staticmethod
    def get(oid, basepath):
        path = Path(basepath) / oid / 'organization.json'
        with path.open('r') as f:
            return json.loads(f.read())

    @staticmethod
    def organizations(collections_root):
        return [
            Organization.get(Path(path).name, collections_root)
            for path in Organization.organization_paths(collections_root)
        ]

    @staticmethod
    def organization_paths(basepath):
        """Returns organization paths.
        TODO use util.find_meta_files()
        """
        paths = []
        regex = [
            regex
            for regex,x,model in identifier.ID_PATTERNS
            if model == 'organization'
        ][0]
        for path in Path(basepath).iterdir():
            m = regex.search(path.name)
            if m:
                paths.append(str(path))
        return util.natural_sort(paths)

    @staticmethod
    def children(organization_path):
        """Returns collection paths.
        """
        path = Path(organization_path)
        collections_root = path.parent
        repo,org = path.name.split('-')
        paths = Collection.collection_paths(collections_root, repo, org)
        collections = [
            identifier.Identifier(path).object()
            for path in paths
        ]
        return collections
