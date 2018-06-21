"""
NOTE: Much of the code in this module used to be in ddr-local
(ddr-local/ddrlocal/ddrlocal/models/__init__.py).  Please refer to that project
for history prior to Feb 2015.

* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

TODO refactor: keep metadata from json_data

TODO refactor: load json_text into an OrderedDict

TODO refactor: put json_data in a object.source dict like ES does.

This way we don't have to worry about field names conflicting with
class methods (e.g. Entity.parent).

ACCESS that dict (actually OrderedDict) via object.source() method.
Lazy loading: don't load unless something needs to access the data

IIRC the only time we need those fields is when we display the object
to the user.

Also we won't have to reload the flippin' .json file multiple times
for things like cmp_model_definition_fields.

TODO indicate in repo_models.MODEL whether to put in .source.
Not as simple as just throwing everything into .source.
Some fields (record_created) are auto-generated.
Others (id) must be first-level attributes of Objects.
Others (status, public) are used in code to do things like inheritance,
yet must be editable in the editor UI.

* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
"""

from DDR.models.collection import Collection, COLLECTION_FILES_PREFIX
from DDR.models.entity import Entity, ListEntity, ENTITY_FILES_PREFIX
from DDR.models.files import File
