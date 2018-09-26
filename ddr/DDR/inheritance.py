import os

from DDR import identifier
from DDR import util


def _child_jsons( path, testing=False ):
    """List all the .json files under path directory; excludes specified dir.
    
    @param path: str Absolute directory path.
    @param testing: boolean
    @returns: list of paths
    """
    return [
        p for p in util.find_meta_files(basedir=path, recursive=True, testing=testing)
        if os.path.dirname(p) != path
    ]

def _selected_field_values( parent_object, inheritables ):
    """Gets list of selected inherited fieldnames and their values from the parent object
    
    @param parent_object: DDRObject
    @param inheritables: list
    @returns: list of (fieldname,value) tuples
    """
    return [
        (field, getattr(parent_object, field))
        for field in inheritables
    ]

def inheritable_fields( MODEL_FIELDS ):
    """Returns a list of fields that can inherit or grant values.
    
    Inheritable fields are marked 'inheritable':True in MODEL_FIELDS.
    
    @param MODEL_FIELDS
    @returns: list
    """
    return [
        f['name']
        for f in MODEL_FIELDS
        if f.get('inheritable', None)
    ]

def selected_inheritables( inheritables, cleaned_data ):
    """Indicates which inheritable fields from the list were selected in the form.
    
    Selector fields are assumed to be BooleanFields named "FIELD_inherit".
    
    @param inheritables: list of field/attribute names.
    @param cleaned_data: form.cleaned_data.
    @returns: list
    """
    fieldnames = {
        '%s_inherit' % field: field
        for field in inheritables
    }
    selected = []
    if fieldnames:
        selected = [
            fieldnames[key]
            for key in cleaned_data.keys()
            if (key in fieldnames.keys()) and cleaned_data[key]
        ]
    return selected
    
def update_inheritables( parent_object, inheritables, cleaned_data ):
    """Update specified inheritable fields of child objects using form data.
    
    @param parent_object: Collection or Entity with values to be inherited.
    @param inheritables: list
    @param cleaned_data: Form cleaned_data from POST.
    @returns: tuple List of changed object Ids, list of changed objects' JSON files.
    """
    child_ids = []
    changed_files = []
    # values of selected inheritable fields from parent
    field_values = _selected_field_values(parent_object, inheritables)
    # load child objects and apply the change
    if field_values:
        for json_path in _child_jsons(parent_object.path):
            child = None
            oid = identifier.Identifier(path=json_path)
            child = oid.object()
            if child:
                # set field if exists in child and doesn't already match parent value
                changed = False
                for field,value in field_values:
                    if hasattr(child, field):
                        existing_value = getattr(child,field)
                        if existing_value != value:
                            setattr(child, field, value)
                            changed = True
                # write json and add to list of changed IDs/files
                if changed:
                    child.write_json()
                    if hasattr(child, 'id'):         child_ids.append(child.id)
                    elif hasattr(child, 'basename'): child_ids.append(child.basename)
                    changed_files.append(json_path)
    return child_ids,changed_files

def inherit( parent, child ):
    """Set inheritable fields in child object with values from parent.
    
    @param parent: A webui.models.Collection or webui.models.Entity
    @param child: A webui.models.Entity or webui.models.File
    """
    for field in parent.inheritable_fields():
        if hasattr(parent, field) and hasattr(child, field):
            setattr(child, field, getattr(parent, field))
