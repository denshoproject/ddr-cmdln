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
        p for p in util.find_meta_files(
            basedir=path, recursive=True, testing=testing
        )
        if os.path.dirname(p) != path
    ]

def _selected_field_values( parent_object, inheritables ):
    """Gets list of selected inherited fieldnames and their values from the parent object
    
    @param parent_object: DDRObject
    @param inheritables: list
    @returns: list of (fieldname,value) tuples
    """
    return [
        (
            '.'.join([parent_object.identifier.model, field]),
            getattr(parent_object, field)
        )
        for field in inheritables
    ]

def _child_field(parent_model_field, child_model):
    """
    @param parent_model_field: str '{parent_model}.{fieldname}'
    @param child_model: str
    @returns: str child field name
    """
    for child_field in identifier.INHERITABLE_FIELDS[parent_model_field]:
        model,field = child_field.split('.')
        if model == child_model:
            return field
    return None

def inheritable_fields( MODEL_FIELDS ):
    """Returns a list of fields that can inherit or grant values.
    
    Inheritable fields are marked 'inheritable':True in MODEL_FIELDS.
    
    @param MODEL_FIELDS
    @returns: list
    """
    return [
        field['name']
        for field in MODEL_FIELDS
        if '.'.join([field['model'], field['name']]) in identifier.INHERITABLE_FIELDS.keys()
    ]

def selected_inheritables( inheritables, cleaned_data ):
    """Indicates listed inheritable fields that are selected in the form.
    
    Selector fields are BooleanFields named "FIELD_inherit".
    
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
    
def update_inheritables( parent_object, selected_fields ):
    """Update specified inheritable fields of child objects
    
    @param parent_object: Collection or Entity
    @param selected_fields: str list of selected inheritable fields
    @returns: tuple (List changed object Ids, list changed objects files)
    """
    child_ids = []
    changed_files = []
    # values of parent_object's selected inheritable fields
    # keys are MODEL_FIELD to match identifier.INHERITABLE_FIELDS
    field_values = _selected_field_values(parent_object, selected_fields)
    # load child objects and apply the change
    if field_values:
        for json_path in _child_jsons(parent_object.path):
            child = identifier.Identifier(path=json_path).object()
            if child:
                # set field if exists in child and doesn't already match
                # parent value
                changed = False
                for model_field,value in field_values:
                    model,field = model_field.split('.')
                    # name of child field (may be diff from parent)
                    # see identifier.INHERITABLE_FIELDS
                    child_field = _child_field(
                        model_field, child.identifier.model
                    )
                    # update if different
                    if child_field and hasattr(child, child_field):
                        existing_value = getattr(child, child_field)
                        if existing_value != value:
                            setattr(child, child_field, value)
                            changed = True
                # write json and add to list of changed IDs/files
                if changed:
                    child.write_json()
                    if hasattr(child, 'id'):
                        child_ids.append(child.id)
                    elif hasattr(child, 'basename'):
                        child_ids.append(child.basename)
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
