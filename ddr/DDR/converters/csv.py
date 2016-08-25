from . import formats


# module function ------------------------------------------------------

def choice_is_valid(valid_values, field, value):
    """Indicates whether value is one of valid values for field.
    """
    if value in valid_values[field]:
	return True
    return False


# csvload_* --- import-from-csv functions ------------------------------
#
# These functions take data from a CSV field and convert it to Python
# data for the corresponding Entity field.
#
    
def load_string(text):
    return formats.normalize_string(text)

def load_datetime(text, datetime_format):
    return formats.text_to_datetime(text, datetime_format)

def load_list(text):
    return formats.text_to_list(text)

def load_kvlist(text):
    return formats.text_to_kvlist(text)
            
def load_labelledlist(text):
    return formats.text_to_labelledlist(text)

def load_listofdicts(text):
    return formats.text_to_listofdicts(text)

def load_rolepeople(text):
    return formats.text_to_rolepeople(text)


# csvdump_* --- export-to-csv functions ------------------------------
#
# These functions take Python data from the corresponding Entity field
# and format it for export in a CSV field.
#

def dump_string(data):
    """Dump stringdata to text suitable for a CSV field.
    
    @param data: str
    @returns: unicode string
    """
    return formats.normalize_string(data)

def dump_datetime(data, datetime_format):
    return formats.datetime_to_text(data, datetime_format)

def dump_list(data):
    return formats.list_to_text(data)

def dump_kvlist(data):
    return formats.kvlist_to_text(data)

def dump_labelledlist(data):
    return formats.labelledlist_to_text(data)

def dump_listofdicts(data):
    return formats.listofdicts_to_text(data)

def dump_rolepeople(data):
    return formats.rolepeople_to_text(data)


# csvvalidate_* --------------------------------------------------------
#
# These functions examine data in a CSV field and return True if valid.
#
