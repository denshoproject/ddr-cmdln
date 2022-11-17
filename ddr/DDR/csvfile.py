from collections import OrderedDict
import logging
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union

from DDR import identifier


def make_row_dict(headers: List[str], row: List[str]) -> OrderedDict:
    """Turns CSV row into a dict with the headers as keys
    
    >>> headers0 = ['id', 'created', 'lastmod', 'title', 'description']
    >>> row0 = ['id', 'then', 'now', 'title', 'descr']
    {'title': 'title', 'description': 'descr', 'lastmod': 'now', 'id': 'id', 'created': 'then'}

    @param headers: List of header field names
    @param row: A single row (list of fields, not dict)
    @returns: OrderedDict
    """
    d = OrderedDict()
    for n in range(0, len(row)):
        d[headers[n]] = _strip_str(row[n])
    return d
    
def make_rowds(
        rows: List[List[str]],
        row_start: int=0,
        row_end: int=9999999
):  # -> Tuple[List[str], List[OrderedDict[str,str]], List[str]]:
    """Takes list of rows (from csv lib) and turns into list of rowds (dicts)
    
    @param rows: list
    @returns: (headers, list of OrderedDicts, list of errors)
    """
    headers = [_strip_str(data) for data in rows.pop(0)]
    rowds = []
    errors = []
    for n,row in enumerate(rows[row_start:row_end]):
        try:
            rowd = make_row_dict(headers, row)
            rowds.append(rowd)
        except Exception as err:
            msg = 'row %s: %s' % (n, str(err))
            errors.append(msg)
    return headers,rowds,errors
    
def make_rows(rowds: List[Dict[str,str]]) -> Tuple[List[str], List[List[str]]]:
    """Takes list of rowds (dicts) and turns into list of rows (for writing CSV)
    
    @param rowds: list of dicts
    @returns: headers,rows
    """
    headers = list(rowds[0].keys())
    rows = [list(rowd.values()) for rowd in rowds]
    return headers,rows

def _strip_str(data: str) -> str:
    if isinstance(data, str):
        data = data.strip()
    return data

def validate_headers(headers: List[str],
                     field_names: List[str],
                     exceptions: List[str],
                     additional: List[str]) -> Dict[str, List[str]]:
    """Validates headers and crashes if problems.
    
    >>> model = 'entity'
    >>> field_names = ['id', 'title', 'notused']
    >>> exceptions = ['notused']
    >>> headers = ['id', 'title']
    >>> validate_headers(model, headers, field_names, exceptions)
    >>> headers = ['id', 'titl']
    >>> validate_headers(model, headers, field_names, exceptions)
    Traceback (most recent call last):
      File "<input>", line 1, in <module>
      File "/usr/local/lib/python2.7/dist-packages/DDR/batch.py", line 319, in validate_headers
        raise Exception('MISSING HEADER(S): %s' % missing_headers)
    Exception: MISSING HEADER(S): ['title']
    
    @param headers: List of field names
    @param field_names: List of field names
    @param exceptions: List of nonrequired field names
    @param additional: List of nonrequired fields which may appear
    """
    
    def ignore_header(header: str) -> bool:
        if header.isupper():
            return True
        return False
    
    def header_is_bad(header: str) -> bool:
        if (header not in field_names) \
        and (header not in additional) \
        and (not ignore_header(header)):
            return True
        return False
    
    def header_or_empty(header: str) -> str:
        # mark blank headers as [blank]
        if header:
            return header
        return '[empty]'
    
    missing_headers = [
        field
        for field in field_names
        if (field not in exceptions)
        and (field not in headers)
    ]
    ignored_headers = [
        header
        for header in headers
        if ignore_header(header)
    ]
    bad_headers = [
        header_or_empty(header)
        for header in headers
        if header_is_bad(header)
    ]
    errs = {}
    if missing_headers:
        errs['Missing headers'] = missing_headers
    if bad_headers:
        errs['Bad headers'] = bad_headers
    return errs
    
def account_row(required_fields: List[str],
                rowd: Dict[str,str]) -> List[str]:
    """Returns list of any required fields that are missing from rowd.
    
    >>> required_fields = ['id', 'title']
    >>> rowd = {'id': 123, 'title': 'title'}
    >>> account_row(required_fields, rowd)
    []
    >>> required_fields = ['id', 'title', 'description']
    >>> account_row(required_fields, rowd)
    ['description']
    
    @param required_fields: List of required field names
    @param rowd: A single row (dict, not list of fields)
    @returns: list of field names
    """
    return [
        f for f in required_fields
        if (f not in list(rowd.keys())) or (not rowd.get(f,None))
    ]

def validate_id(text: str) -> Union[identifier.Identifier, bool]:
    try:
        i = identifier.Identifier(id=text)
        return i
    except:
        pass
    return False

def check_row_values(module, headers, valid_values, rowd):
    """Examines row values and returns names of invalid fields.
    
    TODO refers to lots of globals!!!
    
    @param module: modules.Module object
    @param headers: List of field names
    @param valid_values:
    @param rowd: A single row (dict, not list of fields)
    @returns: list of invalid values
    """
    invalid = []
    if not validate_id(rowd['id']):
        invalid.append('id')
    for field in headers:
        try:
            value = module.function(
                'csvload_%s' % field,
                rowd[field]
            )
            valid = module.function(
                'csvvalidate_%s' % field,
                [valid_values, value]
            )
            if not valid:
                invalid.append(field)
        except KeyError as err:
            msg = '%s: %s' % (field, str(err))
            invalid.append(msg)
        except ValueError as err:
            msg = '%s: %s' % (field, str(err))
            invalid.append(msg)
    return invalid

def find_duplicate_ids(rowds):
    """Look for duplicate object IDs.
    
    @param rowds: list of dicts
    @returns: list of errors (n, duplicate ID)
    """
    errs = []
    ids = []
    for n,rowd in enumerate(rowds):
        if rowd['id'] in ids:
            msg = 'row %s: %s' % (n, rowd['id'])
            errs.append(msg)
        else:
            ids.append(rowd['id'])
    return errs

def find_multiple_cids(rowds):
    """Look for pointers to multiple collections
    
    @param rowds: list of dicts
    @returns: list of errors (n, cid)
    """
    cids = []
    for n,rowd in enumerate(rowds):
        oid = identifier.Identifier(rowd['id'])
        cid = oid.collection().id
        if cid not in cids:
            cids.append(cid)
    if len(cids) > 1:
        return cids
    return []

def find_missing_required(required_fields, rowds):
    """Find rows that are missing values for required fields.
    
    @param required_fields: list
    @param rowds: list of dicts
    @returns: list of errors (n, object ID, bad_fields)
    """
    errs = []
    for n,rowd in enumerate(rowds):
        bad_fields = account_row(required_fields, rowd)
        if bad_fields:
            msg = 'row %s: %s %s' % (n, rowd['id'], bad_fields)
            errs.append(msg)
    return errs

def find_invalid_values(module, headers, valid_values, rowds):
    """Find controlled-vocab fields that contain bad data.
    
    @param module: modules.Module object
    @param headers: List of field names
    @param valid_values:
    @param rowds: list of dicts
    @returns: list of strings (row n, object ID, bad_fields)
    """
    errs = []
    for n,rowd in enumerate(rowds):
        bad_fields = check_row_values(module, headers, valid_values, rowd)
        if bad_fields:
            msg = 'row %s: %s %s' % (n, rowd['id'], bad_fields)
            errs.append(msg)
    return errs
    
def validate_rowds(module, headers, required_fields, valid_values, rowds, find_dupes=True):
    """Examines rows and raises exceptions if problems.
    
    Looks for
    - missing required fields
    - invalid field values
    - duplicate IDs (unless disabled)
    Note: new files have their PARENT ENTITY ID but are not duplicates
    
    @param module: modules.Module object
    @param headers: List of field names
    @param required_fields: List of required field names
    @param valid_values:
    @param rowds: List of row dicts
    @param find_dupes: boolean (should be False for new files)
    """
    errs = {}
    multiple_cids = find_multiple_cids(rowds)
    missing_required = find_missing_required(required_fields, rowds)
    invalid_values = find_invalid_values(module, headers, valid_values, rowds)
    if find_dupes:
        duplicate_ids = find_duplicate_ids(rowds)
        if duplicate_ids:
            errs['Duplicate IDs'] = duplicate_ids
    if multiple_cids:
        errs['Multiple collection IDs'] = multiple_cids
    if missing_required:
        errs['Missing required fields'] = missing_required
    if invalid_values:
        errs['Invalid values'] = invalid_values
    return errs
