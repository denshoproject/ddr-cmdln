import codecs
import csv
import io
import json
import os
import sys
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union


def read_text(path: str) -> str:
    """Read text file; make sure text is in UTF-8.
    
    @param path: str Absolute path to file.
    @returns: str
    """
    if not os.path.exists(path):
        raise IOError('File is missing or unreadable: %s' % path)
    with open(path, 'r') as f:
        return f.read()

def write_text(text: str, path: str):
    """Write text to UTF-8 file.
    
    @param text: unicode
    @param path: str Absolute path to file.
    """
    with open(path, 'w') as f:
        f.write(text)

def append_text(text: str, path: str):
    """Append text to UTF-8 file.
    
    @param text: unicode
    @param path: str Absolute path to file.
    """
    addnewline = False
    if os.path.exists(path):
        addnewline = True
    with open(path, 'a') as f:
        if addnewline:
            f.write('\n')
        f.write(text)


# Some files' XMP data is wayyyyyy too big
csv.field_size_limit(sys.maxsize)
CSV_DELIMITER = ','
CSV_QUOTECHAR = '"'
CSV_QUOTING = csv.QUOTE_ALL

def csv_reader(csvfile):
    """Get a csv.reader object for the file.
    
    @param csvfile: A file object.
    """
    reader = csv.reader(
        csvfile,
        delimiter=CSV_DELIMITER,
        quoting=CSV_QUOTING,
        quotechar=CSV_QUOTECHAR,
    )
    return reader

def csv_writer(csvfile):
    """Get a csv.writer object for the file.
    
    @param csvfile: A file object.
    """
    writer = csv.writer(
        csvfile,
        delimiter=CSV_DELIMITER,
        quoting=CSV_QUOTING,
        quotechar=CSV_QUOTECHAR,
    )
    return writer

def csv_str_writer():
    output = io.StringIO()
    writer = csv.writer(
        output,
        delimiter=CSV_DELIMITER,
        quoting=CSV_QUOTING,
        quotechar=CSV_QUOTECHAR,
    )
    return output,writer

def read_csv(path: str) -> List[Dict[str,str]]:
    """Read specified file, returns list of rows.
    
    >>> path = '/tmp/batch-test_write_csv.csv'
    >>> csv_file = '"id","title","description"\r\n"ddr-test-123","thing 1","nothing here"\r\n"ddr-test-124","thing 2","still nothing"\r\n'
    >>> with open(path, 'w') as f:
    ...    f.write(csv_file)
    >>> batch.read_csv(path)
    [
        ['id', 'title', 'description'],
        ['ddr-test-123', 'thing 1', 'nothing here'],
        ['ddr-test-124', 'thing 2', 'still nothing']
    ]
    
    Throws Exception if file contains text that can't be decoded to UTF-8.
    
    @param path: Absolute path to CSV file
    @returns list of rows
    """
    rows = []
    with open(path, 'r') as f:
        reader = csv_reader(f)
        for row in reader:
            rows.append(row)
    return rows

def write_csv(path: str,
              headers: List[str],
              rows: List[Dict[str,str]],
              append: bool=False):
    """Write header and list of rows to file.
    
    >>> path = '/tmp/batch-test_write_csv.csv'
    >>> headers = ['id', 'title', 'description']
    >>> rows = [
    ...     ['ddr-test-123', 'thing 1', 'nothing here'],
    ...     ['ddr-test-124', 'thing 2', 'still nothing'],
    ... ]
    >>> batch.write_csv(path, headers, rows)
    >>> with open(path, 'r') as f:
    ...    f.read()
    '"id","title","description"\r\n"ddr-test-123","thing 1","nothing here"\r\n"ddr-test-124","thing 2","still nothing"\r\n'
    
    @param path: Absolute path to CSV file
    @param headers: list of strings
    @param rows: list of lists
    @param append: boolean
    """
    if append:
        mode = 'a'
    else:
        mode = 'w'
    with open(path, mode, newline='') as f:
        writer = csv_writer(f)
        if headers:
            writer.writerow(headers)
        for row in rows:
            writer.writerow(row)

def write_csv_str(row: Dict[str,str]) -> str:
    """Write row to CSV formatted str
    
    TODO refactor. This makes a new CSV writer, writes a line, closes the output,
    and discards the writer for each row, so it's probably really inefficient
    """
    output,writer = csv_str_writer()
    writer.writerow(row)
    contents = output.getvalue()
    output.close()
    return contents.strip()
