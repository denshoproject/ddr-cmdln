import codecs
import os
import sys

import simplejson as json
import unicodecsv as csv


def read_text(path):
    """Read text file; make sure text is in UTF-8.
    
    @param path: str Absolute path to file.
    @returns: unicode
    """
    if not os.path.exists(path):
        raise IOError('File is missing or unreadable: %s' % path)
    # TODO use codecs.open utf-8
    with open(path, 'r') as f:
        text = f.read()
    return text

def write_text(text, path):
    """Write text to UTF-8 file.
    
    @param text: unicode
    @param path: str Absolute path to file.
    """
    # TODO use codecs.open utf-8
    with open(path, 'w') as f:
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

def read_csv(path):
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
    try:
        with codecs.open(path, 'rU', 'utf-8') as f:  # the 'U' is for universal-newline mode
            reader = csv_reader(f)
            for row in reader:
                rows.append(row)
    except UnicodeDecodeError:
        bad = []
        with open(path, 'r') as f:
            for n,line in enumerate(f.readlines()):
                try:
                    utf8 = line.decode('utf8', 'strict')
                except UnicodeDecodeError:
                    bad.append(str(n))
        raise Exception(
            'Unicode decoding errors in line(s) %s.' % ','.join(bad)
        )
    return rows

def write_csv(path, headers, rows):
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
    """
    with codecs.open(path, 'wb', 'utf-8') as f:
        writer = csv_writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
