import codecs
import csv
import os
import sys

import simplejson as json


def read_text(path, utf8_strict=False):
    """Read text file; make sure text is in UTF-8.
    
    @param path: str Absolute path to file.
    @param utf8_strict: boolean
    @returns: unicode
    """
    if not os.path.exists(path):
        raise IOError('File is missing or unreadable: %s' % path)
    if utf8_strict:
        try:
            with codecs.open(path, 'rU', 'utf-8') as f:
                return f.read()
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
    else:
        with open(path, 'r') as f:
            return f.read()

def write_text(text, path, utf8_strict=False):
    """Write text to UTF-8 file.
    
    @param text: unicode
    @param path: str Absolute path to file.
    @param utf8_strict: boolean
    """
    if utf8_strict:
        with codecs.open(path, 'w', 'utf-8') as f:
            return f.write(text)
    else:
        with open(path, 'w') as f:
            f.write(text)

def append_text(text, path, utf8_strict=False):
    """Append text to UTF-8 file.
    
    @param text: unicode
    @param path: str Absolute path to file.
    @param utf8_strict: boolean
    """
    addnewline = False
    if os.path.exists(path):
        addnewline = True
    if utf8_strict:
        with codecs.open(path, 'a', 'utf-8') as f:
            if addnewline:
                f.write('\n')
            f.write(text)
    else:
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

def read_csv(path, utf8_strict=False):
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
    @param utf8_strict: boolean
    @returns list of rows
    """
    rows = []
    if utf8_strict:
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
    else:
        with open(path, 'r') as f:
            reader = csv_reader(f)
            for row in reader:
                rows.append(row)
    return rows

def write_csv(path, headers, rows, append=False, utf8_strict=False):
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
    @param utf8_strict: boolean
    """
    if append:
        mode = 'a'
    else:
        mode = 'w'
    if utf8_strict:
        with codecs.open(path, mode, 'utf-8') as f:
            writer = csv_writer(f)
            if headers:
                writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
    else:
        with open(path, mode) as f:
            writer = csv_writer(f)
            if headers:
                writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
