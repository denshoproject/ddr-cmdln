import os

from DDR import config
from DDR import fileio

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'fileio')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)


TEXT = '{"a": 1, "b": 2}'

def test_read_text():
    # TODO test bad chars
    path = os.path.join(TESTING_BASE_DIR, 'read_text.json')
    with open(path, 'w') as f:
        f.write(TEXT)
    # regular
    data = fileio.read_text(path)
    assert data == TEXT
    # utf8_strict
    data = fileio.read_text(path, utf8_strict=True)
    assert data == TEXT
    # clean up
    os.remove(path)

def test_write_text():
    # TODO test bad chars
    path = os.path.join(TESTING_BASE_DIR, 'write_text.json')
    # regular
    fileio.write_text(TEXT, path)
    with open(path, 'r') as f:
        written = f.read()
    assert written == TEXT
    # utf8_strict
    fileio.write_text(TEXT, path, utf8_strict=True)
    with open(path, 'r') as f:
        written = f.read()
    assert written == TEXT
    # clean up
    os.remove(path)

APPEND_TEXT = [
    '000',
    '001',
    '002',
]

def test_append_text():
    path = os.path.join(TESTING_BASE_DIR, 'append_text.json')
    if os.path.exists(path):
        os.remove(path)
    # before start
    assert os.path.exists(path) == False
    # append some lines
    for n in range(0,len(APPEND_TEXT)):
        this = APPEND_TEXT[n]
        fileio.append_text(this, path)
        with open(path, 'r') as f:
            out = f.read()
        expected = '\n'.join(APPEND_TEXT[:n+1])
        assert out == expected
    # clean up
    os.remove(path)

CSV_PATH = os.path.join(TESTING_BASE_DIR, 'write_csv.csv')
CSV_HEADERS = ['id', 'title', 'description']
CSV_ROWS = [
    ['ddr-test-123', 'thing 1', 'nothing here'],
    ['ddr-test-124', 'thing 2', 'still nothing'],
]
CSV_FILE = '"id","title","description"\r\n"ddr-test-123","thing 1","nothing here"\r\n"ddr-test-124","thing 2","still nothing"\r\n'

# TODO test_csv_writer
# TODO test_csv_reader

def test_write_csv():
    # prep
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)
    # test
    fileio.write_csv(CSV_PATH, CSV_HEADERS, CSV_ROWS)
    assert os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'r') as f:
        out = f.read()
    assert out == CSV_FILE
    # cleanup
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)

def test_read_csv():
    # prep
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)
    with open(CSV_PATH, 'w') as f:
        f.write(CSV_FILE)
    # test
    expected = CSV_ROWS
    expected.insert(0, CSV_HEADERS)
    assert fileio.read_csv(CSV_PATH) == expected
    # cleanup
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)
