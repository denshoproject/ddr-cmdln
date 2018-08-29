import os

import fileio


TEXT = '{"a": 1, "b": 2}'

def test_read_text():
    path = '/tmp/test_DDR.fileio.read_text.json'
    with open(path, 'w') as f:
        f.write(TEXT)
    data = fileio.read_text(path)
    assert data == TEXT
    # clean up
    os.remove(path)

def test_write_text():
    path = '/tmp/test_DDR.fileio.write_text.json'
    fileio.write_text(TEXT, path)
    with open(path, 'r') as f:
        written = f.read()
    assert written == TEXT
    # clean up
    os.remove(path)


CSV_PATH = '/tmp/test_DDR.fileio.write_csv.csv'
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
