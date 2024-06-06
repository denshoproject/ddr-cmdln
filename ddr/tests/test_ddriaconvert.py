import csv
import os
from pathlib import Path
import shutil

import pytest

from DDR.cli import ddriaconvert
from DDR import fileio

# ddr-cmdln standalone
TEST_FILES_DIR = Path(os.getcwd()) / 'ddr/tests/iaconvert'
TEST_FILES_TMP = 'iaconvert'


@pytest.fixture(scope="session")
def test_files_dir(tmpdir_factory):
    return tmpdir_factory.mktemp(TEST_FILES_TMP)

def test_doConvert_vh_no_binaries(test_files_dir):
    # prep test data
    entity_csv = test_files_dir / 'vh-entities.csv'
    file_csv = test_files_dir / 'vh-files.csv'
    output_dir = test_files_dir
    binaries_path = ''
    #print(f"TEST {test_files_dir=}")
    #print(f"TEST {entity_csv=}")
    #print(f"TEST {file_csv=}")
    
    # write entity,file CSV using same CSV code used by ddrexport
    fileio.write_csv(
        entity_csv,
        headers=VH_ENTITIES[0].keys(),
        rows=[row.values() for row in VH_ENTITIES]
    )
    # write files CSV
    fileio.write_csv(
        file_csv,
        headers=VH_FILES[0].keys(),
        rows=[row.values() for row in VH_FILES]
    )
    
    # run the command
    output_file = ddriaconvert.doConvert(
        entity_csv, file_csv, output_dir, binaries_path
    )
    
    # examine the results
    print(f"TEST {output_file=}")
    assert Path(output_file).exists()
    # compare output to expected
    output = load_csv_data(output_file)
    #print(f"{output=}")
    for n,item in enumerate(output):
        expected_item = VH_OUTPUT[n]
        for key,val in item.items():
            print(key,val)
            print(key,expected_item[key])
            assert val == expected_item[key]

def load_csv_data(csvpath):
    with open(csvpath, 'r') as f:
        return [row for row in csv.DictReader(f)]


# Sample Visual History data provided by Sata on 2024-06-04

VH_ENTITIES = [
    {'id': 'ddr-densho-1000-540', 'record_created': '2023-07-28T21:21:21PDT-0700', 'record_lastmod': '2024-05-01T12:22:12PDT-0700', 'status': 'completed', 'public': '1', 'sort': '1', 'title': 'Nick Nagatani Interview II', 'description': 'DESCRIPTION.', 'creation': 'June 27, 2023', 'location': 'Culver City, California', 'creators': 'namepart: Nick Nagatani | oh_id: 1050 | role: narrator; namepart: Brian Niiya | role: interviewer; namepart: Yuka Murakami | role: videographer', 'language': 'eng', 'genre': 'interview', 'format': 'vh', 'extent': '2:27:10', 'contributor': 'Densho', 'alternate_id': '', 'digitize_person': 'Dana Hoshide', 'digitize_organization': 'Densho', 'digitize_date': '4/9/2024', 'credit': 'Courtesy of Densho', 'rights': 'cc', 'rights_statement': '', 'topics': '', 'persons': '', 'facility': '', 'chronology': '', 'geography': '', 'parent': '', 'signature_id': 'ddr-densho-1000-540-1-mezzanine-3418adf1ce', 'notes': ''},
    {'id': 'ddr-densho-1000-540-1', 'record_created': '2024-04-29T15:54:59PDT-0700', 'record_lastmod': '2024-05-01T12:22:13PDT-0700', 'status': 'completed', 'public': '1', 'sort': '1', 'title': 'Nick Nagatani Interview II Segment 1', 'description': 'Description 1.', 'creation': 'June 27, 2023', 'location': 'Culver City, California', 'creators': 'namepart: Nick Nagatani | oh_id: 1050 | role: narrator; namepart: Brian Niiya | role: interviewer; namepart: Yuka Murakami | role: videographer', 'language': 'eng', 'genre': 'interview', 'format': 'vh', 'extent': '0:09:50', 'contributor': 'Densho', 'alternate_id': '', 'digitize_person': 'Dana Hoshide', 'digitize_organization': 'Densho', 'digitize_date': '4/9/2024', 'credit': 'Courtesy of Densho', 'rights': 'cc', 'rights_statement': '', 'topics': '', 'persons': '', 'facility': '', 'chronology': 'startdate:1960-01-01 00:00:00.0|term:1960-1969', 'geography': '', 'parent': 'ddr-densho-1000-540', 'signature_id': 'ddr-densho-1000-540-1-mezzanine-3418adf1ce', 'notes': '[int_notes:];[technotes:];[capture_format:];[physmedia_time_in:0:00:06];[physmedia_time_out:0:09:56];[physmedia_id:]'},
    {'id': 'ddr-densho-1000-540-2', 'record_created': '2024-04-29T15:54:59PDT-0700', 'record_lastmod': '2024-05-01T12:22:14PDT-0700', 'status': 'completed', 'public': '1', 'sort': '2', 'title': 'Nick Nagatani Interview II Segment 2', 'description': 'Changes in the L.A. community after the Vietnam War', 'creation': 'June 27, 2023', 'location': 'Culver City, California', 'creators': 'namepart: Nick Nagatani | oh_id: 1050 | role: narrator; namepart: Brian Niiya | role: interviewer; namepart: Yuka Murakami | role: videographer', 'language': 'eng', 'genre': 'interview', 'format': 'vh', 'extent': '0:07:29', 'contributor': 'Densho', 'alternate_id': '', 'digitize_person': 'Dana Hoshide', 'digitize_organization': 'Densho', 'digitize_date': '4/9/2024', 'credit': 'Courtesy of Densho', 'rights': 'cc', 'rights_statement': '', 'topics': '', 'persons': '', 'facility': '', 'chronology': 'startdate:1960-01-01 00:00:00.0|term:1960-1969', 'geography': 'id:"http://vocab.getty.edu/page/tgn/7023900"|term:Los Angeles, California', 'parent': 'ddr-densho-1000-540', 'signature_id': 'ddr-densho-1000-540-2-mezzanine-b9e173a6c8', 'notes': '[int_notes:];[technotes:];[capture_format:];[physmedia_time_in:0:09:58];[physmedia_time_out:0:17:27];[physmedia_id:]'},
]

VH_FILES = [
    {'id': 'ddr-densho-1000-540-1-mezzanine-3418adf1ce', 'external': '1', 'role': 'mezzanine', 'sha1': '3418adf1cefac99a799c72c81a8e6f49ea7ad644', 'sha256': 'a1fe9503ba2345f249b005a4f7c4b86c24c60ecec7a5e7007f860b320926a415', 'md5': '80b39ebaf1d56b25df1ccda8ac2b7d97', 'size': '2654951428', 'basename_orig': 'ddr-densho-1000-540-1.mpg', 'mimetype': 'video/mpeg', 'public': '1', 'rights': 'cc', 'sort': '1', 'label': 'Segment 1', 'digitize_person': 'Hoshide, Dana', 'tech_notes': '', 'external_urls': 'label:Internet Archive download|url:https://archive.org/download/ddr-densho-1000-540-1/ddr-densho-1000-540-1-mezzanine-3418adf1ce.mpg;\\nurl:https://archive.org/download/ddr-densho-1000-540-1/ddr-densho-1000-540-1.mp4', 'links': ''},
    {'id': 'ddr-densho-1000-540-2-mezzanine-b9e173a6c8', 'external': '1', 'role': 'mezzanine', 'sha1': 'b9e173a6c80177edd9394f77f36ef185215b30ed', 'sha256': 'e44bc47fcbbf2e0e56c67b249db0ec17321802cba894e76710ac88a7fa2fabbd', 'md5': 'fc359983bcad5d078101f9ba328fa81e', 'size': '2020349956', 'basename_orig': 'ddr-densho-1000-540-2.mpg', 'mimetype': 'video/mpeg', 'public': '1', 'rights': 'cc', 'sort': '2', 'label': 'Segment 2', 'digitize_person': 'Hoshide, Dana', 'tech_notes': '', 'external_urls': 'label:Internet Archive download|url:https://archive.org/download/ddr-densho-1000-540-2/ddr-densho-1000-540-2-mezzanine-b9e173a6c8.mpg;\\nurl:https://archive.org/download/ddr-densho-1000-540-2/ddr-densho-1000-540-2.mp4', 'links': ''},
]

VH_OUTPUT = [
    {'identifier': 'ddr-densho-1000-540-1', 'file': 'ddr-densho-1000-540-1-mezzanine-3418adf1ce.mpg', 'collection': 'ddr-densho-1000-540', 'mediatype': 'movies', 'description': 'Interview location: Culver City, California<p>Description 1.<p>Segment 1 of 2<p>[ <a href="https://archive.org/details/ddr-densho-1000-540-2">Next segment</a> ]<p>See this item in the <a href="https://ddr.densho.org/" target="blank" rel="nofollow">Densho Digital Repository</a> at: <a href="https://ddr.densho.org/ddr-densho-1000-540-1/" target="_blank" rel="nofollow">https://ddr.densho.org/ddr-densho-1000-540-1/</a>.', 'title': 'Nick Nagatani Interview II Segment 1', 'contributor': 'Densho', 'creator': 'Narrator: Nick Nagatani, Interviewer: Brian Niiya, Videographer: Yuka Murakami', 'date': 'June 27, 2023', 'subject[0]': 'Japanese Americans', 'subject[1]': 'Oral history', 'subject[2]': '', 'licenseurl': 'https://creativecommons.org/licenses/by-nc-sa/4.0/', 'credits': 'Narrator: Nick Nagatani, Interviewer: Brian Niiya, Videographer: Yuka Murakami', 'runtime': '0:09:50'},
    {'identifier': 'ddr-densho-1000-540-2', 'file': 'ddr-densho-1000-540-2-mezzanine-b9e173a6c8.mpg', 'collection': 'ddr-densho-1000-540', 'mediatype': 'movies', 'description': 'Interview location: Culver City, California<p>Changes in the L.A. community after the Vietnam War<p>Segment 2 of 2<p>[ <a href="https://archive.org/details/ddr-densho-1000-540-1">Previous segment</a> ]<p>See this item in the <a href="https://ddr.densho.org/" target="blank" rel="nofollow">Densho Digital Repository</a> at: <a href="https://ddr.densho.org/ddr-densho-1000-540-2/" target="_blank" rel="nofollow">https://ddr.densho.org/ddr-densho-1000-540-2/</a>.', 'title': 'Nick Nagatani Interview II Segment 2', 'contributor': 'Densho', 'creator': 'Narrator: Nick Nagatani, Interviewer: Brian Niiya, Videographer: Yuka Murakami', 'date': 'June 27, 2023', 'subject[0]': 'Japanese Americans', 'subject[1]': 'Oral history', 'subject[2]': '', 'licenseurl': 'https://creativecommons.org/licenses/by-nc-sa/4.0/', 'credits': 'Narrator: Nick Nagatani, Interviewer: Brian Niiya, Videographer: Yuka Murakami', 'runtime': '0:07:29'},
]
