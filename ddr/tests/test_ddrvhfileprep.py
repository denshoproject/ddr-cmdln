import csv
from http import HTTPStatus
import os
from pathlib import Path
import shutil
import urllib

import pytest
import requests

from DDR.cli import ddrvhfileprep
from DDR import fileio

IMG_URLS = [
    'https://archive.org/download/ddr-densho-1000-101-1/ddr-densho-1000-101-1-mezzanine-2f2a3af845.mp4',
]

VH_OUTPUT = [
    {'id': 'ddr-densho-1000-101-1', 'external': 'True', 'role': 'mezzanine', 'basename_orig': 'ddr-densho-1000-101-1-mezzanine-2f2a3af845.mp4', 'mimetype': 'video/mp4', 'public': '1', 'rights': 'cc', 'sort': '1', 'thumb': '', 'label': 'Segment 1', 'digitize_person': 'Hoshide, Dana', 'tech_notes': '', 'external_urls': 'label:Internet Archive download|url:https://archive.org/download/ddr-densho-1000-101-1/ddr-densho-1000-101-1-mezzanine-ef63aeeb4f.mp4;label:Internet Archive stream|url:https://archive.org/download/ddr-densho-1000-101-1/ddr-densho-1000-101-1.mp4', 'links': '', 'sha1': 'ef63aeeb4f6d69018c3c27ffaba034710e107a08', 'sha256': 'e29d578a8ec039420683256e8c2964ab699620ea19201c2618367f5263de685d', 'md5': '11b5bd2945efc845fc9c64199293fefb', 'size': '28380945'}
]


@pytest.fixture(scope="session")
def test_images(tmpdir_factory):
    outputdir = tmpdir_factory.mktemp('vh_output')
    img_paths = []
    for url in IMG_URLS:
        img_filename = Path(url).name
        img_path = tmpdir_factory.mktemp('vh_binaries') / img_filename
        # download to /tmp/
        img_path_tmp = Path('/tmp/') / img_filename
        if not img_path_tmp.exists():
            r = requests.get(url)
            if not r.status_code == HTTPStatus.OK:
                raise Exception(
                    f"ERROR: test_ddrvhfileprep got HTTP {r.status_code} " \
                    f"when downloading {url}.")
            with img_path_tmp.open(mode="wb") as f:
                f.write(r.content)
        if not img_path_tmp.exists():
            raise Exception(
                f"ERROR: test_ddrvhfileprep could not download {url}.")
        # copy to test dir
        shutil.copy(img_path_tmp, img_path)
        img_paths.append(str(img_path))
    return img_paths, str(outputdir)

def test_vhfileprep(tmpdir_factory, test_images):
    img_paths,outputdir = test_images
    print(f"{test_images=}")
    for img_path in img_paths:
        inputdir = str(Path(img_path).parent)
    #print(f"{inputdir=}")
    #print(f"{outputdir=}")
    #print(f"{type(inputdir)=}")
    #print(f"{type(outputdir)=}")

    csvout = ddrvhfileprep.ddrvhfileprep(
        args=[inputdir, outputdir],
        standalone_mode=False
    )

    # examine CSV output
    csvout = Path(csvout)
    print(f"{csvout=}")
    assert csvout.exists()
    output = load_csv_data(csvout)
    #print(f"{output=}")
    for n,row in enumerate(output):
        expected = VH_OUTPUT[n]
        # assert CSV values
        for key,val in row.items():
            print(key,val)
            print(key,expected[key])
            assert val == expected[key]
        # assert file exists
        file_id = expected['id']
        file_role = expected['role']
        file_sha1 = expected['sha1'][:10]
        file_ext = Path(expected['basename_orig']).suffix
        filename = f"{file_id}-{file_role}-{file_sha1}{file_ext}"
        expected_file_path = csvout.parent / filename
        print(f"{expected_file_path=}")
        assert expected_file_path.exists()


def load_csv_data(csvpath):
    with csvpath.open('r') as f:
        return [row for row in csv.DictReader(f)]
