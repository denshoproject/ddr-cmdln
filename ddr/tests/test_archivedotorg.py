# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
from pathlib import Path

from bs4 import BeautifulSoup
import pytest
import requests

from DDR import format_json
from DDR import archivedotorg
from DDR import config
from DDR import identifier


NO_IARCHIVE_ERR = 'ID service is not available.'
def no_iarchive():
    """Returns True if cannot contact IA API; use to skip tests
    """
    try:
        print(archivedotorg.IA_SAMPLE_URL)
        r = requests.get(archivedotorg.IA_SAMPLE_URL, timeout=3)
        print(r.status_code)
        if r.status_code == 200:
            return False
    except ConnectionError:
        print('ConnectionError')
        return True
    return True

def test_is_iaobject():
    class Thing():
        identifier = None; format = None
    
    thing0 = Thing()
    thing0.identifier = identifier.Identifier('ddr-densho-1000-1-1')
    thing0.format = 'vh'
    out0 = archivedotorg.is_iaobject(thing0)
    assert out0 == True
    
    thing1 = Thing()
    thing1.identifier = identifier.Identifier('ddr-densho-1000')
    thing1.format = 'vh'
    out1 = archivedotorg.is_iaobject(thing1)
    assert out1 == True
    
    thing2 = Thing()
    thing2.identifier = identifier.Identifier('ddr-densho-1000-1-1')
    thing2.format = 'img'
    out2 = archivedotorg.is_iaobject(thing2)
    assert out2 == True
    
    thing3 = Thing()
    thing3.identifier = identifier.Identifier('ddr-densho-1000')
    thing3.format = 'img'
    out3 = archivedotorg.is_iaobject(thing3)
    assert out3 == False

def load_ia_json(oid):
    """Load local copy of metadata
    
    $ ia metadata $OBJECTID > /opt/ddr-cmdln/ddr/tests/archivedotorg/$OBJECTID.json
    """
    testdir = Path(os.path.dirname(os.path.abspath(__file__)))
    path = testdir / 'archivedotorg' / f'{oid}.json'
    if path.exists():
        with path.open() as f:
            return json.loads(f.read())
    return {}

def test_format_mimetype():
    class DummyObject():
        pass
    
    # not in Internet Archive
    oid = 'ddr-densho-10-1'; o = DummyObject(); o.format = 'img'
    print(oid)
    meta = load_ia_json(oid)
    assert meta == {}
    out = archivedotorg.format_mimetype(o, meta)
    assert out == ''
    
    oid = 'ddr-densho-400-1'; o = DummyObject(); o.format = 'av'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])    
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'av:audio'
    
    oid = 'ddr-csujad-29-1'; o = DummyObject(); o.format = 'av'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'av:audio'
    
    #oid = 'ddr-csujad-30-19-1'; o = DummyObject(); o.format = 'vh'
    
    oid = 'ddr-densho-1000-1-1'; o = DummyObject(); o.format = 'vh'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])    
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'vh:video'
    
    oid = 'ddr-densho-1020-13'; o = DummyObject(); o.format = 'av'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])    
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'av:video'

    oid = 'ddr-densho-122-4-1'; o = DummyObject(); o.format = 'vh'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])    
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'vh:video'
    
    oid = 'ddr-csujad-51-1'; o = DummyObject(); o.format = 'av'
    print(oid)
    meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, meta['files'])
    assert data
    out = archivedotorg.format_mimetype(o, data)
    assert out == 'av:video'

    
def test_filter_ia_files():
    """
    Prep test metadata thusly:
        ia metadata ddr-densho-10-1-1 | jq '.' > archivedotorg_ddr-densho-10-1-1.json
    
    Index collections
        ddrindex publish -r /var/www/media/ddr/ddr-densho-400
        ddrindex publish -r /var/www/media/ddr/ddr-csujad-30
        ddrindex publish /var/www/media/ddr/ddr-densho-1000
        ddrindex publish -r /var/www/media/ddr/ddr-densho-1000/files/ddr-densho-1000-1
        ddrindex publish -r /var/www/media/ddr/ddr-densho-1020-13
        ddrindex publish /var/www/media/ddr/ddr-densho-122
        ddrindex publish -r /var/www/media/ddr/ddr-densho-122/files/ddr-densho-122-4
    """
    
    oid = 'ddr-densho-400-1'
    ia_meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, ia_meta['files'])
    assert data['mimetype'] == 'audio/mpeg'
    assert data['original'] == 'ddr-densho-400-1-mezzanine-70dda47d00.mp3'
    assert data['files'].get('mp3')
    assert data['files']['mp3'].get('url')
    assert data['files']['mp3']['url'] == 'https://archive.org/download/' \
        'ddr-densho-400-1/ddr-densho-400-1-mezzanine-70dda47d00.mp3'

    #oid = 'ddr-csujad-30-19-1'
    
    oid = 'ddr-densho-1000-1-1'
    ia_meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, ia_meta['files'])
    assert data['mimetype'] == 'video/mpeg'
    assert data['original'] == 'ddr-densho-1000-1-1-mezzanine-0762419626.mpg'
    assert data['files'].get('mpg')
    assert data['files']['mpg'].get('url')
    assert data['files']['mpg']['url'] == 'https://archive.org/download/' \
        'ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mpg'
    assert data['files'].get('mp4')
    assert data['files']['mp4'].get('url')
    assert data['files']['mp4']['url'] == 'https://archive.org/download/' \
        'ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mp4'

    oid = 'ddr-densho-1020-13'
    ia_meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, ia_meta['files'])
    assert data['mimetype'] == 'video/mp4'
    assert data['original'] == 'ddr-densho-1020-13-mezzanine-5c4e884556.ia.mp4'
    assert data['files'].get('mp4')
    assert data['files']['mp4'].get('url')
    assert data['files']['mp4']['url'] == 'https://archive.org/download/' \
        'ddr-densho-1020-13/ddr-densho-1020-13-mezzanine-5c4e884556.ia.mp4'

    oid = 'ddr-densho-122-4-1'
    ia_meta = load_ia_json(oid)
    data = archivedotorg.process_ia_metadata(oid, ia_meta['files'])
    assert data['mimetype'] == 'video/mp4'
    assert data['original'] == 'ddr-densho-122-4-1-mezzanine-51479225cf.mp4'
    assert data['files'].get('mp4')
    assert data['files']['mp4'].get('url')
    assert data['files']['mp4']['url'] == 'https://archive.org/download/' \
        'ddr-densho-122-4-1/ddr-densho-122-4-1-mezzanine-51479225cf.mp4'
    assert not data['files'].get('mpg')
