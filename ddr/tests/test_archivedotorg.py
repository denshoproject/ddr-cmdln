# -*- coding: utf-8 -*-

from datetime import datetime
import os

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


@pytest.mark.skipif(no_iarchive(), reason=NO_IARCHIVE_ERR)
def test_get_ia_meta():
    def get(oid):
        return archivedotorg.get_ia_meta(
            identifier.Identifier(oid, config.MEDIA_BASE).object()
        )
    
    ia_meta = get('ddr-densho-1000-1-1')
    print(f'IA_META {ia_meta}')
    assert ia_meta['files'].get('mp4')
    assert ia_meta['files']['mp4'].get('url')
    assert ia_meta['files']['mp4']['url'] == 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mp4'
    assert ia_meta['mimetype'] == 'video/mpeg'
    assert ia_meta['original'] == 'ddr-densho-1000-1-1-mezzanine-0762419626.mpg'
    print('')

    ia_meta = get('ddr-densho-400-1')
    print(f'IA_META {ia_meta}')
    assert ia_meta['files'].get('mp3')
    assert ia_meta['files']['mp3'].get('url')
    assert ia_meta['files']['mp3']['url'] == 'https://archive.org/download/ddr-densho-400-1/ddr-densho-400-1-mezzanine-70dda47d00.mp3'
    assert ia_meta['mimetype'] == 'audio/mpeg'
    assert ia_meta['original'] == 'ddr-densho-400-1-mezzanine-70dda47d00.mp3'

@pytest.mark.skipif(no_iarchive(), reason=NO_IARCHIVE_ERR)
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

@pytest.mark.skipif(no_iarchive(), reason=NO_IARCHIVE_ERR)
def test_format_mimetype():
    # not in Internet Archive
    o0 = identifier.Identifier('ddr-densho-10-1', config.MEDIA_BASE).object()
    meta0 = archivedotorg.get_ia_meta(o0)
    assert meta0 == {}
    out0 = archivedotorg.format_mimetype(o0, meta0)
    assert out0 == ''
    
    o1 = identifier.Identifier('ddr-densho-1000-1-1', config.MEDIA_BASE).object()
    meta1 = archivedotorg.get_ia_meta(o1)
    assert meta1
    out1 = archivedotorg.format_mimetype(o1, meta1)
    assert out1 == 'vh:video'
    
    o3 = identifier.Identifier('ddr-densho-400-1', config.MEDIA_BASE).object()
    meta3 = archivedotorg.get_ia_meta(o3)
    assert meta3
    out3 = archivedotorg.format_mimetype(o3, meta3)
    assert out3 == 'av:audio'
    
    o2 = identifier.Identifier('ddr-densho-1020-13', config.MEDIA_BASE).object()
    meta2 = archivedotorg.get_ia_meta(o2)
    assert meta2
    out2 = archivedotorg.format_mimetype(o2, meta2)
    assert out2 == 'av:video'
