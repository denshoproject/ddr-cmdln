# -*- coding: utf-8 -*-

from datetime import datetime
import os

from bs4 import BeautifulSoup
import pytest

from . import archivedotorg
from . import identifier


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

def test_iaobject_get():
    o0 = archivedotorg.IAObject().get(
        'ddr-densho-1000-1-1', 200, SEGMENT_XML
    )
    out0 = str(o0)
    out1 = o0.xml_url
    assert out0 == '<DDR.archivedotorg.IAObject ddr-densho-1000-1-1>'
    assert out1 == 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1_files.xml'
    
    o1 = archivedotorg.IAObject().get(
        'ddr-densho-1000-1-1', 200, SEGMENT_XML_NO_FILES
    )
    assert o1 == None

def test_file_url():
    FORMATS = archivedotorg.FORMATS
    o = archivedotorg.IAObject()
    o.id = 'ddr-densho-1000-1-1'
    o.soup = BeautifulSoup(SEGMENT_XML, 'html.parser')
    for tag in o.soup('file', source='original'):
        if os.path.splitext(tag['name'])[1].replace('.','') in FORMATS:
            pass
    out = archivedotorg._file_url(o.id, tag)
    expected = 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1_meta.xml'
    assert out == expected

#def test_iaobject_get_xml():

def test_iaobject_get_original():
    o = archivedotorg.IAObject()
    o.id = 'ddr-densho-1000-1-1'
    o.soup = BeautifulSoup(SEGMENT_XML, 'html.parser')
    out0 = o._get_original()
    expected0 = 'ddr-densho-1000-1-1-mezzanine-0762419626.mpg'
    assert out0 == expected0
    
    o.id = 'ddr-densho-1000-1-1'
    o.soup = BeautifulSoup(SEGMENT_XML_NO_FILES, 'html.parser')
    out1 = o._get_original()
    expected1 = None
    assert out1 == expected1
    
    with pytest.raises(Exception):
        o.id = 'ddr-densho-1000-1-1',
        o.soup = BeautifulSoup(SEGMENT_XML_MULT_FILES, 'html.parser')
        out2 = o._get_original()

def test_iaobject_original_file():
    o = archivedotorg.IAObject()
    o.id = 'ddr-densho-1000-1-1'
    o.xml_url = archivedotorg._xml_url(o.id)
    o.xml = SEGMENT_XML
    o.soup = BeautifulSoup(SEGMENT_XML, 'html.parser')
    o.original = o._get_original()
    o._gather_files_meta()
    o._assign_mimetype()
    print('o.original %s' % o.original)
    print('o.files %s' % o.files)
    f = o.original_file()
    print('f %s' % f)
    out = str(f)
    expected = '<DDR.archivedotorg.IAFile ddr-densho-1000-1-1-mezzanine-0762419626.mpg>'

def test_iaobject_dict():
    o = archivedotorg.IAObject().get(
        'ddr-densho-1000-1-1', 200, SEGMENT_XML
    )
    out = o.dict()
    expected = SEGMENT_DATA
    assert out == expected
    
#def test_iafile_dict():
    
#def test_file_meta():


SEGMENT_XML = """
<files>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.asr.js" source="original">
    <format>Unknown</format>
    <mtime>1478807469</mtime>
    <size>36915</size>
    <md5>1804baea96db733543d60e867185b73c</md5>
    <crc32>5deab6cb</crc32>
    <sha1>170d21c3ff9d93b903784b120f16177f77480ead</sha1>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.asr.srt" source="original">
    <format>SubRip</format>
    <mtime>1478807469</mtime>
    <size>3764</size>
    <md5>eba67f30b05748073e1f9c0064f914ea</md5>
    <crc32>40076942</crc32>
    <sha1>cbcb0e2cd2bd4b38c5c8ef67264d92009ee21dc9</sha1>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mp3" source="derivative">
    <bitrate>96</bitrate>
    <length>03:44</length>
    <format>MP3</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1478805354</mtime>
    <size>2692096</size>
    <md5>19b45227bc97bad0798b1da54c924fa5</md5>
    <crc32>67543729</crc32>
    <sha1>2f50018d54bfffab0e85e48d4a49171bd46cb46f</sha1>
    <height>0</height>
    <width>0</width>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mp4" source="derivative">
    <format>h.264</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471373252</mtime>
    <size>23450965</size>
    <md5>9045f541092e6e8c9b87d0aa5fed757d</md5>
    <crc32>91b3061c</crc32>
    <sha1>7321dd3f500213376db5ecf632c6a5982680d2b4</sha1>
    <length>224.23</length>
    <height>480</height>
    <width>640</width>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mpg" source="original">
    <mtime>1471370469</mtime>
    <size>179126276</size>
    <md5>08a1967b090f4b3957f3460564d8151a</md5>
    <crc32>378d9f87</crc32>
    <sha1>0762419626fed35e60d829d6c5f94e3f49b9a653</sha1>
    <format>MPEG2</format>
    <length>224.21</length>
    <height>480</height>
    <width>640</width>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.ogv" source="derivative">
    <format>Ogg Video</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471373730</mtime>
    <size>16803041</size>
    <md5>3218879c4c53752815682ceceab7e599</md5>
    <crc32>4f85584a</crc32>
    <sha1>245ef23a56ac2d8f17d56b473a02c13c9a0490a9</sha1>
    <length>224.21</length>
    <height>300</height>
    <width>400</width>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.png" source="derivative">
    <format>PNG</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mp3</original>
    <mtime>1478805366</mtime>
    <size>13287</size>
    <md5>24f990615301cea5a55a2a4682b04e46</md5>
    <crc32>8a7dd42c</crc32>
    <sha1>c6584266072971fb1d062686d1a4a5f8c9dd5ac9</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000001.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372577</mtime>
    <size>8403</size>
    <md5>439e87432e8893610a731730d6cabc53</md5>
    <crc32>d8cac77f</crc32>
    <sha1>495bca2cc9ab7f353add41a70feea4b152dfe83c</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000057.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372587</mtime>
    <size>8439</size>
    <md5>769581d15c6e5818de0205583b33775d</md5>
    <crc32>cd27d3da</crc32>
    <sha1>1f509b95cabf67bd87c64a1362bd97ddb81f7ce3</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000087.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372596</mtime>
    <size>8376</size>
    <md5>6dea07b001452015cb26911c5a449393</md5>
    <crc32>371521a2</crc32>
    <sha1>cceee73db826fcafa1ae93675d1b328270d2f479</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000117.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372604</mtime>
    <size>8518</size>
    <md5>2120f22bea4b34d2d1f8d662b62cfc06</md5>
    <crc32>639e2120</crc32>
    <sha1>a2b1128a1c5177e777a823f9446d0c89848fdcf5</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000147.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372613</mtime>
    <size>8406</size>
    <md5>c06d55ac5271622636b1072caeda381f</md5>
    <crc32>122f7696</crc32>
    <sha1>60de95e61d9bcce201f60b9825736855ecf872cd</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000177.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372621</mtime>
    <size>8395</size>
    <md5>ff63c6688c105d8512521a5ca5f432ea</md5>
    <crc32>a3d2b732</crc32>
    <sha1>6fced4dcc14b6bd5f0c815516554b642a1b225b8</sha1>
  </file>
  <file name="ddr-densho-1000-1-1.thumbs/ddr-densho-1000-1-1-mezzanine-0762419626_000207.jpg" source="derivative">
    <format>Thumbnail</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1471372630</mtime>
    <size>8449</size>
    <md5>6a9d8dc23a274cd93ff35f80e87132e4</md5>
    <crc32>42a4b83f</crc32>
    <sha1>ef80cda085f24f7dc55596d5ab4c0e18cc78dfdb</sha1>
  </file>
  <file name="ddr-densho-1000-1-1_archive.torrent" source="metadata">
    <btih>6fc4c68f078fefddecf3b6c3985aa35dba200080</btih>
    <mtime>1483640986</mtime>
    <size>11408</size>
    <md5>977037029778e3a317120c7884d44d35</md5>
    <crc32>66b5fec0</crc32>
    <sha1>2d24d6f3db2b27018538b54bae2db625f9900781</sha1>
    <format>Archive BitTorrent</format>
  </file>
  <file name="ddr-densho-1000-1-1_files.xml" source="original">
    <format>Metadata</format>
    <md5>a6e28a019adb30a53f4f399677c82ca6</md5>
  </file>
  <file name="ddr-densho-1000-1-1_meta.sqlite" source="original">
    <mtime>1471370487</mtime>
    <size>8192</size>
    <md5>67206f2e2f612105300d5492683f5332</md5>
    <crc32>98c63d85</crc32>
    <sha1>3fc2246d0f29556dd6a0a52fb3aace4d12bc7257</sha1>
    <format>Metadata</format>
  </file>
  <file name="ddr-densho-1000-1-1_meta.xml" source="original">
    <mtime>1483640983</mtime>
    <size>1378</size>
    <format>Metadata</format>
    <md5>efa6244c1ec372646f4bbdda9a3d4571</md5>
    <crc32>a27ce23e</crc32>
    <sha1>a1e852e5bdb91b197bdf101e2732f1b1b15d8fe5</sha1>
  </file>
</files>
"""

SEGMENT_XML_NO_FILES = """
<files>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.asr.srt" source="original">
    <format>SubRip</format>
    <mtime>1478807469</mtime>
    <size>3764</size>
    <md5>eba67f30b05748073e1f9c0064f914ea</md5>
    <crc32>40076942</crc32>
    <sha1>cbcb0e2cd2bd4b38c5c8ef67264d92009ee21dc9</sha1>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mp3" source="derivative">
    <bitrate>96</bitrate>
    <length>03:44</length>
    <format>MP3</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1478805354</mtime>
    <size>2692096</size>
    <md5>19b45227bc97bad0798b1da54c924fa5</md5>
    <crc32>67543729</crc32>
    <sha1>2f50018d54bfffab0e85e48d4a49171bd46cb46f</sha1>
    <height>0</height>
    <width>0</width>
  </file>
</files>
"""

SEGMENT_XML_MULT_FILES = """
<files>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mp3" source="original">
    <bitrate>96</bitrate>
    <length>03:44</length>
    <format>MP3</format>
    <original>ddr-densho-1000-1-1-mezzanine-0762419626.mpg</original>
    <mtime>1478805354</mtime>
    <size>2692096</size>
    <md5>19b45227bc97bad0798b1da54c924fa5</md5>
    <crc32>67543729</crc32>
    <sha1>2f50018d54bfffab0e85e48d4a49171bd46cb46f</sha1>
    <height>0</height>
    <width>0</width>
  </file>
  <file name="ddr-densho-1000-1-1-mezzanine-0762419626.mpg" source="original">
    <mtime>1471370469</mtime>
    <size>179126276</size>
    <md5>08a1967b090f4b3957f3460564d8151a</md5>
    <crc32>378d9f87</crc32>
    <sha1>0762419626fed35e60d829d6c5f94e3f49b9a653</sha1>
    <format>MPEG2</format>
    <length>224.21</length>
    <height>480</height>
    <width>640</width>
  </file>
</files>
"""

SEGMENT_DATA = {
    'id': 'ddr-densho-1000-1-1',
    'xml_url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1_files.xml',
    'http_status': 200,
    'original': u'ddr-densho-1000-1-1-mezzanine-0762419626.mpg',
    'mimetype': 'video/mpeg',
    'files': {
        'mpg': {
            'mimetype': 'video/mpeg', 'sha1': u'0762419626fed35e60d829d6c5f94e3f49b9a653', 'name': u'ddr-densho-1000-1-1-mezzanine-0762419626.mpg', 'encoding': None, 'url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mpg', 'format': 'mpg', 'height': u'480', 'width': u'640', 'length': u'224.21', 'title': '', 'size': u'179126276'},
        'mp4': {'mimetype': 'video/mp4', 'sha1': u'7321dd3f500213376db5ecf632c6a5982680d2b4', 'name': u'ddr-densho-1000-1-1-mezzanine-0762419626.mp4', 'encoding': None, 'url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mp4', 'format': 'mp4', 'height': u'480', 'width': u'640', 'length': u'224.23', 'title': '', 'size': u'23450965'},
        'ogv': {'mimetype': 'video/ogg', 'sha1': u'245ef23a56ac2d8f17d56b473a02c13c9a0490a9', 'name': u'ddr-densho-1000-1-1-mezzanine-0762419626.ogv', 'encoding': None, 'url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.ogv', 'format': 'ogv', 'height': u'300', 'width': u'400', 'length': u'224.21', 'title': '', 'size': u'16803041'},
        'mp3': {'mimetype': 'audio/mpeg', 'sha1': u'2f50018d54bfffab0e85e48d4a49171bd46cb46f', 'name': u'ddr-densho-1000-1-1-mezzanine-0762419626.mp3', 'encoding': None, 'url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.mp3', 'format': 'mp3', 'height': u'0', 'width': u'0', 'length': u'03:44', 'title': '', 'size': u'2692096'},
        'png': {'mimetype': 'image/png', 'sha1': u'c6584266072971fb1d062686d1a4a5f8c9dd5ac9', 'name': u'ddr-densho-1000-1-1-mezzanine-0762419626.png', 'encoding': None, 'url': 'https://archive.org/download/ddr-densho-1000-1-1/ddr-densho-1000-1-1-mezzanine-0762419626.png', 'format': 'png', 'height': '', 'width': '', 'length': '', 'title': '', 'size': u'13287'},
    },
}
