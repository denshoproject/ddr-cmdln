import os

from nose.tools import assert_raises
import pytest
import requests

from DDR import config
from DDR import imaging

TEST_FILES = {
    'jpg': {
        'url': 'http://ddr.densho.org/download/media/ddr-densho-2/ddr-densho-2-33-mezzanine-16fe864756-a.jpg',
        'filename': 'test-imaging.jpg',
        'identify': '',
    },
    'tif': {
        'url': 'http://ddr.densho.org/download/media/ddr-densho-2/ddr-densho-2-33-mezzanine-16fe864756.tif',
        'filename': 'test-imaging.tif',
        'identify': '',
    },
    'pdf': {
        'url': 'http://ddr.densho.org/download/media/ddr-hmwf-1/ddr-hmwf-1-577-mezzanine-c714496444.pdf',
        'filename': 'test-imaging.pdf',
        'identify': '',
    },
#    'doc': {
#        'url': '',
#        'filename': 'test-imaging.docx',
#        'identify': '',
#    },
}

# CloudFlare blocks if you don't use a browser user agent
REQUEST_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0"


@pytest.fixture(scope="session")
def test_files(tmpdir_factory):
    """
    Keep downloaded files in TESTING_BASE_DIR instead of pytest tmpdir
    so we don't have to download them every time
    """
    tmpdir = tmpdir_factory.mktemp('images')
    for fmt,data in TEST_FILES.items():
        # /tmp/pytest-of-USER/imaging
        data['path'] = tmpdir / '..' / '..' / data['filename']
        if not data['path'].exists():
            headers = {'user-agent': REQUEST_USER_AGENT}
            r = requests.get(data['url'], headers=headers, stream=True)
            with data['path'].open('wb') as fd:
                for chunk in r.iter_content():
                    fd.write(chunk)
    TEST_FILES['tmpdir'] = tmpdir
    return TEST_FILES
        
def test_analyze_magick(test_files):
    print(test_files['jpg']['path'])
    jpeg = imaging.analyze(str(test_files['jpg']['path']))
    print(jpeg)
    assert jpeg['path'] == test_files['jpg']['path']
    assert jpeg['frames'] == 1
    assert jpeg['format'] == 'JPEG'
    assert jpeg['image'] == True
    #print(test_files['tif']['path'])
    #tiff = imaging.analyze(str(test_files['tif']['path']))
    #print(tiff)
    #assert tiff['path'] == test_files['tif']['path']
    #assert tiff['frames'] == 1
    #assert tiff['format'] == 'TIFF'
    #assert tiff['image'] == True
    print(test_files['pdf']['path'])
    pdf  = imaging.analyze(str(test_files['pdf']['path']))
    print(pdf)
    assert pdf['path'] == test_files['pdf']['path']
    assert pdf['frames'] == 2
    assert pdf['format'] in ['PBM', 'PDF']
    assert pdf['image'] == True
    #docx = imaging.analyze(str(test_files['doc']['path']))
    #assert docx['path'] == None
    #assert docx['frames'] == 1
    #assert docx['format'] == None
    #assert docx['image'] == False

def test_analyze(test_files):
    path0 = '/tmp/missingfile.jpg'
    assert_raises(Exception, imaging.analyze, path0)
    
    path1 = str(test_files['jpg']['path'])
    assert os.path.exists(path1)
    out1 = imaging.analyze(path1)
    expected1 = {
        'std_err': '',
        'std_out': '%s JPEG 1024x588 1024x588+0+0 8-bit Grayscale Gray 256c 124KB 0.000u 0:00.000' % (
            test_files['jpg']['path']
        ),
        'format': 'JPEG',
        'image': True,
        'can_thumbnail': None,
        'frames': 1,
        'path': test_files['jpg']['path'],
    }
    print('out1      %s' % out1)
    print('expected1 %s' % expected1)
    assert out1 == expected1

GEOMETRY = {
    'ok': ['123x123', '123>x123', '123x123>', '123x', 'x123',],
    'bad': ['123',],
}

def test_geometry_is_ok():
    for s in GEOMETRY['ok']:
        assert imaging.geometry_is_ok(s) == True
    for s in GEOMETRY['bad']:
        assert imaging.geometry_is_ok(s) == False

def test_thumbnail(test_files):
    src = str(test_files['jpg']['path'])
    dest = str(test_files['tmpdir'] / 'test-imaging-thumb.jpg')
    geometry = '100x100'
    assert os.path.exists(src)
    imaging.thumbnail(src, dest, geometry)
    assert os.path.exists(dest)

def test_extract_xmp(test_files):
    expected0 = '<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?><x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="Exempi + XMP Core 5.5.0"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description rdf:about=""/></rdf:RDF></x:xmpmeta><?xpacket end="w"?>'.strip()
    out0 = imaging.extract_xmp(
        str(test_files['jpg']['path'])
    ).strip()
    out0 = out0.replace(u'\ufeff', '')
    assert out0 == expected0
