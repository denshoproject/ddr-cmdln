import os

from nose.tools import assert_raises
import requests

import config
import imaging

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'imaging')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

TEST_FILES = {
    'jpg': {
        'url': 'http://ddr.densho.org/download/media/ddr-densho-2/ddr-densho-2-33-mezzanine-16fe864756-a.jpg',
        'path': os.path.join(TESTING_BASE_DIR, 'test-imaging.jpg'),
        'identify': '',
    },
    'tif': {
        'url': 'http://ddr.densho.org/download/media/ddr-densho-2/ddr-densho-2-33-mezzanine-16fe864756.tif',
        'path': os.path.join(TESTING_BASE_DIR, 'test-imaging.tif'),
        'identify': '',
    },
    'pdf': {
        'url': 'http://ddr.densho.org/download/media/ddr-hmwf-1/ddr-hmwf-1-577-mezzanine-c714496444.pdf',
        'path': os.path.join(TESTING_BASE_DIR, 'test-imaging.pdf'),
        'identify': '',
    },
#    'doc': {
#        'url': '',
#        'path': os.path.join(TESTING_BASE_DIR, 'test-imaging.docx'),
#        'identify': '',
#    },
}

# CloudFlare blocks if you don't use a browser user agent
REQUEST_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0"

def _download_test_images():
    for fmt,data in TEST_FILES.iteritems():
        if not os.path.exists(data['path']):
            print(data['url'])
            headers = {'user-agent': REQUEST_USER_AGENT}
            r = requests.get(data['url'], headers=headers, stream=True)
            with open(data['path'], 'wb') as fd:
                for chunk in r.iter_content():
                    fd.write(chunk)
            print(data['path'])
        
def test_analyze_magick():
    _download_test_images()
    print(TEST_FILES['jpg']['path'])
    #print(TEST_FILES['tif']['path'])
    print(TEST_FILES['pdf']['path'])
    jpeg = imaging.analyze(TEST_FILES['jpg']['path'])
    #tiff = imaging.analyze(TEST_FILES['tif']['path'])
    pdf  = imaging.analyze(TEST_FILES['pdf']['path'])
    #docx = imaging.analyze(TEST_FILES['doc']['path'])
    print(jpeg)
    #print(tiff)
    print(pdf)
    assert jpeg['path'] == TEST_FILES['jpg']['path']
    assert jpeg['frames'] == 1
    assert jpeg['format'] == 'JPEG'
    assert jpeg['image'] == True
    #assert tiff['path'] == TEST_FILES['tif']['path']
    #assert tiff['frames'] == 1
    #assert tiff['format'] == 'TIFF'
    #assert tiff['image'] == True
    assert pdf['path'] == TEST_FILES['pdf']['path']
    assert pdf['frames'] == 2
    assert pdf['format'] == 'PBM'
    assert pdf['image'] == True
    #assert docx['path'] == None
    #assert docx['frames'] == 1
    #assert docx['format'] == None
    #assert docx['image'] == False

def test_analyze():
    _download_test_images()
    path0 = os.path.join(TESTING_BASE_DIR, 'missingfile.jpg')
    path1 = TEST_FILES['jpg']['path']
    assert_raises(Exception, imaging.analyze, path0)
    assert os.path.exists(path1)
    out1 = imaging.analyze(path1)
    expected1 = {
        'std_err': '',
        'std_out': '%s JPEG 1024x588 1024x588+0+0 8-bit Grayscale Gray 256c 124KB 0.000u 0:00.000' % (
            TEST_FILES['jpg']['path']
        ),
        'format': 'JPEG',
        'image': True,
        'can_thumbnail': None,
        'frames': 1,
        'path': TEST_FILES['jpg']['path'],
    }
    
    print('out1 %s' % out1)
    print('expected1 %s' % expected1)
    assert out1 == expected1

geometry = {
    'ok': ['123x123', '123>x123', '123x123>', '123x', 'x123',],
    'bad': ['123',],
}
def test_geometry_is_ok():
    for s in geometry['ok']:
        assert imaging.geometry_is_ok(s) == True
    for s in geometry['bad']:
        assert imaging.geometry_is_ok(s) == False

def test_thumbnail():
    _download_test_images()
    src = TEST_FILES['jpg']['path']
    dest = os.path.join(TESTING_BASE_DIR, 'test-imaging-thumb.jpg')
    geometry = '100x100'
    assert os.path.exists(src)
    imaging.thumbnail(src, dest, geometry)
    assert os.path.exists(dest)

def test_extract_xmp():
    _download_test_images()
    expected0 = '<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?><x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="Exempi + XMP Core 5.1.2"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description rdf:about=""/></rdf:RDF></x:xmpmeta><?xpacket end="w"?>'.strip()
    out0 = imaging.extract_xmp(TEST_FILES['jpg']['path']).strip()
    out0 = out0.replace(u'\ufeff', '')
    assert out0 == expected0
