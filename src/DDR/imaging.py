"""
imaging - functions for making thumbnails/access of files

# single-frame image
>>> imaging.thumbnail(src='/tmp/existing-file.tif', dest='/tmp/thumbnail.jpg', geometry='1024x1024')
'/tmp/thumbnail.jpg'

# multi-frame images
>>> imaging.thumbnail(src='/tmp/existing-file.tif', dest='/tmp/thumbnail.jpg', geometry='1024x1024')
'/tmp/thumbnail.jpg'
>>> imaging.thumbnail(src='/tmp/existing-file.pdf', dest='/tmp/thumbnail.jpg', geometry='1024x1024')
'/tmp/thumbnail.jpg'

# Exceptions are raised for non-image files
>>> imaging.thumbnail(src='/tmp/existing-file.docx', dest='/tmp/thumbnail.jpg', geometry='1024x1024')
Traceback (most recent call last):
  ...
CalledProcessError: Command 'identify /tmp/DDRWorkbenchScreenShots.docx' returned non-zero exit status 1

>>> imaging.extract_xmp(path)

"""

from datetime import datetime
import os
import subprocess

import envoy
import libxmp

IDENTIFY_CMD = 'identify "{path}"'
CONVERT_CMD  = "convert {options} \"{src}\"[0] -resize '{geometry}' {dest}"
CONVERT_LARGEFILE_THRESHOLD = 768 * 1000
CONVERT_LARGEFILE_OPTIONS = '-limit memory 2GB -limit map 4GB'


def analyze_magick(std_out, std_err):
    """Returns information about the first frame of the image file.
    
    JPEG:
    /tmp/arpanet-197703.jpg JPEG 800x573 800x573+0+0 8-bit DirectClass 63.2KB 0.000u 0:00.000
    
    TIFF:
    /tmp/ddr-densho-275-1-1_master.tif[0] TIFF 5632x8615 5632x8615+0+0 8-bit DirectClass 147.4MB 0.000u 0:00.000
    /tmp/ddr-densho-275-1-1_master.tif[1] TIFF 625x957 625x957+0+0 8-bit DirectClass 147.4MB 0.000u 0:00.000
    
    PDF:
    /tmp/Asus_E7242_P8Z77-V_LK.pdf[0] PDF 397x598 397x598+0+0 16-bit Bilevel DirectClass 30KB 0.100u 0:00.080
    /tmp/Asus_E7242_P8Z77-V_LK.pdf[1] PDF 397x598 397x598+0+0 16-bit Bilevel DirectClass 30KB 0.090u 0:00.080
    /tmp/Asus_E7242_P8Z77-V_LK.pdf[2] PDF 397x598 397x598+0+0 16-bit Bilevel DirectClass 30KB 0.090u 0:00.080
    
    DOCX:
    identify.im6: no decode delegate for this image format `/tmp/DDRWorkbenchScreenShots.docx' @ error/constitute.c/ReadImage/544.
    
    @param std_out: String
    @param std_err: String
    @returns: str
    """
    analysis = {
        'path': None,
        'format': None,
        'frames': None,
        'can_thumbnail': None,
        'std_out': std_out.strip(),
        'std_err': std_err.strip(),
    }
    lines = std_out.strip().split('\n')
    analysis['frames'] = len(lines)
    line = lines[0]
    # if this is an image the first chunk of line 1 should be the path
    path = line.split(' ')[0].split('[')[0]
    if (os.sep in path) and (not 'identify' in path) and (not 'error' in line):
        analysis['image'] = True
        analysis['path'] = path
    else:
        analysis['image'] = False
    # format of the first frame
    if analysis['image']:
        analysis['format'] = lines[0].split(' ')[1]
    return analysis

def analyze(path):
    """Look at file and return dict of attributes
    
    - path
    - format
    - frames
    - can_thumbnail
    
    @param path: Absolute path to file
    @returns: dict
    """
    if not os.path.exists(path):
        raise Exception('path does not exist %s' % path)
    # test for multiple frames/layers/pages
    # if there are multiple frames, we only want the first one
    cmd = IDENTIFY_CMD.format(path=path)
    r = envoy.run(cmd)
    if r.status_code != 0:
        raise Exception(r.std_err)
    return analyze_magick(r.std_out, r.std_err)

def geometry_is_ok(geometry):
    if ('x' in geometry) and (len(geometry.split('x')) == 2):
        return True
    return False

def _convert_cmd(src, dest, geometry, options=''):
    """Prepare ImageMagick convert command
    
    @param src: Absolute path to source file.
    @param dest: Absolute path to destination file.
    @param geometry: String (ex: '200x200')
    @param options: str
    @returns: str
    """
    if os.path.getsize(src) >= CONVERT_LARGEFILE_THRESHOLD:
        options = CONVERT_LARGEFILE_OPTIONS
    return CONVERT_CMD.format(
        options=options, src=src, geometry=geometry, dest=dest
    )

def thumbnail(src, dest, geometry, options=''):
    """Attempt to make thumbnail
    
    Note: uses Imagemagick 'convert' and 'identify'.
    Note: Writes log to DDRLocalEntity.files_log so entries appear
          alongside add_file() and add_access()
    
    @param src: Absolute path to source file.
    @param dest: Absolute path to destination file.
    @param geometry: String (ex: '200x200')
    @returns: Path to destination file
    """
    assert os.path.exists(src)
    assert os.path.exists(os.path.dirname(dest))
    assert geometry_is_ok(geometry)
    data = {
        'src': src,
        'size_src': os.path.getsize(src),
        'dest': dest,
        'geometry': geometry,
        'analysis': None,
        'convert': None,
        'attempted': None,
        'elapsed': None,
        'status_code': None,
        'std_out': None,
        'std_err': None,
        'exists': None,
        'size': None,
        'islink': None,
    }
    analysis = analyze(src)
    data['analysis'] = analysis
    cmd = _convert_cmd(src, dest, geometry, options)
    data['convert'] = cmd
    start = datetime.now()
    r = envoy.run(cmd)
    data['elapsed'] = str(datetime.now() - start)
    data['attempted'] = True
    data['status_code'] = r.status_code
    data['std_out'] = r.std_out
    data['std_err'] = r.std_err
    data['exists'] = os.path.exists(dest)
    if os.path.exists(dest):
        data['size'] = os.path.getsize(dest)
    data['islink'] = os.path.islink(dest)
    return data

def extract_xmp(path_abs):
    """Attempts to extract XMP data from a file, returns as dict.
    
    @param path_abs: Absolute path to file.
    @return dict NOTE: this is not an XML file!
    """
    xmpfile = libxmp.files.XMPFiles()
    try:
        xmpfile.open_file(path_abs, open_read=True)
    except:
        return None
    xmp = xmpfile.get_xmp()
    if xmp:
        xml = xmp.serialize_to_unicode()
        while xml.find('\n ') > -1:
            xml = xml.replace('\n ', '\n')
        xml = xml.replace('\n','')
        return xml
    return None
