# archivedotorg - Get entity file metadata from Internet Archive
# 
# NOTES/ASSUMPTIONS:
# - All Densho items uploaded to IA are video interviews
# - Each item has an original file (what we uploaded) and some derivatives.
# - The original file is always an MPEG-2.
# - For some items streaming has been disabled and the original file does not
#   appear in publicly-available metadata.


from collections import Counter
import mimetypes
mimetypes.init()
import os
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup
from bs4.element import Tag
import requests

from DDR import config

IA_SAMPLE_URL = 'https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24_files.xml'

IA_DOWNLOAD_URL = 'https://archive.org/download'

# Models that should always be checked for IA content
IA_HOSTED_MODELS = ['segment',]
# Entity.formats that should always be checked for IA content
IA_HOSTED_FORMATS = ['av','vh',]

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24_files.xml
SEGMENT_XML_URL = '{base}/{segmentid}/{segmentid}_files.xml'

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24-mezzanine-2b247c16c0.mp4
FILE_DOWNLOAD_URL = '{base}/{segmentid}/{fileid}'
FORMATS = ['mp3', 'mp4', 'h.264', 'mpg', 'mpeg2', 'ogv', 'ogg video', 'png',]
FIELDNAMES = ['sha1','size','length','height','width','title',]

DUMMY_OBJECTS = {
    'av': 'ddr-csujad-28-1',
    'vh': 'ddr-densho-1000-28-1',
}


def get_ia_meta(o):
    """Get object record from Internet Archive; use dummy record if unpublished
    
    @param o: models.Entity
    @returns: dict or None
    """
    if is_iaobject(o):
        iaobject = IAObject(o.identifier.id)
        if iaobject.http_status == 200:
            return iaobject.dict()
        else:
            # Couldn't find object in IA - use a dummy
            oid = DUMMY_OBJECTS.get(o.format, None)
            if oid:
                iaobject = IAObject(oid)
                if iaobject.http_status == 200:
                    return iaobject.dict()
    return {}

def is_iaobject(o):
    """Determines whether or not to check Internet Archive for this object
    
    @param o: Identifier
    @returns: boolean
    """
    if (o.identifier.model in IA_HOSTED_MODELS) \
    or (o.format in IA_HOSTED_FORMATS):
        return True
    return False


class IAObject():
    id = ''
    format = ''
    xml_url = ''
    http_status = -1
    original = ''
    mimetype = ''
    files = {}
    
    def dict(self):
        return {
            'id': self.id,
            'xml_url': self.xml_url,
            'http_status': self.http_status,
            'original': self.original,
            'mimetype': self.mimetype,
            'files': self.files,
        }

    def __init__(self, oid, http_status=None, xml=None, *args, **kwargs):
        """Get segment file metadata from Archive.org
         
        @param oid: str object ID
        @param http_status: int (optional)
        @param xml: str (optional)
        @returns: dict
        """
        self.id = oid
        self.xml_url = SEGMENT_XML_URL.format(
            base=IA_DOWNLOAD_URL,
            segmentid=self.id
        )
        if http_status and xml:
            self.http_status,xml = http_status,xml
        else:
            r = requests.get(self.xml_url, timeout=config.REQUESTS_TIMEOUT)
            if r.status_code == 200:
                self.http_status,xml = r.status_code,r.text
            else:
                self.http_status,xml = r.status_code,''
        if xml:
            self.soup = BeautifulSoup(xml, 'html.parser')
            self.original = self._get_original(self.soup)
            if not self.original:
                # interview entities don't have any actual files
                return None
            self._gather_files_meta(self.soup)
            self.mimetype = mimetypes.guess_type(self.original)[0]
        
    def _get_original(self, soup):
        """Returns filename of original master object
        
        Archive.org objects correspond to DDR *Entity* or *Segment*, not *File*.
        Archive.org XML lists <file> tags which each contain an <original> tag.
        Some files are second-order derivatives (e.g. thumbnail from MP3 make
        from the original MPEG-2) and just getting the first or last <original>
        may not be correct file.  Return the <original> that appears most often.
        
        @returns: str Filename
        """
        counter = Counter()
        for f in soup.findAll('file'):
            if f.find('original'):
                filename = f.find('original').string
                counter[filename] += 1
        return counter.most_common()[0][0]
    
    def _gather_files_meta(self, soup):
        """Populate self.files with info for supported formats
        """
        for format_ in FORMATS:
            for tag in soup.files.children:
                if isinstance(tag, Tag) and (tag['name'].endswith(format_)):
                    mimetype,encoding = mimetypes.guess_type(tag['name'])
                    f = {
                        'name': tag['name'],
                        'format': format_,
                        'url': FILE_DOWNLOAD_URL.format(
                            base=IA_DOWNLOAD_URL,
                            segmentid=self.id,
                            fileid=tag['name']
                        ),
                        'mimetype': mimetype,
                        'encoding': encoding,
                    }
                    for field in FIELDNAMES:
                        try:
                            f[field] = tag.find(field).contents[0]
                        except AttributeError:
                            f[field] = ''
                        except IndexError:
                            f[field] = ''
                    self.files[format_] = f
