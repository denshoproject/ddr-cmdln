from collections import OrderedDict
import mimetypes
mimetypes.init()
import os

from bs4 import BeautifulSoup
from bs4.element import Tag
import requests

IA_DOWNLOAD_URL = 'https://archive.org/download'

# Models that should always be checked for IA content
IA_HOSTED_MODELS = ['segment',]
# Entity.formats that should always be checked for IA content
IA_HOSTED_FORMATS = ['av','vh',]

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24_files.xml
SEGMENT_XML_URL = '{base}/{segmentid}/{segmentid}_files.xml'

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24-mezzanine-2b247c16c0.mp4
FILE_DOWNLOAD_URL = '{base}/{segmentid}/{fileid}'
FORMATS = ['mp3', 'mp4', 'mpg', 'ogv', 'png',]
FIELDNAMES = ['sha1','size','length','height','width','title',]


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
    files = OrderedDict()
    
    def __repr__(self):
        return "<%s.%s %s>" % (
            self.__module__,
            self.__class__.__name__,
            self.id
        )

    @staticmethod
    def get(oid, http_status=None, xml=None):
        """Get segment file metadata from Archive.org
         
        @param oid: str object ID
        @param http_status: int (optional)
        @param xml: str (optional)
        @returns: dict
        """
        o = IAObject()
        o.id = oid
        o.xml_url = _xml_url(o.id)
        if http_status and xml:
            o.http_status,o.xml = http_status,xml
        else:
            o.http_status,o.xml = get_xml(oid)
        o.soup = BeautifulSoup(o.xml, 'html.parser')
        o.original = o._get_original()
        if not o.original:
            # interview entities don't have any actual files
            return None
        o._gather_files_meta()
        o._assign_mimetype()
        return o
    
    def _get_original(self):
        """
        Archive.org objects correspond to the DDR *Entity* or *Segment*, not *File*.
        Archive.org objects have many files, each marked as "original" or "derivative".
        Many files marked "original" are not actually original, e.g. *.asr.srt.
        One file marked "original" *should* be the one uploaded by Densho.
        Files uploaded by Densho are in a limited set of formats.
        """
        files = [
            tag for tag in self.soup('file', source='original')
            if os.path.splitext(tag['name'])[1].replace('.','') in FORMATS
        ]
        if not files:
            return None
        if len(files) > 1:
            raise Exception('Found multiple original files for %s' % self.id)
        orig = files[0]
        return orig['name']
    
    def _gather_files_meta(self):
        for format_ in FORMATS:
            for tag in self.soup.files.children:
                if isinstance(tag, Tag) and (format_ in tag['name']):
                    self.files[format_] = self._file_meta(format_, tag)
    
    def _file_meta(self, format_, tag):
        f = IAFile()
        f.name = tag['name']
        f.format = format_
        f.url = _file_url(self.id, tag)
        f.mimetype,f.encoding = mimetypes.guess_type(tag['name'])
        for field in FIELDNAMES:
            f._fields.append(field)
            try:
                setattr(f, field, tag.find(field).contents[0])
            except AttributeError:
                setattr(f, field, '')
        return f
    
    def _assign_mimetype(self):
        for format_,f in self.files.iteritems():
            if f.name == self.original:
                self.mimetype = f.mimetype
                break
    
    def original_file(self):
        base,ext = os.path.splitext(self.original)
        ext = ext.replace('.','')
        return self.files[ext]
    
    def dict(self):
        return {
            'id': self.id,
            'xml_url': self.xml_url,
            'http_status': self.http_status,
            'original': self.original,
            'mimetype': self.mimetype,
            'files': {
                format_:f.dict()
                for format_,f in self.files.iteritems()
            }
        }

class IAFile():
    name = ''
    format = ''
    url = ''
    mimetype = ''
    encoding = ''
    _fields = []
    
    def __repr__(self):
        return "<%s.%s %s>" % (
            self.__module__,
            self.__class__.__name__,
            self.name
        )

    def dict(self):
        data = {
            'name': self.name,
            'format': self.format,
            'url': self.url,
            'mimetype': self.mimetype,
            'encoding': self.encoding,
        }
        for field in self._fields:
            data[field] = getattr(self, field)
        return data

    
def _xml_url(oid):
    return SEGMENT_XML_URL.format(
        base=IA_DOWNLOAD_URL,
        segmentid=oid
    )

def get_xml(oid):
    """HTTP request for IA metadata, returns HTTP code w data
    
    @param oid: str object ID
    @returns: http_status,xml int,str
    """
    r = requests.get(_xml_url(oid))
    if r.status_code == 200:
        return r.status_code,r.text
    return r.status_code,''

def _file_url(oid, tag):
    return FILE_DOWNLOAD_URL.format(
        base=IA_DOWNLOAD_URL,
        segmentid=oid,
        fileid=tag['name']
    )
