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
    files = OrderedDict()
    
    def __repr__(self):
        return "<%s.%s %s>" % (
            self.__module__,
            self.__class__.__name__,
            self.id
        )

    def __init__(self, oid, http_status=None, xml=None, *args, **kwargs):
        """Get segment file metadata from Archive.org
         
        @param oid: str object ID
        @param http_status: int (optional)
        @param xml: str (optional)
        @returns: dict
        """
        self.id = oid
        self.xml_url = _xml_url(self.id)
        if http_status and xml:
            self.http_status,self.xml = http_status,xml
        else:
            self.http_status,self.xml = get_xml(oid)
        self.soup = BeautifulSoup(self.xml, 'html.parser')
        self.original = self._get_original()
        if not self.original:
            # interview entities don't have any actual files
            return None
        self._gather_files_meta()
        self._assign_mimetype()
    
    def _get_original(self):
        """Returns filename of original master object
        
        Archive.org objects correspond to DDR *Entity* or *Segment*, not *File*.
        Archive.org objects have files, each marked "original" or "derivative".
        Many files marked "original" are not actually original, e.g. *.asr.srt.
        One file marked "original" *should* be the one uploaded by Densho.
        Files uploaded by Densho are in a limited set of formats.
        
        NO: <file name="ddr-densho-1000-210-1-mezzanine-a709bc73aa.asr.js" source="original">
        NO: <file name="ddr-densho-1000-210-1-mezzanine-a709bc73aa.mp3" source="derivative">
        YES: <file name="ddr-densho-1000-210-1-mezzanine-a709bc73aa.mpg" source="original">
        
        @returns: str Filename
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
        """Populate self.files with info for supported formats
        """
        for format_ in FORMATS:
            for tag in self.soup.files.children:
                if isinstance(tag, Tag) and (tag['name'].endswith(format_)):
                    self.files[format_] = IAFile(self.id, format_, tag)
    
    def _assign_mimetype(self):
        """Assign self the mimetype of the original file
        """
        self.mimetype = mimetypes.guess_type(self.original)[0]
    
    def original_file(self):
        """
        """
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
    
    def __init__(self, oid, format_, tag):
        self.name = tag['name']
        self.format = format_
        self.url = _file_url(oid, tag)
        self.mimetype,self.encoding = mimetypes.guess_type(tag['name'])
        for field in FIELDNAMES:
            self._fields.append(field)
            try:
                setattr(self, field, tag.find(field).contents[0])
            except AttributeError:
                setattr(self, field, '')
    
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
