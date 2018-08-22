import json

import bs4
import requests

from django.conf import settings

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


def xml_url(oid):
    """Given an object id, returns an Internet Archive URL
    
    @param oid: str object ID
    @returns: url str
    """
    return SEGMENT_XML_URL.format(base=IA_DOWNLOAD_URL, segmentid=oid)
    
def get_xml(oid):
    """HTTP request for IA metadata, returns HTTP code w data
    
    @param oid: str object ID
    @returns: http_status,xml int,str
    """
    r = requests.get(xml_url(oid))
    if r.status_code == 200:
        return r.status_code,r.text
    return r.status_code,''

def object_meta(oid, status, xml):
    """Get segment file metadata from Archive.org
    
    @param oid: str object ID
    @param xml: str
    @returns: dict
    """
    data = {
        'xml_url': xml_url(oid),
        'status':status,
        'formats': {},
    }
    soup = bs4.BeautifulSoup(xml, 'html.parser')
    for tag in soup.files.children:
        if isinstance(tag, bs4.element.Tag):
            for format_ in FORMATS:
                if format_ in tag['name']:
                    data['formats'][format_] = {
                        'format': format_,
                        'url': FILE_DOWNLOAD_URL.format(
                            base=IA_DOWNLOAD_URL,
                            segmentid=oid,
                            fileid=tag['name']
                        )
                    }
                    for field in FIELDNAMES:
                        try:
                            data['formats'][format_][field] = tag.find(field).contents[0]
                        except AttributeError:
                            data['formats'][format_][field] = ''
    return data
