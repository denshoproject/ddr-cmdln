import json

import bs4
import requests

from django.conf import settings

IA_DOWNLOAD_URL = 'https://archive.org/download'

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24_files.xml
SEGMENT_XML_URL = '{base}/{segmentid}/{segmentid}_files.xml'

# https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24-mezzanine-2b247c16c0.mp4
FILE_DOWNLOAD_URL = '{base}/{segmentid}/{fileid}'
FORMATS = ['mp3', 'mp4', 'mpg', 'ogv', 'png',]
FIELDNAMES = ['sha1','size','length','height','width','title',]


def download_segment_meta(sid):
    """Get segment file metadata from Archive.org
    """
    data = {}
    data['xml_url'] = SEGMENT_XML_URL.format(base=IA_DOWNLOAD_URL, segmentid=sid)
    r = requests.get(data['xml_url'])
    data['status'] = r.status_code
    data['formats'] = {}
    if r.status_code != 200:
        return data
    soup = bs4.BeautifulSoup(r.text, 'html.parser')
    for tag in soup.files.children:
        if isinstance(tag, bs4.element.Tag):
            for format_ in FORMATS:
                if format_ in tag['name']:
                    data['formats'][format_] = {}
                    data['formats'][format_]['format'] = format_
                    data['formats'][format_]['url'] = FILE_DOWNLOAD_URL.format(
                        base=IA_DOWNLOAD_URL,
                        segmentid=sid,
                        fileid=tag['name']
                    )
                    for field in FIELDNAMES:
                        try:
                            data['formats'][format_][field] = tag.find(field).contents[0]
                        except AttributeError:
                            data['formats'][format_][field] = ''
    return data
