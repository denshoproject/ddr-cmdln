import json
import mimetypes
mimetypes.init()
import subprocess
import sys

IA_SAMPLE_URL = 'https://archive.org/download/ddr-densho-1003-1-24/ddr-densho-1003-1-24_files.xml'

# Models that should always be checked for IA content
IA_HOSTED_MODELS = ['segment',]
# Entity.formats that should always be checked for IA content
IA_HOSTED_FORMATS = ['av','vh',]


def get_ia_meta(o):
    """Get object record from Internet Archive; use dummy record if unpublished
    
    @param o: models.Entity
    @returns: dict or None
    """
    data = {}
    if is_iaobject(o):
        iaobject = get_ia_metadata(o.identifier.id)
        data = process_ia_metadata(o.identifier.id, iaobject['files'])
    return data

def is_iaobject(o):
    """Determines whether or not to check Internet Archive for this object
    
    @param o: Identifier
    @returns: boolean
    """
    if (o.identifier.model in IA_HOSTED_MODELS) \
    or (o.format in IA_HOSTED_FORMATS):
        return True
    return False

def get_ia_metadata(oid):
    """Use official IA client to get metadata
    """
    cmd = f'ia metadata {oid}'
    try:
        out = subprocess.check_output(cmd.split()).decode()
    except FileNotFoundError:
        msg = "Internet Archive `ia` command required for this operation. " \
            "Install using 'pip install -U internetarchive'."
        raise Exception(msg)
    return json.loads(out)

def process_ia_metadata(oid, files_list):
    """Filter IA files list

    ddr-densho-400-1    (mp3, simple entity)
                           --> (template: entity/detail-audio.html)
    ddr-csujad-30-19-1  (mp3, VH interview segment)
                           --> (template: entity/segment-audio.html)
    ddr-densho-1000-1-1 (mpg, standard VH that we prepared)
                           --> (template: entity/segment-video.html)
    ddr-densho-1020-13  (original video mp4; IA-created streaming video)
                           --> (template: entity/detail-video.html)
    ddr-densho-122-4-1  (external video, stream-only/no download from IA)
                           --> (template: entity/segment-video.html)
    """
    data = {
        'id': oid,
        #'xml_url': iaobject.xml_url,
        #'http_status': 200,
        'original': '',
        'mimetype': '',
        'files': {},
    }
    
    # IA metadata includes files we're not interested in.
    # Keep only these formats.
    FORMATS_ALLOW = [
        'mp4', 'h.264', 'h.264 ia', 'mpg', 'mpeg2', 'mpeg4',
        'ogv', 'ogg video',
        'mp3', 'vbr mp3',
    ]
    # Use generic format names for backwards compatibility with ddr-public
    FORMATS_REPLACE = {
        #'h.264 ia' - IA streaming don't replace see below
        'h.264': 'mp4',
        'mpeg2': 'mpg',
        'mpeg4': 'mpg',
        'ogg video': 'ogv',
        'vbr mp3': 'mp3',
    }
    files = {}
    for f in files_list:
        if f['format'].lower() in FORMATS_ALLOW:
            fmt = f['format'].lower()
            if fmt in FORMATS_REPLACE.keys():
                fmt = FORMATS_REPLACE[fmt]
            f['format'] = fmt
            f['mimetype'],encoding = mimetypes.guess_type(f['name'])
            f['url'] = f'https://archive.org/download/{oid}/{f["name"]}'
            files[fmt] = f
    # IA makes a smaller derivative MP4 for streaming large files.
    # Replace 'mp4' with the streaming file if present.
    for key,f in files.items():
        if (key == 'h.264 ia'):
            files['mp4'] = files.pop('h.264 ia')
            files['mp4']['source'] = 'streaming'
    for key,f in files.items():
        if f['source'] == 'streaming':
            data['original'] = f['name']
    if not data.get('original'):
        for key,f in files.items():
            if f['source'] == 'original':
                data['original'] = f['name']
    
    if data['original']:
        data['mimetype'],encoding = mimetypes.guess_type(data['original'])
    data['files'] = files
    return data

def format_mimetype(o, meta):
    """Returns object format and mimetype if present in Internet Archive
    
    @param o: models.Entity
    @param meta: dict
    @returns: str
    """
    if meta:
        return ':'.join([
            o.format,
            meta['mimetype'].split('/')[0]
        ])
    return ''
