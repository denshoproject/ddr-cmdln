import json
import mimetypes
mimetypes.init()
import re
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
        oid = o.identifier.id
        # Certain objects point to an external/non-Densho IA video
        external_id = external_ia_id(o)
        if external_id:
            oid = external_id
        # Get metadata from Internet Archive via their "ia metadata" tool
        iameta = get_ia_metadata(oid)
        if iameta:
            if 'error' in iameta.keys():
                raise FileNotFoundError(iameta)
            # format just the info DDR needs
            data = process_ia_metadata(o.identifier.id, iameta['files'])
            if external_id:
                # rewrite MP4 URL for external object
                data = fix_external_mp4_url(iameta, data)
        else:
            raise FileNotFoundError(f'No Internet Archive data for {o.identifier.id}.')
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

def get_ia_metadata(oid: str) -> dict:
    """Use official IA client to get metadata for an IA object
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
        'mpeg4': 'mp4',
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
            # filter out weird outliers
            if ('history/files/' in f['name']) or ( '~1~' in f['name']):
                break
            # looks good
            files[fmt] = f
    # IA makes a smaller derivative MP4 for streaming large files.
    # Replace 'mp4' with the streaming file if present.
    file_items = [(key,f) for key,f in files.items()]
    while file_items:
        key,f = file_items.pop(0)
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
    # All files are derivatives (original not available, ex: ddr-densho-122-4-1)
    # Find derivative that is the same type (i.e. video/*) as the original
    if not data.get('original'):
        for f in files.values():
            # Some files have a '~1~' weird outlier as their original which
            # confuses the mimetype guesser.
            if '~1~' in f['original']:
                f['original'] = f['original'].replace('.~1~', '')
            file_mimetype,encoding = mimetypes.guess_type(f['name'])
            orig_mimetype,encoding = mimetypes.guess_type(f['original'])
            if file_mimetype.split('/')[0] == orig_mimetype.split('/')[0]:
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

# functions for supporting external IA media
# See https://github.com/denshoproject/ddr-cmdln/issues/245
# See https://github.com/denshoproject/ddr-public/issues/230

# "... [ia_external_id:EXTERNALID]; ..."
EXTERNAL_OBJECT_ID_PATTERN = re.compile(r'ia_external_id:(\w+)')

def external_ia_id(o):
    """Look in DDR object's notes for external/non-Densho IA object ID

    Certain DDR objects point to IA media that was not uploaded by Densho
    example: IA Hundred Films project

    These objects will have a special marker in their notes field in
    this format: "... [ia_external_id:EXTERNALID]; ..."  Example:
    "...[ia_external_id:cabemrc_000010];..."

    """
    if hasattr(o, 'alternate_id') and isinstance(o.alternate_id, str):
        match = re.search(EXTERNAL_OBJECT_ID_PATTERN, o.alternate_id)
        if match:
            return match.groups()[0]
    return None

def fix_external_mp4_url(iameta, data):
    """Fix mp4 file URL for external objects

    process_ia_metadata() assembles URLs for MP4s in the following form:
    https://archive.org/download/ddr-testing-40439-1/cabemrc_000010_access.mp4
    It assumes the MP4s were uploaded by Densho

    iameta dict: Output of "ia metadata EXTERNALID"
    data dict: Output of process_ia_metadata
    """
    iaserver = iameta['server']
    iadir = iameta['dir']
    filename = data['files']['mp4']['name']
    data['files']['mp4']['url'] = f"https://{iaserver}{iadir}/{filename}"
    return data
