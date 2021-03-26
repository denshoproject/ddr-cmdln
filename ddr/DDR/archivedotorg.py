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

FORMATS = [
    'mp3', 'vbr mp3',
    'mp4', 'h.264', 'h.264 ia', 'mpg', 'mpeg2', 'mpeg4', 'ogv', 'ogg video',
    'png',
]
CONVERT_FORMATS = {
    'vbr mp3': 'mp3',
    'h.264': 'mp4',
    'mpeg2': 'mpg',
    'mpeg4': 'mpg',
    'ogg video': 'ogv',
}


def get_ia_meta(o):
    """Get object record from Internet Archive; use dummy record if unpublished
    
    @param o: models.Entity
    @returns: dict or None
    """
    data = {}
    if is_iaobject(o):
        data = {
            'id': o.identifier.id,
            #'xml_url': iaobject.xml_url,
            #'http_status': 200,
            'original': '',
            'mimetype': '',
            'files': {},
        }
        cmd = f'ia metadata {o.identifier.id}'
        try:
            out = subprocess.check_output(cmd.split()).decode()
        except FileNotFoundError:
            msg = "Internet Archive `ia` command required for this operation. " \
                "Install using 'pip install -U internetarchive'."
            raise Exception(msg)
        iaobject = json.loads(out)
        files = {}
        # only include the files we're interested in
        for f in iaobject.get('files', []):
            if f['format'].lower() in FORMATS:
                fmt = f['format'].lower()
                if fmt in CONVERT_FORMATS.keys():
                    # stay compatible with legacy code
                    fmt = CONVERT_FORMATS[fmt]
                f['format'] = fmt
                f['mimetype'],encoding = mimetypes.guess_type(f['name'])
                f['url'] = f'https://archive.org/download/{o.identifier.id}/{f["name"]}'
                files[fmt] = f
        # IA makes a smaller derivative MP4 for streaming
        # Replace the mp4 if 'h.264 ia' is present and call it 'original'.
        for key,f in files.items():
            if (key == 'h.264 ia'):
                files['mp4'] = files.pop('h.264 ia')
                files['mp4']['source'] = 'streaming'
        # pick out the original file, but prefer the streaming one
        for key,f in files.items():
            if f['source'] == 'streaming':
                data['original'] = f['name']
        if not data.get('original'):
            for key,f in files.items():
                if f['source'] == 'original':
                    data['original'] = f['name']
        # mimetype for the original file
        if data['original']:
            data['mimetype'],encoding = mimetypes.guess_type(data['original'])
        data['files'] = files
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
