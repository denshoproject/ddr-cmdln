# ddr-vhprep.py
# Checksums, renames and creates base metadata for vh segments 
# Input data:
# - Dir(s) of mpeg segs named in denshovh format
# - CSV file with raw segment metadata
# Output:
# - Dir(s) of segs named in ddr format
# - fmetadata.csv in input denshovh dir

import csv
import datetime
import hashlib
import mimetypes
import os
from pathlib import Path
import shutil
import sys

import click

from DDR import identifier

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('inputdir')
@click.argument('outputdir')
def ddrvhfileprep(inputdir, outputdir):
    """Preps csv for DDR file import for VH binaries.
    
    This command preps a CSV for DDR file import from a directory of VH binaries.
    It assumes that the binary files are named with their associated DDR ID.
    The output CSV is not entirely complete; it will need some manual editing
    before import.
    
    \b
    EXAMPLE
    $ ddrvhfileprep.py ./vh_binaries ./output
    """
    inputerrs = 0
    if not os.path.isdir(inputdir):
        click.echo(f"ERROR: Input path does not exist: {inputdir}")
    if not os.path.exists(outputdir):
        click.echo(f"ERROR: Output path does not exist: {outputdir}")
    if inputerrs:
        sys.exit(1)

    started = datetime.datetime.now()

    csvout = process_seg_dir(inputdir,outputdir)
    
    finished = datetime.datetime.now()
    elapsed = finished - started
    
    print('Started: {}'.format(started))
    print('Finished: {}'.format(finished))
    print('Elapsed: {}'.format(elapsed))

    return csvout


"""
files_cols:
id,external,role,basename_orig,mimetype,public,rights,sort,thumb,label,
digitize_person,tech_notes,external_urls,links,sha1,sha256,md5,size
"""

CSVCOLS = [ 'id',
            'external',
            'role',
            'basename_orig',
            'mimetype',
            'public',
            'rights',
            'sort',
            'thumb',
            'label',
            'digitize_person',
            'tech_notes',
            'external_urls',
            'links',
            'sha1',
            'sha256',
            'md5',
            'size']
# Assumes standard IA stream is an mp4
IASTREAMEXT = 'mp4'

def file_hash(path, algo='sha1'):
    if algo == 'sha256':
        h = hashlib.sha256()
    elif algo == 'md5':
        h = hashlib.md5()
    else:
        h = hashlib.sha1()
    block_size=65536
    f = open(path, 'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        h.update(data)
    f.close()
    return h.hexdigest()    

def process_seg_dir(dpath,outpath):
    # Init output data list
    odata = []
    # Get list of files for vh dir
    for fname in os.listdir(dpath):
        #print "process_seg_dir processing: " + filename
        if fname.startswith('ddr-'):
            thisfile = os.path.join(dpath,fname)
            print("Processing file: {}".format(thisfile))
            # get ddrid from filename
            fidentifier = identifier.Identifier(Path(fname).stem)
            segment = fidentifier.parent()
            segid = segment.id
            segno = str(segment.idparts['sid'])
            interview = segment.parent()
            intid = interview.id
            # Init row dict
            orow = {}
            orow['id'] = segid
            orow['external'] = True
            orow['role'] = "mezzanine"
            orow['basename_orig'] = fname
            orow['public'] = 1
            orow['rights'] = "cc"
            orow['sort'] = segno
            orow['digitize_person'] = "Hoshide, Dana"
            orow['label'] = "Segment {}".format(segno)

            orow['md5'] = file_hash(thisfile,'md5')
            print("md5: {}".format(orow['md5']))
            orow['sha1'] = file_hash(thisfile,'sha1')
            print("sha1: {}".format(orow['sha1']))
            orow['sha256'] = file_hash(thisfile,'sha256')
            print("sha256: {}".format(orow['sha256']))
            orow['size'] = os.path.getsize(thisfile)
            print("size: {}".format(orow['size']))
            orow['mimetype'] = mimetypes.guess_type(thisfile)[0]
            print("mimetype: {}".format(orow['mimetype']))

            file_sha = orow['sha1'][:10]
            file_ext = Path(fname).suffix
            newfname = segid + '-mezzanine-' + file_sha + file_ext
            orow['external_urls'] = "label:Internet Archive download|" \
                f"url:https://archive.org/download/{segid}/{newfname};"
            # Assumes that the stream file will be an MP4
            orow['external_urls'] += "label:Internet Archive stream|" \
                "url:https://archive.org/download/" \
                f"{segid}/{segid}.{IASTREAMEXT}"
            odata.append(orow)

            # Copy file with new name to output path
            newfile = os.path.join(outpath, newfname)
            fpath = os.path.join(dpath, fname)
            shutil.copyfile(fpath, newfile)
            print("Copied new file: {}".format(newfname))
    
    csvout = os.path.join(outpath,'fileimport.csv')
    with open(csvout, 'w') as csvfile:    
        writer = csv.DictWriter(csvfile, fieldnames=CSVCOLS)
        writer.writeheader()
        writer.writerows(odata)

    return csvout


if __name__ == '__main__':
    ddrvhfileprep()
