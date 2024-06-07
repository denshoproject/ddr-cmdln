# ddr-vhprep.py
# Checksums, renames and creates base metadata for vh segments 
# Input data:
# - Dir(s) of mpeg segs named in denshovh format
# - CSV file with raw segment metadata
# Output:
# - Dir(s) of segs named in ddr format
# - fmetadata.csv in input denshovh dir

import sys, datetime, csv, shutil, os, hashlib, re, mimetypes

import click

description = """Preps csv for DDR file import for VH binaries."""

epilog = """
This command preps a CSV for DDR file import from a directory of VH binaries. It 
assumes that the binary files are named with their associated DDR ID. The 
output CSV is not entirely complete; it will need some manual editing before 
import.

EXAMPLE
  $ ddr-vhprep.py ./vh_binaries ./output
"""

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
            p = re.compile('ddr\-[a-z_0-9]+\-[0-9]{1,}\-[0-9]{1,}\-[0-9]{1,}')
            m = p.match(fname)
            segid = m.group()
            intid = segid[:segid.rfind('-')]
            segno = segid[segid.rfind('-')+1:]
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

            newfname = segid + '-mezzanine-' + orow['sha1'][:10] + fname[fname.rfind('.'):]
            orow['external_urls'] = "label:Internet Archive download|url:https://archive.org/download/{}/{};".format(segid,newfname)
            # Assumes that the stream file will be an MP4
            orow['external_urls'] += "label:Internet Archive stream|url:https://archive.org/download/{}/{}.{}".format(segid,segid,IASTREAMEXT)
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


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('inputpath')
@click.argument('outputpath')
def ddrvhfileprep(inputpath, outputpath):
    started = datetime.datetime.now()
    inputerrs = ''
    if not os.path.isdir(inputpath):
        inputerrs + 'Input path does not exist: {}\n'.format(inputpath)
    if not os.path.exists(outputpath):
        inputerrs + 'Output path does not exist: {}'.format(outputpath)
    if inputerrs != '':
        print('Error -- script exiting...\n{}'.format(inputerrs))
    else:
        process_seg_dir(inputpath,outputpath)
    
    finished = datetime.datetime.now()
    elapsed = finished - started
    
    print('Started: {}'.format(started))
    print('Finished: {}'.format(finished))
    print('Elapsed: {}'.format(elapsed))
    
    return

if __name__ == '__main__':
    ddrvhfileprep()
