from datetime import datetime
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union

import click
import envoy

from DDR import config
from DDR import docstore
from DDR import fileio
from DDR import identifier
from DDR import util

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)
    

@click.command()
@click.argument('fileroles')
@click.argument('collection')
@click.argument('destbase')
@click.option('--force','-f',  is_flag=True, help='Force')
def ddrpubcopy(fileroles, collection, destbase, force):
    """ddrpubcopy - Copies binaries from collection to dest dir for publication.
    
    \b
    This command copies specified types of binaries from a collection to a
    destination folder.  Destination files are in a very simple hierarchy (just
    files within a single directory per collection) that is suitable for use by
    ddr-public.
     
    \b
    ddr-pubcopy produces a very simple file layout:
        $BASE/$COLLECTION_ID/$FILENAME
    
    \b
    Example:
        ddrpubcopy mezzanine,transcript /var/www/media/ddr/ddr-test-123 /media/USBHARDDRIVE
    """
    collection = Path(collection)
    destbase = Path(destbase)
    if destbase == collection:
        click.echo('ERROR: Source and destination are the same!')
        sys.exit(1)
    
    started = datetime.now()
    LOG = destbase / 'ddrpubcopy.log'
    
    cid = collection.name
    destdir = destbase / cid
    # if collection dir doesn't exist in destdir, mkdir
    if not destdir.exists():
        destdir.mkdir(parents=True)
    
    logprint(LOG, 'Source      %s' % collection)
    logprint(LOG, 'Destination %s' % destdir)
    
    if fileroles == 'all':
        roles = [role for fileroles in identifier.VALID_COMPONENTS['role']]
    else:
        roles = fileroles.replace(' ','').split(',')
    logprint(LOG, 'Copying {}'.format(', '.join(roles)))
    
    files = find_files(collection)
    to_copy = filter_files(files, roles, force, LOG)
    errs = rsync_files(to_copy, collection, destdir, LOG)
    if errs:
        logprint(LOG, '%s FAILS:' % len(errs))
        for path in errs:
            logprint(LOG, path)
        logprint(LOG, 'Note: Numbers represent rsync exit codes')
    
    finished = datetime.now()
    elapsed = finished - started
    logprint(LOG, 'DONE!')
    logprint_nots(LOG, '%s elapsed' % elapsed)
    click.echo('')


def dtfmt(dt: datetime) -> str:
    """Consistent date format.
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

def logprint(filename: Path, msg: str):
    """Print to log file and console, with timestamp.
    """
    msg = '%s - %s\n' % (dtfmt(datetime.now()), msg)
    fileio.append_text(msg, str(filename))
    click.echo(msg.strip('\n'))

def logprint_nots(filename: Path, msg: str):
    """Print to log file and console, no timestamp.
    """
    msg = '%s\n' % msg
    fileio.append_text(msg, str(filename))
    click.echo(msg.strip('\n'))

def find_files(collection_path: Path) -> List[Path]:
    """List files using git-annex-find.
    
    Only includes files present in local filesystem.
    This avoids rsync errors for missing files.
    """
    os.chdir(collection_path)
    r0 = envoy.run('git annex find')
    files = r0.std_out.strip().split('\n')
    # strip out blank lines
    return [Path(path) for path in files if path]

def filter_files(files: List[Path],
                 roles: List[str],
                 force: bool,
                 LOG: Path) -> List[Path]:
    """Binary and access files for each File in roles
    """
    logprint(LOG, '%s files in filesystem' % len(files))
    # load objects
    # extract file IDs from list of files, rm duplicates
    oids = sorted(list(set([
        os.path.splitext(os.path.basename(
            str(path).replace(config.ACCESS_FILE_SUFFIX, '')
        ))[0]
        for path in files
    ])))
    oidentifiers = [
        identifier.Identifier(oid, config.MEDIA_BASE)
        for oid in oids
    ]
    parents = {
        oid: oi.object()
        for oid,oi in docstore._all_parents(oidentifiers).items()
    }
    publishable = [
        x['identifier'].object()
        for x in docstore.publishable(oidentifiers, parents)
        if x['action'] == 'POST'
    ] 
    logprint(LOG, '%s publishable objects' % len(publishable))
    # list binaries and access files
    binaries = [o.path_rel for o in publishable]
    accesses = [o.access_rel for o in publishable]
    paths = sorted(list(set(binaries + accesses)))
    logprint(LOG, '%s files' % len(paths))
    return paths

def rsync_files(to_copy: List[Path],
                collection_path: Path,
                destdir: Path,
                LOG: Path) -> List[str]:
    os.chdir(collection_path)
    errs = []
    for n,f in enumerate(to_copy):
        src = collection_path / f
        src = f
        dest = destdir / f
        cmd = 'rsync --copy-links %s %s/' % (src, destdir)
        logprint(LOG, '%s/%s %s' % (n, len(to_copy), cmd))
        r1 = envoy.run(cmd)
        if r1.status_code:
            logprint(LOG, 'STATUS {}'.format(r1.status_code))
            errs.append((r1.status_code,src))
    return errs
