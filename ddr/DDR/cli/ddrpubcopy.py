from datetime import datetime
import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union

import click

from DDR import config
from DDR import docstore
from DDR import fileio
from DDR import identifier
from DDR import storage
from DDR import util

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s %(levelname)-8s %(message)s',
    stream=sys.stdout,
)
    

@click.command()
@click.argument('fileroles')
@click.argument('sourcedir')
@click.argument('destbase')
@click.option('--force','-f',  is_flag=True, help='Force')
@click.option('--b2sync','-b',  is_flag=True, help='Sync with Backblaze (requires environment vars)')
@click.option('--rsync','-r',  help='Rsync to specified target')
def ddrpubcopy(fileroles, sourcedir, destbase, force, b2sync, rsync):
    """ddrpubcopy - Copies binaries from source dir to dest dir for publication.
    
    \b
    This command copies specified types of binaries from a collection to a
    destination folder.  Destination files are in a very simple hierarchy (just
    files within a single directory per collection) that is suitable for use by
    ddr-public.
    
    \b
    ddr-pubcopy produces a very simple file layout:
        $BASE/$COLLECTION_ID/$FILENAME

    \b
    An individual run of ddrpubcopy may copy from a subdirectory of a collection
    but these files will go into the collection's destination folder.
    Example:
        $ ddrpubcopy mezzanine /var/www/media/ddr/ddr-test-123 /media/USBHARDDRIVE
    
    \b
    Use the --b2sync/-b flag to upload files to Backblaze after rsyncing them
    to the dest dir.  In order to use this flag you must define the following
    environment values:
        B2KEYID   Backblaze Key ID
        B2APPKEY  Backblaze Application Key
        B2BUCKET  Backblaze target bucket name
    Example:
        $ export  B2KEYID=REDACTED
        $ export B2APPKEY=REDACTED
        $ export B2BUCKET=REDACTED
        $ ddrpubcopy mezzanine,transcript /var/www/media/ddr/ddr-test-123 \\
            /media/USBHARDDRIVE --b2sync
    
    \b
    Use the --rsync/-r arg to rsync files to another server after rsyncing them
    to the dest dir.  You will be asked for your password when rsyncing starts.
    Example:
        $ ddrpubcopy mezzanine,transcript /var/www/media/ddr/ddr-test-123 \\
            /media/USBHARDDRIVE --rsync=USER@HOST:/var/www/media
    """
    # validate inputs
    if fileroles == 'all':
        roles = identifier.VALID_COMPONENTS['role']
    else:
        roles = fileroles.replace(' ','').split(',')
        for role in roles:
            if role not in identifier.VALID_COMPONENTS['role']:
                valid_roles = ','.join(identifier.VALID_COMPONENTS['role'])
                click.echo(f'ERROR: File role "{role}" is invalid.')
                click.echo(f'Valid roles: {valid_roles}, or all')
                sys.exit(1)
    sourcedir = Path(sourcedir)
    destbase = Path(destbase)
    if destbase == sourcedir:
        click.echo('ERROR: Source and destination are the same!')
        sys.exit(1)
    try:
        sourcedir_oi = identifier.Identifier(path=sourcedir)
        cidentifier = sourcedir_oi.collection()
    except:
        click.echo('ERROR: Source dir must be part of a DDR collection.')
        sys.exit(1)
    # b2
    B2KEYID = os.environ.get('B2KEYID')
    B2APPKEY = os.environ.get('B2APPKEY')
    B2BUCKET = os.environ.get('B2BUCKET')
    if b2sync:
        if not (B2KEYID and B2APPKEY and B2BUCKET):
            click.echo(
                'ERROR: --b2sync requires environment variables ' \
                'B2KEYID, B2APPKEY, B2BUCKET'
            )
            sys.exit(1)
        click.echo('Backblaze: authenticating')
        try:
            backblaze = storage.Backblaze(B2KEYID, B2APPKEY, B2BUCKET)
        except Exception as err:
            click.echo(f'ERROR: {err}')
            sys.exit(1)
        
    # prepare
    started = datetime.now()
    LOG = destbase / 'ddrpubcopy.log'
    # Note: Files from subdirectories of a collection will all go to a single
    # collection tmpdir
    destdir = destbase / cidentifier.id
    # if collection dir doesn't exist in destdir, mkdir
    if not destdir.exists():
        destdir.mkdir(parents=True)
    
    # do the work
    logprint(LOG, f'Finding files')
    files = find_files(sourcedir, LOG)
    logprint(LOG, f'found {len(files)}')    
    logprint(LOG, 'Filtering: {}'.format(','.join(roles)))
    to_copy = filter_files(files, roles, force, LOG)
    if to_copy:
        num = len(to_copy)
        for n,path_status in enumerate(
                rsync_to_tmpdir(to_copy, cidentifier.path_abs(), destdir, LOG)
        ):
            path,status = path_status
            if status != 'exists':
                logprint(LOG, f'{n}/{num} {status} {path}')
        num_files = len([f for f in destdir.iterdir()])
        logprint(LOG, f'{num_files} files in {destdir}')
        if b2sync:
            logprint(LOG, f'Backblaze: syncing {destdir}')
            for line in backblaze.sync_dir(destdir, basedir=cidentifier.id):
                # b2sdk output is passed to STDOUT
                pass
        if rsync:
            for output in rsync_to_target(destdir, rsync, LOG):
                if output:
                    logprint(LOG, output)
    finished = datetime.now()
    elapsed = finished - started
    logprint(LOG, 'DONE!')
    logprint_nots(LOG, '%s elapsed' % elapsed)


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

def _subproc(cmd):
    """Run a command and yield stdout as it appears
    """
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

def find_files(sourcedir: Path, LOG) -> List[Path]:
    """List files using git-annex-find.
    
    Only includes files present in local filesystem.
    This avoids rsync errors for missing files.
    """
    os.chdir(sourcedir)
    cmd = 'git annex find'
    return [
        Path(path.strip())
        for path in [
            line for line in _subproc(cmd.split())
        ]
        if path
    ]

def filter_files(files: List[Path],
                 roles: List[str],
                 force: bool,
                 LOG: Path) -> List[Path]:
    """Binary and access files for each File in roles
    """
    num_files_total = len(files)
    logprint(LOG, f'{num_files_total} collection files')
    # load objects
    # extract file IDs from list of files, rm duplicates
    oids = sorted(list(set([
        os.path.splitext(os.path.basename(
            str(path).replace(config.ACCESS_FILE_SUFFIX, '')
        ))[0]
        for path in files
    ])))
    num_objects_total = len(oids)
    oidentifiers = [
        oi
        for oi in [
            identifier.Identifier(oid, config.MEDIA_BASE)
            for oid in oids
        ]
        if oi.idparts['role'] in roles
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
    num_objects_publishable = len(publishable)
    logprint(LOG, f'{num_objects_publishable}/{num_objects_total} publishable objects')
    # list binaries and access files
    binaries = [
        Path(o.path_rel) for o in publishable if Path(o.path_abs).exists()
    ]
    accesses = [
        Path(o.access_rel) for o in publishable if Path(o.access_abs).exists()
    ]
    paths = sorted(list(set(binaries + accesses)))
    num_files_tocopy = len(paths)
    logprint(LOG, f'{num_files_tocopy}/{num_files_total} publishable files')
    return paths

def rsync_to_tmpdir(to_copy: List[Path],
                collection_path: Path,
                destdir: Path,
                LOG: Path) -> List[str]:
    """Rsync files from collection to S3-style bucket tmpdir
    
    Skip files already present in destdir
    """
    os.chdir(collection_path)
    errs = []
    for n,f in enumerate(to_copy):
        src = collection_path / f
        src = f
        dest = destdir / f.name
        if dest.exists():
            yield dest,'exists'
        else:
            cmd = 'rsync --copy-links %s %s/' % (src, destdir)
            for x in _subproc(cmd.split()):
                pass
            yield dest,'rsync'

def rsync_to_target(
        tmpdir: Path,
        target: str,
        LOG: Path) -> List[str]:
    """Rsync from tmpdir/COLLECTIONID to target
    """
    os.chdir(tmpdir)
    cmd = f'rsync -avz {tmpdir} {target}'
    logprint(LOG, cmd)
    for x in _subproc(cmd.split()):
        yield x
