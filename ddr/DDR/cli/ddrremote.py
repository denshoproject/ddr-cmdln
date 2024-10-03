DESCRIPTION = """ddrremote - Tools for working with DDR git-annex special remotes"""

HELP = """Tools for working with DDR git-annex special remotes.

check - Verifies that all the annex files in the collection are present in
        the specified special remote. Optionally writes to --log.
copy  - Runs git annex copy to the specified special remote, and
        optionally writes to --log.
recap - Reports info about recent collection modifications.

- Works best if the ddr user has passwordless SSH keys for each remote.
"""


from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from time import sleep
import traceback

import click
import humanize

from DDR import config
from DDR import dvcs

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'


def dtfmt(dt=None):
    if not dt:
        dt = datetime.now()
    return dt.strftime(TIMESTAMP_FORMAT)

def log(logfile, line):
    if logfile:
        with logfile.open('a') as f:
            f.write(f"{line}\n")
    click.echo(line)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--debug','-d', is_flag=True, default=False)
def ddrremote(debug):
    """ddrremote - Tools for working with DDR git-annex special remotes

    \b
    See "ddrremote help" for examples.
    """
    if debug:
        click.echo('Debug mode is on')


@ddrremote.command()
def help():
    """Detailed help and usage examples
    """
    click.echo(HELP)


@ddrremote.command()
@click.option('-f','--fast', is_flag=True, default=False, help='Faster check using git annex info.')
@click.option('-l','--logdir', default=None, help='Write output to log file in directory.')
@click.option('-v','--verbose', is_flag=True, default=False, help='Show all files not just missing ones.')
@click.option('-w','--wait', default=0, help='Wait N seconds after checking a collection.')
@click.argument('remote')
@click.argument('collection')
def check(fast, logdir, verbose, wait, remote, collection):
    """Check that annex files in the collection are present in the special remote
    
    Under the hood, uses the git annex checkpresentkey plumbing command,
    which reaches out to the special remote.
    
    Write output to files for use by compare command:
    ddrremote check REMOTE COLLECTIONPATH | tee /PATH/REMOTE/COLLECTION
    """
    collection_path = Path(collection).absolute()
    cid = collection_path.name
    prefix = f"{dtfmt()} ddrremote check"
    logfile = Path(logdir) / f"{cid}.log" if logdir else None
    starttime = datetime.now()
    log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} START")
    if fast:
        size_here,size_remote,diff = annex_info_remote(collection_path, remote)
        endtime = datetime.now(); elapsed = endtime - starttime
        def natural(filesize):
            if filesize == None: return '---'
            if filesize == 0: return '0'
            return humanize.naturalsize(filesize).replace(' ','') if filesize else '---'
        here_nat = natural(size_here)
        remote_nat = natural(size_remote)
        diff_nat = natural(diff)
        log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} DONE {elapsed} {here_nat} here {remote_nat} remote {diff_nat} missing")
    else:
        startdir = os.getcwd()
        annex_files = annex_find(collection_path)
        errors = 0
        missing = 0
        for rel_path in annex_files:
            output = checkpresentkey(collection_path, rel_path, remote)
            if output.split()[0] == 'ok':
                if (verbose):
                    log(logfile, f"{prefix} {output}")
            else:
                errors += 1
                if 'missing' in output:
                    missing += 1
                log(logfile, f"{prefix} {output}")
        endtime = datetime.now(); elapsed = endtime - starttime
        log(logfile, f"{dtfmt()} ddrremote check {remote} {collection_path} DONE {str(elapsed)} {len(annex_files)} files {errors} errs {missing} missing")
    if wait:
        sleep(int(wait))

def annex_find(collection_path):
    """Gets list of relative file paths using git annex find
    """
    os.chdir(collection_path)
    return [
        Path(relpath)
        for relpath in subprocess.check_output(
                'git annex find --include "*"', stderr=subprocess.STDOUT, shell=True,
                encoding='utf-8'
        ).strip().splitlines()
    ]

def checkpresentkey(collection_path, rel_path, remote):
    # get file's annex filename AKA key
    os.chdir(collection_path)
    key = None
    rpath = str(rel_path.resolve())
    if rel_path.is_symlink() and '.git/annex/objects' in rpath and 'SHA' in rpath:
        key = rel_path.resolve().name
    if not key:
        return f"ERROR nokey {str(rel_path)}"
    # git annex checkpresentkey
    cmd = f"git annex checkpresentkey {key} {remote}"
    try:
        # checkpresentkey returns 0 if file is present
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, shell=True,
            encoding='utf-8'
        )
        if output == '':
            return f"ok {str(rel_path)}"
    except subprocess.CalledProcessError as err:
        # checkpresentkey returns 1 if file is *absent*
        if err.returncode == 1:
            return f"ERROR missing {remote} {str(rel_path)}"
        # checkpresentkey returns 100 if something went wrong
        return f"ERROR {err}"
    return f"ERROR fellout {str(rel_path)}"

def annex_info_remote(collection_path, remote):
    """Get size diff between here and specified remote
    
    Uses `git annex info .`
    """
    os.chdir(collection_path)
    cmd = 'git annex info . --bytes --json'
    data = json.loads(subprocess.check_output(
        cmd, stderr=subprocess.STDOUT, shell=True,
        encoding='utf-8'
    ))
    here = None
    for r in data['repositories containing these files']:
        if r['here']:
            r['size'] = int(r['size'])
            here = r; continue
    there = None
    for r in data['repositories containing these files']:
        if remote in r['description']:
            r['size'] = int(r['size'])
            there = r; continue
    if there:
        return here['size'], there['size'], here['size'] - there['size']
    if here:
        return here['size'],None,None
    return None,None,None


@ddrremote.command()
@click.option('-l','--logdir', default=None, help='Write output to log file in directory.')
@click.option('-j','--jobs', default='', help='Run N transfer jobs in parallel.')
@click.option('-b','--backoff', default=0.0, help='Wait N seconds between files.')
@click.option('-w','--wait', default=0, help='Wait N seconds after checking a collection.')
@click.argument('remote')
@click.argument('collection')
def copy(logdir, jobs, backoff, wait, remote, collection):
    """git annex copy collection files to the remote and log
    
    Runs `git annex copy -c annex.sshcaching=true . --to=REMOTE`
    and adds info to the output.
    
    Remember to set B2_ACCOUNT_ID and B2_APP_KEY before attempting to copy
    to Backblaze remotes.
    
    See `git annex help copy` for more information about --jobs.
    """
    logfile = Path(logdir) / f"{cid}.log" if logdir else None
    try:
        repo = dvcs.repository(collection)
    except dvcs.git.exc.InvalidGitRepositoryError:
        log(logfile, f"{dtfmt()} ddrremote copy {remote} {collection} DONE ERROR Does not appear to be a Git repository")
        sys.exit(1)
    if remote not in [r['name'] for r in dvcs.remotes(repo)]:
        log(logfile, f"{dtfmt()} ddrremote copy {remote} {collection} DONE ERROR Collection has no remote '{remote}'")
        sys.exit(1)
    if jobs and not (jobs.isnumeric() or jobs == 'cpus'):
        click.echo('--jobs must be an int or "cpus"')
        sys.exit(1)
    collection_path = Path(collection).absolute()
    cid = collection_path.name
    prefix = f"{dtfmt()} ddrremote"
    # ok go
    starttime = datetime.now()
    os.chdir(collection_path)
    log(logfile, f"{dtfmt()} ddrremote copy {remote} {collection_path} START")
    files = len(annex_find(collection_path))
    ok = 0; copied = 0; errors = 0
    if backoff:
        for relpath in annex_find(collection):
            ok,copied,errors = _analyze_annex_copy_output(
                _annex_copy_file(relpath, remote),
                remote, ok, copied, errors, prefix, logfile
            )
            if backoff:
                sleep(float(backoff))
    else:
        ok,copied,errors = _analyze_annex_copy_output(
            _annex_copy_all(collection, remote, jobs),
            remote, ok, copied, errors, prefix, logfile
        )
    operation = f"{dtfmt()} ddrremote copy {remote} {collection_path}"
    elapsed = str(datetime.now() - starttime)
    status = f"files:{files} ok:{ok} copied:{copied} errs:{errors}"
    log(logfile, f"{operation} DONE {elapsed} {status}")
    if wait:
        sleep(float(wait))

def _annex_copy_all(collection_path, remote, jobs=''):
    """git annex copy . and return output or error
    """
    # TODO yield lines instead of returning one big str
    if jobs:
        jobs = f"--jobs={jobs}"
    try:
        return subprocess.check_output(
            f"git annex copy -c annex.sshcaching=true {jobs} . --to {remote}",
            stderr=subprocess.STDOUT, shell=True, encoding='utf-8'
        )
    except subprocess.CalledProcessError as err:
        return f"ERROR {str(err)}"

def _annex_copy_file(relpath, remote):
    """git annex copy FILE and return output or error
    """
    try:
        return subprocess.check_output(
            f"git annex copy -c annex.sshcaching=true {relpath} --to {remote}",
            stderr=subprocess.STDOUT, shell=True, encoding='utf-8'
        )
    except subprocess.CalledProcessError as err:
        return f"ERROR {str(err)}"

def _analyze_annex_copy_output(output, remote, ok, copied, errors, prefix, logfile):
    """Process git annex copy output, count number of files total and copied
    
    Sample logfiles for regular and Backblaze operations
    ANNEX_COPY_REGULAR_SKIPPED, ANNEX_COPY_REGULAR_COPIED
    ANNEX_COPY_BACKBLAZE_SKIPPED, ANNEX_COPY_BACKBLAZE_COPIED
    """
    for line in output.splitlines():
        line = line.strip()
        if ('error' in line.lower()) \
        or ('non-zero exit status' in line) \
        or ("couldn't upload" in line):
            errors += 1
            log(logfile, f"ERROR {line}")
            log(logfile, traceback.format_exc().strip())
        elif 'b2' in remote:
            # backblaze
            if 'copy' in line:
                if (line[-2:] == 'ok') and (not f"(to {remote}...)" in line):
                    ok += 1
                elif f"(to {remote}...)" in line:
                    copied += 1
            log(logfile, f"{prefix} {line}")
        else:
            # everything else(?)
            if f"(checking {remote}...)" in line:
                log(logfile, f"{prefix} {line}")
                if line[-2:] == 'ok':
                    ok += 1
            else:
                if 'sending incremental file list' in line:
                    copied += 1
                log(logfile, line)
    return ok,copied,errors

# samples of `git annex copy` output

ANNEX_COPY_REGULAR_SKIPPED = """
2024-10-02T08:11:09 ddrremote copy files/ddr-densho-513-3/files/ddr-densho-513-3-mezzanine-a4d6b2b0c1.jpg (checking hq-backup-montblanc...) ok
"""

ANNEX_COPY_REGULAR_COPIED = """
2024-10-02T08:11:09 ddrremote copy files/ddr-densho-513-4/files/ddr-densho-513-4-master-1c7108900b-a.jpg (checking hq-backup-montblanc...) (to hq-backup-montblanc...)
sending incremental file list
689/
689/888/
689/888/SHA256E-s181216--c196542d3f64713c70662bf5a8a7e30a78d02b7273b0d0a1f2cafb1cccbcb193.jpg/
689/888/SHA256E-s181216--c196542d3f64713c70662bf5a8a7e30a78d02b7273b0d0a1f2cafb1cccbcb193.jpg/SHA256E-s181216--c196542d3f64713c70662bf5a8a7e30a78d02b7273b0d0a1f2cafb1cccbcb1
93.jpg
         32,768  18%    0.00kB/s    0:00:00
        181,216 100%  141.57MB/s    0:00:00 (xfr#1, to-chk=0/5)
ok
"""

ANNEX_COPY_BACKBLAZE_SKIPPED = """
copy files/ddr-csujad-31-4/files/ddr-csujad-31-4-transcript-c057f5a2e1.pdf ok
"""

ANNEX_COPY_BACKBLAZE_COPIED = """
copy files/ddr-csujad-31-6/files/ddr-csujad-31-6-mezzanine-83a48fe38e-a.jpg (to b2...)
ok
"""
ANNEX_COPY_BACKBLAZE_ERROR = """
Command 'git annex copy -c annex.sshcaching=true files/ddr-ajah-1-10/files/ddr-ajah-1-10-24/files/ddr-ajah-1-10-24-master-404314826e.mpg --to b2' returned non-zero exit status 1.
"""

def _test_analyze_annex_copy_output(logfile):
    files=0; ok=0; copied=0; errors=0
    prefix='000 ddrremote'
    logfile = Path(logfile)
    click.echo('(ok,copied,errors)')
    click.echo('ANNEX_COPY_REGULAR_SKIPPED')
    click.echo(_analyze_annex_copy_output(ANNEX_COPY_REGULAR_SKIPPED.strip(), 'hq-backup-montblanc', ok, copied, errors, prefix, logfile))
    click.echo('ANNEX_COPY_REGULAR_COPIED')
    click.echo(_analyze_annex_copy_output(ANNEX_COPY_REGULAR_COPIED.strip(), 'hq-backup-montblanc', ok, copied, errors, prefix, logfile))
    click.echo('ANNEX_COPY_BACKBLAZE_SKIPPED')
    click.echo(_analyze_annex_copy_output(ANNEX_COPY_BACKBLAZE_SKIPPED.strip(), 'b2', ok, copied, errors, prefix, logfile))
    click.echo('ANNEX_COPY_BACKBLAZE_COPIED')
    click.echo(_analyze_annex_copy_output(ANNEX_COPY_BACKBLAZE_COPIED.strip(), 'b2', ok, copied, errors, prefix, logfile))
    click.echo('ANNEX_COPY_BACKBLAZE_ERROR')
    click.echo(_analyze_annex_copy_output(ANNEX_COPY_BACKBLAZE_ERROR.strip(), 'b2', ok, copied, errors, prefix, logfile))


if __name__ == '__main__':
    ddrremote()
