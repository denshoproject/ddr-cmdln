DESCRIPTION = """ddrremote - Tools for working with DDR git-annex special remotes"""

HELP = """Tools for working with DDR git-annex special remotes.

check - Verifies that all the annex files in the collection are present in
        the specified special remote. Optionally writes to --log.
copy  - Runs git annex copy to the specified special remote, and
        optionally writes to --log.

- Works best if the ddr user has passwordless SSH keys for each remote.
"""


from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

import click

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
@click.option('-l','--logfile', default=None, help='Write output to log file.')
@click.option('-v','--verbose', is_flag=True, default=False, help='Show all files not just missing ones.')
@click.argument('remote')
@click.argument('collection')
def check(logfile, verbose, remote, collection):
    """Check that annex files in the collection are present in the special remote
    
    Under the hood, uses the git annex checkpresentkey plumbing command,
    which reaches out to the special remote.
    
    Write output to files for use by compare command:
    ddrremote check REMOTE COLLECTIONPATH | tee /PATH/REMOTE/COLLECTION
    """
    if logfile:
        logfile = Path(logfile)
    prefix = f"{dtfmt()} ddrremote check"
    starttime = datetime.now()
    log(logfile, f"{prefix} START")
    startdir = os.getcwd()
    collection_path = Path(collection)
    annex_files = annex_find(collection_path)
    for rel_path in annex_files:
        output = checkpresentkey(collection_path, rel_path, remote)
        if output.split()[0] == 'ok':
            if (verbose):
                log(logfile, f"{prefix} {output}")
        else:
            log(logfile, f"{prefix} {output}")
    endtime = datetime.now(); elapsed = endtime - starttime
    log(logfile, f"{prefix} DONE {len(annex_files)} files checked in {str(elapsed)}")

def annex_find(collection_path):
    """Gets list of relative file paths using git annex find
    """
    os.chdir(collection_path)
    return [
        Path(relpath)
        for relpath in subprocess.check_output(
                'git annex find', stderr=subprocess.STDOUT, shell=True,
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
            return f"ok {remote} {str(rel_path)}"
    except subprocess.CalledProcessError as err:
        # checkpresentkey returns 1 if file is *absent*
        if err.returncode == 1:
            return f"ERROR missing {remote} {str(rel_path)}"
        # checkpresentkey returns 100 if something went wrong
        return f"ERROR {err}"
    return f"ERROR fellout {str(rel_path)}"


@ddrremote.command()
@click.option('-l','--logfile', default=None, help='Write output to log file.')
@click.argument('remote')
@click.argument('collection')
def copy(logfile, remote, collection):
    """git annex copy collection files to the remote and log
    """
    if logfile:
        logfile = Path(logfile)
    prefix = f"{dtfmt()} ddrremote"
    starttime = datetime.now()
    log(logfile, f"{prefix} copy START")
    for line in _annex_copy(collection, remote).splitlines():
        if line.find('copy') == 0:
            log(logfile, f"{prefix} {line}")
        else:
            log(logfile, line)
    endtime = datetime.now(); elapsed = endtime - starttime
    log(logfile, f"{prefix} copy DONE in {str(elapsed)}")

def _annex_copy(collection_path, remote):
    # TODO yield lines instead of returning one big str
    os.chdir(collection_path)
    cmd = f"git annex copy . --to {remote}"
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True,encoding='utf-8')
    except subprocess.CalledProcessError as err:
        return f"ERROR {str(err)}"


if __name__ == '__main__':
    ddrremote()
