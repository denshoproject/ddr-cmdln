import os

import click

from DDR import config
from DDR.identifier import Identifier
from DDR import models
from DDR import util

ALGORITHMS = ['md5', 'sha1', 'sha256']


@click.command()
@click.argument('repo')
@click.option('--verbose', '-v', is_flag=True, help='Lots of output.')
def ddrcheckbinaries(repo, verbose=False):
    """ddrcheckbinaries - Find binaries that don't match metadata hashes.
    
    \b
    Example:
        $ ddrcheckbinaries /var/www/media/base/ddr-testing-141
    """
    filepaths = util.find_meta_files(
        repo, recursive=1, model='file', force_read=True
    )
    hits = check_files(filepaths, verbose)


def check_file(json_path, verbose=False):
    fi = Identifier(json_path)
    f = models.File.from_identifier(fi)

    if not os.path.exists(f.path_abs):
        result = ['missing', f.path_abs]
        print result
        return result
    
    mismatches = []
    md5 = util.file_hash(f.path_abs, 'md5')
    if not (md5 == f.md5):
        mismatches.append['md5']
    sha1 = util.file_hash(f.path_abs, 'sha1')
    if not (sha1 == f.sha1):
        mismatches.append['sha1']
    sha256 = util.file_hash(f.path_abs, 'sha256')
    if not (sha256 == f.sha256):
        mismatches.append['sha256']
    # SHA256 hash from the git-annex filename
    annex_sha256 = os.path.basename(
        os.path.realpath(f.path_abs)
    ).split('--')[1]
    if not (sha256 == annex_sha256):
        mismatches.append['annex_sha256']
    
    if mismatches:
        mismatches.append(json_path)
        print mismatches
    
    return mismatches
    
def check_files(filepaths, verbose=False):
    hits = []
    for json_path in filepaths:
        mismatches = check_file(json_path)
        if mismatches:
            hits.append(mismatches)
    return hits
