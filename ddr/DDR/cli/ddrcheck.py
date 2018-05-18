"""ddrcheck - Validate DDR documents in a collection

Tests whether items in collection can be instantiated without errors.
Use ddr-transform to test if objects can be saved (remember to clean up after).
"""

import sys

import click

from DDR import util

@click.command()
@click.argument('collection_path')
def ddrcheck(collection_path):
    print('Gathering files in %s' % collection_path)
    paths = util.find_meta_files(
        collection_path, recursive=1,
        model=None, files_first=False, force_read=False, testing=0
    )
    print('Checking files...')
    for item in util.validate_paths(paths):
        n,path,err = item
        print('%s/%s ERROR %s - %s' % (n, len(paths), path, err))
    print('Checked %s files' % len(paths))
