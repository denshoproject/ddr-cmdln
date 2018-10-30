import codecs
from datetime import datetime
import logging
import os
import shutil
import sys

import chardet
import click
import git
import simplejson as json

from DDR import fileio
from DDR import models
from DDR import util


@click.command()
@click.argument('repo_url')
@click.option('--destdir','-d', default='/tmp/ddrcheckencoding',
              help='(optional) Temporary destination directory.')
@click.option('--verbose', '-v', is_flag=True,
              help='Lots of output. Important lines prefixed with "%%%%".')
@click.option('--csv', '-c', is_flag=True,
              help='Print output in CSV-friendly form.')
@click.option('--headers', '-h', is_flag=True,
              help='Print CSV headers (requires -c).')
@click.option('--json', '-j', is_flag=True,
              help='Print output in JSON-friendly form.')
def ddrcheckencoding(repo_url, destdir, verbose, csv, headers, json):
    """ddrcheckencoding - Checks collection repository for non-UTF-8 chars.
    
    \b
    Example:
        $ ddr-checkencoding git@mits.densho.org:ddr-test-123.git /var/www/media/base/temp
    
    Clones collection repo to specified location, loads every JSON file
    in the collection with strict UTF-8 encoding, then removes the
    directory.  This should surface any UTF-8 encoding problems.
    """
    check_encoding(repo_url, destdir, verbose, csv, headers, json)


def check_encoding(repo_url, destdir, verbose=False, csv=False, headers=False, json=False):
    collection_id = extract_collection_id(repo_url)
    repo_path = os.path.join(destdir, collection_id)
    out(verbose, collection_id)
    out(verbose, repo_path)
    
    # if verbose, add marker to important lines
    if verbose:
        prefix = '%% '
    else:
        prefix = ''
    
    if csv and headers:
        print('%scollection id, files, defects, elapsed' % prefix)
        
    start = datetime.now()
    out(verbose, start)
    
    out(verbose, 'clone %s %s' % (repo_url, repo_path))
    repo = clone(repo_url, repo_path)
    out(verbose, repo)
    
    out(verbose, 'analyzing')
    paths = util.find_meta_files(repo_path, recursive=True)
    defects = analyze_files(paths, verbose)
    
    out(verbose, 'cleaning up')
    clean(repo_path)
    
    end = datetime.now()
    elapsed = end - start
    out(verbose, end)
    
    if csv:
        print '%s%s' % (
            prefix,
            ','.join([
                str(collection_id),
                str(len(paths)),
                str(len(defects)),
                str(elapsed)
            ])
        )
    elif json:
        data = {
            'collection id': collection_id,
            'files': len(paths),
            'defects': len(defects),
            'elapsed': str(elapsed),
            }
        print '%s%s' % (
            prefix,
            json.dumps(data)
        )
    else:
        print('%s%s, %s files, %s bad, %s elapsed' % (
            prefix,
            collection_id,
            len(paths),
            len(defects),
            elapsed
        ))

def out(verbose, text):
    if verbose:
        print(text)

def extract_collection_id(url):
    # git@mits.densho.org:REPO.git
    return os.path.splitext(url.split(':')[1])[0]

def clone(url, destpath):
    """Simple clone of repo (not ddr-clone).
    
    @param url: 
    @param destpath: 
    """
    return git.Repo.clone_from(url, destpath)

def clean(repo_path):
    """rm repo from filesystem
    
    @param repo_path: 
    """
    shutil.rmtree(repo_path)

def analyze_files(paths, verbose=False):
    """Opens files with strict encoding; lists paths that throw exceptions
    
    @param paths: list
    @param verbose: boolean
    @returns: list of defective paths
    """
    defects = []
    for path in paths:
        bad = 0
        try:
            text = fileio.read_text(path, utf8_strict=True)
        except:
            bad += 1
            defects.append(path)
            text = fileio.read_text(path)
            guess = chardet.detect(text)
            if verbose:
                print('\n| %s %s' % (path, guess))
        if (not bad) and verbose:
            sys.stdout.write('.')
    if len(paths) and verbose:
        print('')
    return defects
