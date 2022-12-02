from datetime import datetime
import hashlib
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union

from DDR import config
from DDR import fileio
from DDR import identifier


# TODO type hints
def find_meta_files(basedir, recursive=False, model=None, files_first=False, force_read=False):
    """Lists absolute paths to .json files in basedir; saves copy if requested.
    
    Skips/excludes .git directories.
    TODO depth (go down N levels from basedir)
    
    NOTE: Looked at replacing this with pathlib rglob[1] but this
    function is consistently faster.
    [1] list(pathlib.Path(path).rglob('*.json'))
    
    @param basedir: Absolute path
    @param recursive: Whether or not to recurse into subdirectories.
    @param model: list Restrict to the named model ('collection','entity','file').
    @param files_first: If True, list files,entities,collections; otherwise sort.
    @param force_read: If True, always searches for files instead of using cache.
    @returns: list of paths
    """
    CACHE_FILENAME = '.metadata_files'
    CACHE_PATH = os.path.join(basedir, CACHE_FILENAME)
    EXCLUDES = ['.git', '*~']
    paths = []
    if os.path.exists(CACHE_PATH) and not force_read:
        paths = [
            line.strip()
            for line in fileio.read_text(CACHE_PATH).splitlines()
            if '#' not in line
        ]
    else:
        if recursive:
            paths = _search_recursive(basedir, model, EXCLUDES)
        else:
            paths = _search_directory(basedir, EXCLUDES)
    # files_first is useful for docstore.index
    if files_first:
        return [path for path in paths if path_matches_model(path, 'file')] \
            + [path for path in paths if path_matches_model(path, 'entity')] \
            + [path for path in paths if path_matches_model(path, 'collection')]
    return paths

def _search_recursive(basedir: str, model: str, excludes: List[str]) -> List[str]:
    """Recursively search directory.
    """
    paths = []
    for root, dirs, files in os.walk(basedir):
        # don't go down into .git directory
        if '.git' in dirs:
            dirs.remove('.git')
        for f in files:
            if f.endswith('.json'):
                path = os.path.join(root, f)
                if (not _excluded(path, excludes)) and path_matches_model(path, model):
                    paths.append(path)
    return paths

def _search_directory(basedir: str, excludes: List[str]) -> List[str]:
    """Search only the specified directory.
    """
    paths = []
    for f in os.listdir(basedir):
        if f.endswith('.json'):
            path = os.path.join(basedir, f)
            if (not _excluded(path, excludes)):
                paths.append(path)
    return paths

def _excluded(path: str, excludes: List[str]) -> bool:
    """True if path contains one excluded strings
    """
    for x in excludes:
        if x in path:
            return True
    return False

def path_matches_model(path: str, model: str) -> bool:
    """True if matches specified model or model is blank
    """
    if model:
        if re.search(identifier.META_FILENAME_REGEX[model], path):
            return True
        else:
            return False
    return True

def natural_sort(l: List[str]) -> List[str]:
    """Sort the given list in the way that humans expect.
    src: http://www.codinghorror.com/blog/2007/12/sorting-for-humans-natural-sort-order.html
    """
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    l.sort( key=alphanum_key )
    return l

def natural_order_string(id: str) -> str:
    """Convert a collection/entity ID into form that can be sorted naturally.
    
    @param id: A valid format DDR ID
    """
    alnum = re.findall('\d+', id)
    if not alnum:
        raise Exception('Valid DDR ID required.')
    return alnum.pop()

def file_hash(path, algo='sha1'):
    if algo == 'sha256':
        h = hashlib.sha256()
    elif algo == 'md5':
        h = hashlib.md5()
    else:
        h = hashlib.sha1()
    block_size=1024
    f = open(path, 'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        h.update(data)
    f.close()
    return h.hexdigest()

def normalize_text(text: str) -> str:
    """Strip text, convert line endings, etc.
    
    TODO make this work on lists, dict values
    TODO handle those ^M chars
    
    >>> normalize_text('  this is a test')
    'this is a test'
    >>> normalize_text('this is a test  ')
    'this is a test'
    >>> normalize_text('this\r\nis a test')
    'this\\nis a test'
    >>> normalize_text('this\ris a test')
    'this\\nis a test'
    >>> normalize_text('this\\nis a test')
    'this\\nis a test'
    >>> normalize_text(['this is a test'])
    ['this is a test']
    >>> normalize_text({'this': 'is a test'})
    {'this': 'is a test'}
    """
    def process(t):
        try:
            t = t.strip()
            t = t.replace('\r\n', '\n').replace('\r', '\n').replace('\n', '\\n')
        except AttributeError:
            pass # doesn't work on ints and lists :P
        return t
    if isinstance(text, str):
        return process(text)
    return text

def validate_paths(paths: List[str]) -> List[Tuple[int, str, Exception]]:
    """Tests whether a list of paths can be instantiated without errors

    @param paths: list
    @returns: list of (n, path, Exception)
    """
    bad = []
    for n,path in enumerate(paths):
        try:
            identifier.Identifier(path=path).object()
        except Exception as err:
            bad.append((n, path, err))
    return bad

EMAIL_PATTERN = re.compile(
    r"^[\w\.\+\-]+\@[\w]+\.[a-z]{2,3}$"
)

def validate_email(email: str) -> bool:
    if len(email) > 6:
        if re.match(EMAIL_PATTERN, email) != None:
            return True
    return False


class FileLogger():
    path = None
    
    def __repr__(self):
        return "<%s.%s '%s'>" % (self.__module__, self.__class__.__name__, self.path)

    def __init__(self, log_path=None, identifier=None, base_dir=config.LOG_DIR):
        if identifier:
            self.path = FileLogger.log_path(identifier, base_dir)
        elif log_path:
            if isinstance(log_path, str):
                self.path = Path(log_path)
            elif isinstance(log_path, Path):
                self.path = log_path
            else:
                raise Exception('Bad arg for log_path')
        if not self.path.parent.exists():
            os.makedirs(self.path.parent)

    def entry(self, status, msg, blank=False):
        """Returns log of add_files activity; adds an entry if status,msg given.
        
        @param ok: Boolean. ok or not ok.
        @param msg: Text message.
        @parap blank: bool Print blank line if true
        @returns log: A text file.
        """
        if blank:
            fileio.append_text('', str(self.path))
            return
        dt = datetime.now(config.TZ).strftime('%Y-%m-%d %H:%M:%S,%f')
        entry = f'{dt} {status.upper():8} {msg}'
        fileio.append_text(entry, str(self.path))

    def debug(self, msg): self.entry('debug', msg)
    def info(self, msg): self.entry('info', msg)
    def warning(self, msg): self.entry('warning', msg)
    def error(self, msg): self.entry('error', msg)
    def critical(self, msg): self.entry('critical', msg)

    def crash(self, msg, exception=Exception):
        """Write to addfile log and raise an exception."""
        self.error(msg)
        raise exception(msg)

    def blank(self): self.entry('blank', '', True)

    def log(self):
        log = ''
        if self.path.exists():
            log = fileio.read_text(self.path)
        return log

    @staticmethod
    def log_path(identifier, base_dir=config.LOG_DIR):
        """Generates path to collection addfiles.log.
        
        Previously each entity had its own addfile.log.
        Going forward each collection will have a single log file.
            /STORE/log/REPO-ORG-CID-addfile.log
        
        @param identifier: Identifier
        @param base_dir: [optional] str
        @returns: absolute path to logfile
        """
        return Path(config.LOG_DIR) / 'addfile' / identifier.collection_id() / f'{identifier.id}.log'
