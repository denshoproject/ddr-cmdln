from datetime import datetime
from exceptions import Exception
import logging
logger = logging.getLogger(__name__)
import os
import shutil
import sys
import traceback

from DDR import changelog
from DDR import config
from DDR import dvcs
from DDR import fileio
from DDR import identifier
from DDR import imaging
from DDR import util

FILE_BINARY_FIELDS = [
    'sha1', 'sha256', 'md5', 'size',
]

# Decision table for various ways to process file data for batch operations
# +---binary present
# |+--external
# ||+-attrs present
# ||| KEY                    LABEL              ACTIONS
# 000 nobin,local,noattrs    ERROR
# 001 nobin,local,attrs      ERROR
# 010 nobin,external,noattrs ERROR
# 011 nobin,external,attrs   new-external-nobin Binary external; use CSV attrs; dont process local file
# 100 bin,local,noattrs      new-internal       Binary in repo; calc bin attrs (ignore CSV attrs)
# 101 bin,local,attrs        new-internal       Binary in repo; calc bin attrs (ignore CSV attrs)
# 110 bin,external,noattrs   new-external-bin   Binary external; calc bin attrs; rename bin w fileID
# 111 bin,external,attrs     new-external-bin   Binary external; calc bin attrs; rename bin w fileID

FILE_IMPORT_ACTIONS = {
'nobin,external,attrs':{'label':'new-external-nobin', 'attrs':'fromcsv',   'ingest':0, 'rename':0, 'access':0},
'bin,local,noattrs':   {'label':'new-internal',       'attrs':'calculate', 'ingest':1, 'rename':0, 'access':1},
'bin,local,attrs':     {'label':'new-internal',       'attrs':'calculate', 'ingest':1, 'rename':0, 'access':1},
'bin,external,noattrs':{'label':'new-external-bin',   'attrs':'calculate', 'ingest':0, 'rename':1, 'access':0},
'bin,external,attrs':  {'label':'new-external-bin',   'attrs':'calculate', 'ingest':0, 'rename':1, 'access':0},
}

def import_actions(rowd, file_present):
    """Decide actions when importing file from CSV
    
    @param rowd: dict Row from CSV; see DDR.batch and DDR.csvfile
    @param file_present: bool Whether or not binary is present in filesystem
    @returns: dict
    """
    factors = []
    # binary present
    if file_present: factors.append('bin')
    else:            factors.append('nobin')
    # external
    external = rowd.get('external') and rowd['external']
    if external: factors.append('external')
    else:        factors.append('local')
    # file attrs
    bin_attrs = [
        fieldname
        for fieldname in FILE_BINARY_FIELDS
        if rowd.get(fieldname) and rowd[fieldname]
    ]
    if bin_attrs: factors.append('attrs')
    else:         factors.append('noattrs')
    # DECIDE
    key = ','.join(factors)
    if FILE_IMPORT_ACTIONS.get(key):
        return FILE_IMPORT_ACTIONS[key]
    raise Exception('No file ingest action for "%s".' % key)


class FileExistsException(Exception):
    pass

class FileMissingException(Exception):
    pass


class AddFileLogger():
    logpath = None
    
    def __repr__(self):
        return "<%s.%s '%s'>" % (self.__module__, self.__class__.__name__, self.logpath)
    
    def entry(self, ok, msg ):
        """Returns log of add_files activity; adds an entry if status,msg given.
        
        @param ok: Boolean. ok or not ok.
        @param msg: Text message.
        @returns log: A text file.
        """
        entry = '[{}] {} - {}'.format(datetime.now(config.TZ).isoformat('T'), ok, msg)
        fileio.append_text(entry, self.logpath)
    
    def ok(self, msg): self.entry('ok', msg)
    def not_ok(self, msg): self.entry('not ok', msg)
    
    def log(self):
        log = ''
        if os.path.exists(self.logpath):
            log = fileio.read_text(self.logpath)
        return log

    def crash(self, msg, exception=Exception):
        """Write to addfile log and raise an exception."""
        self.not_ok(msg)
        raise exception(msg)


def add_file(rowd, entity, git_name, git_mail, agent,
             tmp_dir=config.MEDIA_BASE, log_path=None, show_staged=True):
    """Add local or external file, with or without access.
    
    @param rowd: dict Row from a CSV import file in dict form.
    @param entity: Entity object
    @param git_name: Username of git committer.
    @param git_mail: Email of git committer.
    @param agent: str (optional) Name of software making the change.
    @param log_path: str (optional) Absolute path to addfile log
    @param show_staged: boolean Log list of staged files
    @return File,repo,log
    """
    f = None
    repo = None
    if log_path:
        log = addfile_logger(log_path=log_path)
    else:
        log = addfile_logger(identifier=entity.identifier)
    
    log.ok('------------------------------------------------------------------------')
    log.ok('DDR.models.Entity.add_local_file: START')
    log.ok('rowd: %s' % rowd)
    log.ok('parent: %s' % entity.id)
    
    src_path = rowd.pop('basename_orig')
    
    actions = import_actions(rowd, os.path.exists(src_path))
    log.ok('actions %s' % actions)
    
    log.ok('Actions: attrs %s' % actions['attrs'])
    if actions['attrs'] == 'calculate':
        src_size,md5,sha1,sha256,xmp = file_info(src_path, log)
    elif actions['attrs'] == 'fromcsv':
        src_size = rowd['size']
        md5,sha1,sha256 = rowd['md5'], rowd['sha1'], rowd['sha256']
        xmp = rowd.get('xmp')
    
    file_ = file_object(
        file_identifier(entity, rowd, sha1, log),
        entity, rowd,
        src_path, src_size,
        md5, sha1, sha256, xmp,
        log
    )
    
    log.ok('Actions: rename %s' % actions['rename'])
    if actions['rename']:
        copy_in_place(src_path, file_, log)
    
    annex_files = []
    
    log.ok('Actions: ingest %s' % actions['ingest'])
    if actions['ingest']:
        copy_to_file_path(file_, src_path, log)
        annex_files.append(file_.path_abs)
    
    log.ok('Actions: access %s' % actions['access'])
    if actions['access']:
        access_path = make_access_file(src_path, file_.access_abs, log)
        log.ok('Attaching access file')
        if access_path and os.path.exists(access_path):
            file_.set_access(access_path, entity)
            annex_files.append(file_.access_abs)
            log.ok('| file_.access_rel: %s' % file_.access_rel)
            log.ok('| file_.access_abs: %s' % file_.access_abs)
        else:
            log.not_ok('no access file')
    
    log.ok('Writing file and entity rowd')
    exit,status,git_files = file_.save(
        git_name, git_mail, agent, parent=entity, commit=False
    )

    log.ok('Staging files')
    repo = stage_files(
        entity,
        [path.replace('%s/' % file_.collection_path, '') for path in git_files],
        [path.replace('%s/' % file_.collection_path, '') for path in annex_files],
        log, show_staged=show_staged
    )
    # IMPORTANT: Files are only staged! Be sure to commit!
    # IMPORTANT: changelog is not staged!
    return file_,repo,log

def _log_path(identifier, base_dir=config.LOG_DIR):
    """Generates path to collection addfiles.log.
    
    Previously each entity had its own addfile.log.
    Going forward each collection will have a single log file.
        /STORE/log/REPO-ORG-CID-addfile.log
    
    @param identifier: Identifier
    @param base_dir: [optional] str
    @returns: absolute path to logfile
    """
    return os.path.join(
        base_dir, 'addfile',
        identifier.collection_id(),
        '%s.log' % identifier.id
    )

def addfile_logger(identifier=None, log_path=None, base_dir=config.LOG_DIR):
    """Gets an AddFileLogger object for an Identifier OR specified path.
    
    @param identifier: Identifier
    @param log_path: str
    @param base_dir: [optional] str
    @returns: AddFileLogger
    """
    assert identifier or log_path
    log = AddFileLogger()
    if identifier:
        log.logpath = _log_path(identifier, base_dir)
    elif log_path:
        log.logpath = log_path
    logdir = os.path.dirname(log.logpath)
    if not os.path.exists(logdir):
        os.makedirs(logdir)
    return log

def check_dir(label, path, log, mkdir=False, perm=os.W_OK):
    log.ok('| check dir %s (%s)' % (path, label))
    if mkdir and not os.path.exists(path):
        os.makedirs(path)
    if not os.path.exists(path):
        log.crash(
            '%s does not exist: %s' % (label, path),
            FileMissingException
        )
        return False
    if not os.access(path, perm):
        log.crash('%s not has %s permission: %s' % (label, perm, path))
        return False
    return True

def checksums(src_path, log):
    md5    = util.file_hash(src_path, 'md5');    log.ok('| md5: %s' % md5)
    sha1   = util.file_hash(src_path, 'sha1');   log.ok('| sha1: %s' % sha1)
    sha256 = util.file_hash(src_path, 'sha256'); log.ok('| sha256: %s' % sha256)
    if not (sha1 and md5 and sha256):
        log.crash('Could not calculate checksums')
    return md5,sha1,sha256

def destination_path(src_path, dest_dir, fidentifier):
    src_basename = os.path.basename(src_path)
    src_ext = os.path.splitext(src_basename)[1]
    dest_basename = '{}{}'.format(fidentifier.id, src_ext)
    return os.path.join(dest_dir, dest_basename)

def temporary_path(src_path, base_dir, fidentifier):
    src_basename = os.path.basename(src_path)
    return os.path.join(
        base_dir,
        'tmp', 'file-add',
        fidentifier.collection_id(),
        fidentifier.parent_id(),
        src_basename
    )

def temporary_path_renamed(tmp_path, dest_path):
    return os.path.join(
        os.path.dirname(tmp_path),
        os.path.basename(dest_path)
    )

def access_path(file_class, tmp_path_renamed):
    access_filename = file_class.access_filename(tmp_path_renamed)
    return os.path.join(
        os.path.dirname(tmp_path_renamed),
        os.path.basename(access_filename)
    )

def copy_to_workdir(src_path, tmp_path, tmp_path_renamed, log):
    if not os.path.exists(os.path.dirname(tmp_path)):
        os.makedirs(os.path.dirname(tmp_path))
    log.ok('| cp %s %s' % (src_path, tmp_path))
    shutil.copy(src_path, tmp_path)
    os.chmod(tmp_path, 0644)
    if os.path.exists(tmp_path):
        log.ok('| done')
    else:
        log.crash('Copy failed!')
    log.ok('| Renaming %s -> %s' % (
        os.path.basename(tmp_path),
        os.path.basename(tmp_path_renamed)
    ))
    shutil.move(tmp_path, tmp_path_renamed)
    if not os.path.exists(tmp_path_renamed) and not os.path.exists(tmp_path):
        log.crash('File rename failed: %s -> %s' % (tmp_path, tmp_path_renamed))

def copy_to_file_path(file_, src_path, log):
    log.ok('Copying')
    log.ok('| cp %s %s' % (src_path, file_.path_abs))
    if not os.path.exists(file_.entity_files_path):
        os.makedirs(file_.entity_files_path)
    shutil.copy(src_path, file_.path_abs)
    os.chmod(file_.path_abs, 0644)
    if os.path.exists(file_.path_abs):
        log.ok('| done')
    else:
        log.crash('Copy failed!')
        raise Exception(
            'Failed to copy file(s) to destination repo'
        )

def make_access_file(src_path, access_dest_path, log):
    log.ok('Making access file')
    if os.path.exists(access_dest_path):
        log.not_ok('Access tmpfile already exists: %s' % access_dest_path)
    log.ok('| %s' % access_dest_path)
    try:
        data = imaging.thumbnail(
            src_path,
            access_dest_path,
            geometry=config.ACCESS_FILE_GEOMETRY,
            options=config.ACCESS_FILE_OPTIONS,
        )
        # identify
        log.ok('| identify: %s' % data['analysis']['std_out'])
        if data['analysis'].get('std_err'):
            log.not_ok('| identify: %s' % data['analysis']['std_err'])
        # convert
        log.ok('| %s' % data['convert'])
        log.ok('| convert: status:%s exists:%s islink:%s size:%s' % (
            data['status_code'],
            data['exists'],
            data['islink'],
            data['size'],
        ))
        if data.get('std_err'):
            log.not_ok('| convert: %s' % data['std_err'])
        if not data['exists']:
            log.not_ok('Access file was not created!')
        if not data['size']:
            log.not_ok('Dest file created but zero length!')
        if data['islink']:
            log.not_ok('DEST FILE IS A SYMLINK!')
        #
        tmp_access_path = data['dest']
        log.ok('| done')
    except:
        # write traceback to log and continue on
        log.not_ok(traceback.format_exc().strip())
        tmp_access_path = None
    return tmp_access_path

def write_object_metadata(obj, tmp_dir, log):
    tmp_json = os.path.join(tmp_dir, os.path.basename(obj.json_path))
    log.ok('| %s' % tmp_json)
    fileio.write_text(obj.dump_json(), tmp_json)
    if not os.path.exists(tmp_json):
        log.crash('Could not write file metadata %s' % tmp_json)
    return tmp_json

def move_files(files, log):
    failures = []
    for tmp,dest in files:
        log.ok('| mv %s %s' % (tmp,dest))
        shutil.move(tmp,dest)
        if not os.path.exists(dest):
            log.not_ok('FAIL')
            failures.append(tmp)
            break
    return failures

def reverse_files_list(files):
    return [(dest,tmp) for tmp,dest in files]

def move_new_files_back(files, failures, log):
    # one of files failed to copy, so move all back to tmp
    # these are new files
    log.not_ok('%s failures: %s' % (len(failures), failures))
    log.not_ok('Moving new files back to tmp_dir')
    reverse = reverse_files_list(files)
    fails = []
    try:
        fails = move_files(files, log)
    except:
        msg = "Unexpected error:", sys.exc_info()[0]
        log.not_ok(msg)
        raise
    finally:
        log.not_ok('Failed to place one or more files to destination repo')
        for fail in fails:
            log.not_ok('| %s' % fail)
        log.crash('Bailing out. We are done here.')

def move_existing_files_back(files, log):
    # these are files that already exist in repo
    log.ok('| mv %s %s' % (tmp_entity_json, entity.json_path))
    shutil.move(tmp_entity_json, entity.json_path)
    if not os.path.exists(entity.json_path):
        log.crash('Failed to place entity.json in destination repo')

def rename_in_place(source, new_name, log):
    """Rename original file in-place
    """
    src_dir = os.path.dirname(source)
    dest = os.path.join(src_dir, os.path.basename(new_name))
    log.ok('| mv %s %s' % (source, dest))
    shutil.move(source, dest)

def copy_in_place(src_path, file_, log):
    """Make copy of original file in same dir
    """
    base,ext = os.path.splitext(src_path)
    dest = os.path.join(
        os.path.dirname(base),
        os.path.basename(file_.identifier.path_abs()) + ext
    )
    log.ok('| cp %s %s' % (src_path, dest))
    shutil.copy(src_path, dest)

def predict_staged(already, planned):
    """Predict which files will be staged, accounting for modifications
    
    When running a batch import there will already be staged files when this function is called.
    Some files to be staged will be modifications (e.g. entity.json).
    Predicts the list of files that will be staged if this round of add_file succeeds.
    how many files SHOULD be staged after we run this?
    
    @param already: list Files already staged.
    @param planned: list Files to be added/modified in this operation.
    @returns: list
    """
    additions = [path for path in planned if path not in already]
    total = sorted(list(set(already + additions)))
    return total

def stage_files(entity, git_files, annex_files, log, show_staged=True):
    """Stage files; check before and after to ensure all files get staged

    @param entity: DDR.models.entities.Entity
    @param git_files: list
    @param annex_files: list
    @param log: AddFileLogger
    @param show_staged: bool
    @returns: repo
    """
    repo = dvcs.repository(entity.collection_path)
    log.ok('| repo %s' % repo)
    
    log.ok('| BEFORE staging')
    staged_before,modified_before,untracked_before = repo_status(repo, log)
    
    stage_these = sorted(list(set(git_files + annex_files)))
    log.ok('| staging %s files:' % len(stage_these))
    for path in stage_these:
        log.ok('|   %s' % path)
    
    stage_ok = False
    staged = []
    try:
        log.ok('| git stage')
        dvcs.stage(repo, git_files)
        log.ok('| annex stage')
        dvcs.annex_stage(repo, annex_files)
        log.ok('| ok')
    except:
        # FAILED! print traceback to addfile log
        log.not_ok(traceback.format_exc().strip())
        
    log.ok('| AFTER staging')
    staged_after,modified_after,untracked_after = repo_status(repo, log)
    
    # Crash if not staged
    still_modified = [path for path in stage_these if path in modified_after]
    if still_modified:
        log.not_ok('These files are still modified')
        for path in still_modified:
            log.not_ok('| %s' % path)
        log.crash('Add file aborted, see log file for details: %s' % log.logpath)
    
    return repo

def repo_status(repo, log):
    """Logs staged, modified, and untracked files and returns same
    
    @param repo
    @param log
    @returns: staged,modified,untracked
    """
    log.ok('| %s' % repo)
    staged = dvcs.list_staged(repo)
    modified = dvcs.list_modified(repo)
    untracked = dvcs.list_untracked(repo)
    log.ok('|   %s staged, %s modified, %s untracked' % (
        len(staged), len(modified), len(untracked),
    ))
    for path in staged:
        log.ok('|   staged: %s' % path)
    for path in modified:
        log.ok('|   modified: %s' % path)
    for path in untracked:
        log.ok('|   untracked: %s' % path)
    return staged, modified, untracked

def file_info(src_path, log):
    log.ok('Examining source file')
    check_dir('| src_path', src_path, log, mkdir=False, perm=os.R_OK)
    size = os.path.getsize(src_path)
    log.ok('| file size %s' % size)
    # TODO check free space on dest
    log.ok('| hashing')
    md5,sha1,sha256 = checksums(src_path, log)
    log.ok('| md5 %s' % md5)
    log.ok('| sha1 %s' % sha1)
    log.ok('| sha256 %s' % sha256)
    log.ok('| extracting XMP data')
    xmp = imaging.extract_xmp(src_path)
    return size,md5,sha1,sha256,xmp

def file_identifier(entity, data, sha1, log):
    log.ok('Identifier')
    # note: we can't make this until we have the sha1
    idparts = entity.identifier.idparts
    idparts['model'] = 'file'
    idparts['role'] = data['role']
    idparts['sha1'] = sha1[:10]
    log.ok('| idparts %s' % idparts)
    fidentifier = identifier.Identifier(idparts, entity.identifier.basepath)
    log.ok('| identifier %s' % fidentifier)
    return fidentifier

def file_object(fidentifier, entity, data, src_path, src_size, md5, sha1, sha256, xmp, log):
    log.ok('File object')
    file_ = fidentifier.object_class().create(fidentifier, parent=entity)
    file_.basename_orig = os.path.basename(src_path)
    # add extension to path_abs
    basename_ext = os.path.splitext(file_.basename_orig)[1]
    path_abs_ext = os.path.splitext(file_.path_abs)[1]
    if basename_ext and not path_abs_ext:
        file_.path_abs = file_.path_abs + basename_ext
        log.ok('| basename_ext %s' % basename_ext)
    file_.role = data['role']
    log.ok('| file_ %s' % file_)
    log.ok('| file_.basename_orig: %s' % file_.basename_orig)
    log.ok('| file_.path_abs: %s' % file_.path_abs)
    log.ok('| file_.mimetype: %s' % file_.mimetype)
    # remove 'id' from forms/CSV data so it doesn't overwrite file_.id later
    if data.get('id'):
        data.pop('id')
    # form data
    for field in data:
        setattr(file_, field, data[field])
    # args
    file_.size = src_size
    file_.sha1 = sha1
    file_.md5 = md5
    file_.sha256 = sha256
    file_.xmp = xmp
    log.ok('| file_.size:   %s' % file_.size)
    log.ok('| file_.sha1:   %s' % file_.sha1)
    log.ok('| file_.md5:    %s' % file_.md5)
    log.ok('| file_.sha256: %s' % file_.sha256)
    # Batch import CSV files often have the ID of the file-role or entity
    # instead of the file. Add file ID again to make sure the field has
    # the correct value.
    file_.id = fidentifier.id
    return file_

def add_access( entity, ddrfile, src_path, git_name, git_mail, agent='', log_path=None, show_staged=True ):
    """Generate new access file for entity
    
    This method breaks out of OOP and manipulates entity.json directly.
    Thus it needs to lock to prevent other edits while it does its thing.
    Writes a log to ${entity}/addfile.log, formatted in pseudo-TAP.
    This log is returned along with a File object.
    
    TODO Refactor this function! It is waaay too long!
    
    @param entity: Entity object
    @param ddrfile: File
    @param src_path: str Absolute path to the access file (ddrfile.path_abs)
    @param git_name: Username of git committer.
    @param git_mail: Email of git committer.
    @param agent: str (optional) Name of software making the change.
    @param log_path: str (optional) Absolute path to addfile log
    @param show_staged: boolean Log list of staged files
    @returns: file_,repo,log,next_op
    """
    f = None
    repo = None
    if log_path:
        log = addfile_logger(log_path=log_path)
    else:
        log = addfile_logger(identifier=entity.identifier)
    
    log.ok('------------------------------------------------------------------------')
    log.ok('DDR.models.Entity.add_access: START')
    log.ok('entity: %s' % entity.id)
    log.ok('ddrfile: %s' % ddrfile)
    
    log.ok('Checking files/dirs')
    check_dir('| src_path', src_path, log, mkdir=False, perm=os.R_OK)
    
    log.ok('Identifier')
    log.ok('| file_id %s' % ddrfile.id)
    log.ok('| basepath %s' % entity.identifier.basepath)
    fidentifier = identifier.Identifier(ddrfile.id, entity.identifier.basepath)
    log.ok('| identifier %s' % fidentifier)
    file_class = fidentifier.object_class()

    dest_path = destination_path(src_path, entity.files_path, fidentifier)
    tmp_path = temporary_path(src_path, config.MEDIA_BASE, fidentifier)
    tmp_path_renamed = temporary_path_renamed(tmp_path, dest_path)
    access_dest_path = access_path(file_class, tmp_path_renamed)
    dest_dir = os.path.dirname(dest_path)
    tmp_dir = os.path.dirname(tmp_path)
    # this is the final path of the access file
    access_final_path = ddrfile.identifier.path_abs('access')
    
    log.ok('Checking files/dirs')
    check_dir('| tmp_dir', tmp_dir, log, mkdir=True, perm=os.W_OK)
    check_dir('| dest_dir', dest_dir, log, mkdir=True, perm=os.W_OK)
    
    log.ok('Making access file')
    tmp_access_path = make_access_file(src_path, access_dest_path, log)
    
    log.ok('File object')
    file_ = ddrfile
    log.ok('| file_ %s' % file_)
    
    # if new tmp_access_path and access_dest_path are same, declare success and quit
    existing_sha1 = None
    tmp_sha1 = None
    if os.path.exists(access_final_path):
        # if src_path is an existing file, it's probably a git-annex symlink
        # we want to compare two actual files, not a file and a symlink
        access_final_path_real = os.path.realpath(access_final_path)
        existing_sha1 = util.file_hash(access_final_path_real, 'sha1')
        log.ok('| existing_sha1: %s' % existing_sha1)
    if os.path.exists(access_dest_path):
        tmp_sha1 = util.file_hash(access_dest_path, 'sha1')
        log.ok('| tmp_sha1:      %s' % tmp_sha1)
    if tmp_sha1 == existing_sha1:
        log.ok('New access file same as existing. Nothing to see here, move along.')
        return file_,repo,log,'pass'
    
    log.ok('Writing object metadata')
    tmp_file_json = write_object_metadata(file_, tmp_dir, log)
    #tmp_entity_json
    
    # WE ARE NOW MAKING CHANGES TO THE REPO ------------------------
    
    log.ok('Moving files to dest_dir')
    new_files = []
    if tmp_access_path and os.path.exists(tmp_access_path):
        new_files.append([tmp_access_path, file_.access_abs])
    mvnew_fails = move_files(new_files, log)
    if mvnew_fails:
        log.not_ok('Failed to place one or more new files to destination repo')
        move_new_files_back(new_files, mvnew_fails, log)
    else:
        log.ok('| all files moved')
    
    # file metadata will only be copied if everything else was moved
    log.ok('Moving file .json to dest_dir')
    existing_files = [
        (tmp_file_json, file_.json_path)
    ]
    mvold_fails = move_files(existing_files, log)
    if mvold_fails:
        log.not_ok('Failed to update metadata in destination repo')
        move_existing_files_back(existing_files, mvold_fails, log)
    else:
        log.ok('| all files moved')
    
    log.ok('Staging files')
    git_files = [
        file_.json_path_rel
    ]
    annex_files = [
        file_.access_rel
    ]
    repo = stage_files(entity, git_files, annex_files, new_files, log, show_staged=show_staged)
    
    # IMPORTANT: Files are only staged! Be sure to commit!
    # IMPORTANT: changelog is not staged!
    return file_,repo,log,'continue'

def replace_access(repo, file_, src_path):
    """Replaces existing access file and git-annex-adds new one.
    
    IMPORTANT: This is meant to be used in the context of a larger batch operation.
    It STAGES files but does not commit them.  It does not prepare a changelog.
    
    @returns: list annex_files
    """
    logging.debug('replace_access({}, {}, {})'.format(repo, file_, src_path))
    # If we just copy the new file over the top of the old one
    # Git thinks it's a state change and doesn't see the new file
    # so git-rm and then copy.
    if os.path.exists(file_.access_abs):
        logging.debug('rm {}'.format(file_.access_rel))
        repo.git.rm('--force', file_.access_rel)
    logging.debug('cp {} {}'.format(src_path, file_.access_rel))
    shutil.copyfile(src_path, file_.access_abs)
    return [file_.access_rel]

def add_file_commit(entity, file_, repo, log, git_name, git_mail, agent):
    log.ok('add_file_commit(%s, %s, %s, %s, %s, %s)' % (file_, repo, log, git_name, git_mail, agent))
    staged = dvcs.list_staged(repo)
    modified = dvcs.list_modified(repo)
    if staged and not modified:
        log.ok('All files staged.')
        log.ok('Updating changelog')
        path = file_.path_abs.replace('{}/'.format(entity.path), '')
        changelog_messages = ['Added entity file {}'.format(path)]
        if agent:
            changelog_messages.append('@agent: %s' % agent)
        changelog.write_changelog_entry(
            entity.changelog_path, changelog_messages, git_name, git_mail)
        log.ok('git add %s' % entity.changelog_path_rel)
        git_files = [entity.changelog_path_rel]
        dvcs.stage(repo, git_files)
        
        log.ok('Committing')
        commit = dvcs.commit(repo, 'Added entity file(s)', agent)
        log.ok('commit: {}'.format(commit.hexsha))
        committed = dvcs.list_committed(repo, commit)
        committed.sort()
        log.ok('files committed:')
        for f in committed:
            log.ok('| %s' % f)
        
    else:
        log.not_ok('%s files staged, %s files modified' % (len(staged),len(modified)))
        log.not_ok('staged %s' % staged)
        log.not_ok('modified %s' % modified)
        log.not_ok('Can not commit!')
        raise Exception('Could not commit bc %s unstaged files: %s' % (len(modified), modified))
    return file_,repo,log
