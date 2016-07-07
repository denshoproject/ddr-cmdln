from datetime import datetime
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
        entry = '[{}] {} - {}\n'.format(datetime.now().isoformat('T'), ok, msg)
        with open(self.logpath, 'a') as f:
            f.write(entry)
    
    def ok(self, msg): self.entry('ok', msg)
    def not_ok(self, msg): self.entry('not ok', msg)
    
    def log(self):
        log = ''
        if os.path.exists(self.logpath):
            with open(self.logpath, 'r') as f:
                log = f.read()
        return log

    def crash(self, msg):
        """Write to addfile log and raise an exception."""
        self.not_ok(msg)
        raise Exception(msg)

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
    log.ok('check dir %s (%s)' % (path, label))
    if mkdir and not os.path.exists(path):
        os.makedirs(path)
    if not os.path.exists(path):
        log.crash('%s does not exist' % label)
        return False
    if not os.access(path, perm):
        log.crash('%s not has permission %s' % (label, perm))
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
    os.rename(tmp_path, tmp_path_renamed)
    if not os.path.exists(tmp_path_renamed) and not os.path.exists(tmp_path):
        log.crash('File rename failed: %s -> %s' % (tmp_path, tmp_path_renamed))

def make_access_file(src_path, access_dest_path, log):
    log.ok('| %s' % access_dest_path)
    try:
        data = imaging.thumbnail(
            src_path,
            access_dest_path,
            geometry=config.ACCESS_FILE_GEOMETRY
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
        os.rename(tmp,dest)
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
    os.rename(tmp_entity_json, entity.json_path)
    if not os.path.exists(entity.json_path):
        log.crash('Failed to place entity.json in destination repo')

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
    total = already + additions
    return total

def stage_files(entity, git_files, annex_files, new_files, log, show_staged=True):
    # TODO move to DDR.dvcs?
    repo = dvcs.repository(entity.collection_path)
    log.ok('| repo %s' % repo)
    # These vars will be used to determine if stage operation is successful.
    # If called in batch operation there may already be staged files.
    # stage_planned   Files added/modified by this function call
    # stage_already   Files that were already staged
    # stage_predicted List of staged files that should result from this operation.
    # stage_new       Files that are being added.
    stage_planned = git_files + annex_files
    stage_already = dvcs.list_staged(repo)
    stage_predicted = predict_staged(stage_already, stage_planned)
    stage_new = [x for x in stage_planned if x not in stage_already]
    log.ok('| %s files to stage:' % len(stage_planned))
    for sp in stage_planned:
        log.ok('|   %s' % sp)
    stage_ok = False
    staged = []
    try:
        log.ok('git stage')
        dvcs.stage(repo, git_files)
        log.ok('annex stage')
        dvcs.annex_stage(repo, annex_files)
        log.ok('ok')
        staged = dvcs.list_staged(repo)
    except:
        # FAILED! print traceback to addfile log
        log.not_ok(traceback.format_exc().strip())
    finally:
        if show_staged:
            log.ok('| %s files staged:' % len(staged))
            log.ok('show_staged %s' % show_staged)
            for sp in staged:
                log.ok('|   %s' % sp)
        if len(staged) == len(stage_predicted):
            log.ok('| %s files staged (%s new, %s modified)' % (
                len(staged), len(stage_new), len(stage_already))
            )
            stage_ok = True
        else:
            log.not_ok('%s new files staged (should be %s)' % (
                len(staged), len(stage_predicted))
            )
        if not stage_ok:
            log.not_ok('File staging aborted. Cleaning up')
            # try to pick up the pieces
            # mv files back to tmp_dir
            # TODO Properly clean up git-annex-added files.
            #      This clause moves the *symlinks* to annex files but leaves
            #      the actual binaries in the .git/annex objects dir.
            for tmp,dest in new_files:
                if os.path.islink(dest):
                    log.not_ok('| link (not moving) %s' % dest)
                else:
                    log.not_ok('| mv %s %s' % (dest,tmp))
                    os.rename(dest,tmp)
            log.not_ok('finished cleanup. good luck...')
            log.crash('Add file aborted, see log file for details: %s' % log.logpath)
    return repo

def add_file(entity, src_path, role, data, git_name, git_mail, agent='', log_path=None, show_staged=True):
    """Add file to entity
    
    This method breaks out of OOP and manipulates entity.json directly.
    Thus it needs to lock to prevent other edits while it does its thing.
    Writes a log to ${entity}/addfile.log, formatted in pseudo-TAP.
    This log is returned along with a File object.
    
    IMPORTANT: Files are only staged! Be sure to commit!
    
    @param src_path: Absolute path to an uploadable file.
    @param role: Keyword of a file role.
    @param data: 
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
    log.ok('DDR.models.Entity.add_file: START')
    log.ok('entity: %s' % entity.id)
    log.ok('data: %s' % data)
    
    log.ok('Examining source file')
    check_dir('| src_path', src_path, log, mkdir=False, perm=os.R_OK)
    
    src_size = os.path.getsize(src_path)
    log.ok('| file size %s' % src_size)
    # TODO check free space on dest
    
    md5,sha1,sha256 = checksums(src_path, log)
    
    log.ok('| extracting XMP data')
    xmp = imaging.extract_xmp(src_path)
    
    log.ok('Identifier')
    # note: we can't make this until we have the sha1
    idparts = {
        'role': role,
        'sha1': sha1[:10],
    }
    log.ok('| idparts %s' % idparts)
    fidentifier = entity.identifier.child('file', idparts, entity.identifier.basepath)
    log.ok('| identifier %s' % fidentifier)
    file_class = fidentifier.object_class()
    
    dest_path = destination_path(src_path, entity.files_path, fidentifier)
    tmp_path = temporary_path(src_path, config.MEDIA_BASE, fidentifier)
    tmp_path_renamed = temporary_path_renamed(tmp_path, dest_path)
    access_dest_path = access_path(file_class, tmp_path_renamed)
    dest_dir = os.path.dirname(dest_path)
    tmp_dir = os.path.dirname(tmp_path)
    
    log.ok('Checking files/dirs')
    check_dir('| tmp_dir', tmp_dir, log, mkdir=True, perm=os.W_OK)
    check_dir('| dest_dir', dest_dir, log, mkdir=True, perm=os.W_OK)
    
    log.ok('Copying to work dir')
    copy_to_workdir(src_path, tmp_path, tmp_path_renamed, log)
    
    log.ok('Making access file')
    tmp_access_path = make_access_file(src_path, access_dest_path, log)
    
    log.ok('File object')
    file_ = file_class(path_abs=dest_path, identifier=fidentifier)
    file_.basename_orig = os.path.basename(src_path)
    # add extension to path_abs
    basename_ext = os.path.splitext(file_.basename_orig)[1]
    path_abs_ext = os.path.splitext(file_.path_abs)[1]
    if basename_ext and not path_abs_ext:
        file_.path_abs = file_.path_abs + basename_ext
        log.ok('| basename_ext %s' % basename_ext)
    file_.size = src_size
    file_.role = role
    file_.sha1 = sha1
    file_.md5 = md5
    file_.sha256 = sha256
    file_.xmp = xmp
    log.ok('| file_ %s' % file_)
    log.ok('| file_.basename_orig: %s' % file_.basename_orig)
    log.ok('| file_.path_abs: %s' % file_.path_abs)
    log.ok('| file_.size: %s' % file_.size)
    # form data
    for field in data:
        setattr(file_, field, data[field])
    
    log.ok('Attaching access file')
    if tmp_access_path and os.path.exists(tmp_access_path):
        file_.set_access(tmp_access_path, entity)
        log.ok('| file_.access_rel: %s' % file_.access_rel)
        log.ok('| file_.access_abs: %s' % file_.access_abs)
    else:
        log.not_ok('no access file')
    
    log.ok('Attaching file to entity')
    entity.files.append(file_)
    if file_ in entity.files:
        log.ok('| done')
    else:
        log.crash('Could not add file to entity.files!')
    
    log.ok('Writing object metadata')
    tmp_file_json = write_object_metadata(file_, tmp_dir, log)
    tmp_entity_json = write_object_metadata(entity, tmp_dir, log)
    
    # WE ARE NOW MAKING CHANGES TO THE REPO ------------------------
    
    log.ok('Moving files to dest_dir')
    new_files = [
        (tmp_path_renamed, file_.path_abs),
        (tmp_file_json, file_.json_path),
    ]
    if tmp_access_path and os.path.exists(tmp_access_path):
        new_files.append(
            (tmp_access_path, file_.access_abs)
        )
    mvnew_fails = move_files(new_files, log)
    if mvnew_fails:
        log.not_ok('Failed to place one or more new files to destination repo')
        move_new_files_back(new_files, mvnew_fails, log)
    else:
        log.ok('| all files moved')
    
    # entity metadata will only be copied if everything else was moved
    log.ok('Moving entity.json to dest_dir')
    existing_files = [
        (tmp_entity_json, entity.json_path)
    ]
    mvold_fails = move_files(existing_files, log)
    if mvold_fails:
        log.not_ok('Failed to update metadata in destination repo')
        move_existing_files_back(existing_files, mvold_fails, log)
    else:
        log.ok('| all files moved')
    
    log.ok('Staging files')
    git_files = [
        entity.json_path_rel,
        file_.json_path_rel
    ]
    annex_files = [
        file_.path_abs.replace('%s/' % file_.collection_path, '')
    ]
    if file_.access_abs and os.path.exists(file_.access_abs):
        annex_files.append(file_.access_abs.replace('%s/' % file_.collection_path, ''))
    repo = stage_files(entity, git_files, annex_files, new_files, log, show_staged=show_staged)
    
    # IMPORTANT: Files are only staged! Be sure to commit!
    # IMPORTANT: changelog is not staged!
    return file_,repo,log

def add_access( entity, ddrfile, git_name, git_mail, agent='', log_path=None, show_staged=True ):
    """Generate new access file for entity
    
    This method breaks out of OOP and manipulates entity.json directly.
    Thus it needs to lock to prevent other edits while it does its thing.
    Writes a log to ${entity}/addfile.log, formatted in pseudo-TAP.
    This log is returned along with a File object.
    
    TODO Refactor this function! It is waaay too long!
    
    @param ddrfile: File
    @param git_name: Username of git committer.
    @param git_mail: Email of git committer.
    @param agent: str (optional) Name of software making the change.
    @param log_path: str (optional) Absolute path to addfile log
    @param show_staged: boolean Log list of staged files
    @return file_ File object
    """
    f = None
    repo = None
    if log_path:
        log = addfile_logger(log_path=log_path)
    else:
        log = addfile_logger(identifier=entity.identifier)
    
    src_path = ddrfile.path_abs
    
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
    
    log.ok('Checking files/dirs')
    check_dir('| tmp_dir', tmp_dir, log, mkdir=True, perm=os.W_OK)
    check_dir('| dest_dir', dest_dir, log, mkdir=True, perm=os.W_OK)
    
    log.ok('Making access file')
    tmp_access_path = make_access_file(src_path, access_dest_path, log)
    
    log.ok('File object')
    file_ = ddrfile
    log.ok('| file_ %s' % file_)
    
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
    return file_,repo,log

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
