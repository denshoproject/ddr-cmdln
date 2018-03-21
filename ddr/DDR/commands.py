from functools import wraps
import logging
import os
import re
import shutil
import sys

import envoy
import git

from DDR import config
from DDR import storage
from DDR import dvcs
from DDR.changelog import write_changelog_entry
from DDR.organization import group_repo_level, repo_level, repo_annex_get, read_group_file

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(MODULE_PATH, 'templates')
GITIGNORE_TEMPLATE = os.path.join(TEMPLATE_PATH, 'gitignore.tpl')



def requires_network(f):
    """Indicate that function requires network access; check if can connect to gitolite server.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        gitolite = dvcs.Gitolite(config.GITOLITE)
        gitolite.initialize()
        if not gitolite.connected:
            logging.error('Cannot connect to git server {}'.format(gitolite.server))
            return 1,'cannot connect to git server {}'.format(gitolite.server)
        return f(*args, **kwargs)
    return wrapper

def local_only(f):
    """Indicate that function requires no network access.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper

def command(f):
    """Indicate that function is a command-line command.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        logging.debug('------------------------------------------------------------------------')
        logging.debug('{}.{}({}, {})'.format(f.__module__, f.__name__, args, kwargs))
        return f(*args, **kwargs)
    return wrapper


description="""Various commands for manipulating DDR collections and entities."""

epilog="""
More than you thought you wanted to know about the collection command.
"""

OPERATIONS = [
    'clocal',
    'create',
    'clone',
    'destroy',
    'status',
    'astatus',
    'update',
    'sync',
    'ecreate',
    'edestroy',
    'eupdate',
    'eadd',
    'pull',
    'push',
    ]



def commit_files(repo, message, git_files=[], annex_files=[]):
    """git-add and git-annex-add files and commit them
    
    @param repo: GitPython Repo object
    @param message: String
    @param git_files: List of filenames relative to repo root.
    @param annex_files: List of filenames relative to repo root.
    @return: GitPython Repo object
    """
    added = annex_files + git_files
    added.sort()
    logging.debug('    files added:         {}'.format(added))
    
    if annex_files:
        repo.git.annex('add', annex_files)
    if git_files:
        repo.index.add(git_files)
    
    staged = dvcs.list_staged(repo)
    staged.sort()
    logging.debug('    files staged:        {}'.format(staged))
    # TODO cancel commit if list of staged doesn't match list of files added?
    
    commit = repo.index.commit(message)
    logging.debug('    commit: {}'.format(commit.hexsha))
    
    committed = dvcs.list_committed(repo, commit)
    committed.sort()
    logging.debug('    files committed:     {}'.format(committed))
    # TODO complain if list of committed files doesn't match lists of added and staged files?
    
    return repo



#@command
@local_only
def devices():
    return 0,storage.devices()

#@command
@local_only
def mounted_devices():
    return 0,storage.mounted_devices()

@command
@local_only
def mount( device_file, label ):
    """Command-line function for mounting specified device on local system.
    """
    return 0,storage.mount(device_file, label)

@command
@local_only
def umount( device_file ):
    """Command-line function for UNmounting specified device on local system.
    """
    return 0,storage.umount(device_file)

@command
@local_only
def remount( device_file, label ):
    """Command-line function for unmounting and remounting specified device on local system.
    """
    return 0,storage.remount(device_file, label)

@command
@local_only
def mount_point( path ):
    return 0,storage.mount_point(path)

@command
@local_only
def storage_status( path ):
    return 0,storage.storage_status(path)


@command
@requires_network
def clone(user_name, user_mail, identifier, dest_path):
    """Command-line function for cloning an existing collection.
    
    Clones existing collection object from workbench server.
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param identifier: Identifier
    @param dest_path: str
    @return: message ('ok' if successful)
    """
    git_url = '{}:{}.git'.format(config.GITOLITE, identifier.id)
    repo = git.Repo.clone_from(git_url, dest_path)
    logging.debug('    git clone {}'.format(git_url))
    if repo:
        logging.debug('    OK')
    else:
        logging.error('    COULD NOT CLONE!')
        return 1,'could not clone'
    if os.path.exists(identifier.path_abs('git')):
        logging.debug('    .git/ is present')
    else:
        logging.error('    .git/ IS MISSING!')
        return 1,'.git/ is missing'
    # git annex init if not already existing
    if not os.path.exists(identifier.path_abs('annex')):
        logging.debug('    git annex init')
        repo.git.annex('init')
    #
    repo.git.checkout('master')
    dvcs.git_set_configs(repo, user_name, user_mail)
    dvcs.annex_set_configs(repo, user_name, user_mail)
    drive_label = storage.drive_label(repo.working_dir)
    dvcs.annex_set_description(repo, dvcs.annex_status(repo), drive_label=drive_label)
    dvcs.remote_add(repo, git_url, config.GIT_REMOTE_NAME)
    return 0,'ok'


@command
@requires_network
def create(user_name, user_mail, identifier, templates, agent=''):
    """Command-line function for creating a new collection.
    
    Clones a blank collection object from workbench server, adds files, commits.
    
    - clones new repo from gitolite server
    # Easier to have Gitolite create repo then clone (http://sitaramc.github.com/gitolite/repos.html)
    # than to add existing to Gitolite (http://sitaramc.github.com/gitolite/rare.html#existing).
    local requests CID from workbench API
    background:collection init: $ collection -cCID -oinit]
    background:collection init: $ git clone git@mits:ddr-ORG-C
        $ git clone git@mits:ddr-densho-1
        Cloning into 'ddr-densho-1'...
        Initialized empty Git repository in /home/git/repositories/ddr-densho-1.git/
        warning: You appear to have cloned an empty repository.
    background:entity init: $ git annex init
    background:entity init: $ git add changelog control ead.xml .gitignore
    background:entity init: $ git commit
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param identifier: Identifier
    @param templates: List of metadata templates (absolute paths).
    @param agent: (optional) Name of software making the change.
    @return: message ('ok' if successful)
    """
    gitolite = dvcs.Gitolite(config.GITOLITE)
    gitolite.initialize()
    if identifier.id in gitolite.collections():
        raise Exception("'%s' already exists -- clone instead." % identifier.id)
    git_url = '{}:{}.git'.format(config.GITOLITE, identifier.id)
    repo = git.Repo.clone_from(git_url, identifier.path_abs())
    logging.debug('    git clone {}'.format(git_url))
    if repo:
        logging.debug('    OK')
    else:
        logging.error('    COULD NOT CLONE!')
    if os.path.exists(identifier.path_abs('git')):
        logging.debug('    .git/ is present')
    else:
        logging.error('    .git/ IS MISSING!')
    # there is no master branch at this point
    dvcs.remote_add(repo, git_url, config.GIT_REMOTE_NAME)
    dvcs.git_set_configs(repo, user_name, user_mail)
    dvcs.annex_set_configs(repo, user_name, user_mail)
    git_files = []
    
    # copy template files to collection
    for src in templates:
        if os.path.exists(src):
            dst = os.path.join(identifier.path_abs(), os.path.basename(src))
            logging.debug('cp %s, %s' % (src, dst))
            shutil.copy(src, dst)
            if os.path.exists(dst):
                git_files.append(dst)
            else:
                logging.error('COULD NOT COPY %s' % src)

    # instantiate now that we have collection dir and some templates
    object_class = identifier.object_class()
    collection = object_class(identifier.path_abs())
    
    # add control, .gitignore, changelog
    control   = collection.control()
    gitignore = collection.gitignore()
    
    # prep log entries
    changelog_messages = ['Initialized collection {}'.format(collection.id)]
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message(changelog_messages[0], agent=agent)
    
    write_changelog_entry(collection.changelog_path,
                          changelog_messages,
                          user_name, user_mail)
    if os.path.exists(control.path):
        git_files.append(control.path_rel)
    else:
        logging.error('    COULD NOT CREATE control')
    if os.path.exists(collection.gitignore_path):
        git_files.append(collection.gitignore_path_rel)
    else:
        logging.error('    COULD NOT CREATE .gitignore')
    if os.path.exists(collection.changelog_path):
        git_files.append(collection.changelog_path_rel)
    else:
        logging.error('    COULD NOT CREATE changelog')
    
    # add files and commit
    repo = commit_files(repo, commit_message, git_files, [])
    # master branch should be created by this point
    # git annex init
    logging.debug('    git annex init')
    repo.git.annex('init')
    if os.path.exists(os.path.join(collection.path, '.git', 'annex')):
        logging.debug('    .git/annex/ OK')
    else:
        logging.error('    .git/annex/ IS MISSING!')
    
    # manual version of git-annex-sync - see notes for DDR.commands.sync.
    logging.debug('git push %s git-annex' % config.GIT_REMOTE_NAME)
    repo.git.checkout('git-annex')
    repo.git.push(config.GIT_REMOTE_NAME, 'git-annex')
    logging.debug('git push %s master' % config.GIT_REMOTE_NAME)
    repo.git.checkout('master')
    repo.git.push(config.GIT_REMOTE_NAME, 'master')
    logging.debug('OK')
    
    drive_label = storage.drive_label(repo.working_dir)
    dvcs.annex_set_description(repo, dvcs.annex_status(repo), drive_label=drive_label)
    return 0,'ok'


@command
@local_only
def destroy(agent=''):
    """Command-line function for removing  an entire collection's files from the local system.
    
    Does not remove files from the server!  That will remain a manual operation.
    
    @param agent: (optional) Name of software making the change.
    @return: message ('ok' if successful)
    """
    return 1,'not implemented yet'


#@command
@local_only
def status(collection, short=False):
    """Command-line function for running git status on collection repository.
    
    @param collection: Collection
    @return: message ('ok' if successful)
    """
    return dvcs.repo_status(dvcs.repository(collection.path))


@command
#@requires_network
def annex_status(collection):
    """Command-line function for running git annex status on collection repository.
    
    @param collection: Collection
    @return: message ('ok' if successful)
    """
    return dvcs.annex_status(dvcs.repository(collection.path))


@command
#@requires_network
def fetch(collection):
    """Command-line function for fetching latest changes to git repo from origin/master.
    
    @param collection: Collection
    @return: message ('ok' if successful)
    """
    return dvcs.fetch(dvcs.repository(collection.path))


@command
@local_only
def update(user_name, user_mail, collection, updated_files, agent='', commit=False):
    """Command-line function for commiting changes to the specified file.
    
    NOTE: Does not push to the workbench server.
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param updated_files: List of relative paths to updated file(s).
    @param agent: (optional) Name of software making the change.
    @param commit: (optional) Commit files after staging them.
    @return: message ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    if repo:
        logging.debug('    git repo {}'.format(collection.path))
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    
    # prep log entries
    changelog_messages = []
    for f in updated_files:
        changelog_messages.append('Updated collection file(s) {}'.format(f))
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message('Updated metadata file(s)', agent=agent)
    
    # write changelog
    write_changelog_entry(collection.changelog_path,
                          changelog_messages,
                          user_name, user_mail)
    if os.path.exists(collection.changelog_path):
        updated_files.append(collection.changelog_path)
    else:
        logging.error('    COULD NOT UPDATE changelog')
    
    if commit:
        # add files and commit
        repo = commit_files(repo, commit_message, updated_files, [])
    return 0,'ok'


@command
@requires_network
def sync(user_name, user_mail, collection):
    """Sync repo with bare clone on hub server; replaces git-annex-sync.
    
    Git-annex has a "sync" command for communicating annex changes between
    repositories, but it is designed to be used between non-bare repositories.
    Normally Git does not support pushing to non-bare repositories, and
    git-annex does some trickery involving "synced/BRANCH" branches to make
    this work.
    Reference: http://git-annex.branchable.com/sync/
    
    When git-annex-sync is used between a non-bare repo and a bare repo
    (e.g. between a local repo and our hub server running Gitolite),
    the "synced/master" branches do not get merged in to master and syncing
    no longer works.  Therefore it is necessary to sync manually.
    
    If you think you want to use git-annex-sync, remember that we tried this
    in commit 1857a7aa3f and it did not work and we reverted to manual syncing.
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @return: message ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    logging.debug('repo: %s' % repo)
    drive_label = storage.drive_label(repo.working_dir)
    dvcs.annex_set_description(repo, dvcs.annex_status(repo), drive_label=drive_label)
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    # list remotes
    logging.debug('remotes')
    for remote in dvcs.remotes(repo):
        logging.debug('- %s %s' % (remote['name'], remote['target']))
    # sync
    logging.debug('git annex sync')
    out = repo.git.annex('sync')
    logging.debug(out)
    return 0,'ok'


@command
@local_only
def entity_create(user_name, user_mail, collection, eidentifier, updated_files, templates, agent=''):
    """Command-line function for creating an entity and adding it to the collection.
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param eidentifier: Identifier
    @param updated_files: List of updated files (relative to collection root).
    @param templates: List of entity metadata templates (absolute paths).
    @param agent: (optional) Name of software making the change.
    @return: message ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    git_files = []
    
    # entity dir
    if not os.path.exists(eidentifier.path_abs()):
        os.makedirs(eidentifier.path_abs())
    
    # copy template files to entity
    for src in templates:
        if os.path.exists(src):
            dst = os.path.join(eidentifier.path_abs(), os.path.basename(src))
            logging.debug('cp %s, %s' % (src, dst))
            shutil.copy(src, dst)
            if os.path.exists(dst):
                git_files.append(dst)
            else:
                logging.error('COULD NOT COPY %s' % src)
    
    # instantiate now that we have entity dir and some templates
    object_class = eidentifier.object_class()
    entity = object_class(eidentifier.path_abs())
    
    # entity control
    econtrol = entity.control()
    if os.path.exists(econtrol.path):
        git_files.append(econtrol.path)
    else:
        logging.error('    COULD NOT CREATE control')
    # update collection control
    ccontrol = collection.control()
    ccontrol.update_checksums(collection)
    ccontrol.write()
    git_files.append(ccontrol.path)
    
    # prep ENTITY log entries
    entity_changelog_messages = ['Initialized entity {}'.format(entity.id),]
    if agent:
        entity_changelog_messages.append('@agent: %s' % agent)
    # prep COLLECTION log entries
    changelog_messages = ['Initialized entity {}'.format(entity.id),]
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message(changelog_messages[0], agent=agent)
    
    # ENTITY changelog
    write_changelog_entry(entity.changelog_path,
                          entity_changelog_messages,
                          user=user_name, email=user_mail)
    if os.path.exists(entity.changelog_path):
        git_files.append(entity.changelog_path)
    else:
        logging.error('    COULD NOT CREATE changelog')
    # COLLECTION changelog
    write_changelog_entry(collection.changelog_path,
                          changelog_messages,
                          user=user_name, email=user_mail)
    git_files.append(collection.changelog_path)
    
    # add updated collection files
    for src in updated_files:
        git_files.append(src)
    
    # add files and commit
    repo = commit_files(repo, commit_message, git_files, [])
    return 0,'ok'


@command
@local_only
def entity_destroy(user_name, user_mail, collection, entity, agent=''):
    """Command-line function for creating an entity and adding it to the collection.
    
    - check that paths exist, etc
    - intantiate collection, repo objects
    - remove entity dir
    - update control and changelog
    - commit everything
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param entity: Entity
    @param agent: (optional) Name of software making the change.
    @return: message ('ok' if successful)
    """
    if not os.path.exists(collection.path_abs):
        raise Exception('collection_path not found: %s' % collection.path_abs)
    if not os.path.exists(entity.path_abs):
        raise Exception('entity not found: %s' % entity.path_abs)
    
    repo = dvcs.repository(collection.path_abs, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    git_files = []
    
    # remove entity directory
    # NOTE: entity files must be removed at this point so the entity will be
    # properly removed from the control file
    git = repo.git
    git.rm('-rf', entity.path_abs)
    
    # update collection control
    ccontrol = collection.control()
    ccontrol.update_checksums(collection)
    ccontrol.write()
    git_files.append(ccontrol.path)
    
    # prep collection log entries
    changelog_messages = ['Deleted entity {}'.format(entity.id),]
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message(changelog_messages[0], agent=agent)
    
    # collection changelog
    write_changelog_entry(collection.changelog_path,
                          changelog_messages,
                          user=user_name, email=user_mail)
    git_files.append(collection.changelog_path)
    
    # commit
    repo = commit_files(repo, commit_message, git_files)
    return 0,'ok'


@command
@local_only
def file_destroy(user_name, user_mail, collection, entity, rm_files, updated_files, agent='', commit=True):
    """Remove file and metadata
    
    - check that paths exist, etc
    - intantiate collection, repo objects
    - remove entity dir
    - update control and changelog
    - commit everything
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param entity: Entity
    @param rm_files: List of paths to files to delete (relative to entity files dir).
    @param updated_files: List of paths to updated file(s), relative to entitys.
    @param agent: (optional) Name of software making the change.
    @param commit: (optional) Commit files after staging them.
    @return: exit,message,touched_files ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    
    # updated file paths are relative to collection root
    git_files = [os.path.join('files', entity.id, f) for f in updated_files]
    
    # remove the files
    # NOTE: File must be removed from filesystem at this point
    # so the File will be properly removed from the control file
    for f in rm_files:
        repo.git.rm('-rf', f)
    
    # update entity control
    econtrol = entity.control()
    econtrol.update_checksums(entity)
    econtrol.write()
    git_files.append(econtrol.path_rel)
    
    # update entity changelog
    changelog_files = [
        # dont list access files in changelog
        # TODO use a models.File function to ID the original file
        f for f in rm_files
        if ('-a.jpg' not in f) and ('.json' not in f)
    ]
    changelog_messages = [
        'Deleted file {}'.format(os.path.basename(f))
        for f in changelog_files
    ]
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    write_changelog_entry(
        entity.changelog_path,
        changelog_messages,
        user_name, user_mail
    )
    
    git_files.append(entity.changelog_path_rel)
    dvcs.stage(repo, git_files)
    if commit:
        commit_obj = dvcs.commit(repo, 'Deleted file(s)', agent)
    return 0,'ok',git_files


@command
@local_only
def entity_update(user_name, user_mail, collection, entity, updated_files, agent='', commit=True):
    """Command-line function for committing changes to the specified entity file.
    
    NOTE: Does not push to the workbench server.
    Updates entity changelog but NOT in collection changelog.
    Makes an entry in git log.
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param entity: Entity
    @param updated_files: List of paths to updated file(s), relative to entitys.
    @param agent: (optional) Name of software making the change.
    @param commit: (optional) Commit files after staging them.
    @return: message ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    
    # entity file paths are relative to collection root
    git_files = []
    for f in updated_files:
        git_files.append( os.path.join( 'files', entity.id, str(f)) )
    
    # entity changelog
    entity_changelog_messages = []
    for f in updated_files:
        p = os.path.join(entity.id, f)
        entity_changelog_messages.append('Updated entity file {}'.format(p))

    # prep log entries
    if agent:
        entity_changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message('Updated entity file(s)', agent=agent)
    
    write_changelog_entry(entity.changelog_path,
                          entity_changelog_messages,
                          user=user_name, email=user_mail)
    git_files.append(entity.changelog_path_rel)
    if commit:
        # add files and commit
        repo = commit_files(repo, commit_message, git_files, [])
    return 0,'ok'


@command
@local_only
def entity_annex_add(user_name, user_mail, collection, entity, updated_files, new_annex_files, agent=''):
    """Command-line function for git annex add-ing a file and updating metadata.
    
    All this function does is git annex add the file, update changelog and
    mets.xml, and commit.
    It does not copy the file into the entity dir.
    It does not mark the file as master/mezzanine/access/etc or edit any metadata.
    It does not perform any background processing on the file.
    
    TODO Refactor this when ddr-local models moved into ddr-cmdln
    WARNING - UGLY HACK!
    The 'entity' arg is intended to allow ddr-local to pass in Entity
    objects and use their checksums() method.
    
    @param user_name: Username for use in changelog, git log
    @param user_mail: User email address for use in changelog, git log
    @param collection: Collection
    @param entity: Entity
    @param updated_files: list of paths to updated files (relative to collection repo).
    @param new_annex_files: List of paths to new files (relative to entity files dir).
    @param agent: (optional) Name of software making the change.
    @return: message ('ok' if successful)
    """
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    git_files = []
    annex_files = []
    
    if not os.path.exists(collection.annex_path):
        logging.error('    .git/annex IS MISSING!')
        return 1,'.git/annex IS MISSING!'
    if not os.path.exists(entity.path):
        logging.error('    Entity does not exist: {}'.format(entity.id))
        return 1,'entity does not exist: {}'.format(entity.id)
    if not os.path.exists(entity.files_path):
        logging.error('    Entity files_path does not exist: {}'.format(entity.id))
        return 1,'entity files_path does not exist: {}'.format(entity.id)
    
    # new annex files
    new_files_rel_entity = []
    for new_file in new_annex_files:
        # paths: absolute, relative to collection repo, relative to entity_dir
        new_file_abs = os.path.join(entity.files_path, new_file)
        if not os.path.exists(new_file_abs):
            logging.error('    File does not exist: {}'.format(new_file_abs))
            return 1,'File does not exist: {}'.format(new_file_abs)
        new_file_rel = os.path.join(entity.files_path_rel, new_file)
        new_file_rel_entity = new_file_abs.replace('{}/'.format(entity.path), '')
        new_files_rel_entity.append(new_file_rel_entity)
        annex_files.append(new_file_rel)
    
    # updated files
    [git_files.append(updated_file) for updated_file in updated_files]
    
    # update entity control
    econtrol = entity.control()
    econtrol.update_checksums(entity)
    econtrol.write()
    git_files.append(econtrol.path_rel)
    
    # prep log entries
    changelog_messages = ['Added entity file {}'.format(f) for f in new_files_rel_entity]
    if agent:
        changelog_messages.append('@agent: %s' % agent)
    commit_message = dvcs.compose_commit_message('Added entity file(s)', agent=agent)
    
    # update entity changelog
    write_changelog_entry(entity.changelog_path,
                          changelog_messages,
                          user_name, user_mail)
    git_files.append(entity.changelog_path_rel)
    
    # add files and commit
    repo = commit_files(repo, commit_message, git_files, annex_files)
    return 0,'ok'


@command
@requires_network
def annex_push(collection, file_path_rel):
    """Push a git-annex file to workbench.

    Example file_paths:
        ddr-densho-1-1/files/video1.mov
        ddr-densho-42-17/files/image35.jpg
        ddr-one-35-248/files/newspaper.pdf
    
    $ git annex copy PATH --to=REMOTE
    
    @param collection: Collection
    @param file_path_rel: Path to file relative to collection root
    @return: message ('ok' if successful)
    """
    file_path_abs = os.path.join(collection.path, file_path_rel)
    logging.debug('    collection.path {}'.format(collection.path))
    logging.debug('    file_path_rel {}'.format(file_path_rel))
    logging.debug('    file_path_abs {}'.format(file_path_abs))
    if not os.path.exists(collection.path):
        logging.error('    NO COLLECTION AT {}'.format(collection.path))
        return 1,'no collection'
    if not os.path.exists(collection.annex_path):
        logging.error('    NO GIT ANNEX AT {}'.format(collection.annex_path))
        return 1,'no annex'
    if not os.path.exists(file_path_abs):
        logging.error('    NO FILE AT {}'.format(file_path_abs))
        return 1,'no file'
    # let's do this thing
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    logging.debug('    git annex copy -t {} {}'.format(config.GIT_REMOTE_NAME, file_path_rel))
    stdout = repo.git.annex('copy', '-t', config.GIT_REMOTE_NAME, file_path_rel)
    logging.debug('\n{}'.format(stdout))
    # confirm that it worked
    whereis = dvcs.annex_whereis_file(repo, file_path_rel)
    if whereis['success']:
        remotes = [r['description'] for r in whereis['whereis'] if not r['here']]
        logging.debug('    present in remotes {}'.format(remotes))
        logging.debug('    it worked: {}'.format(config.GIT_REMOTE_NAME in remotes))
    logging.debug('    DONE')
    return 0,'ok'


@command
@requires_network
def annex_pull(collection, file_path_rel):
    """git-annex copy a file from workbench.

    Example file_paths:
        ddr-densho-1-1/files/video1.mov
        ddr-densho-42-17/files/image35.jpg
        ddr-one-35-248/files/newspaper.pdf
        
    @param collection: Collection
    @param file_path_rel: Path to file relative to collection root.
    @return: message ('ok' if successful)
    """
    file_path_abs = os.path.join(collection.path, file_path_rel)
    logging.debug('    collection.path {}'.format(collection.path))
    logging.debug('    file_path_rel {}'.format(file_path_rel))
    logging.debug('    file_path_abs {}'.format(file_path_abs))
    if not os.path.exists(collection.path):
        logging.error('    NO COLLECTION AT {}'.format(collection.path))
        return 1,'no collection'
    if not os.path.exists(collection.annex_path):
        logging.error('    NO GIT ANNEX AT {}'.format(collection.annex_path))
        return 1,'no annex'
    # let's do this thing
    repo = dvcs.repository(collection.path, user_name, user_mail)
    repo.git.checkout('master')
    dvcs.remote_add(repo, collection.git_url, config.GIT_REMOTE_NAME)
    logging.debug('    git annex copy -t {} {}'.format(config.GIT_REMOTE_NAME, file_path_rel))
    stdout = repo.git.annex('copy', '-f', config.GIT_REMOTE_NAME, file_path_rel)
    logging.debug('\n{}'.format(stdout))
    # confirm that it worked
    exists = os.path.exists(file_path_abs)
    lexists = os.path.lexists(file_path_abs)
    islink = os.path.islink(file_path_abs)
    itworked = (exists and lexists and islink)
    logging.debug('    it worked: {}'.format(itworked))
    logging.debug('    DONE')
    return 0,'ok'


@command
@local_only
def sync_group(groupfile, local_base, local_name, remote_base, remote_name):
    """
    """
    logging.debug('reading group file: %s' % groupfile)
    repos = read_group_file(groupfile)
    ACCESS_SUFFIX = config.ACCESS_FILE_APPEND + config.ACCESS_FILE_EXTENSION
    
    def logif(txt):
        t = txt.strip()
        if t:
            logging.debug(t)
    
    for r in repos:
        repo_path = os.path.join(local_base, r['id'])
        logging.debug('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')
        logging.debug('repo_path: %s' % repo_path)
        
        # clone/update
        if os.path.exists(repo_path):
            logging.debug('updating %s' % repo_path)
            repo = dvcs.repository(repo_path)
            repo.git.fetch('origin')
            repo.git.checkout('master')
            repo.git.pull('origin', 'master')
            repo.git.checkout('git-annex')
            repo.git.pull('origin', 'git-annex')
            repo.git.checkout('master')
            logging.debug('ok')
        else:
            url = '%s:%s.git' % (config.GITOLITE, r['id'])
            logging.debug('cloning %s' % url)
            repo = git.Repo.clone_from(url, r['id'])
            repo.git.config('annex.sshcaching', 'false')
            logging.debug('ok')
        
        remote_path = os.path.join(remote_base, r['id'])
        # local -> remote
        dvcs.remote_add(git.Repo(repo_path, search_parent_directories=True), remote_path, remote_name)
        # remote -> local
        dvcs.remote_add(git.Repo(remote_path, search_parent_directories=True), repo_path, local_name)
        
        # annex sync
        logging.debug('annex sync')
        response = repo.git.annex('sync')
        logif(response)
        
        # annex get
        level = r['level']
        logging.debug('level: %s' % level)
        if level == 'access':
            for root, dirs, files in os.walk(repo_path):
                if '.git' in dirs: # exclude .git dir
                    dirs.remove('.git')
                for f in files:
                    if f.endswith(ACCESS_SUFFIX):
                        path_rel = os.path.join(root, f).replace(repo_path, '')[1:]
                        response = repo.git.annex('get', path_rel)
                        logif(response)
        elif level == 'all':
            logging.debug('git annex get .')
            response = repo.git.annex('get', '.')
            logif(response)
        logging.debug('DONE')
        
    return 0,'ok'
