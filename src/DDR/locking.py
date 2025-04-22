import os
from typing import Any, Dict, List, Match, Optional, Set, Tuple, Union


def lock(lock_path: str, text: str) -> str:
    """Writes lockfile to collection dir; complains if can't.
    
    Celery tasks don't seem to know their own task_id, and there don't
    appear to be any handlers that can be called just *before* a task
    is fired. so it appears to be impossible for a task to lock itself.
    
    This method should(?) be called immediately after starting the task:
    >> result = collection_sync.apply_async((args...), countdown=2)
    >> lock_status = collection.lock(result.task_id)
    
    >>> path = '/tmp/ddr-testing-123'
    >>> os.mkdir(path)
    >>> c = Collection(path)
    >>> c.lock('abcdefg')
    'ok'
    >>> c.lock('abcdefg')
    'locked'
    >>> c.unlock('abcdefg')
    'ok'
    >>> os.rmdir(path)
    
    TODO return 0 if successful
    
    @param lock_path
    @param text
    @returns 'ok' or 'locked'
    """
    if os.path.exists(lock_path):
        return 'locked'
    with open(lock_path, 'w') as f:
        f.write(text)
    return 'ok'

def unlock(lock_path: str, text: str) -> str:
    """Removes lockfile or complains if can't
    
    This method should be called by celery Task.after_return()
    See "Abstract classes" section of
    http://celery.readthedocs.org/en/latest/userguide/tasks.html#custom-task-classes
    
    >>> path = '/tmp/ddr-testing-123'
    >>> os.mkdir(path)
    >>> c = Collection(path)
    >>> c.lock('abcdefg')
    'ok'
    >>> c.unlock('xyz')
    'task_id miss'
    >>> c.unlock('abcdefg')
    'ok'
    >>> c.unlock('abcdefg')
    'not locked'
    >>> os.rmdir(path)
    
    TODO return 0 if successful
    
    @param lock_path
    @param text
    @returns 'ok', 'not locked', 'task_id miss', 'blocked'
    """
    if not os.path.exists(lock_path):
        return 'not locked'
    with open(lock_path, 'r') as f:
        lockfile_text = f.read().strip()
    if lockfile_text and (lockfile_text != text):
        return 'miss'
    os.remove(lock_path)
    if os.path.exists(lock_path):
        return 'blocked'
    return 'ok'

def locked(lock_path: str) -> Union[str, bool]:
    """Returns contents of lockfile if collection repo is locked, False if not
    
    >>> c = Collection('/tmp/ddr-testing-123')
    >>> c.locked()
    False
    >>> c.lock('abcdefg')
    'ok'
    >>> c.locked()
    'abcdefg'
    >>> c.unlock('abcdefg')
    'ok'
    >>> c.locked()
    False
    
    @param lock_path
    """
    if os.path.exists(lock_path):
        with open(lock_path, 'r') as f:
            text = f.read().strip()
        return text
    return False
