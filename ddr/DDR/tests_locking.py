from datetime import datetime
import os

import config
import locking


# locking.lock
# locking.unlock
# locking.locked
def test_locking():
    lock_path = '/tmp/test-lock-%s' % datetime.now(config.TZ).strftime('%Y%m%dT%H%M%S')
    text = 'we are locked. go away.'
    # before locking
    assert locking.locked(lock_path) == False
    assert locking.unlock(lock_path, text) == 'not locked'
    # locking
    assert locking.lock(lock_path, text) == 'ok'
    # locked
    assert locking.locked(lock_path) == text
    assert locking.lock(lock_path, text) == 'locked'
    assert locking.unlock(lock_path, 'not the right text') == 'miss'
    # unlocking
    assert locking.unlock(lock_path, text) == 'ok'
    # unlocked
    assert locking.locked(lock_path) == False
    assert locking.unlock(lock_path, text) == 'not locked'
    assert not os.path.exists(lock_path)
