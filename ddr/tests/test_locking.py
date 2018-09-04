from datetime import datetime
import os

from DDR import config
from DDR import locking

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'locking')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

# locking.lock
# locking.unlock
# locking.locked
def test_locking():
    lock_path = os.path.join(TESTING_BASE_DIR, 'test-lock-%s' % datetime.now(config.TZ).strftime('%Y%m%dT%H%M%S'))
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
