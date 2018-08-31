import ConfigParser
import os

from nose.tools import assert_raises

from DDR import config

TESTING_BASE_DIR = os.path.join(config.TESTING_BASE_DIR, 'config')
if not os.path.exists(TESTING_BASE_DIR):
    os.makedirs(TESTING_BASE_DIR)

FAKE_CONFIG_FILES = [
    os.path.join(TESTING_BASE_DIR, 'ddr.cfg'),
    os.path.join(TESTING_BASE_DIR, 'local.cfg'),
]

def test_read_configs():
    assert_raises(config.NoConfigError, config.read_configs, FAKE_CONFIG_FILES)
