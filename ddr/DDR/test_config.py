import ConfigParser

from nose.tools import assert_raises

import config


FAKE_CONFIG_FILES = ['/tmp/NOT_HERE/ddr.cfg', '/tmp/NOT_HERE/local.cfg']

def test_read_configs():
    assert_raises(config.NoConfigError, config.read_configs, FAKE_CONFIG_FILES)
