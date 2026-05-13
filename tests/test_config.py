import pytest

from DDR import config

FAKE_CONFIG_FILES = [
    'ddr.cfg',
    'local.cfg',
]

def test_read_configs(tmpdir):
    config_files = [
        str(tmpdir / filename)
        for filename in FAKE_CONFIG_FILES
    ]
    with pytest.raises(config.NoConfigError):
        config.read_configs(config_files)
