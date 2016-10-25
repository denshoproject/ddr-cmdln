import ConfigParser
import os
import sys

import pytz


CONFIG_FILES = [
    '/etc/ddr/ddr.cfg',       '/etc/ddr/local.cfg',
    '/etc/ddr/ddrlocal.cfg',  '/etc/ddr/ddrlocal-local.cfg',
    '/etc/ddr/ddrpublic.cfg', '/etc/ddr/ddrpublic-local.cfg',
    '/etc/ddr/idservice.cfg', '/etc/ddr/idservice-local.cfg',
]

class NoConfigError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def read_configs(paths):
    config = ConfigParser.ConfigParser()
    configs_read = config.read(paths)
    if not configs_read:
        raise NoConfigError('No config file!')
    return config


config = read_configs(CONFIG_FILES)

DEBUG = config.get('cmdln', 'debug')

INSTALL_PATH = config.get('cmdln','install_path')
REPO_MODELS_PATH = config.get('cmdln','repo_models_path')
if REPO_MODELS_PATH not in sys.path:
    sys.path.append(REPO_MODELS_PATH)
MEDIA_BASE = config.get('cmdln','media_base')
LOG_DIR = config.get('local', 'log_dir')
LOG_FILE = config.get('local','log_file')
LOG_LEVEL = config.get('local', 'log_level')

try:
    DEFAULT_TIMEZONE = config.get('cmdln','default_timezone')
except:
    DEFAULT_TIMEZONE = 'America/Los_Angeles'
TZ = pytz.timezone(DEFAULT_TIMEZONE)
TIME_FORMAT = config.get('cmdln','time_format')
DATETIME_FORMAT = config.get('cmdln','datetime_format')

ACCESS_FILE_APPEND = config.get('cmdln','access_file_append')
ACCESS_FILE_EXTENSION = config.get('cmdln','access_file_extension')
ACCESS_FILE_GEOMETRY = config.get('cmdln','access_file_geometry')
FACETS_PATH = os.path.join(REPO_MODELS_PATH, 'vocab')
MAPPINGS_PATH = os.path.join(REPO_MODELS_PATH, 'docstore', 'mappings.json')
TEMPLATE_EAD = os.path.join(REPO_MODELS_PATH, 'templates', 'ead.xml')
TEMPLATE_METS = os.path.join(REPO_MODELS_PATH, 'templates', 'mets.xml')
TEMPLATE_CJSON = config.get('cmdln','template_cjson')
TEMPLATE_EJSON = config.get('cmdln','template_ejson')

CGIT_URL = config.get('workbench','cgit_url')
GIT_REMOTE_NAME = 'origin'  # config.get('workbench','remote')
GITOLITE = config.get('workbench','gitolite')
WORKBENCH_LOGIN_TEST = config.get('workbench','login_test_url')
WORKBENCH_LOGIN_URL = config.get('workbench','workbench_login_url')
WORKBENCH_LOGOUT_URL = config.get('workbench','workbench_logout_url')
WORKBENCH_NEWCOL_URL = config.get('workbench','workbench_newcol_url')
WORKBENCH_NEWENT_URL = config.get('workbench','workbench_newent_url')
WORKBENCH_REGISTER_EIDS_URL = config.get('workbench','workbench_register_eids_url')
WORKBENCH_URL = config.get('workbench','workbench_url')
WORKBENCH_USERINFO = config.get('workbench','workbench_userinfo_url')

IDSERVICE_API_BASE = config.get('idservice', 'api_base')
IDSERVICE_LOGIN_URL = IDSERVICE_API_BASE + '/rest-auth/login/'
IDSERVICE_LOGOUT_URL = IDSERVICE_API_BASE + '/rest-auth/logout/'
IDSERVICE_USERINFO_URL = IDSERVICE_API_BASE + '/rest-auth/user/'
IDSERVICE_NEXT_OBJECT_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/next/{model}/'
IDSERVICE_CHECKIDS_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/check/'
IDSERVICE_REGISTERIDS_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/create/'

DOCSTORE_HOST = config.get('public','docstore_host')
DOCSTORE_INDEX = config.get('public','docstore_index')

VOCAB_TERMS_URL = config.get('local', 'vocab_terms_url')
