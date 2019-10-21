import ConfigParser
import os
import sys

import pytz


CONFIG_FILES = [
    '/etc/ddr/ddr.cfg',       '/etc/ddr/local.cfg',
    '/etc/ddr/ddrlocal.cfg',  '/etc/ddr/ddrlocal-local.cfg',
    '/etc/ddr/ddridservice.cfg', '/etc/ddr/ddridservice-local.cfg',
]

class NoConfigError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def read_configs(paths):
    cfg = ConfigParser.ConfigParser()
    configs_read = cfg.read(paths)
    if not configs_read:
        raise NoConfigError('No config file!')
    return cfg,configs_read

def _parse_alt_timezones(text):
    """Parses contents of [cmdln]alt_timezones
    Format: ORG:TIMEZONENAME;ORG:TIMEZONENAME
    Example: hmwf:America/Boise;janm:America/Los_Angeles
    NOTE: TIMEZONENAMEs must be valid IANA timezones.
    """
    data = {}
    for item in [item for item in text.strip().split(';') if item]:
        key,val = item.strip().split(':')
        if key not in data.keys():
            data[key] = pytz.timezone(val)
    return data


CONFIG,CONFIGS_READ = read_configs(CONFIG_FILES)

DEBUG = CONFIG.get('cmdln', 'debug')

INSTALL_PATH = CONFIG.get('cmdln','install_path')
REPO_MODELS_PATH = CONFIG.get('cmdln','repo_models_path')
if REPO_MODELS_PATH not in sys.path:
    sys.path.append(REPO_MODELS_PATH)

APP_METADATA = {}

MEDIA_BASE = CONFIG.get('cmdln','media_base')
# Location of Repository 'ddr' repo, which should contain repo_models
# for the Repository.

LOG_DIR = CONFIG.get('local', 'log_dir')
LOG_FILE = CONFIG.get('local','log_file')
LOG_LEVEL = CONFIG.get('local', 'log_level')

UTF8_STRICT = CONFIG.getboolean('cmdln','utf8_strict')

try:
    DEFAULT_TIMEZONE = CONFIG.get('cmdln','default_timezone')
except:
    DEFAULT_TIMEZONE = 'America/Los_Angeles'
TZ = pytz.timezone(DEFAULT_TIMEZONE)
ALT_TIMEZONES = _parse_alt_timezones(CONFIG.get('cmdln','alt_timezones'))
DATETIME_FORMAT = CONFIG.get('cmdln','datetime_format')
DATE_FORMAT = CONFIG.get('cmdln','date_format')
TIME_FORMAT = CONFIG.get('cmdln','time_format')
PRETTY_DATETIME_FORMAT = CONFIG.get('cmdln','pretty_datetime_format')
PRETTY_DATE_FORMAT = CONFIG.get('cmdln','pretty_date_format')
PRETTY_TIME_FORMAT = CONFIG.get('cmdln','pretty_time_format')
# Format used in Elasticsearch mapping.json
# Elasticsearch uses the Joda-Time formatting:
# http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
#ELASTICSEARCH_DATETIME_MAPPING = "yyyy-MM-dd'T'HH:mm:ssZ"
ELASTICSEARCH_DATETIME_MAPPING = "yyyy-MM-dd'T'HH:mm:ss"
# Format used when posting documents to Elasticsearch
# As of 2016-10-31 our ES mappings don't have timezone
# We can't reindex so ES datetimes must be timezone-naive for now.
#ELASTICSEARCH_DATETIME_FORMAT  = "%Y-%m-%dT%H:%M:%S%z"
ELASTICSEARCH_DATETIME_FORMAT  = "%Y-%m-%dT%H:%M:%S"

ACCESS_FILE_APPEND = CONFIG.get('cmdln','access_file_append')
ACCESS_FILE_EXTENSION = CONFIG.get('cmdln','access_file_extension')
ACCESS_FILE_SUFFIX = ACCESS_FILE_APPEND + ACCESS_FILE_EXTENSION
ACCESS_FILE_GEOMETRY = CONFIG.get('cmdln','access_file_geometry')
ACCESS_FILE_OPTIONS  = CONFIG.get('cmdln','access_file_options')

THUMBNAIL_GEOMETRY   = CONFIG.get('cmdln','thumbnail_geometry')
THUMBNAIL_COLORSPACE = 'sRGB'
THUMBNAIL_OPTIONS    = CONFIG.get('cmdln','thumbnail_options')

TEMPLATE_EAD = os.path.join(REPO_MODELS_PATH, 'templates', 'ead.xml')
TEMPLATE_METS = os.path.join(REPO_MODELS_PATH, 'templates', 'mets.xml')
TEMPLATE_EAD_JINJA2 = os.path.join(REPO_MODELS_PATH, 'templates', 'ead.xml.j2')
TEMPLATE_METS_JINJA2 = os.path.join(REPO_MODELS_PATH, 'templates', 'mets.xml.j2')
TEMPLATE_CJSON = CONFIG.get('cmdln','template_cjson')
TEMPLATE_EJSON = CONFIG.get('cmdln','template_ejson')

REQUESTS_TIMEOUT = 30

CGIT_URL = CONFIG.get('workbench','cgit_url')
GIT_REMOTE_NAME = 'origin'  # CONFIG.get('workbench','remote')
GITOLITE = CONFIG.get('workbench','gitolite')
WORKBENCH_LOGIN_TEST = CONFIG.get('workbench','login_test_url')
WORKBENCH_LOGIN_URL = CONFIG.get('workbench','workbench_login_url')
WORKBENCH_LOGOUT_URL = CONFIG.get('workbench','workbench_logout_url')
WORKBENCH_NEWCOL_URL = CONFIG.get('workbench','workbench_newcol_url')
WORKBENCH_NEWENT_URL = CONFIG.get('workbench','workbench_newent_url')
WORKBENCH_REGISTER_EIDS_URL = CONFIG.get('workbench','workbench_register_eids_url')
WORKBENCH_URL = CONFIG.get('workbench','workbench_url')
WORKBENCH_USERINFO = CONFIG.get('workbench','workbench_userinfo_url')

IDSERVICE_API_BASE = CONFIG.get('cmdln', 'idservice_api_base')
IDSERVICE_LOGIN_URL = IDSERVICE_API_BASE + '/rest-auth/login/'
IDSERVICE_LOGOUT_URL = IDSERVICE_API_BASE + '/rest-auth/logout/'
IDSERVICE_USERINFO_URL = IDSERVICE_API_BASE + '/rest-auth/user/'
IDSERVICE_NEXT_OBJECT_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/next/{model}/'
IDSERVICE_CHECKIDS_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/check/'
IDSERVICE_REGISTERIDS_URL = IDSERVICE_API_BASE + '/objectids/{objectid}/create/'

DOCSTORE_ENABLED = CONFIG.getboolean('local','docstore_enabled')
DOCSTORE_TIMEOUT = int(CONFIG.get('local','docstore_timeout'))
DOCSTORE_HOST_LOCAL = CONFIG.get('local','docstore_host')
DOCSTORE_HOST = CONFIG.get('public','docstore_host')
RESULTS_PER_PAGE = 25
ELASTICSEARCH_MAX_SIZE = 10000
ELASTICSEARCH_DEFAULT_LIMIT = RESULTS_PER_PAGE
# Location of snapshot backups. Should match value of path.repo setting
# in /etc/elasticsearch/elasticsearch.yml on cluster
ELASTICSEARCH_PATH_REPO = CONFIG.get('public','docstore_path_repo')

VOCABS_URL = CONFIG.get('cmdln', 'vocabs_url')
# Separator between path elements for hierarchical precoordinated terms.
# e.g. 'Geographic communities: Washington: Seattle'
VOCABS_PRECOORD_PATH_SEP = ' -- '

# vocab.get_vocabs will cache data here
VOCABS = {}  # keys = vocab keyword

TESTING_BASE_DIR = '/tmp/ddr-cmdln-testing'
