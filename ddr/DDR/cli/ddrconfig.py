import logging
import sys

import click

from DDR import config


@click.command()
def ddrconfig():
    """ddrconfig - Prints configs available to ddr-cmdln and sys.path.
    """
    print('CONFIG_FILES')
    for path in config.CONFIG_FILES:
        print('- %s' % path)

    VARNAMES = [
        'DEBUG',
        'INSTALL_PATH',
        'MEDIA_BASE',
        'LOG_DIR',
        'LOG_FILE',
        'LOG_LEVEL',
        'TIME_FORMAT',
        'DATETIME_FORMAT',
        'ACCESS_FILE_APPEND',
        'ACCESS_FILE_EXTENSION',
        'ACCESS_FILE_GEOMETRY',
        'FACETS_PATH',
        'MAPPINGS_PATH',
        'TEMPLATE_EJSON',
        'TEMPLATE_EAD',
        'TEMPLATE_METS',
        'CGIT_URL',
        'GIT_REMOTE_NAME',
        'GITOLITE',
        'WORKBENCH_LOGIN_TEST',
        'WORKBENCH_LOGIN_URL',
        'WORKBENCH_LOGOUT_URL',
        'WORKBENCH_NEWCOL_URL',
        'WORKBENCH_NEWENT_URL',
        'WORKBENCH_REGISTER_EIDS',
        'WORKBENCH_URL',
        'WORKBENCH_USERINFO',
        'IDSERVICE_API_BASE',
        'IDSERVICE_LOGIN_URL',
        'IDSERVICE_LOGOUT_URL',
        'IDSERVICE_USERINFO_URL',
        'IDSERVICE_NEXT_OBJECT_URL',
        'IDSERVICE_CHECKIDS_URL',
        'IDSERVICE_REGISTERIDS_URL',
        'DOCSTORE_HOST',
        'VOCABS_URL',
        'REPO_MODELS_PATH',
    ]
    for varname in VARNAMES:
        if hasattr(config, varname):
            print('{:<30}{}'.format(varname, getattr(config, varname)))
        else:
            print('{:<30}---'.format(varname))
    
    print('sys.path')
    for path in sys.path:
        print('- %s' % path)
