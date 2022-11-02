#!/usr/bin/env python

import codecs
import os
import re
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(here, *parts), 'r').read()

def find_version(*file_paths):
    #version_file = read(*file_paths)
    #version_match = re.search(r"^VERSION = ['\"]([^'\"]*)['\"]",
    #                          version_file, re.M)
    #if version_match:
    #    return version_match.group(1)
    #raise RuntimeError("Unable to find version string.")
    return read(*file_paths)

long_description = read('README.rst')

setup(
    url = 'https://github.com/densho/ddr-cmdln/',
    download_url = 'https://github.com/densho/ddr-cmdln.git',
    name = 'ddr-cmdln',
    description = 'ddr-cmdln',
    long_description = long_description,
    version = find_version('..', 'VERSION'),
    #license = 'TBD',
    author = 'Geoffrey Jost',
    author_email = 'geoffrey.jost@densho.org',
    platforms = 'Linux',
    classifiers = [  # https://pypi.python.org/pypi?:action=list_classifiers
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Environment :: Console',
        'Intended Audience :: Other Audience',
        #'License :: OSI Approved :: TBD',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Other/Nonlisted Topic',
        'Topic :: Sociology :: History',
    ],
    install_requires = [
        'nose',
        'Click',
    ],
    packages = [
        'DDR',
        'DDR.cli',
    ],
    include_package_data = True,
    package_dir = {
        'DDR': 'DDR'
    },
    package_data = {'DDR': [
        '*.tpl',
        'models/*',
        'templates/*',
    ]},
    entry_points='''
        [console_scripts]
        ddr2=DDR.cli.ddr:ddr
        ddrcheck=DDR.cli.ddrcheck:ddrcheck
        ddrcheckbinaries=DDR.cli.ddrcheckbinaries:ddrcheckbinaries
        ddrcheckencoding=DDR.cli.ddrcheckencoding:ddrcheckencoding
        ddrconfig=DDR.cli.ddrconfig:ddrconfig
        ddrdesc=DDR.cli.ddrdesc:ddrdesc
        ddrexport=DDR.cli.ddrexport:ddrexport
        ddrindex=DDR.cli.ddrindex:ddrindex
        ddrinfo=DDR.cli.ddrinfo:ddrinfo
        ddrimport=DDR.cli.ddrimport:ddrimport
        ddrnames=DDR.cli.ddrnames:ddrnames
        ddrpubcopy=DDR.cli.ddrpubcopy:ddrpubcopy
        ddrsignatures=DDR.cli.ddrsignatures:ddrsignatures
        ddrtransform=DDR.cli.ddrtransform:ddrtransform
        ddrvocab=DDR.cli.ddrvocab:ddrvocab
    ''',
    scripts = [
        'bin/ddr',
        'bin/ddr-backup',
        'bin/ddr-batch',
        'bin/ddr-filter',
        'bin/ddr-massupdate',
        'bin/ddr-missing',
        'bin/ddr-update',
        'bin/ddrdensho255fix',
    ],
)
