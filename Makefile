PROJECT=ddr
APP=ddrcmdln
USER=ddr
SHELL = /bin/bash

APP_VERSION := $(shell cat VERSION)
GIT_SOURCE_URL=https://github.com/densho/ddr-cmdln

# current branch name minus dashes or underscores
PACKAGE_BRANCH := $(shell git rev-parse --abbrev-ref HEAD | tr -d _ | tr -d -)
# current commit hash
PACKAGE_COMMIT := $(shell git log -1 --pretty="%h")
# current commit date minus dashes
PACKAGE_TIMESTAMP := $(shell git log -1 --pretty="%ad" --date=short | tr -d -)

PACKAGE_SERVER=ddr.densho.org/static/ddrcmdln

SRC_REPO_CMDLN=https://github.com/densho/ddr-cmdln.git
SRC_REPO_CMDLN_ASSETS=https://github.com/densho/ddr-cmdln-assets.git
SRC_REPO_DEFS=https://github.com/densho/ddr-defs.git
SRC_REPO_VOCAB=https://github.com/densho/densho-vocab.git
SRC_REPO_MANUAL=https://github.com/densho/ddr-manual.git

INSTALL_BASE=/opt
INSTALLDIR=$(INSTALL_BASE)/ddr-cmdln
REQUIREMENTS=$(INSTALLDIR)/requirements.txt
PIP_CACHE_DIR=$(INSTALL_BASE)/pip-cache

CWD := $(shell pwd)
INSTALL_CMDLN=/opt/ddr-cmdln
INSTALL_CMDLN_ASSETS=/opt/ddr-cmdln/ddr-cmdln-assets
INSTALL_DEFS=/opt/ddr-defs
INSTALL_VOCAB=/opt/densho-vocab
INSTALL_MANUAL=/opt/ddr-manual

COMMIT_CMDLN := $(shell git -C $(INSTALL_CMDLN) log --decorate --abbrev-commit --pretty=oneline -1)
COMMIT_DEFS := $(shell git -C $(INSTALL_DEFS) log --decorate --abbrev-commit --pretty=oneline -1)
COMMIT_VOCAB := $(shell git -C $(INSTALL_VOCAB) log --decorate --abbrev-commit --pretty=oneline -1)

VIRTUALENV=$(INSTALL_CMDLN)/venv/cmdln

CONF_BASE=/etc/ddr
CONF_PRODUCTION=$(CONF_BASE)/ddrlocal.cfg
CONF_LOCAL=$(CONF_BASE)/ddrlocal-local.cfg

LOG_BASE=/var/log/ddr
INVENTORY_LOG_BASE=$(LOG_BASE)/inventory

DDR_REPO_BASE=/var/www/media/ddr

MEDIA_BASE=/var/www
MEDIA_ROOT=$(MEDIA_BASE)/media

# Release name e.g. jessie
DEBIAN_CODENAME := $(shell lsb_release -sc)
# Release numbers e.g. 8.10
DEBIAN_RELEASE := $(shell lsb_release -sr)
# Sortable major version tag e.g. deb8
DEBIAN_RELEASE_TAG = deb$(shell lsb_release -sr | cut -c1)

PYTHON_VERSION=
IMAGEMAGICK_CONF=
LIBEXEMPI3_PKG=
OPENJDK_PKG=
ifeq ($(DEBIAN_CODENAME), bullseye)
	PYTHON_VERSION=3.9
	IMAGEMAGICK_CONF=imagemagick-policy.xml.deb11
	LIBEXEMPI3_PKG=libexempi8
	OPENJDK_PKG=openjdk-17-jre-headless
endif
ifeq ($(DEBIAN_CODENAME), bookworm)
	PYTHON_VERSION=3.11.2
	IMAGEMAGICK_CONF=imagemagick-policy.xml.deb12
	LIBEXEMPI3_PKG=libexempi8
	OPENJDK_PKG=openjdk-17-jre-headless
endif
ifeq ($(DEBIAN_CODENAME), trixie)
	PYTHON_VERSION=3.11.6
	IMAGEMAGICK_CONF=imagemagick-policy.xml.deb13
	LIBEXEMPI3_PKG=libexempi8
	OPENJDK_PKG=openjdk-17-jre-headless
endif

ELASTICSEARCH=elasticsearch-7.3.1-amd64.deb
# wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.3.1.deb

TGZ_BRANCH := $(shell python3 bin/package-branch.py)
TGZ_FILE=$(APP)_$(APP_VERSION)
TGZ_DIR=$(INSTALL_CMDLN)/$(TGZ_FILE)
TGZ_CMDLN=$(TGZ_DIR)/ddr-cmdln
TGZ_CMDLN_ASSETS=$(TGZ_DIR)/ddr-cmdln/ddr-cmdln-assets
TGZ_DEFS=$(TGZ_DIR)/ddr-defs
TGZ_VOCAB=$(TGZ_DIR)/densho-vocab
TGZ_MANUAL=$(TGZ_DIR)/ddr-manual

# Adding '-rcN' to VERSION will name the package "ddrlocal-release"
# instead of "ddrlocal-BRANCH"
DEB_BRANCH := $(shell python3 bin/package-branch.py)
DEB_ARCH=amd64
DEB_NAME_BULLSEYE=$(APP)-$(DEB_BRANCH)
DEB_NAME_BOOKWORM=$(APP)-$(DEB_BRANCH)
DEB_NAME_TRIXIE=$(APP)-$(DEB_BRANCH)
# Application version, separator (~), Debian release tag e.g. deb8
# Release tag used because sortable and follows Debian project usage.
DEB_VERSION_BULLSEYE=$(APP_VERSION)~deb11
DEB_VERSION_BOOKWORM=$(APP_VERSION)~deb12
DEB_VERSION_TRIXIE=$(APP_VERSION)~deb13
DEB_FILE_BULLSEYE=$(DEB_NAME_BULLSEYE)_$(DEB_VERSION_BULLSEYE)_$(DEB_ARCH).deb
DEB_FILE_BOOKWORM=$(DEB_NAME_BOOKWORM)_$(DEB_VERSION_BOOKWORM)_$(DEB_ARCH).deb
DEB_FILE_TRIXIE=$(DEB_NAME_TRIXIE)_$(DEB_VERSION_TRIXIE)_$(DEB_ARCH).deb
DEB_VENDOR=Densho.org
DEB_MAINTAINER=<geoffrey.jost@densho.org>
DEB_DESCRIPTION=Densho Digital Repository editor
DEB_BASE=opt/ddr-cmdln


# Type make print-VARNAME to print value of any variable
print-%: ; @echo $*=$($*)


debug:
	@echo "ddr-cmdln: $(COMMIT_CMDLN)"
	@echo "ddr-defs:  $(COMMIT_DEFS)"
	@echo "densho-vocab: $(COMMIT_VOCAB)"


.PHONY: help


help:
	@echo "--------------------------------------------------------------------------------"
	@echo "ddr-cmdln make commands"
	@echo ""
	@echo "Most commands have subcommands (ex: install-ddr-cmdln, restart-supervisor)"
	@echo ""
	@echo "get     - Clones ddr-cmdln, ddr-defs, densho-vocab."
	@echo "install - Performs complete install. See also: make howto-install"
	@echo "test    - Run unit tests"
	@echo ""
	@echo "vbox-guest     - Installs VirtualBox Guest Additions"
	@echo "network-config - Installs standard network conf (CHANGES IP TO 192.168.56.101!)"
	@echo ""
	@echo "deb       - Makes a DEB package install file."
	@echo "remove    - Removes Debian packages for dependencies."
	@echo "uninstall - Deletes 'compiled' Python files. Leaves build dirs and configs."
	@echo "clean     - Deletes files created while building app, leaves configs."
	@echo ""

howto-install:
	@echo "HOWTO INSTALL"
	@echo "# Basic Debian netinstall"
	@echo "#edit /etc/network/interfaces"
	@echo "#reboot"
	@echo "apt-get update && apt-get upgrade"
	@echo "apt-get install -u openssh ufw"
	@echo "ufw allow 22/tcp"
	@echo "ufw allow 80/tcp"
	@echo "ufw allow 9001/tcp"
	@echo "ufw allow 9200/tcp"
	@echo "ufw enable"
	@echo "apt-get install --assume-yes make"
	@echo "git clone $(SRC_REPO_CMDLN) $(INSTALL_CMDLN)"
	@echo "cd $(INSTALL_CMDLN)"
	@echo "make install"
	@echo "#make branch BRANCH=develop"
	@echo "#make install"
	@echo "# Place copy of 'ddr' repo in $(DDR_REPO_BASE)/ddr."
	@echo "#make install-defs"
	@echo "#make install-vocab"
	@echo "#make enable-bkgnd"
	@echo "#make migrate"
	@echo "make restart"


get: get-app get-ddr-defs get-densho-vocab

install: install-prep install-app install-configs

test: test-app

coverage: coverage-app

uninstall: uninstall-app uninstall-configs

clean: clean-app


install-prep: ddr-user install-core git-config install-misc-tools

ddr-user:
	-addgroup --gid=1001 ddr
	-adduser --uid=1001 --gid=1001 --home=/home/ddr --shell=/bin/bash --disabled-login --gecos "" ddr
	-addgroup ddr plugdev
	-addgroup ddr vboxsf
	printf "\n\n# ddrlocal: Activate virtualnv on login\nsource $(VIRTUALENV)/bin/activate\n" >> /home/ddr/.bashrc; \

apt-backports:
ifeq "$(DEBIAN_CODENAME)" "buster"
	cp $(INSTALLDIR)/conf/ddr-buster-backports.list /etc/apt/sources.list.d/
	apt-get update
endif

install-core:
	apt-get --assume-yes install bzip2 curl gdebi-core git-core logrotate ntp p7zip-full wget

git-config:
	git config --global alias.st status
	git config --global alias.co checkout
	git config --global alias.br branch
	git config --global alias.ci commit

install-misc-tools:
	@echo ""
	@echo "Installing miscellaneous tools -----------------------------------------"
	apt-get --assume-yes install ack-grep byobu elinks htop mg multitail


# Copies network config into /etc/network/interfaces
# CHANGES IP ADDRESS TO 192.168.56.101!
network-config:
	@echo ""
	@echo "Configuring network ---------------------------------------------"
	-cp $(INSTALL_CMDLN)/conf/network-interfaces.$(DEBIAN_CODENAME) /etc/network/interfaces
	@echo "/etc/network/interfaces updated."
	@echo "New config will take effect on next reboot."


# Installs VirtualBox Guest Additions and prerequisites
vbox-guest:
	@echo ""
	@echo "Installing VirtualBox Guest Additions ---------------------------"
	@echo "In the VM window, click on \"Devices > Install Guest Additions\"."
	apt-get --quiet install build-essential module-assistant
	m-a prepare
	mount /media/cdrom
	sh /media/cdrom/VBoxLinuxAdditions.run
	-addgroup ddr vboxsf


get-elasticsearch:
	wget -nc -P /tmp/downloads http://$(PACKAGE_SERVER)/$(ELASTICSEARCH)

install-elasticsearch: install-core
	@echo ""
	@echo "Elasticsearch ----------------------------------------------------------"
# Elasticsearch is configured/restarted here so it's online by the time script is done.
	apt-get --assume-yes install $(OPENJDK_PKG)
	-gdebi --non-interactive /tmp/downloads/$(ELASTICSEARCH)
#cp $(INSTALL_CMDLN)/conf/elasticsearch.yml /etc/elasticsearch/
#chown root:root /etc/elasticsearch/elasticsearch.yml
#chmod 644 /etc/elasticsearch/elasticsearch.yml
# 	@echo "${bldgrn}search engine (re)start${txtrst}"
	-service elasticsearch stop
	-systemctl disable elasticsearch.service

enable-elasticsearch:
	systemctl enable elasticsearch.service

disable-elasticsearch:
	systemctl disable elasticsearch.service

remove-elasticsearch:
	apt-get --assume-yes remove $(OPENJDK_PKG) elasticsearch


install-virtualenv:
	@echo ""
	@echo "install-virtualenv -----------------------------------------------------"
	apt-get --assume-yes install python3-pip python3-venv
	python3 -m venv $(VIRTUALENV)
	source $(VIRTUALENV)/bin/activate; \
	pip3 install -U --cache-dir=$(PIP_CACHE_DIR) uv

install-setuptools: install-virtualenv
	@echo ""
	@echo "install-setuptools -----------------------------------------------------"
	apt-get --assume-yes install python3-dev
	source $(VIRTUALENV)/bin/activate; \
	uv pip install -U --cache-dir=$(PIP_CACHE_DIR) setuptools

install-dependencies: apt-backports
	@echo ""
	@echo "install-dependencies ---------------------------------------------------"
	apt-get --assume-yes install python3-dev python3-pip python3-venv ntp
	apt-get --assume-yes install libxml2-dev libxslt1-dev libz-dev pmount udisks2
	apt-get --assume-yes install imagemagick libssl-dev libxml2 libxml2-dev libxslt1-dev
	apt-get --assume-yes install $(LIBEXEMPI3_PKG)
	apt-get --assume-yes install ack-grep
	apt-get --assume-yes install git-annex git-core

mkdirs: mkdir-ddr-cmdln


get-app: get-ddr-cmdln get-ddr-cmdln-assets get-ddr-manual

pip-download: pip-download-cmdln

install-app: install-dependencies install-ddr-cmdln install-configs

test-app: test-ddr-cmdln

coverage-app: coverage-ddr-cmdln

uninstall-app: uninstall-ddr-cmdln uninstall-ddr-manual uninstall-configs

clean-app: clean-ddr-cmdln clean-ddr-manual


get-ddr-cmdln:
	@echo ""
	@echo "get-ddr-cmdln ----------------------------------------------------------"
	git status | grep "On branch"
	if test -d $(INSTALL_CMDLN); \
	then cd $(INSTALL_CMDLN) && git pull; \
	else git clone $(SRC_REPO_CMDLN) $(INSTALL_CMDLN); \
	fi

get-ddr-cmdln-assets:
	@echo ""
	@echo "get-ddr-cmdln-assets ---------------------------------------------------"
	if test -d $(INSTALL_CMDLN_ASSETS); \
	then cd $(INSTALL_CMDLN_ASSETS) && git pull; \
	else git clone $(SRC_REPO_CMDLN_ASSETS) $(INSTALL_CMDLN_ASSETS); \
	fi

setup-ddr-cmdln:
	git status | grep "On branch"
	source $(VIRTUALENV)/bin/activate; uv pip install .

pip-download-cmdln:
	source $(VIRTUALENV)/bin/activate; \
	uv pip download --no-binary=:all: --destination-directory=$(INSTALL_CMDLN)/vendor -r $(INSTALL_CMDLN)/requirements.txt

install-ddr-cmdln: install-virtualenv install-setuptools git-safe-dir
	@echo ""
	@echo "install-ddr-cmdln ------------------------------------------------------"
	git status | grep "On branch"
	source $(VIRTUALENV)/bin/activate; uv pip install .
	source $(VIRTUALENV)/bin/activate; uv pip install -U --cache-dir=$(PIP_CACHE_DIR) internetarchive

install-testing: install-virtualenv install-setuptools
	@echo ""
	@echo "install-ddr-cmdln ------------------------------------------------------"
	git status | grep "On branch"
	source $(VIRTUALENV)/bin/activate; uv pip install .[testing]

git-safe-dir:
	@echo ""
	@echo "git-safe-dir -----------------------------------------------------------"
	sudo -u ddr git config --global --add safe.directory $(INSTALL_CMDLN)
	sudo -u ddr git config --global --add safe.directory $(INSTALL_DEFS)
	sudo -u ddr git config --global --add safe.directory $(INSTALL_VOCAB)

mkdir-ddr-cmdln:
	@echo ""
	@echo "mkdir-ddr-cmdln --------------------------------------------------------"
	-mkdir $(LOG_BASE)
	chown -R ddr:ddr $(LOG_BASE)
	chmod -R 775 $(LOG_BASE)
	-mkdir $(INVENTORY_LOG_BASE)
	chown -R ddr:ddr $(INVENTORY_LOG_BASE)
	chmod -R 775 $(INVENTORY_LOG_BASE)
	-mkdir -p $(MEDIA_ROOT)
	chown -R ddr:ddr $(MEDIA_ROOT)
	chmod -R 775 $(MEDIA_ROOT)

test-ddr-cmdln:
	@echo ""
	@echo "test-ddr-cmdln ---------------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; pytest --disable-warnings tests/

coverage-ddr-cmdln:
	@echo ""
	@echo "coverage-ddr-cmdln -----------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; pytest --cov-config=ddr-cmdln/.coveragerc --cov-report=html --cov=DDR ddr-cmdln/tests/

mypy-ddr-cmdln:
	@echo ""
	@echo "mypy-ddr-cmdln ---------------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; mypy src/DDR/

uninstall-ddr-cmdln:
	@echo ""
	@echo "uninstall-ddr-cmdln ----------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; uv pip uninstall -y .

clean-ddr-cmdln:
	-rm -Rf $(INSTALL_CMDLN)/ddr/build
	-rm -Rf $(INSTALL_CMDLN)/ddr/ddr_cmdln.egg-info
	-rm -Rf $(INSTALL_CMDLN)/ddr/dist


get-ddr-defs:
	@echo ""
	@echo "get-ddr-defs -----------------------------------------------------------"
	git status | grep "On branch"
	if test -d $(INSTALL_DEFS); \
	then cd $(INSTALL_DEFS) && git pull; \
	else git clone $(SRC_REPO_DEFS) $(INSTALL_DEFS); \
	fi


get-densho-vocab:
	@echo ""
	@echo "get-densho-vocab -------------------------------------------------------"
	if test -d $(INSTALL_VOCAB); \
	then cd $(INSTALL_VOCAB) && git pull; \
	else git clone $(SRC_REPO_VOCAB) $(INSTALL_VOCAB); \
	fi


install-configs:
	@echo ""
	@echo "configuring ddr-cmdln --------------------------------------------------"
# base settings file
	-mkdir /etc/ddr
	cp $(INSTALL_CMDLN)/conf/ddrlocal.cfg $(CONF_PRODUCTION)
	chown root:root $(CONF_PRODUCTION)
	chmod 644 $(CONF_PRODUCTION)
	touch $(CONF_LOCAL)
	chown ddr:ddr $(CONF_LOCAL)
	chmod 640 $(CONF_LOCAL)
	-mkdir -p /etc/ImageMagick-6/
	-cp /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xml.orig
	cp $(INSTALL_CMDLN)/conf/$(IMAGEMAGICK_CONF) /etc/ImageMagick-6/policy.xml

uninstall-configs:
	-rm $(CONF_PRODUCTION)


get-ddr-manual:
	@echo ""
	@echo "get-ddr-manual ---------------------------------------------------------"
	git status | grep "On branch"
	if test -d $(INSTALL_MANUAL); \
	then cd $(INSTALL_MANUAL) && git pull; \
	else git clone $(SRC_REPO_MANUAL) $(INSTALL_MANUAL); \
	fi

install-ddr-manual:
	@echo ""
	@echo "install-ddr-manual -----------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; \
	uv pip install -U --cache-dir=$(PIP_CACHE_DIR) sphinx
	source $(VIRTUALENV)/bin/activate; \
	cd $(INSTALL_MANUAL) && make html
	rm -Rf $(MEDIA_ROOT)/manual
	mv $(INSTALL_MANUAL)/build/html $(MEDIA_ROOT)/manual

uninstall-ddr-manual:
	uv pip uninstall -y sphinx

clean-ddr-manual:
	-rm -Rf $(INSTALL_MANUAL)/build


tgz-local:
	rm -Rf $(TGZ_DIR)
	git clone $(INSTALL_CMDLN) $(TGZ_CMDLN)
	git clone $(INSTALL_CMDLN_ASSETS) $(TGZ_CMDLN_ASSETS)
	git clone $(INSTALL_DEFS) $(TGZ_DEFS)
	git clone $(INSTALL_VOCAB) $(TGZ_VOCAB)
	git clone $(INSTALL_MANUAL) $(TGZ_MANUAL)
	cd $(TGZ_CMDLN); git checkout develop; git checkout master
	cd $(TGZ_CMDLN_ASSETS); git checkout develop; git checkout master
	cd $(TGZ_DEFS); git checkout develop; git checkout master
	cd $(TGZ_VOCAB); git checkout develop; git checkout master
	cd $(TGZ_MANUAL); git checkout develop; git checkout master
	tar czf $(TGZ_FILE).tgz $(TGZ_FILE)
	rm -Rf $(TGZ_DIR)


tgz:
	rm -Rf $(TGZ_DIR)
	git clone $(SRC_REPO_CMDLN) $(TGZ_CMDLN)
	git clone $(SRC_REPO_CMDLN_ASSETS) $(TGZ_CMDLN_ASSETS)
	git clone $(SRC_REPO_DEFS) $(TGZ_DEFS)
	git clone $(SRC_REPO_VOCAB) $(TGZ_VOCAB)
	git clone $(SRC_REPO_MANUAL) $(TGZ_MANUAL)
	cd $(TGZ_CMDLN); git checkout develop; git checkout master
	cd $(TGZ_CMDLN_ASSETS); git checkout develop; git checkout master
	cd $(TGZ_DEFS); git checkout develop; git checkout master
	cd $(TGZ_VOCAB); git checkout develop; git checkout master
	cd $(TGZ_MANUAL); git checkout develop; git checkout master
	tar czf $(TGZ_FILE).tgz $(TGZ_FILE)
	rm -Rf $(TGZ_DIR)


# http://fpm.readthedocs.io/en/latest/
install-fpm:
	@echo "install-fpm ------------------------------------------------------------"
	apt-get install --assume-yes ruby ruby-dev rubygems build-essential
	gem install --no-document fpm

# https://stackoverflow.com/questions/32094205/set-a-custom-install-directory-when-making-a-deb-package-with-fpm
# https://brejoc.com/tag/fpm/
deb: deb-bookworm

deb-bullseye:
	@echo ""
	@echo "FPM packaging (bullseye) -----------------------------------------------"
	-rm -Rf $(DEB_FILE_BULLSEYE)
# Copy .git/ dir from master worktree
	python3 bin/deb-prep-post.py before
# Make package
	fpm   \
	--verbose   \
	--input-type dir   \
	--output-type deb   \
	--name $(DEB_NAME_BULLSEYE)   \
	--version $(DEB_VERSION_BULLSEYE)   \
	--package $(DEB_FILE_BULLSEYE)   \
	--url "$(GIT_SOURCE_URL)"   \
	--vendor "$(DEB_VENDOR)"   \
	--maintainer "$(DEB_MAINTAINER)"   \
	--description "$(DEB_DESCRIPTION)"   \
	--depends "git-annex"   \
	--depends "git-core"   \
	--depends "imagemagick"   \
	--depends "libexempi8"   \
	--depends "libssl-dev"   \
	--depends "libxml2"   \
	--depends "libxml2-dev"   \
	--depends "libxslt1-dev"   \
	--depends "libz-dev"   \
	--depends "pmount"   \
	--depends "python3-dev"   \
	--depends "python3-pip"   \
	--depends "python3-venv"   \
	--depends "udisks2"   \
	--after-install "bin/fpm-after-install.sh"   \
	--chdir $(INSTALL_CMDLN)   \
	bin=$(DEB_BASE)   \
	conf/ddrlocal.cfg=etc/ddr/ddrlocal.cfg   \
	conf/README-logs=$(LOG_BASE)/README  \
	conf=$(DEB_BASE)   \
	COPYRIGHT=$(DEB_BASE)   \
	ddr-cmdln-assets=$(DEB_BASE)   \
	../ddr-defs=opt   \
	../densho-vocab=opt   \
	files=$(DEB_BASE)   \
	.git=$(DEB_BASE)   \
	.gitattributes=$(DEB_BASE)   \
	.gitignore=$(DEB_BASE)   \
	INSTALL.rst=$(DEB_BASE)   \
	LICENSE=$(DEB_BASE)   \
	Makefile=$(DEB_BASE)   \
	mypy.ini=$(DEB_BASE)   \
	NOTES=$(DEB_BASE)   \
	pyproject.toml=$(DEB_BASE)   \
	README.rst=$(DEB_BASE)   \
	src=$(DEB_BASE)   \
	tests=$(DEB_BASE)   \
	tox.ini=$(DEB_BASE)   \
	venv=$(DEB_BASE)   \
	VERSION=$(DEB_BASE)
# Put worktree pointer file back in place
	python3 bin/deb-prep-post.py after

deb-bookworm:
	@echo ""
	@echo "FPM packaging (bookworm) -----------------------------------------------"
	-rm -Rf $(DEB_FILE_BOOKWORM)
# Copy .git/ dir from master worktree
	python3 bin/deb-prep-post.py before
# Make package
	fpm   \
	--verbose   \
	--input-type dir   \
	--output-type deb   \
	--name $(DEB_NAME_BOOKWORM)   \
	--version $(DEB_VERSION_BOOKWORM)   \
	--package $(DEB_FILE_BOOKWORM)   \
	--url "$(GIT_SOURCE_URL)"   \
	--vendor "$(DEB_VENDOR)"   \
	--maintainer "$(DEB_MAINTAINER)"   \
	--description "$(DEB_DESCRIPTION)"   \
	--depends "git-annex"   \
	--depends "git-core"   \
	--depends "imagemagick"   \
	--depends "libexempi8"   \
	--depends "libssl-dev"   \
	--depends "libxml2"   \
	--depends "libxml2-dev"   \
	--depends "libxslt1-dev"   \
	--depends "libz-dev"   \
	--depends "pmount"   \
	--depends "python3-dev"   \
	--depends "python3-pip"   \
	--depends "python3-venv"   \
	--depends "udisks2"   \
	--after-install "bin/fpm-after-install.sh"   \
	--chdir $(INSTALL_CMDLN)   \
	bin=$(DEB_BASE)   \
	conf/ddrlocal.cfg=etc/ddr/ddrlocal.cfg   \
	conf/README-logs=$(LOG_BASE)/README  \
	conf=$(DEB_BASE)   \
	COPYRIGHT=$(DEB_BASE)   \
	ddr-cmdln-assets=$(DEB_BASE)   \
	../ddr-defs=opt   \
	../densho-vocab=opt   \
	files=$(DEB_BASE)   \
	.git=$(DEB_BASE)   \
	.gitattributes=$(DEB_BASE)   \
	.gitignore=$(DEB_BASE)   \
	INSTALL.rst=$(DEB_BASE)   \
	LICENSE=$(DEB_BASE)   \
	Makefile=$(DEB_BASE)   \
	mypy.ini=$(DEB_BASE)   \
	NOTES=$(DEB_BASE)   \
	pyproject.toml=$(DEB_BASE)   \
	README.rst=$(DEB_BASE)   \
	src=$(DEB_BASE)   \
	tests=$(DEB_BASE)   \
	tox.ini=$(DEB_BASE)   \
	venv=$(DEB_BASE)   \
	VERSION=$(DEB_BASE)
# Put worktree pointer file back in place
	python3 bin/deb-prep-post.py after

deb-trixie:
	@echo ""
	@echo "FPM packaging (trixie) -----------------------------------------------"
	-rm -Rf $(DEB_FILE_TRIXIE)
# Copy .git/ dir from master worktree
	python3 bin/deb-prep-post.py before
# Make package
	fpm   \
	--verbose   \
	--input-type dir   \
	--output-type deb   \
	--name $(DEB_NAME_TRIXIE)   \
	--version $(DEB_VERSION_TRIXIE)   \
	--package $(DEB_FILE_TRIXIE)   \
	--url "$(GIT_SOURCE_URL)"   \
	--vendor "$(DEB_VENDOR)"   \
	--maintainer "$(DEB_MAINTAINER)"   \
	--description "$(DEB_DESCRIPTION)"   \
	--depends "git-annex"   \
	--depends "git-core"   \
	--depends "imagemagick"   \
	--depends "libexempi8"   \
	--depends "libssl-dev"   \
	--depends "libxml2"   \
	--depends "libxml2-dev"   \
	--depends "libxslt1-dev"   \
	--depends "libz-dev"   \
	--depends "pmount"   \
	--depends "python3-dev"   \
	--depends "python3-pip"   \
	--depends "python3-venv"   \
	--depends "udisks2"   \
	--after-install "bin/fpm-after-install.sh"   \
	--chdir $(INSTALL_CMDLN)   \
	bin=$(DEB_BASE)   \
	conf/ddrlocal.cfg=etc/ddr/ddrlocal.cfg   \
	conf/README-logs=$(LOG_BASE)/README  \
	conf=$(DEB_BASE)   \
	COPYRIGHT=$(DEB_BASE)   \
	ddr-cmdln-assets=$(DEB_BASE)   \
	../ddr-defs=opt   \
	../densho-vocab=opt   \
	files=$(DEB_BASE)   \
	.git=$(DEB_BASE)   \
	.gitattributes=$(DEB_BASE)   \
	.gitignore=$(DEB_BASE)   \
	INSTALL.rst=$(DEB_BASE)   \
	LICENSE=$(DEB_BASE)   \
	Makefile=$(DEB_BASE)   \
	mypy.ini=$(DEB_BASE)   \
	NOTES=$(DEB_BASE)   \
	pyproject.toml=$(DEB_BASE)   \
	README.rst=$(DEB_BASE)   \
	src=$(DEB_BASE)   \
	tests=$(DEB_BASE)   \
	tox.ini=$(DEB_BASE)   \
	venv=$(DEB_BASE)   \
	VERSION=$(DEB_BASE)
# Put worktree pointer file back in place
	python3 bin/deb-prep-post.py after
