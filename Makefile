PROJECT=ddr
APP=ddrcmdln
USER=ddr

SHELL = /bin/bash
DEBIAN_CODENAME := $(shell lsb_release -sc)
DEBIAN_RELEASE := $(shell lsb_release -sr)
VERSION := $(shell cat VERSION)

GIT_SOURCE_URL=https://github.com/densho/ddr-cmdln

# current branch name minus dashes or underscores
PACKAGE_BRANCH := $(shell git rev-parse --abbrev-ref HEAD | tr -d _ | tr -d -)
# current commit hash
PACKAGE_COMMIT := $(shell git log -1 --pretty="%h")
# current commit date minus dashes
PACKAGE_TIMESTAMP := $(shell git log -1 --pretty="%ad" --date=short | tr -d -)

SRC_REPO_CMDLN=https://github.com/densho/ddr-cmdln.git
SRC_REPO_DEFS=https://github.com/densho/ddr-defs.git

INSTALL_BASE=/opt
INSTALL_CMDLN=$(INSTALL_BASE)/ddr-cmdln
INSTALL_DEFS=$(INSTALL_CMDLN)/ddr-defs

VIRTUALENV=$(INSTALL_CMDLN)/venv/ddrcmdln

CONF_BASE=/etc/ddr
CONF_PRODUCTION=$(CONF_BASE)/ddrlocal.cfg
CONF_LOCAL=$(CONF_BASE)/ddrlocal-local.cfg

LOG_BASE=/var/log/ddr

DDR_REPO_BASE=/var/www/media/ddr

MEDIA_BASE=/var/www
MEDIA_ROOT=$(MEDIA_BASE)/media

FPM_BRANCH := $(shell git rev-parse --abbrev-ref HEAD | tr -d _ | tr -d -)
FPM_ARCH=amd64
FPM_NAME=$(APP)-$(FPM_BRANCH)
FPM_FILE=$(FPM_NAME)_$(VERSION)_$(FPM_ARCH).deb
FPM_VENDOR=Densho.org
FPM_MAINTAINER=<geoffrey.jost@densho.org>
FPM_DESCRIPTION=Densho Digital Repository CLI tools
FPM_BASE=opt/ddr-cmdln


.PHONY: help


help:
	@echo "--------------------------------------------------------------------------------"
	@echo "ddr-cmdln make commands"
	@echo ""
	@echo "Most commands have subcommands (ex: install-ddr-cmdln, restart-supervisor)"
	@echo ""
	@echo "get     - Clones ddr-local, ddr-cmdln, ddr-defs, wgets static files & ES pkg."
	@echo "install - Performs complete install. See also: make howto-install"
	@echo ""
	@echo "vbox-guest     - Installs VirtualBox Guest Additions"
	@echo "network-config - Installs standard network conf (CHANGES IP TO 192.168.56.101!)"
	@echo "get-ddr-defs   - Downloads ddr-defs to $(INSTALL_DEFS)."
	@echo "branch BRANCH=[branch] - Switches ddr-local and ddr-cmdln repos to [branch]."
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
	@echo "ufw enable"
	@echo "apt-get install --assume-yes make"
	@echo "git clone $(SRC_REPO_CMDLN) $(INSTALL_CMDLN)"
	@echo "cd $(INSTALL_CMDLN)"
	@echo "make install"
	@echo "#make branch BRANCH=develop"
	@echo "#make install"
	@echo "# Place copy of 'ddr' repo in $(DDR_REPO_BASE)/ddr."
	@echo "#make install-defs"


get: get-app get-ddr-defs

install: install-prep install-app install-configs

uninstall: uninstall-app uninstall-configs

clean: clean-app


install-prep: ddr-user install-core git-config install-misc-tools

ddr-user:
	-addgroup ddr plugdev
	-addgroup ddr vboxsf
	printf "\n\n# ddrlocal: Activate virtualnv on login\nsource $(VIRTUALENV)/bin/activate\n" >> /home/ddr/.bashrc; \

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
	-cp $(INSTALL_CMDLN)/conf/network-interfaces /etc/network/interfaces
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


install-virtualenv:
	@echo ""
	@echo "install-virtualenv -----------------------------------------------------"
	apt-get --assume-yes install python-six python-pip python-virtualenv python-dev
	test -d $(VIRTUALENV) || virtualenv --distribute --setuptools $(VIRTUALENV)
	source $(VIRTUALENV)/bin/activate; \
	pip install -U bpython appdirs blessings curtsies greenlet packaging pygments pyparsing setuptools wcwidth
#	virtualenv --relocatable $(VIRTUALENV)  # Make venv relocatable


install-dependencies: install-core install-misc-tools install-daemons install-git-annex
	@echo ""
	@echo "install-dependencies ---------------------------------------------------"
	apt-get --assume-yes install python-pip python-virtualenv
	apt-get --assume-yes install python-dev
	apt-get --assume-yes install git-core git-annex libxml2-dev libxslt1-dev libz-dev pmount udisks
	apt-get --assume-yes install imagemagick libexempi3 libssl-dev python-dev libxml2 libxml2-dev libxslt1-dev supervisor

mkdirs: mkdir-ddr-cmdln mkdir-ddr-local


get-app: get-ddr-cmdln

install-app: install-git-annex install-virtualenv install-ddr-cmdln install-configs

uninstall-app: uninstall-ddr-cmdln uninstall-configs

clean-app: clean-ddr-cmdln


install-git-annex:
	apt-get --assume-yes install git-core git-annex

get-ddr-cmdln:
	@echo ""
	@echo "get-ddr-cmdln ----------------------------------------------------------"
	if test -d $(INSTALL_CMDLN); \
	then cd $(INSTALL_CMDLN) && git pull; \
	else cd $(INSTALL_BASE) && git clone $(SRC_REPO_CMDLN); \
	fi

setup-ddr-cmdln:
	source $(VIRTUALENV)/bin/activate; \
	cd $(INSTALL_CMDLN)/ddr && python setup.py install

install-ddr-cmdln: install-virtualenv mkdir-ddr-cmdln
	@echo ""
	@echo "install-ddr-cmdln ------------------------------------------------------"
	apt-get --assume-yes install git-core git-annex libxml2-dev libxslt1-dev libz-dev pmount udisks
	source $(VIRTUALENV)/bin/activate; \
	cd $(INSTALL_CMDLN)/ddr && python setup.py install
	source $(VIRTUALENV)/bin/activate; \
	cd $(INSTALL_CMDLN)/ddr && pip install -U -r $(INSTALL_CMDLN)/requirements.txt

mkdir-ddr-cmdln:
	@echo ""
	@echo "mkdir-ddr-cmdln --------------------------------------------------------"
	-mkdir $(LOG_BASE)
	chown -R ddr.root $(LOG_BASE)
	chmod -R 755 $(LOG_BASE)
	-mkdir -p $(MEDIA_ROOT)
	chown -R ddr.root $(MEDIA_ROOT)
	chmod -R 755 $(MEDIA_ROOT)

uninstall-ddr-cmdln: install-virtualenv
	@echo ""
	@echo "uninstall-ddr-cmdln ----------------------------------------------------"
	source $(VIRTUALENV)/bin/activate; \
	cd $(INSTALL_CMDLN)/ddr && pip uninstall -y -r $(INSTALL_CMDLN)/requirements.txt

clean-ddr-cmdln:
	-rm -Rf $(INSTALL_CMDLN)/ddr/build

mkdir-ddr-local:
	@echo ""
	@echo "mkdir-ddr-local --------------------------------------------------------"
# logs dir
	-mkdir $(LOG_BASE)
	chown -R ddr.root $(LOG_BASE)
	chmod -R 755 $(LOG_BASE)
# media dir
	-mkdir -p $(MEDIA_ROOT)
	chown -R ddr.root $(MEDIA_ROOT)
	chmod -R 755 $(MEDIA_ROOT)


get-ddr-defs:
	@echo ""
	@echo "get-ddr-defs -----------------------------------------------------------"
	if test -d $(INSTALL_DEFS); \
	then cd $(INSTALL_DEFS) && git pull; \
	else cd $(INSTALL_CMDLN) && git clone $(SRC_REPO_DEFS) $(INSTALL_DEFS); \
	fi

branch:
	cd $(INSTALL_CMDLN)/ddr; python ./bin/git-checkout-branch.py $(BRANCH)


install-configs:
	@echo ""
	@echo "install-configs --------------------------------------------------------"
# base settings file
	-mkdir /etc/ddr
	cp $(INSTALL_CMDLN)/conf/ddrlocal.cfg $(CONF_PRODUCTION)
	chown root.root $(CONF_PRODUCTION)
	chmod 644 $(CONF_PRODUCTION)
	touch $(CONF_LOCAL)
	chown ddr.root $(CONF_LOCAL)
	chmod 640 $(CONF_LOCAL)

uninstall-configs:
	-rm $(CONF_PRODUCTION)


git-status:
	@echo "------------------------------------------------------------------------"
	cd $(INSTALL_CMDLN) && git status


# http://fpm.readthedocs.io/en/latest/
# https://stackoverflow.com/questions/32094205/set-a-custom-install-directory-when-making-a-deb-package-with-fpm
# https://brejoc.com/tag/fpm/
deb:
	@echo ""
	@echo "FPM packaging ----------------------------------------------------------"
	-rm -Rf $(FPM_FILE)
	virtualenv --relocatable $(VIRTUALENV)  # Make venv relocatable
	fpm   \
	--verbose   \
	--input-type dir   \
	--output-type deb   \
	--name $(FPM_NAME)   \
	--version $(VERSION)   \
	--package $(FPM_FILE)   \
	--url "$(GIT_SOURCE_URL)"   \
	--vendor "$(FPM_VENDOR)"   \
	--maintainer "$(FPM_MAINTAINER)"   \
	--description "$(FPM_DESCRIPTION)"   \
	--depends "git-annex"   \
	--depends "git-core"   \
	--depends "imagemagick"   \
	--depends "libexempi3"   \
	--depends "libssl-dev"   \
	--depends "libwww-perl"   \
	--depends "libxml2"   \
	--depends "libxml2-dev"   \
	--depends "libxslt1-dev"   \
	--depends "libz-dev"   \
	--depends "pmount"   \
	--depends "python-dev"   \
	--depends "python-pip"   \
	--depends "python-six"   \
	--depends "python-virtualenv"   \
	--depends "udisks"   \
	--after-install "bin/after-install.sh"   \
	--chdir $(INSTALL_CMDLN)   \
	conf/ddrlocal.cfg=etc/ddr/ddrlocal.cfg   \
	conf/README-logs=$(LOG_BASE)/README  \
	conf=$(FPM_BASE)   \
	COPYRIGHT=$(FPM_BASE)   \
	ddr=$(FPM_BASE)   \
	ddr-defs=$(FPM_BASE)   \
	.git=$(FPM_BASE)   \
	.gitignore=$(FPM_BASE)   \
	INSTALL.rst=$(FPM_BASE)   \
	LICENSE=$(FPM_BASE)   \
	Makefile=$(FPM_BASE)   \
	README.rst=$(FPM_BASE)   \
	venv=$(FPM_BASE)   \
	VERSION=$(FPM_BASE)
