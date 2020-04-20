INSTALL
=======

For more complete instructions, see the "Admins/Workstations" section
in `ddr-manual <https://github.com/densho/ddr-manual/>`_.

The `ddr-cmdln` install and update scripts below have been tested on
Debian 8.0 running as a VM in VirtualBox.


Three Ways To Install
---------------------

There are three ways to install `ddr-cmdln`:

- package file (.deb)
- package repository (apt-get)
- manual (git clone)


Package File Install (.deb)
---------------------------

If you have a `ddrcmdln-BRANCH_VERSION_ARCH.deb` file you can install
using the `gdebi` command.  The `virtualenv` is installed ready to go
and Debian packaged dependencies (Nginx, Redis, etc) are automatically
installed as required.
::
    $ sudo apt-get install gdebi
    $ sudo gdebi ddrcmdln-BRANCH_VERSION_ARCH.deb
    ...

The result is the same as a manual install but is faster since you
don't have to build the virtualenv and lets you completely remove the
install if you so choose.

After the install completes you can use `make` commands to manage the
installation.

NOTE: you will **not** receive automatic updates from the repository!

**Uninstalling**

See the "Uninstalling" heading under the next section.


Package Repository Install (apt-get)
------------------------------------

It is recommended to install `ddr-cmdln` from a package repository,
since your install will receive upgrades automatically along with
other packages.

**Adding the Repository**

To use our repository you must first add the packaging signing key
using the `apt-key` tool and then add the repository itself to your
list of APT sources. Commands for accomplishing this are listed below
(for completeness we include commands to install curl and the apt
tools - you may already have these installed).
::
    $ sudo apt-get update && sudo apt-get install curl apt-transport-https gnupg
    ...
    $ sudo curl -s http://packages.densho.org/debian/keys/archive.asc | sudo apt-key add -
    ...
    $ echo "deb http://packages.densho.org/debian/ jessie main" | sudo tee /etc/apt/sources.list.d/packages_densho_org_debian.list
    ...

**Installing the Package**

You can now install the DDR Editor with a single command:
::
    $ sudo apt-get update && sudo apt-get install ddrcmdln-master
    ...

If you wish to use the develop branch instead of the master branch,
remove `ddrcmdln-master` and install `ddrcmdln-develop`.  Switching
branches in a package install is not recommended, as updates will
probably damage your install.  If you want to switch branches you
should consider a source install.

**Uninstalling**

A normal `apt-get remove` uninstalls the software from your system,
leaving config and data files in place.
::
    $ sudo apt-get remove ddrcmdln-master
    ...

To completely remove all files installed as part of `ddr-cmdln`
(e.g. configs, static, and media files), use `apt-get purge`.
IMPORTANT: this removes the `/media/` directory which contains your
data!
::
    $ sudo apt-get purge ddrcmdln-master
    ...
    $ sudo rm /etc/apt/sources.list.d/packages_densho_org_debian.list && apt-get update
    ...


Manual Install (git clone)
--------------------------

You can also install manually by cloning the `ddr-cmdln` Git
repository.  This method requires you to build the `virtualenv` and
install prerequisites but is the best method if you are going to work
on the `ddr-cmdln` project.

Technically you can clone `ddr-cmdln` anywhere you want but `make
install` will attempt to install the app in `/opt/ddr-cmdln` so you
might as well just clone it to that location.
::
    $ sudo apt-get update && apt-get upgrade
    $ sudo apt-get install git make
    $ sudo git clone https://github.com/densho/ddr-cmdln.git /opt/ddr-cmdln
    $ cd /opt/ddr-cmdln/

If you want to modify any of the files you must give yourself permissions.
::
   $ sudo chown -R USER.USER /opt/ddr-cmdln

Create a `ddr` user.  The DDR application will run as this user, and
all repository files will be owned by this user.
::
    $ cd /opt/ddr-cmdln/
    $ sudo adduser ddr

Git-cloning is a separate step from the actual installation.  GitHub
may ask you for passwords.
::
    $ cd /opt/ddr-cmdln/
    $ sudo make get

This step installs dependencies from Debian packages, installs Python
dependencies in a virtualenv, and places static assets and config
files in their places.
::
    $ cd /opt/ddr-cmdn/
    $ sudo make install

Problems installing `lxml` may be due to memory constraints,
especially if Elasticsearch is running, which it will be if you've
installed `ddr-local` and run `make enable-bkgnd`.

Install config files.
::
    $ cd /opt/ddr-cmdn/
    $ sudo make install-configs


POST-INSTALL
============


Usage
-----

In order to use `ddr-cmdln` you must activate its `virtualenv` which
is located in `/opt/ddr-cmdln/venv/ddrcmdln`.
::
    USER@HOST:~$ su ddr
    ddr@HOST:~$ source /opt/ddr-cmdln/venv/ddrcmdln/bin/activate
    (ddrcmdln)ddr@HOST:~$

Several configuration settings for `ddr-cmdln` are different from
those in `ddr-local`.  Edit `/etc/ddr/ddrlocal-local.cfg` and change
at least these values:
::
    [cmdln]
    install_path=/opt/ddr-cmdln
    repo_models_path=/opt/ddr-cmdln/ddr-defs/


Makefile
--------

The `ddr-cmdln` makefile has a number of useful options for
installing, removing, stopping, restarting, and otherwise interacting
with parts of the editor.  Run `make` with no arguments for a list or
(better) look through the Makefile itself.
::
    $ make


Settings Files
--------------

Default settings are in `/etc/ddr/ddrlocal.cfg`.  Please do not edit
this file.  Settings in `/etc/ddr/ddrlocal-local.cfg` will override
the defaults.

Rather than listing settings files here, examine the `deb` task in
`Makefile`, as all the config files are listed there.


Models Definitions
------------------

NOTE: `ddr-defs` is installed automatically by `make get`.

If you installed from a package the latest model definitions should be
installed in the `ddr-cmdln` directory.  If you installed from source
the definitions should have been downloaded as part of `make get`.  If
for some reason they are absent you can clone a copy thusly:
::
    $ sudo make get-ddr-defs

If you want to install the model definitions in some non-standard
location, you can clone them:
::
    $ sudo git clone https://github.com/densho/ddr-defs.git /PATH/TO/ddr-defs/


Controlled Vocabularies
-----------------------

NOTE: `densho-vocab` is installed automatically by `make get`

::
   $ sudo make get-densho-vocab

If you want to install the model definitions in some non-standard
location, you can clone them:
::
    $ sudo git clone https://github.com/densho/densho-vocab.git /PATH/TO/densho-vocab/


Network Config
--------------

The Makefile can install a networking config file which sets the VM
to use a standard IP address (192.168.56.101).
::
    $ sudo make network-config
    $ sudo reboot

Network config will take effect after the next reboot.


VirtualBox Guest Additions
--------------------------

The Makefile can install VirtualBox Guest Additions, which is required
for accessing shared directories on the host system.
::
    $ sudo make vbox-guest

This step requires you to click "Devices > Insert Guest Additions CD
Image" in the device window.


Gitolite keys
-------------

The `ddr` user requires SSL keys in order to synchronize local
collection repositories with those on the main Gitolite server.  Setup
is beyond this INSTALL so please see `ddr-manual`.
