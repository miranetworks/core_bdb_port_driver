!!! Things relating to local app config has changed !!!

Synopsis
========
We are using cuttlefish for generating our `app.config` and `vm.args` files on application startup (i.e. `/etc/init.d/mira-core-bdb-driver start`).

On startup we check to see if `/home/developer/mira_bdb_port_driver/bin/cuttlefish` and `/etc/default/mira-core-bdb-driver.conf exist, and then use cuttlefish to generate `app.DATETIME.conf` and `vm.DATETIME.args` in /home/developer/mira_bdb_port_driver/etc/.

Cuttlefish overview
========

* cuttlefish generate app.config and vm.args file using as input the `/etc/default/mira-core-bdb-driver.conf` file and the `.schema` files located in `/home/developer/mira_bdb_port_driver/lib/`.

* It dynamically generate new filenames for app.config and vm.args that contain a date/time stamp, and it echo back a string that you then should use to construct the commandline for starting your VM.

* Settings in `/home/developer/mira_bdb_port_driver/etc/advanced.config` will override the settings in `/etc/default/mira-core-bdb-driver.conf` and any defaults defined in the cuttlefish schema files.

* If `/home/developer/mira_bdb_port_driver/etc/app.config` and `/home/developer/mira_bdb_port_driver/etc/vm.args exist`, cuttlfish will not take effect, and the files will be used "as-is" to start the app. 

The purpose of this directory
=========

Any host-specific OS level config files can live here i.e.

HOST001.mira-core-bdb-driver.conf
HOST002.mira-core-bdb-driver.conf
HOST002.mira-core-bdb-driver.conf
HOST003.mira-core-bdb-driver.conf

Where HOST001 - HOST003 refer to the value returned by running 
```
hostname -s
```
on the target host.

The package manager needs to install `/etc/default/mira-core-bdb-driver.conf`

So it will either use `/home/developer/mira_bdb_port_driver/config/$(hostname -s).mira-core-bdb-driver.conf` or `/home/developer/mira_bdb_port_driver/etc/mira-core-bdb-driver.conf`

EOF
