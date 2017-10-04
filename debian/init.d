#!/bin/sh
### BEGIN INIT INFO
# Provides:          mira_bdb_port_driver
# Required-Start:    $local_fs $remote_fs
# Required-Stop:     $local_fs $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Mira BDB Port Driver
# Description:       Mira BDB Port Driver
### END INIT INFO

set -e


case $1 in
    start|start_boot|console|console_clean|console_boot|foreground)
        mkdir -p -m 0777 /tmp/erl_pipes
        export RELX_REPLACE_OS_VARS=true
        export RELX_OUT_FILE_PATH="/home/bdb_port_driver/mira_bdb_port_driver/releases/default"
        ;;
    *)
        ;;
esac


su bdb_port_driver -c 'cd /home/bdb_port_driver/mira_bdb_port_driver; . erlang_env.sh; ./bin/mira_bdb_port_driver "$@"' -- $0 "$@"
