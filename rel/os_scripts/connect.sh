#!/bin/bash
set -e

cd "${BASH_SOURCE%/*}/.."
. erlang_env.sh

bin/mira_bdb_port_driver attach
