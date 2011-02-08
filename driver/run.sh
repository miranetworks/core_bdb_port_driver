#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/run.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

echo "ROOTDIR = ${ROOTDIR}"

$BINDIR/erl -pa ./ebin -s mira_bdb_port_driver -config config/mira_bdb_port_driver

cd ${START_DIR}

#EOF
