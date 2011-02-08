#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/run.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

echo "ROOTDIR = ${ROOTDIR}"

$BINDIR/erl -pa ./ebin -s mira_ekvs_bdb_client -config config/mira_ekvs_bdb_client

cd ${START_DIR}

#EOF
