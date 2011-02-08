#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/clean.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

rm -fr Emakefile
rm -fr erl_crash.dump
rm -fr *.rel
rm -fr ebin/*.beam
rm -fr ebin/*.app
rm -fr *access*
rm -fr *.log
rm -fr *.tar.gz
rm -fr *.script
rm -fr *.boot
rm -fr *.out

cd ${START_DIR}

#EOF
