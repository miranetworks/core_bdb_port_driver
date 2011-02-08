#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/test.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

./clean.sh

./make.sh
RES=$?

if [ ! $RES -eq 0 ]; then
    echo "${BUILD_DIR}/make.sh failed"
    cd ${START_DIR}
    exit 1
fi

rm -fr "${BUILD_DIR}/test.sh.out"

$BINDIR/erl -noshell -pa ./ebin -s ${APP_NAME}_tests_run_all "test" > "${BUILD_DIR}/test.sh.out"


RES=`cat "${BUILD_DIR}/test.sh.out" | grep -iE "fail" | wc -l`

if [ ${RES} -eq 0 ]; then
    cd ${START_DIR}
    exit 0
else
    cat "${BUILD_DIR}/test.sh.out"
    cd ${START_DIR}
    exit 1
fi


cd ${START_DIR}

#EOF
