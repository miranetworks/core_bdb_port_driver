#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/make.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

cat ${BUILD_DIR}/auto_gen/app.template | sed "s|%APP_NAME%|${APP_NAME}|g" | sed "s|%APP_DVER%|${APP_DVER}|g" | sed "s|%APP_MODULES%||g" > ${BUILD_DIR}/ebin/${APP_NAME}.app

if [ ${ERTS_VER} -ge 574 ]; then
    DONT_USE_EUNIT=""
else
    DONT_USE_EUNIT="{d, 'DONT_USE_EUNIT'},"
fi

cat ${BUILD_DIR}/auto_gen/Emakefile.template | sed "s|%DONT_USE_EUNIT%|${DONT_USE_EUNIT}|g" > ${BUILD_DIR}/Emakefile


rm -fr ${BUILD_DIR}/make.sh.out

$BINDIR/erl -noshell -make > ${BUILD_DIR}/make.sh.out

RES=`cat ${BUILD_DIR}/make.sh.out | grep -vE "Recompile:" | wc -l`

if [ ${RES} -eq 0 ]; then
    cd ${START_DIR}
    exit 0
else 
    cat ${BUILD_DIR}/make.sh.out | grep -v "Recompile:" | sed "s|^|${BUILD_DIR}/|g"
    cd ${START_DIR}
    exit 0
fi

#EOF
