#!/bin/bash

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/make.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

mkdir -p "./ebin"

cat ${BUILD_DIR}/auto_gen/app.template | sed "s|%APP_NAME%|${APP_NAME}|g" | sed "s|%APP_DVER%|${APP_DVER}|g" | sed "s|%APP_MODULES%||g" > ${BUILD_DIR}/ebin/${APP_NAME}.app

cat ${BUILD_DIR}/auto_gen/Emakefile.template > ${BUILD_DIR}/Emakefile

ERL_USR_INCLUDE_DIR="${BINDIR}/../../usr/include"
ERL_EI=`ls ${BINDIR}/../../lib/ | grep "erl_interface-"`
ERL_EI_INCLUDE_DIR="${BINDIR}/../../lib/${ERL_EI}/include"

ERL_INCLUDES="-I${ERL_USR_INCLUDE_DIR} -I${ERL_EI_INCLUDE_DIR}"

cat ${BUILD_DIR}/auto_gen/Makefile.priv | sed "s|%ERLANG_INCLUDES%|${ERL_INCLUDES}|g" > ${BUILD_DIR}/priv/Makefile

rm -fr ${BUILD_DIR}/make.sh.out

cd priv

make >  ${BUILD_DIR}/make.sh.out
RES=$?

if [ ${RES} -eq 2 ]; then
    cd ${START_DIR}
    cat ${BUILD_DIR}/make.sh.out
    exit 1

fi

cd ../

$BINDIR/erl -noshell -make >> ${BUILD_DIR}/make.sh.out

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
