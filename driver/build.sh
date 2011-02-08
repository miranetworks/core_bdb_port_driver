#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/build.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

KERNEL_DVER=`ls ${ROOTDIR}/lib | grep -m 1 -E "^kernel" | sed 's/kernel-//g'`
STDLIB_DVER=`ls ${ROOTDIR}/lib | grep -m 1 -E "^stdlib" | sed 's/stdlib-//g'`

./clean.sh

./make.sh
RES=$?

if [ ! $RES -eq 0 ]; then
    echo "${BUILD_DIR}/make.sh failed"
    cd ${START_DIR}
    exit 1
fi


cat ${BUILD_DIR}/auto_gen/rel.template | sed "s|%APP_NAME%|${APP_NAME}|g" | sed  "s|%ERTS_DVER%|${ERTS_DVER}|g" | sed "s|%APP_VER%|${APP_VER}|g" | sed "s|%APP_DVER%|${APP_DVER}|g" | sed "s|%KERNEL_DVER%|${KERNEL_DVER}|g" | sed "s|%STDLIB_DVER%|${STDLIB_DVER}|g" > ${RELFILE}.rel

if [ ! -e "${RELFILE}.rel" ]; then
    echo "${BUILD_DIR}/${RELFILE}.rel not found"
    cd ${START_DIR}
    exit 1
fi

NUM_SRCS=`ls ${BUILD_DIR}/src/*.erl | wc -l`
NUM_TESTS=`ls ${BUILD_DIR}/tests/*.erl | wc -l`
NUM_BEAMS=`ls ${BUILD_DIR}/ebin/*.beam | wc -l`

TOTAL=$(($NUM_SRCS + $NUM_TESTS))

if [ $TOTAL -eq $NUM_BEAMS ]; then

    MODS=`ls ${BUILD_DIR}/ebin | grep -E "[.]beam$" | sed "s|[.]beam|,|g"`

    MODS=`echo $MODS | sed "s|,$||g"`

    cat auto_gen/app.template | sed "s|%APP_NAME%|${APP_NAME}|g" | sed "s|%APP_DVER%|${APP_DVER}|g" | sed "s|%APP_MODULES%|${MODS}|g" > ebin/${APP_NAME}.app

    $BINDIR/erl -noshell -pa ./ebin/ -s bdb_port_driver_build_rel start "${RELFILE}" > /dev/null

    if [ -e ${RELFILE}.tar.gz ]; then

        cd ${START_DIR}
        exit 0

    else

        echo "${BUILD_DIR}/${RELFILE}.tar.gz not found"
        cd ${START_DIR}
        exit 1

    fi

else
    echo "${BUILD_DIR}/build.sh failed"
    cd ${START_DIR}
    exit 1
fi

#EOF
