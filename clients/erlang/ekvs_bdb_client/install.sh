#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/install.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

source ./env

./build.sh

if [ -e ${RELFILE}.tar.gz ]; then

    TGT="${APP_NAME}-${APP_DVER}"

    cp ${BUILD_DIR}/${RELFILE}.tar.gz ${ROOTDIR}/releases/.

    if [ -e ${ROOTDIR}/releases/${RELFILE}.tar.gz ]; then

        ${BINDIR}/erl -noshell -eval "{ok, _} = release_handler:start_link(), release_handler:remove_release(\"${TGT}\"), {ok, \"${TGT}\"} = release_handler:unpack_release(\"${RELFILE}\"), init:stop()."

        cd ${START_DIR}
        exit 0

    else
        echo "copy to ${ROOTDIR}/releases/${RELFILE}.tar.gz failed"
        cd ${START_DIR}
        exit 1
    fi

else

    echo "${BUILD_DIR}/${RELFILE}.tar.gz not found"
    cd ${START_DIR}
    exit 1

fi



#EOF
