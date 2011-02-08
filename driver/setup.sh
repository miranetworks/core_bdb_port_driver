#!/bin/bash

#$Id$

START_DIR=`pwd`

BUILD_DIR=`echo $0 | sed "s|/setup.sh||"`

cd ${BUILD_DIR}

BUILD_DIR=`pwd`

AVAIL_ERTS=`ls /home/erlang/lib`

ERTS=$1

if [ -z "${ERTS}" ]; then

    echo "The following ERTS installations were found"
    echo "    ${AVAIL_ERTS}"
    echo ""
    read -p "Please select the ERTS you want to build for: " ERTS

fi

ERTS=`echo "${ERTS}" | awk '{print $1}'`

if [ -z "${ERTS}" ]; then
    echo "aborted"
    cd ${START_DIR}
    exit 1
fi


FOUND=`echo "${AVAIL_ERTS}" | grep -E "^${ERTS}$"`

if [ -z "${FOUND}" ]; then
    echo "erts ${ERTS} not found"
    cd ${START_DIR}
    exit 1 

else

    APP_NAME=`cat ${BUILD_DIR}/auto_gen/app_name`

    ERTS_DIR=`ls /home/erlang/lib/${ERTS}/lib/erlang/ | grep "erts"`
    ERTS_DVER=`echo ${ERTS_DIR} | sed 's/erts-//g'`
    ERTS_VER=`echo ${ERTS_DIR} | sed 's/erts-//g' | awk -F"[.]" '{print $1$2$3}'`
    
    ROOTDIR=/home/erlang/lib/${ERTS}/lib/erlang
    RELDIR=${ROOTDIR}/releases

    RELFILE=${APP_NAME}_${APP_VER}_erts_${ERTS_VER}

    echo "#!/bin/bash" > env
    echo "APP_NAME=${APP_NAME}" >> env
    echo "ERTS_DVER=${ERTS_DVER}" >> env
    echo "ERTS_VER=${ERTS_VER}" >> env
    echo "ROOTDIR=${ROOTDIR}" >> env
    echo "BINDIR=\$ROOTDIR/${ERTS_DIR}/bin" >> env
    echo "EMU=beam" >> env
    echo "PROGNAME=\`echo \$0 | sed 's/.*\///'\`" >> env


    echo 'APP_DVER=`cat auto_gen/version`' >> env
    echo 'APP_VER=`echo "${APP_DVER}" | sed "s|[.]||g"`' >> env
    echo 'RELFILE=${APP_NAME}_${APP_VER}_erts_${ERTS_VER}' >> env

    echo "export APP_NAME" >> env
    echo "export ERTS_DVER" >> env
    echo "export ERTS_VER" >> env
    echo "export EMU" >> env
    echo "export ROOTDIR" >> env
    echo "export BINDIR" >> env
    echo "export PROGNAME" >> env
    echo "export APP_VER" >> env
    echo "export APP_DVER" >> env
    echo "export RELFILE" >> env

    #echo "echo \"ROOTDIR = ${ROOTDIR}\"" >> env
    echo "#EOF" >> env

    chmod +x env

    cd ${START_DIR}
    exit 0

fi

#EOF
