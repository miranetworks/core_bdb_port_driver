#!/bin/bash

PATH=/bin:/usr/bin:/sbin:/usr/sbin

DBPIDFILE=$1

PID=`cat $DBPIDFILE`

PID=`ps -ef | grep ${PID} | grep "kvs_bdb" | awk '{print $2}'`

if [ -z "${PID}" ]; then
    echo "No process with pid [${PID}]!"

    exit 1

else

    kill -15 ${PID}

    sleep 5

    echo -n "" > ${DBPIDFILE}

    exit 0

fi

#EOF
