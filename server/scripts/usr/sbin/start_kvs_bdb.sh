#!/bin/bash


PATH=/bin:/usr/bin:/sbin:/usr/sbin
EXE=/usr/sbin/kvs_bdb

DBNAME=$1
DBHOME=$2
DBPIDFILE=$3
IP=$4
PORT=$5
LOGGING=$6

nohup ${EXE} -d ${DBHOME} -n ${DBNAME} -i ${IP} -p ${PORT} ${LOGGING} > /dev/null &
PID=$!

echo -n ${PID} > ${DBPIDFILE}

#EOF
