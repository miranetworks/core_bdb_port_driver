#!/bin/bash

RES=`wget "http://localhost:8070/nagios" -O - -o /dev/null`

case "${RES}" in
mira_bdb_port_driver\ OK*)
    echo "OK"
    ;;
*)
    echo "ERROR - ${RES}"
    ;;
esac
