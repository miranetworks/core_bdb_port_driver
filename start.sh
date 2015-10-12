#!/bin/sh 
cd `dirname $0`

GEN_CFG_DIR=./.generated.configs/
APP_CONF=$GEN_CFG_DIR/mira-core-bdb-driver.conf

mkdir -p $GEN_CFG_DIR
rm -fr $GEN_CFG_DIR/app.*.config
rm -fr $GEN_CFG_DIR/vm.*.args

exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -s mira_bdb_port_driver 
