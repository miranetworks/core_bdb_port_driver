#!/bin/sh 
cd `dirname $0`

GEN_CFG_DIR=./.generated.configs/
SCHEMA_DIR=./rel/schemas/
APP_CONF=$GEN_CFG_DIR/mira-core-bdb-driver.conf

mkdir -p $GEN_CFG_DIR
rm -fr $GEN_CFG_DIR/app.*.config
rm -fr $GEN_CFG_DIR/vm.*.args

if [ ! -f $APP_CONF ]; then
    echo "Generating default cuttlefish config file"
    ./rel/files/gen_config $APP_CONF $SCHEMA_DIR
fi

exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -s mira_bdb_port_driver 
