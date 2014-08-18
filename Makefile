
.PHONY: all deps app clean test

./ebin/mira_bdb_port_driver.app: src/*.erl include/*.hrl test/*.erl
	./rebar compile

deps:
	./rebar get-deps

app: deps ./ebin/mira_bdb_port_driver.app c_src/kvs_bdb_drv.c c_src/kvs_bdb_drv.h

run: app
	erl -smp enable +A 4 -pa ./ebin -pa ./deps/*/ebin -sname bdbdev@$(shell hostname -s)

benchmark: app
	erl -smp enable +A 16 -pa ./ebin -pa ./deps/*/ebin -sname bdbdev2@$(shell hostname -s) -s bdb_store_benchmark
#	erl -smp enable +S 2:2 +sbt ns +A 2 -pa ./ebin -pa ./deps/*/ebin -sname bdbdev2@$(shell hostname -s) -s bdb_store_benchmark -no-shell


clean:
	rm -fr priv/*.so
	rm -fr c_src/*.o
	rm -fr .eunit
	rm -fr erl_crash.dump
	./rebar clean

test: app
	mkdir -p .eunit
	./rebar skip_deps=true eunit

all: clean app test
	@echo "Done."
