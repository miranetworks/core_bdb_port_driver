
.PHONY: all deps app clean test

./ebin/mira_bdb_port_driver.app: src/*.erl include/*.hrl test/*.erl
	./rebar compile

deps:
	./rebar get-deps

app: deps ./ebin/mira_bdb_port_driver.app

run: app
	erl -smp disable -pa ./ebin -pa ./deps/*/ebin -sname bdbdev@$(shell hostname -s)

clean:
	rm -fr .eunit
	rm -fr erl_crash.dump
	./rebar clean

test: app
	mkdir -p .eunit
	./rebar skip_deps=true eunit

all: clean app test
	@echo "Done."
