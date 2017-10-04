.PHONY: all test citest clean distclean tags run release

all:
	rebar3 compile

test:
	rebar3 do eunit $(if $(suite),--suite=$(suite)), cover

citest: clean
	rebar3 as test do xref, eunit, covertool generate

clean:
	rebar3 clean

distclean: clean
	rm -fR .rebar3/ _build/ rebar.lock tags

tags:
	ctags -R include/ src/

run:
	rebar3 release
	_build/default/rel/mira_bdb_port_driver/bin/mira_bdb_port_driver console

release:
	rebar3 as prod release

include docker/docker.mk

include debian/debian.mk
