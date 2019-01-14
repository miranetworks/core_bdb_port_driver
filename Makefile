.PHONY: all test citest clean distclean tags

all:
	rebar3 compile

test:
	rebar3 do eunit $(if $(suite),--suite=$(suite)), cover

citest: clean
	rebar3 as test do eunit, covertool generate

clean:
	rebar3 clean

distclean: clean
	rm -fR .rebar3/ _build/ rebar.lock tags

tags:
	ctags -R include/ src/

include docker/docker.mk

include debian/debian.mk
