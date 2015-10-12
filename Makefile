# Generated by codegen_tool

SHELL := /bin/bash # Allows for more functionality than /bin/sh

.PHONY: all deps clean deps-clean test release new-test gen-config ctags analyze 

APP_VERSION = $(shell grep vsn src/mira_bdb_port_driver.app.src | grep -Eo '["].+["]' | sed 's|"||g')

all: deps ctags src/mira_bdb_port_driver.app.src
	@./rebar -q compile

deps:
	@./rebar -q get-deps

clean:
	@./rebar -r -q clean
	@rm -rf .generated.configs/
	@rm -rf log/*
	@rm -rf mnesia_*
	@rm -f erl_crash.dump
	@rm -f mira-core-bdb-driver.conf
	@rm -f mira-core-bdb-driver_*.deb

deps-clean: clean
	@./rebar -q delete-deps

run: all
	mkdir -p ./log
	mkdir -p ./priv
	rm -fr priv/ssl
	./start.sh

release: all
	@./rebar -q --force generate


citest: test

test: all
	rm -rf .eunit/
	@./rebar skip_deps=true eunit $(if $(suite),suite=$(suite))

#to create a new test suite: make new-test suite=xyz (Where xyz is the module name)
new-test:
	./test/new_suite.sh $(suite)

ctags:
	- ctags -R src/ test/ include/

analyze: all
	dialyzer ebin/ --fullpath --verbose -Wno_unused

install:
	mkdir -p $(DESTDIR)/home/developer/lib/$(APP_VERSION)


include docker/docker.mk

