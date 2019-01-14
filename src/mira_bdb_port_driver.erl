-module(mira_bdb_port_driver).

-export([start/0, start_link/0, stop/0]).

-define(START_DEPS, [

    sasl

    ,asn1,crypto,public_key,ssl,inets

    ,xmerl,compiler,syntax_tools

    ]).

start_link() ->
    start_deps(),
    mira_bdb_port_driver_sup:start_link().

start() ->
    start_deps().

stop() ->
    stop_deps().

ensure_started(App) ->
    case application:start(App) of
    ok ->
         ok;
    {error, {already_started, App} } ->
         ok
    end.

start_deps() ->
    [ok = ensure_started(D) || D <- ?START_DEPS],
    ok.

stop_deps() ->
    [ok = application:stop(D) || D <- lists:reverse(?START_DEPS)],
    ok.

%EOF
