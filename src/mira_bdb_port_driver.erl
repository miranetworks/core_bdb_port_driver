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
    start_deps(),
    application:start(mira_cc_subsdb_fe).

stop() ->
    Res = application:stop(mira_cc_subsdb_fe),
    stop_deps(),
    Res.

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
