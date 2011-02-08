%$Id$
-module(mira_ekvs_bdb_client).
-behaviour(application).

-export([
        start/2,
        start/0,
        stop/1,

        shutdown/0
        ]).

%Interface functions
start(_Type, _Args) ->
	mira_ekvs_bdb_client_monitor:start_link().


start() ->
	application:start(mira_ekvs_bdb_client),
    ok.
   
stop(_State)->
	io:format("~p stopping!~n", [?MODULE]),
	ok.

shutdown()->

    application:stop(yaws),

    application:stop(mira_ekvs_bdb_client),

    init:stop().

%EOF
