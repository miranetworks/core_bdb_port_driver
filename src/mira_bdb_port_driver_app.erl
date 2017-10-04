-module(mira_bdb_port_driver_app).

-behaviour(application).

-export([
         start/2, 
         stop/1
        ]).


start(_StartType, _StartArgs) ->
    mira_bdb_port_driver_sup:start_link().


stop(_State) ->
    ok.
