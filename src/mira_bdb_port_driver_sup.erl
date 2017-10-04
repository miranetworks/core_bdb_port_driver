-module(mira_bdb_port_driver_sup).

-behaviour(supervisor).

% API
-export([
         start_link/0
        ]).

% Callbacks
-export([
         init/1
        ]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init(_) ->
    ChildSpecs = 
    [
    ],
    {ok, {#{strategy => one_for_one}, ChildSpecs}}.
