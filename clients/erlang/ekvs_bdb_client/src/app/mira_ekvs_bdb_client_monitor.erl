% $Id$
-module(mira_ekvs_bdb_client_monitor).
-behaviour(supervisor).

-export([
        start_link/0,
        init/1
        ]).

% Interface functions
start_link() -> 
	Res = supervisor:start_link({global, ?MODULE}, ?MODULE, []),

    ekvs_bdb_client_yaws_fe:start(),

    Res.

init(_Args) -> 

	RestartSpec = {one_for_one, 1, 10},

    Specs = [make_client_spec(C) || C <- pt_util_app:get_app_param(mira_ekvs_bdb_client, stores, [])],

	{ok, { RestartSpec, Specs }}.

make_client_spec({StoreName, Ip, Port, NumSessions})->

    {StoreName, 
        {ekvs_bdb_client_supervisor, start_link, [StoreName, Ip, Port, NumSessions]},
        permanent, infinity, supervisor, [ekvs_bdb_client_supervisor]}.

% EOF
