% $Id$
-module(ekvs_bdb_client_supervisor).
-behaviour(supervisor).

-export([
        start_link/4,
        init/1
        ]).

% Interface functions
start_link(StoreName, Ip, Port, SessionCount) -> 
	supervisor:start_link({global, {?MODULE, StoreName}}, ?MODULE, [StoreName, Ip, Port, SessionCount]).

init([StoreName, Ip, Port, SessionCount]) -> 

	RestartSpec = {one_for_all, 1, 10},

    SessionSupSpec = {session_sup,
                        {ekvs_bdb_client_session_supervisor, start_link, [StoreName, Ip, Port, SessionCount]},
                        permanent, infinity, supervisor, [ekvs_bdb_client_session_supervisor]},

    ClientSpec = {client,
                    {ekvs_bdb_client, start_link, [StoreName, SessionCount]},
                        permanent, 10000, worker, [ekvs_bdb_client]},

	{ok, { RestartSpec, [SessionSupSpec, ClientSpec] }}.

% EOF
