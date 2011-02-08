% $Id$
-module(ekvs_bdb_client_session_supervisor).
-behaviour(supervisor).

-export([
        start_link/4,
        init/1
        ]).

% Interface functions
start_link(StoreName, Ip, Port, SessionCount) -> 
	supervisor:start_link({global, {?MODULE, StoreName}}, ?MODULE, [StoreName, Ip, Port, SessionCount]).

init([StoreName, Ip, Port, SessionCount]) -> 

	RestartSpec = {one_for_one, 1, 10},

    CntrlSession = make_session_spec(StoreName, "control", Ip, Port),

    SessionSpecs = [make_session_spec(StoreName, Idx, Ip, Port) || Idx <- lists:seq(1, SessionCount)],

	{ok, { RestartSpec, [CntrlSession | SessionSpecs] }}.

make_session_spec(StoreName, Id, Ip, Port) ->

    {{session, Id}, 
        {ekvs_bdb_client_session, start_link, [StoreName, Id, Ip, Port]}, 
        permanent, 10000, worker, [ekvs_bdb_client_session]}.



% EOF
