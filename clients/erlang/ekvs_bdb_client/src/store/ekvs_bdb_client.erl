% $Id$
-module(ekvs_bdb_client).
-behavior(gen_server).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2
        ]).

%API functions
-export([
        start_link/2,
        stop/1,

        set/3,
        get/2,
        remove/2,
        bulk_get/3,

        set_bin/3,
        get_bin/2,
        remove_bin/2,
        bulk_get_bin/3,

        count/1,

        flush/1,

        next_session/1

        ]).

-define(REQUEST_TIMEOUT_MS, 30000).

-record(server_state, {store_name, num_sessions = 0, last_session_idx = 0}).

% Interface functions
start_link(StoreName, NumSessions) ->
    gen_server:start_link(make_name(StoreName), ?MODULE, [StoreName, NumSessions], []).

stop(StoreName) ->
    gen_server:cast(make_name(StoreName), stop).

set(StoreName, Key, Value)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:set(StoreName, Id, Key, Value).

get(StoreName, Key)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:get(StoreName, Id, Key).

remove(StoreName, Key)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:remove(StoreName, Id, Key).

bulk_get(StoreName, Offset, Count)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:bulk_get(StoreName, Id, Offset, Count).

set_bin(StoreName, Key, Value)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:set_bin(StoreName, Id, Key, Value).

get_bin(StoreName, Key)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:get_bin(StoreName, Id, Key).

remove_bin(StoreName, Key)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:remove_bin(StoreName, Id, Key).

bulk_get_bin(StoreName, Offset, Count)->
    Id = next_session(StoreName),

    ekvs_bdb_client_session:bulk_get_bin(StoreName, Id, Offset, Count).

count(StoreName)->
    ekvs_bdb_client_session:count(StoreName, "control").

flush(StoreName)->
    ekvs_bdb_client_session:flush(StoreName, "control").

next_session(StoreName)->
    gen_server:call(make_name(StoreName), next_session, ?REQUEST_TIMEOUT_MS).

% Callback functions
init([StoreName, NumSessions]) when (NumSessions >= 1) ->

    erlang:process_flag(trap_exit, true),

    ServerState = #server_state {
                                store_name       = StoreName,
                                num_sessions     = NumSessions,
                                last_session_idx = NumSessions - 1
                                },

    {ok, ServerState}.

% handle_call callback
handle_call(next_session, _From, #server_state {last_session_idx = LastIdx, num_sessions = NumSessions} = ServerState) ->

    Result = (LastIdx + 1) rem NumSessions,

    {reply, Result + 1, ServerState#server_state{last_session_idx = Result}};

handle_call(Call, _From, ServerState) ->
    io:format("Unsupported call [~p] in ~p!~n", [Call, ?MODULE]),
    {reply, "not supported", ServerState}.
    
% handle_cast callback
handle_cast(stop, ServerState) ->
    {stop, normal, ServerState};

handle_cast(Cast, ServerState) ->
    io:format("Unsupported cast [~p] in ~p!~n", [Cast, ?MODULE]),
    {noreply, ServerState}.
        
% handle_info callback
handle_info(Info, ServerState) ->
    io:format("Unsupported info [~p] in ~p!~n", [Info, ?MODULE]),
    {noreply, ServerState}.
        
% code_change callback
code_change(_OldVsn, ServerState, _Extra) ->
    io:format("Unsupported code change in ~p!~n", [?MODULE]),
    {ok, ServerState}.

terminate(Reason, #server_state {store_name = StoreName}) ->
    io:format("[~p] ~p Terminating [~p]~n", [?MODULE, StoreName, Reason]),
    ok.

%Worker piggies
make_name(StoreName)->
    {global, {?MODULE, StoreName}}.

%EOF
