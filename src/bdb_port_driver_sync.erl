-module(bdb_port_driver_sync).
-behavior(gen_server).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
        ]).


% Interface exports
-export([
        start_link/2,
        sync/1
        ]).

-define(NAME(X), {global, {?MODULE, X}}).

start_link(Db, SyncIntervalMs) ->
    gen_server:start_link(?NAME(Db), ?MODULE, [Db, SyncIntervalMs], []).

sync(Db)->
    gen_server:call(?NAME(Db), sync).

init([Db, SyncIntervalMs]) ->

    process_flag(trap_exit, true),

    {ok, {Db, SyncIntervalMs}, SyncIntervalMs}.

handle_call(sync, _From, {Db, SyncIntervalMs}) ->
    ok = bdb_store:sync(Db),
    {reply, ok, {Db, SyncIntervalMs}, SyncIntervalMs};

handle_call(_, _, {Db, SyncIntervalMs}) ->
    {noreply, {Db, SyncIntervalMs}, SyncIntervalMs}.

handle_cast(_, {Db, SyncIntervalMs}) ->
    {noreply, {Db, SyncIntervalMs}, SyncIntervalMs}.

handle_info(timeout, {Db, SyncIntervalMs}) ->
    ok = bdb_store:sync(Db),
    {noreply, {Db, SyncIntervalMs}, SyncIntervalMs};
handle_info(_, {Db, SyncIntervalMs}) ->
    {noreply, {Db, SyncIntervalMs}, SyncIntervalMs}.

code_change(_,State,_)->
    {ok, State}.

terminate(_, _) ->
    ok.

%EOF
