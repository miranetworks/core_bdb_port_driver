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
        start_link/3,
        sync/1,
        get_interval/1,
        set_interval/2
        ]).

-define(NAME(X), {global, {?MODULE, X}}).

-record(state, {db, interval_ms = 30000, last_sync = calendar:universal_time(), callback = undefined }).

start_link(Db, SyncIntervalMs) ->
    start_link(Db, SyncIntervalMs, undefined).

start_link(Db, SyncIntervalMs, Fun) ->
    gen_server:start_link(?NAME(Db), ?MODULE, [Db, SyncIntervalMs, Fun], []).

sync(Db)->
    gen_server:call(?NAME(Db), sync, infinity).

get_interval(Db) ->
    gen_server:call(?NAME(Db), get_interval, infinity).
    
set_interval(Db, SyncIntervalMs) when is_integer(SyncIntervalMs) and (SyncIntervalMs > 0) ->
    gen_server:call(?NAME(Db), {set_interval, SyncIntervalMs}, infinity).
 
init([Db, SyncIntervalMs, Fun]) ->

    process_flag(trap_exit, true),

    {ok, #state{db = Db, interval_ms = SyncIntervalMs, callback = Fun}, 100}.

handle_call(get_interval, _From, #state{interval_ms = SyncIntervalMs} = State) ->
    {reply, {ok, SyncIntervalMs}, State, 100};

handle_call({set_interval, NewIntervalMs}, _From, State) ->
    {reply, ok, State#state{interval_ms = NewIntervalMs}, NewIntervalMs};

handle_call(sync, _From, #state{db = Db, callback = Fun} = State) ->

    ok = sync_and_do_callback(Db, Fun),

    {reply, ok, State#state{last_sync = calendar:universal_time()}, 100};

handle_call(_, _, State) ->
    {noreply, State, 100}.

handle_cast(_, State) ->
    {noreply, State, 100}.

handle_info(timeout, #state{db = Db, interval_ms = SyncIntervalMs, callback = Fun, last_sync = LastSync} = State) ->

    NowSecs = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),

    LastSecs = calendar:datetime_to_gregorian_seconds(LastSync),

    if (((NowSecs - LastSecs) * 1000) >= SyncIntervalMs) ->

        ok = sync_and_do_callback(Db, Fun),

        {noreply, State#state{last_sync = calendar:universal_time()}, SyncIntervalMs};

    true ->
        {noreply, State, 100}

    end;


handle_info(_, State) ->
    {noreply, State, 100}.

code_change(_,State,_)->
    {ok, State}.

terminate(_, _) ->
    ok.

sync_and_do_callback(Db, Fun) ->

    T1 = os:timestamp(),

    ok = bdb_store:sync(Db),

    DiffMs = trunc(timer:now_diff(os:timestamp(), T1)/1000),

    if (Fun =/= undefined) ->
        catch(Fun(Db, DiffMs)),
        ok;
    true ->
        ok
    end.

%EOF
