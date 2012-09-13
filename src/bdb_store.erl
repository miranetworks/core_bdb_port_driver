-module(bdb_store).
-behaviour(supervisor).

-export([
        start_link/3,
        init/1
        ]).

-export([set/3, get/2, del/2, count/1, sync/1, bulk_get/3, truncate/1, compact/1, fold/4, foldr/4, fold_nonlock/4, foldr_nonlock/4, 
        fold/5, foldr/5, fold_nonlock/5, foldr_nonlock/5, get_sync_interval/1, set_sync_interval/2, map/3]).


set(DbName, Key, Value)->
    bdb_port_driver_proxy:set(DbName, Key, Value).

get(DbName, Key)-> 
    bdb_port_driver_proxy:get(DbName, Key).

del(DbName, Key)->
    bdb_port_driver_proxy:del(DbName, Key).

count(DbName)->
    bdb_port_driver_proxy:count(DbName).

sync(DbName)->
    bdb_port_driver_proxy:sync(DbName).

get_sync_interval(DbName) ->
    bdb_port_driver_sync:get_interval(DbName).

set_sync_interval(DbName, IntervalMs) when is_integer(IntervalMs) and (IntervalMs > 0) ->
    bdb_port_driver_sync:set_interval(DbName, IntervalMs).

bulk_get(DbName, Offset, Count)->
    bdb_port_driver_proxy:bulk_get(DbName, Offset, Count).

truncate(DbName)->
    bdb_port_driver_proxy:truncate(DbName).

compact(DbName)->
    bdb_port_driver_proxy:compact(DbName).

%Locking fold funcs - i.e. db is locked until action is completed !
fold(DbName, Fun, Acc, BatchSize) when BatchSize > 0 ->
    fold(DbName, Fun, Acc, 1, BatchSize).

fold(DbName, Fun, Acc, Offset, BatchSize) when BatchSize > 0 ->
    bdb_port_driver_proxy:fold(DbName, Fun, Acc, Offset, BatchSize).

foldr(DbName, Fun, Acc, BatchSize) when BatchSize > 0 ->
    {ok, Count} = bdb_port_driver_proxy:count(DbName),
    foldr(DbName, Fun, Acc, Count, BatchSize).

foldr(DbName, Fun, Acc, Offset, BatchSize) when BatchSize > 0 ->

    if (Offset >= BatchSize) ->
        bdb_port_driver_proxy:foldr(DbName, Fun, Acc, max(Offset - BatchSize + 1, 1), BatchSize);
    true ->
        bdb_port_driver_proxy:foldr(DbName, Fun, Acc, 1, Offset)
    end.

%Non-locking fold funcs - i.e. db is not lockec during the operation ..
fold_nonlock(DbName, Fun, Acc, BatchSize) when BatchSize > 0 ->
    fold_nonlock(DbName, Fun, Acc, 1, BatchSize).

fold_nonlock(DbName, Fun, Acc, Offset, BatchSize) when BatchSize > 0 ->
    do_fold_nonlock(DbName, Fun, Acc, Offset, BatchSize).

foldr_nonlock(DbName, Fun, Acc, BatchSize) when BatchSize > 0 ->
    {ok, Count} = bdb_port_driver_proxy:count(DbName),
    foldr_nonlock(DbName, Fun, Acc, Count, BatchSize).

foldr_nonlock(DbName, Fun, Acc, Offset, BatchSize) when BatchSize > 0 ->

    if (Offset >= BatchSize) ->
        do_foldr_nonlock(DbName, Fun, Acc, max(Offset - BatchSize + 1, 1), BatchSize);
    true ->
        do_foldr_nonlock(DbName, Fun, Acc, 1, Offset)
    end.

%% @doc Map the value associated with a key to a new one<br>
%% NOTE: Fun should be lightweight and should not lock-up<br> 
%%      
-spec map(string(), binary(), fun((binary()) -> {update, binary()} | delete | ignore)) -> {ok, {updated, binary()} | {deleted, binary()} | {ignored, binary()}} | {error, not_found} | {error, any()}.
map(DbName, Key, Fun) when is_binary(Key) and is_function(Fun, 1) ->
    bdb_port_driver_proxy:map(DbName, Key, Fun).

% Interface functions
start_link(DbName, DataDir, Options) -> 
	supervisor:start_link({global, {?MODULE, DbName}}, ?MODULE, [DbName, DataDir, Options]).

init([DbName, DataDir, Options]) -> 

	RestartSpec = {rest_for_one, 1, 10},

    PortDrvSpec = 
    {port_driver, 
        {bdb_port_driver, start_link, [DbName, DataDir, Options]},
        permanent, 600000, worker, [bdb_port_driver]},

    PortDrvProxySpec = 
    {port_driver_proxy, 
        {bdb_port_driver_proxy, start_link, [DbName]},
        permanent, 600000, worker, [bdb_port_driver_proxy]},


    {ok, { RestartSpec, [PortDrvSpec, PortDrvProxySpec] ++

        case check_autosync(Options) of
        {ok, SyncIntervalMs, Fun} ->   

            [{bdb_port_driver_sync,
                {bdb_port_driver_sync, start_link, [DbName, SyncIntervalMs, Fun]},
                permanent, 600000, worker, [bdb_port_driver_sync]}];

        _ ->
            []

        end 
    
    }}.

    

%Worker funcs
do_fold_nonlock(DbName, Fun, Acc, Start, BatchSize) ->

    case bdb_port_driver_proxy:bulk_get(DbName, Start, BatchSize) of
    {ok, []} ->

        {ok, Acc};

    {ok, KVList} ->

        NewAcc = loop_fold_nonlock(Fun, Acc, KVList),

        do_fold_nonlock(DbName, Fun, NewAcc, Start + length(KVList), BatchSize)

    end.

do_foldr_nonlock(DbName, Fun, Acc, Start, BatchSize) when Start >= 1 ->

    case bdb_port_driver_proxy:bulk_get(DbName, Start, BatchSize) of
    {ok, []} ->

        {ok, Acc};

    {ok, KVList} ->

        NewAcc = loop_fold_nonlock(Fun, Acc, lists:reverse(KVList)),

        do_foldr_nonlock(DbName, Fun, NewAcc, Start - length(KVList), BatchSize)

    end;

do_foldr_nonlock(_DbName, _Fun, Acc, _Start, _BatchSize) ->
    {ok, Acc}.



loop_fold_nonlock(Fun, Acc, [{LKey, LValue} | T]) ->

    NewAcc = Fun(list_to_binary(LKey), list_to_binary(LValue), Acc),

    loop_fold_nonlock(Fun, NewAcc, T);

loop_fold_nonlock(_Fun, Acc, []) -> Acc.

check_autosync(Options) ->

    case lists:keysearch(sync, 1, Options) of
    {value, {sync, IntervalMs, Fun}}  when is_integer(IntervalMs) and (IntervalMs > 0) ->
        {ok, IntervalMs, Fun};

    {value, {sync, IntervalMs}}  when is_integer(IntervalMs) and (IntervalMs > 0) ->
        {ok, IntervalMs, undefined};


    _ ->
        false
    end.

% EOF
