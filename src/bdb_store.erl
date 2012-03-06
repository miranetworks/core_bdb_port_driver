-module(bdb_store).
-behaviour(supervisor).

-export([
        start_link/3,
        init/1
        ]).

-export([set/3, get/2, del/2, count/1, sync/1, bulk_get/3, truncate/1, compact/1, fold/4, fold_nonlock/4]).


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

bulk_get(DbName, Offset, Count)->
    bdb_port_driver_proxy:bulk_get(DbName, Offset, Count).

truncate(DbName)->
    bdb_port_driver_proxy:truncate(DbName).

compact(DbName)->
    bdb_port_driver_proxy:compact(DbName).

fold(DbName, Fun, Acc, BatchSize)->
    bdb_port_driver_proxy:fold(DbName, Fun, Acc, BatchSize).

fold_nonlock(DbName, Fun, Acc, BatchSize) ->
    do_fold_nonlock(DbName, Fun, Acc, 1, BatchSize).


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

	{ok, { RestartSpec, [PortDrvSpec, PortDrvProxySpec] }}.

%Worker funcs
do_fold_nonlock(DbName, Fun, Acc, Start, BatchSize) ->

    case bdb_port_driver_proxy:bulk_get(DbName, Start, BatchSize) of
    {ok, []} ->

        {ok, Acc};

    {ok, KVList} ->

        NewAcc = loop_fold_nonlock(Fun, Acc, KVList),

        do_fold_nonlock(DbName, Fun, NewAcc, Start + length(KVList), BatchSize)

    end.

loop_fold_nonlock(Fun, Acc, [{LKey, LValue} | T]) ->

    NewAcc = Fun(list_to_binary(LKey), list_to_binary(LValue), Acc),

    loop_fold_nonlock(Fun, NewAcc, T);

loop_fold_nonlock(_Fun, Acc, []) -> Acc.



% EOF
