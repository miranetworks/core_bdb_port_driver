-module(bdb_store).
-behaviour(supervisor).

-export([
        start_link/3,
        init/1
        ]).

-export([set/3, get/2, del/2, count/1, sync/1, bulk_get/3, truncate/1, compact/1, fold/4, foldr/4, fold_nonlock/4, foldr_nonlock/4, 
        fold/5, foldr/5, fold_nonlock/5, foldr_nonlock/5, get_sync_interval/1, set_sync_interval/2, map/3]).

-type foldfun()::fun((binary(), binary(), any()) -> any()). 

%% @doc Set the value of a key
-spec set(string(), binary(), binary()) -> ok | {error, any()}.
set(DbName, Key, Value)->
    bdb_port_driver_proxy:set(DbName, Key, Value).

%% @doc Get the value of a key as a list
-spec get(string(), binary()) -> {ok, list()} | {error, not_found} | {error, any()}.
get(DbName, Key)-> 
    bdb_port_driver_proxy:get(DbName, Key).

%% @doc Delete a key
-spec del(string(), binary()) -> ok | {error, any()}.
del(DbName, Key)->
    bdb_port_driver_proxy:del(DbName, Key).

%% @doc Return the amount of keys in the table
-spec count(string()) -> non_neg_integer() | {error, any()}.
count(DbName)->
    bdb_port_driver_proxy:count(DbName).

%% @doc Flush in-memory content to disc
-spec sync(string()) -> ok | {error, any()}.
sync(DbName)->
    bdb_port_driver_proxy:sync(DbName).

%% @doc Returns the sync interval in millis
%%
%% NOTE: This will only work if {sync, ...} was passed in as an option to bdb_store:start_link(...)
-spec get_sync_interval(string()) -> {ok, non_neg_integer()} | {error, any()}.
get_sync_interval(DbName) ->
    bdb_port_driver_sync:get_interval(DbName).

%% @doc Sets the sync interval in millis
%%
%% NOTE: This will only work if {sync, ...} was passed in as an option to bdb_store:start_link(...)
-spec set_sync_interval(string(), non_neg_integer()) -> ok.
set_sync_interval(DbName, IntervalMs) when is_integer(IntervalMs) and (IntervalMs > 0) ->
    bdb_port_driver_sync:set_interval(DbName, IntervalMs).

%% @doc Return a list of {K,V} tuples in range Offset, Offset + Count
%%
%% NOTE: This function might return less records than requested, if the amount of requested records could not be fitted into buffer_size (as per start_link options)
%%
%% NOTE: K and V are lists, the original binaries that were passed in as per set(DbName, Key, Value) can be determined by list_to_binary() 
-spec bulk_get(string(), pos_integer(), non_neg_integer()) -> {ok, list({list(),list()})} | {error, any()}.
bulk_get(DbName, Offset, Count) ->
    bdb_port_driver_proxy:bulk_get(DbName, Offset, Count).

%% @doc Truncates (i.e. DELETE EVERYTHING) the table
-spec truncate(string()) -> ok.
truncate(DbName)->
    bdb_port_driver_proxy:truncate(DbName).

%% @doc Compact the table
%%
%%NOTE: Does NOT free up disk space
%%
%%NOTE: Can take long, locks table for duration.
-spec compact(string()) -> ok.
compact(DbName)->
    bdb_port_driver_proxy:compact(DbName).

%% @doc Fold over the items in the table.
%%
%% Fun(K, V, Acc0) will be called for each Key,Value pair in the table 
%%
%% Where Acc0 will initially be what was passed in as Acc, and then whatever the previous call to Fun returned.
%%
%% Batch size essentially translates into bulk_get(DbName, Offset, BatchSize) for reading the items from the table
%%
%% NOTE: Locks the table for the duration.
%%
-spec fold(string(), foldfun(), any(), pos_integer()) -> any().
fold(DbName, Fun, Acc, BatchSize) when BatchSize > 0 ->
    fold(DbName, Fun, Acc, 1, BatchSize).

%% @doc Fold over the items in the table, starting at Offset.
%%
%% Same as fold/4, just starts at Offset opposed to 1
%%
%% NOTE: Offset should be in [1, NumberOfKeysInTable]
%%
%% NOTE: Locks the table for the duration.
-spec fold(string(), foldfun(), any(), pos_integer(), pos_integer()) -> any().
fold(DbName, Fun, Acc, Offset, BatchSize) when (BatchSize >= 1) and (Offset >= 1) ->
    bdb_port_driver_proxy:fold(DbName, Fun, Acc, Offset, BatchSize).

%% @doc Fold over the items in the table, starting at the last element.
%%
%% Same as fold/4, just applying Fun in reverse order (i.e. start with the last element, working forard).
%%
%% NOTE: Locks the table for the duration.
-spec foldr(string(), foldfun(), any(), pos_integer()) -> any().
foldr(DbName, Fun, Acc, BatchSize) when BatchSize >= 1 ->
    {ok, Count} = bdb_port_driver_proxy:count(DbName),
    if (Count > 0) ->
        foldr(DbName, Fun, Acc, Count, BatchSize);
    true ->
        {ok, Acc}
    end.

%% @doc Fold over the items in the table, starting at the last element.
%%
%% Same as fold/5, just start at Offset working forward (i.e. towards the first item in the table)
%%
%% NOTE: Locks the table for the duration.
-spec foldr(string(), foldfun(), any(), pos_integer(), pos_integer()) -> any().
foldr(DbName, Fun, Acc, Offset, BatchSize) when (BatchSize >= 1) and (Offset >= 1) ->

    if (Offset >= BatchSize) ->
        bdb_port_driver_proxy:foldr(DbName, Fun, Acc, max(Offset - BatchSize + 1, 1), BatchSize);
    true ->
        bdb_port_driver_proxy:foldr(DbName, Fun, Acc, 1, Offset)
    end.

%% @doc Same as fold/4, but does not lock the table.
-spec fold_nonlock(string(), foldfun(), any(), pos_integer()) -> any().
fold_nonlock(DbName, Fun, Acc, BatchSize) when BatchSize >= 1 ->
    fold_nonlock(DbName, Fun, Acc, 1, BatchSize).

%% @doc Same as fold/5, but does not lock the table.
-spec fold_nonlock(string(), foldfun(), any(), pos_integer(), pos_integer()) -> any().
fold_nonlock(DbName, Fun, Acc, Offset, BatchSize) when (BatchSize >= 1) and (Offset >= 1) ->
    do_fold_nonlock(DbName, Fun, Acc, Offset, BatchSize).

%% @doc Same as foldr/4, but does not lock the table.
-spec foldr_nonlock(string(), foldfun(), any(), pos_integer()) -> any().
foldr_nonlock(DbName, Fun, Acc, BatchSize) when BatchSize >= 1 ->
    {ok, Count} = bdb_port_driver_proxy:count(DbName),
    if (Count > 0) ->
        foldr_nonlock(DbName, Fun, Acc, Count, BatchSize);
    true ->
        {ok, Acc}
    end.

%% @doc Same as foldr/5, but does not lock the table.
-spec foldr_nonlock(string(), foldfun(), any(), pos_integer(), pos_integer()) -> any().
foldr_nonlock(DbName, Fun, Acc, Offset, BatchSize) when (BatchSize >= 1) and (Offset >= 1) ->
    if (Offset >= BatchSize) ->
        do_foldr_nonlock(DbName, Fun, Acc, max(Offset - BatchSize + 1, 1), BatchSize);
    true ->
        do_foldr_nonlock(DbName, Fun, Acc, 1, Offset)
    end.

%% @doc Map the value associated with a key to a new one
%%      
%% NOTE: Fun should be lightweight and should not lock-up
%%      
-spec map(string(), binary(), fun((binary()) -> {update, binary()} | delete | ignore)) -> {ok, {updated, binary()} | {deleted, binary()} | {ignored, binary()}} | {error, not_found} | {error, any()}.
map(DbName, Key, Fun) when is_binary(Key) and is_function(Fun, 1) ->
    bdb_port_driver_proxy:map(DbName, Key, Fun).

% Interface functions
%% @doc Look at bdb_port_driver:start_link(....) for more info.
%%
-spec start_link(string(), string(), list()) -> {ok, pid()}.
start_link(DbName, DataDir, Options) -> 
	supervisor:start_link({global, {?MODULE, DbName}}, ?MODULE, [DbName, DataDir, Options]).

%% @hidden
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
%% @hidden
do_fold_nonlock(DbName, Fun, Acc, Start, BatchSize) ->

    case bdb_port_driver_proxy:bulk_get(DbName, Start, BatchSize) of
    {ok, []} ->

        {ok, Acc};

    {ok, KVList} ->

        NewAcc = loop_fold_nonlock(Fun, Acc, KVList),

        do_fold_nonlock(DbName, Fun, NewAcc, Start + length(KVList), BatchSize)

    end.

%% @hidden
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


%% @hidden
loop_fold_nonlock(Fun, Acc, [{LKey, LValue} | T]) ->

    NewAcc = Fun(list_to_binary(LKey), list_to_binary(LValue), Acc),

    loop_fold_nonlock(Fun, NewAcc, T);

loop_fold_nonlock(_Fun, Acc, []) -> Acc.

%% @hidden
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
