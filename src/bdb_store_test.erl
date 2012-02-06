%$Id:%
-module(bdb_store_test).

-export([
        go_insert/1, spawn_go_insert/1,
        go_lookup/1, spawn_go_lookup/1,
        go_delete/1, spawn_go_delete/1
        ]).


go_insert(Count)->
    bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    erlang:spawn(?MODULE, spawn_go_insert, [Count]).


spawn_go_insert(Count)->

    T1 = now(),

    loop_insert(Count),

    ok = bdb_store:sync("test"),

    T2 = now(),

    DiffMs = trunc(timer:now_diff(T2, T1)/1000),

    io:format("Inserting ~p items took ~p Ms~n", [Count, DiffMs]).

go_lookup(Count)->
    bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    erlang:spawn(?MODULE, spawn_go_lookup, [Count]).


spawn_go_lookup(Count)->

    T1 = now(),

    loop_lookup(Count),

    T2 = now(),

    DiffMs = trunc(timer:now_diff(T2, T1)/1000),

    io:format("Reading ~p items took ~p Ms~n", [Count, DiffMs]).

go_delete(Count)->
    bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    erlang:spawn(?MODULE, spawn_go_delete, [Count]).


spawn_go_delete(Count)->

    T1 = now(),

    loop_delete(Count),

    ok = bdb_store:sync("test"),

    T2 = now(),

    DiffMs = trunc(timer:now_diff(T2, T1)/1000),

    io:format("Deleting ~p items took ~p Ms~n", [Count, DiffMs]).


loop_insert(0)->
    ok;
loop_insert(N)->

    Key = "K" ++ integer_to_list(N),

    Value = "V" ++ integer_to_list(N),

    ok = bdb_store:set("test", list_to_binary(Key), list_to_binary(Value)),

    loop_insert(N - 1).

loop_lookup(0)->
    ok;
loop_lookup(N)->

    Key = "K" ++ integer_to_list(N),

    {ok, _} = bdb_store:get("test", list_to_binary(Key)),

    loop_lookup(N -1).

loop_delete(0)->
    ok;
loop_delete(N)->

    Key = "K" ++ integer_to_list(N),

    ok = bdb_store:del("test", list_to_binary(Key)),

    loop_delete(N -1).


%EOF
