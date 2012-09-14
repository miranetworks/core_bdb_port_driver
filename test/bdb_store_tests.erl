-module(bdb_store_tests).


-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

insert_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    ?assertEqual({ok, 100}, bdb_store:count("test")),

    ?assertEqual(ok, loop_lookup(100)),

    ?assertEqual({ok, 100},  bdb_store:count("test")),

    ?assertEqual(ok, loop_delete(100)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:sync("test")),

    ?assertEqual({ok, 0},  bdb_store:count("test")),


    ?assertEqual(ok, loop_insert(100)),

    ?assertEqual(ok, bdb_store:sync("test")),

    ?assertEqual({ok, 100},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:truncate("test")),


    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:sync("test")),

    kill(Pid).

fold_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    F = fun (_, _, Acc) ->

        Acc + 1

    end,

    ?assertEqual({ok, 100}, bdb_store:fold("test", F, 0, 1000)),

    kill(Pid).

fold2_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    F = fun (_, _, Acc) ->

        Acc + 1

    end,

    ?assertEqual({ok, 100}, bdb_store:fold("test", F, 0, 1, 1000)),
    ?assertEqual({ok, 51}, bdb_store:fold("test", F,  0, 50, 1000)),
    ?assertEqual({ok, 1}, bdb_store:fold("test", F,  0, 100, 1000)),
    ?assertEqual({ok, 0}, bdb_store:fold("test", F,  0, 101, 1000)),

    kill(Pid).

foldr_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(10)),

    F = fun (K, _, Acc) ->

        [binary_to_term(K) | Acc]

    end,

    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr("test", F, [], 1000)),
    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr("test", F, [], 1)),

    ?assertEqual(ok, bdb_store:truncate("test")),
    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual({ok, []}, bdb_store:foldr("test", F, [], 1000)),
    ?assertEqual({ok, []}, bdb_store:foldr("test", F, [], 1)),
   
    kill(Pid).

foldr2_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(10)),

    F = fun (K, _, Acc) ->

        [binary_to_term(K) | Acc]

    end,

    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr("test", F, [], 10, 1000)),
    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr("test", F, [], 10, 1)),
    ?assertEqual({ok, [1,2,3,4,5]}, bdb_store:foldr("test", F, [], 5, 1000)),
    ?assertEqual({ok, [1]}, bdb_store:foldr("test", F, [], 1, 1000)),

    kill(Pid).


fold_nonlock_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    F = fun (_, _, Acc) ->

        Acc + 1

    end
        {ok, Acc
        {ok, Acc}},

    ?assertEqual({ok, 101}, bdb_store:fold_nonlock("test", F, 1, 1000)),

    kill(Pid).

fold_nonlock2_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    F = fun (_, _, Acc) ->

        Acc + 1

    end,

    ?assertEqual({ok, 100}, bdb_store:fold_nonlock("test", F, 0, 1, 1000)),
    ?assertEqual({ok, 99}, bdb_store:fold_nonlock("test", F, 0, 2, 1000)),
    ?assertEqual({ok, 51}, bdb_store:fold_nonlock("test", F, 0, 50, 1000)),
    ?assertEqual({ok, 1}, bdb_store:fold_nonlock("test", F, 0, 100, 1000)),
    ?assertEqual({ok, 0}, bdb_store:fold_nonlock("test", F, 0, 101, 1000)),

    kill(Pid).

foldr_nonlock_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(10)),

    F = fun (K, _, Acc) ->

        [binary_to_term(K) | Acc]

    end,

    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr_nonlock("test", F, [], 1000)),
    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr_nonlock("test", F, [], 1)),

    ?assertEqual(ok, bdb_store:truncate("test")),
    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual({ok, []}, bdb_store:foldr_nonlock("test", F, [], 1000)),
    ?assertEqual({ok, []}, bdb_store:foldr_nonlock("test", F, [], 1)),
   
    kill(Pid).

foldr_nonlock2_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(10)),

    F = fun (K, _, Acc) ->

        [binary_to_term(K) | Acc]

    end,

    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr_nonlock("test", F, [], 10, 1000)),
    ?assertEqual({ok, [1,2,3,4,5,6,7,8,9,10]}, bdb_store:foldr_nonlock("test", F, [], 10, 1)),
    ?assertEqual({ok, [1,2,3,4,5]}, bdb_store:foldr_nonlock("test", F, [], 5, 1000)),
    ?assertEqual({ok, [1]}, bdb_store:foldr_nonlock("test", F, [], 1, 1000)),

    kill(Pid).

bulk_get_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    ?assertMatch({ok, _}, bdb_store:bulk_get("test", 1, 1000)),

    {ok, BG1} = bdb_store:bulk_get("test", 1, 1000),

    ?assertEqual(100, length(BG1)),

    ExpectedKey = binary_to_list(term_to_binary(50)),

    ?assertEqual({ok, [{ExpectedKey, "V50"}]}, bdb_store:bulk_get("test", 50, 1)),

    ?assertEqual(ok, bdb_store:compact("test")),

    ?assertEqual({ok, 100},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:sync("test")),
    
    ?assertEqual({ok, [{ExpectedKey, "V50"}]}, bdb_store:bulk_get("test", 50, 1)),

    kill(Pid).

sync_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}, {sync, 5000}]),

    ?assert(is_pid(Pid)),

    ?assert(undefined =/= global:whereis_name({bdb_port_driver_sync, "test"})),

    ?assertEqual(ok, bdb_port_driver_sync:sync("test")),

    ?assertEqual(ok, bdb_store:sync("test")),

    ?assertEqual({ok, 5000}, bdb_store:get_sync_interval("test")),
    ?assertEqual(ok, bdb_store:set_sync_interval("test", 10000)),
    ?assertEqual({ok, 10000}, bdb_store:get_sync_interval("test")),

    kill(Pid).

map_test() ->

    ok = error_logger:tty(true),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual(ok, bdb_store:set("test", <<"mapkey">>, <<"mapvalue1">>)),
    ?assertEqual({ok, "mapvalue1"}, bdb_store:get("test", <<"mapkey">>)),

    FunOk = fun (_OldValue) -> {update, <<"FunOk">>} end,
    ?assertEqual({error, not_found}, bdb_store:map("test", <<"mapkey_does_not_exist">>, FunOk)),
    ?assertEqual({ok, {updated, <<"FunOk">>}}, bdb_store:map("test", <<"mapkey">>, FunOk)),
    ?assertEqual({ok, "FunOk"}, bdb_store:get("test", <<"mapkey">>)),

    FunError = fun (_OldValue) -> error end,
    ?assertEqual({error, error}, bdb_store:map("test", <<"mapkey">>, FunError)),
    ?assertEqual({ok, "FunOk"}, bdb_store:get("test", <<"mapkey">>)),

    FunException = fun (_OldValue) -> erlang:throw({'EXIT', "bla"}) end,
    ?assertEqual({error, {'EXIT', "bla"}}, bdb_store:map("test", <<"mapkey">>, FunException)),
    ?assertEqual({ok, "FunOk"}, bdb_store:get("test", <<"mapkey">>)),

    FunIgnore = fun (_OldValue) -> ignore end,
    ?assertEqual({ok, {ignored, <<"FunOk">>}}, bdb_store:map("test", <<"mapkey">>, FunIgnore)),
    ?assertEqual({ok, "FunOk"}, bdb_store:get("test", <<"mapkey">>)),

    FunDelete = fun (_OldValue) -> delete end,
    ?assertEqual({ok, {deleted, <<"FunOk">>}}, bdb_store:map("test", <<"mapkey">>, FunDelete)),
    ?assertEqual({error, not_found}, bdb_store:get("test", <<"mapkey">>)),

    kill(Pid).

loop_insert(0)->
    ok;
loop_insert(N)->

    Key = term_to_binary(N),

    Value = "V" ++ integer_to_list(N),

    ok = bdb_store:set("test", Key, list_to_binary(Value)),

    loop_insert(N - 1).

loop_lookup(0)->
    ok;
loop_lookup(N)->

    Key = term_to_binary(N),

    Expectedvalue = "V" ++ integer_to_list(N),

    {ok, Expectedvalue} = bdb_store:get("test", Key),

    loop_lookup(N -1).

loop_delete(0)->
    ok;
loop_delete(N)->

    Key = term_to_binary(N),

    ok = bdb_store:del("test", Key),

    loop_delete(N -1).

kill(Pid) ->
    unlink(Pid),
    monitor(process, Pid),
    exit(Pid, shutdown),
    receive {'DOWN', _, process, Pid, _} -> ok end.



%EOF
