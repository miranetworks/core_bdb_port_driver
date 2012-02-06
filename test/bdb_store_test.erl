-module(bdb_store_test).


-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

insert_test() ->

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual(ok, bdb_store:truncate("test")),

    ?assertEqual(ok, bdb_store:sync("test")),

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

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, loop_insert(100)),

    F = fun (_, _, Acc) ->

        Acc + 1

    end,

    ?assertEqual({ok, 100}, bdb_store:fold("test", F, 0, 1000)),

    ?assertMatch({ok, _}, bdb_store:bulk_get("test", 1, 1000)),

    {ok, BG1} = bdb_store:bulk_get("test", 1, 1000),

    ?assertEqual(100, length(BG1)),

    ExpectedKey = binary_to_list(term_to_binary(50)),

    ?assertEqual({ok, [{ExpectedKey, "V50"}]}, bdb_store:bulk_get("test", 50, 1)),


    ?assertEqual(ok, bdb_store:compact("test")),

    ?assertEqual({ok, 100},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:sync("test")),
    
    ?assertEqual({ok, [{ExpectedKey, "V50"}]}, bdb_store:bulk_get("test", 50, 1)),

    unlink(Pid),

    exit(Pid, kill).



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


%EOF
