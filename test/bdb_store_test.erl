-module(bdb_store_test).


-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

insert_test() ->

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual(ok, loop_insert(100)),

    ?assertEqual({ok, 100}, bdb_store:count("test")),

    ?assertEqual(ok, loop_lookup(100)),

    ?assertEqual({ok, 100},  bdb_store:count("test")),

    ?assertEqual(ok, loop_delete(100)),

    ?assertEqual({ok, 0},  bdb_store:count("test")),

    ?assertEqual(ok, bdb_store:sync("test")),

    unlink(Pid),

    exit(Pid, kill).



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
