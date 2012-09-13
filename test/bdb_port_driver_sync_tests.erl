-module(bdb_port_driver_sync_tests).

-include_lib("eunit/include/eunit.hrl").


start_link_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assert(undefined == global:whereis_name({bdb_port_driver_sync, "test"})),

    {ok, Pid2} = bdb_port_driver_sync:start_link("test", 5000),

    ?assert(is_pid(Pid2)),

    ?assert(undefined =/= global:whereis_name({bdb_port_driver_sync, "test"})),

    ?assertEqual(ok, bdb_port_driver_sync:sync("test")),

    unlink(Pid2),

    exit(Pid2, kill),

    unlink(Pid),

    exit(Pid, kill).

start_link2_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assert(undefined == global:whereis_name({bdb_port_driver_sync, "test"})),

    F = fun(_Db, _SyncTimeMs) ->
        ok
    end,

    {ok, Pid2} = bdb_port_driver_sync:start_link("test", 5000, F),

    ?assert(is_pid(Pid2)),

    ?assert(undefined =/= global:whereis_name({bdb_port_driver_sync, "test"})),

    ?assertEqual(ok, bdb_port_driver_sync:sync("test")),

    unlink(Pid2),

    exit(Pid2, kill),

    unlink(Pid),

    exit(Pid, kill).

set_get_interval_test() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    F = fun(_Db, _SyncTimeMs) ->
        ok
    end,

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}, {sync, 5000, F}]),

    ?assert(is_pid(Pid)),

    ?assert(undefined =/= global:whereis_name({bdb_port_driver_sync, "test"})),

    ?assertEqual({ok, 5000}, bdb_port_driver_sync:get_interval("test")),
    ?assertEqual(ok, bdb_port_driver_sync:set_interval("test", 1000)),
    ?assertEqual({ok, 1000}, bdb_port_driver_sync:get_interval("test")),


    unlink(Pid),

    exit(Pid, kill).

actual_sync_test_() ->

    {timeout, 5, [fun() ->

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    Me = self(),

    F = fun(_Db, _SyncTimeMs) ->
        Me ! synced
    end,

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}, {sync, 100, F}]),

    receive 
    synced ->
        ok
    end,

    unlink(Pid),

    exit(Pid, kill)

    end]}.



handle_call_test() ->

    ?assertEqual({reply, {ok, 5000}, {state, "test", 5000, {{1970,1,1}, {0,0,0}}, undefined}, 100}, bdb_port_driver_sync:handle_call(get_interval, self(), {state, "test", 5000, {{1970,1,1}, {0,0,0}},  undefined})),
    ?assertEqual({reply, ok, {state, "test", 1000, {{1970,1,1}, {0,0,0}}, undefined}, 1000}, bdb_port_driver_sync:handle_call({set_interval, 1000}, self(), {state, "test", 5000, {{1970,1,1}, {0,0,0}}, undefined})),

    ?assertEqual({noreply, {a,b}, 100}, bdb_port_driver_sync:handle_call(bla, self(), {a,b})).

handle_cast_test() ->
    ?assertEqual({noreply, {a,b}, 100}, bdb_port_driver_sync:handle_cast(bla, {a,b})).

handle_info_test() ->

    ?assertEqual({noreply, {a,b}, 100}, bdb_port_driver_sync:handle_info(bla, {a,b})),

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertMatch({noreply, {state, "test", 5000, _, undefined}, 5000}, bdb_port_driver_sync:handle_info(timeout, {state, "test", 5000, {{1970, 1,1}, {0,0,0}}, undefined }  )),

    unlink(Pid),

    exit(Pid, kill).
    
code_change_test() ->
    ?assertEqual({ok, b}, bdb_port_driver_sync:code_change(a,b,c)).

terminate_test()->
    ?assertEqual(ok, bdb_port_driver_sync:terminate(dontcare, dontcare)).


%EOF
