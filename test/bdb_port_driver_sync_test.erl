-module(bdb_port_driver_sync_test).

-include_lib("eunit/include/eunit.hrl").


handle_call_test() ->
    ?assertEqual({noreply, {a,b}, b}, bdb_port_driver_sync:handle_call(bla, self(), {a,b})).

handle_cast_test() ->
    ?assertEqual({noreply, {a,b}, b}, bdb_port_driver_sync:handle_cast(bla, {a,b})).

handle_info_test() ->

    ?assertEqual({noreply, {a,b}, b}, bdb_port_driver_sync:handle_info(bla, {a,b})),

    ok = error_logger:tty(false),

    ?assertCmd("rm -fr ./data"),

    {ok, Pid} =  bdb_store:start_link("test", "./data", [{txn_enabled, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual({noreply, {"test", 5000}, 5000}, bdb_port_driver_sync:handle_info(timeout, {"test", 5000})),

    unlink(Pid),

    exit(Pid, kill).
    
code_change_test() ->
    ?assertEqual({ok, b}, bdb_port_driver_sync:code_change(a,b,c)).

terminate_test()->
    ?assertEqual(ok, bdb_port_driver_sync:terminate(dontcare, dontcare)).


%EOF
