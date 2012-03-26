-module(bdb_port_driver_proxy_test).

-include_lib("eunit/include/eunit.hrl").

handle_call_test() ->
    ?assertEqual({noreply, d}, bdb_port_driver_proxy:handle_call(bla, self(), d)).

handle_cast_test() ->
    ?assertEqual({noreply, d}, bdb_port_driver_proxy:handle_cast(bla, d)).

handle_info_test() ->
    ?assertEqual({noreply, d}, bdb_port_driver_proxy:handle_info(bla, d)).

terminate_test() ->
    ?assertEqual(ok, bdb_port_driver_proxy:terminate(dontcare, dontcare)).

code_change_test() ->
    ?assertEqual({ok, b}, bdb_port_driver_proxy:code_change(a, b, c)).

%EOF
