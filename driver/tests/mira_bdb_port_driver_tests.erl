%$Id$
-module(mira_bdb_port_driver_tests).


-ifndef(DONT_USE_EUNIT).

-include_lib("eunit/include/eunit.hrl").

-else.

-export([run_all/0]).

-define(assertEqual(X,Y), case (X =:= Y) of true -> ok; _ -> io:format("~p:~p Test failed~n", [?MODULE, ?LINE]), error end).


run_all()->

    ok = exmaple1_test().


-endif.

exmaple1_test()->
    ?assertEqual(ok, error).



%EOF
