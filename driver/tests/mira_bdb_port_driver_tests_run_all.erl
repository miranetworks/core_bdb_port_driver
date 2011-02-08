%$Id$
-module(mira_bdb_port_driver_tests_run_all).


-ifndef(DONT_USE_EUNIT).

-include_lib("eunit/include/eunit.hrl").

-export([test/0]).

test()->

    eunit:test(mira_bdb_port_driver),

    init:stop().

-else.


-export([test/0]).

test()->

    mira_bdb_port_driver_tests:run_all(),

    init:stop().



-endif.

%EOF
