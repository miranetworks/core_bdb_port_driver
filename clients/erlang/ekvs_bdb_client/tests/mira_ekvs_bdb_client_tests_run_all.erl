%$Id$
-module(mira_ekvs_bdb_client_tests_run_all).


-ifndef(DONT_USE_EUNIT).

-include_lib("eunit/include/eunit.hrl").

-export([test/0]).

test()->

    eunit:test(mira_ekvs_bdb_client),

    init:stop().

-else.


-export([test/0]).

test()->

    mira_ekvs_bdb_client_tests:run_all(),

    init:stop().



-endif.

%EOF
