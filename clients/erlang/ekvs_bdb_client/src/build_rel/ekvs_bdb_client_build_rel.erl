% $Id$
-module(ekvs_bdb_client_build_rel).

-export([
        start/1
        ]).

% Interface functions
start(RelFile)->
    ok = systools:make_script(RelFile),
    ok = systools:make_tar(RelFile, [{dirs, [src, include, config, scripts]}]),
    halt().

%EOF
