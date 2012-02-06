% $Id$
-module(bdb_port_driver_build_rel).

-export([
        start/1
        ]).

% Interface functions
start(RelFile)->
    ok = systools:make_script(RelFile),
    ok = systools:make_tar(RelFile, [{dirs, [src, priv]}]),
    halt().

%EOF
