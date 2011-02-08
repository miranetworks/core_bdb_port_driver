%$Id$
-module(ekvs_bdb_client_yaws_fe_nagios_req_handler).

-export([
        out/1
        ]).

out(_HttpReq)->
    [{status, 200}, {content, "text/plain", "ok"}].


%EOF
