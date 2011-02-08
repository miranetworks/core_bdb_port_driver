%$Id$
-module(ekvs_bdb_client_yaws_fe_count_handler).

-export([
        out/1
        ]).


%Expecting ?store=storename

-record(count_req, {store_name = ""}).

out(HttpReq)->

    Params = pt_util_http:fix_params(yaws_api:parse_query(HttpReq)),

    Req = make_req(#count_req{}, Params),    

    case catch(ekvs_bdb_client:count(Req#count_req.store_name)) of
    {ok, Count} ->
        Result = "status=ok&value=" ++ yaws_api:url_encode(integer_to_list(Count));

    Error ->
        Result = "status=error&message=" ++ yaws_api:url_encode(lists:flatten(io_lib:format("~p", [Error])))

    end,

    [{status, 200}, {content, "text/plain", Result}].


make_req(Req, [])->
    Req;
make_req(Req, [{Param, Value} | T])->

    case Param of
    "store" ->
        NewReq = Req#count_req{store_name = Value};

    _ ->
        NewReq = Req
    end,

    make_req(NewReq, T).




%EOF
