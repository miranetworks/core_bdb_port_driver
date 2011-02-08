%$Id$
-module(ekvs_bdb_client_yaws_fe_get_handler).

-export([
        out/1
        ]).


%Expecting ?store=storename&key=key

-record(get_req, {store_name = "", key = ""}).

out(HttpReq)->

    Params = pt_util_http:fix_params(yaws_api:parse_query(HttpReq)),

    Req = make_req(#get_req{}, Params),    

    case catch(ekvs_bdb_client:get(Req#get_req.store_name,  Req#get_req.key)) of
    {ok, Value} ->
        Result = "status=found&value=" ++ yaws_api:url_encode(Value);

    {error, not_found} ->
        Result = "status=not_found&value=";

    Error ->
        Result = "status=error&message=" ++ yaws_api:url_encode(lists:flatten(io_lib:format("~p", [Error])))

    end,

    [{status, 200}, {content, "text/plain", Result}].


make_req(Req, [])->
    Req;
make_req(Req, [{Param, Value} | T])->

    case Param of
    "store" ->
        NewReq = Req#get_req{store_name = Value};
    "key" ->
        NewReq = Req#get_req{key = Value};

    _ ->
        NewReq = Req
    end,

    make_req(NewReq, T).




%EOF
