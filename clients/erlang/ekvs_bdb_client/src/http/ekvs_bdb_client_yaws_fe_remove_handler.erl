%$Id$
-module(ekvs_bdb_client_yaws_fe_remove_handler).

-export([
        out/1
        ]).


%Expecting ?store=storename&key=key

-record(remove_req, {store_name = "", key = ""}).

out(HttpReq)->

    Params = pt_util_http:fix_params(yaws_api:parse_query(HttpReq)),

    Req = make_req(#remove_req{}, Params),    

    case catch(ekvs_bdb_client:remove(Req#remove_req.store_name,  Req#remove_req.key)) of 
    ok -> 
        Result = "status=ok&message="; 
 
    Error -> 
        Result = "status=error&message=" ++ yaws_api:url_encode(lists:flatten(io_lib:format("~p", [Error]))) 
 
    end, 

    [{status, 200}, {content, "text/plain", Result}].


make_req(Req, [])->
    Req;
make_req(Req, [{Param, Value} | T])->

    case Param of
    "store" ->
        NewReq = Req#remove_req{store_name = Value};
    "key" ->
        NewReq = Req#remove_req{key = Value};

    _ ->
        NewReq = Req
    end,

    make_req(NewReq, T).




%EOF
