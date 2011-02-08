%$Id$
-module(ekvs_bdb_client_yaws_fe_bulk_get_handler).

-export([
        out/1
        ]).


%Expecting ?store=storename&offset=recoffset&count=reccount

-record(get_req, {store_name = "", offset = 1, count = 1}).

out(HttpReq)->

    Params = pt_util_http:fix_params(yaws_api:parse_query(HttpReq)),

    Req = make_req(#get_req{}, Params),    

    case catch(ekvs_bdb_client:bulk_get(Req#get_req.store_name,  Req#get_req.offset, Req#get_req.count)) of
    {ok, []} ->
        Result = "status=ok&count=0";

    {ok, Kvp} ->
        Result = "status=ok&count=" ++ yaws_api:url_encode(integer_to_list(length(Kvp))) ++ traverse_build_response(0, Kvp);

    Error ->
        Result = "status=error&message=" ++ yaws_api:url_encode(lists:flatten(io_lib:format("~p", [Error])))

    end,

    [{status, 200}, {content, "text/plain", Result}].

traverse_build_response(_N, [])->
    [];
traverse_build_response(N, [{K,V} | T]) ->

    StrN = to_string(N),
    StrK = to_string(K),
    StrV = to_string(V),

    "&key" ++ StrN ++ "=" ++ yaws_api:url_encode(StrK) ++ "&value" ++ StrN ++ "=" ++ yaws_api:url_encode(StrV) ++ traverse_build_response(N + 1, T).

to_string(Value) when is_list(Value)->
    Value;
to_string(Value) when is_integer(Value)->
    integer_to_list(Value);
to_string(Value) when is_atom(Value)->
    atom_to_list(Value);
to_string(Value) when is_float(Value)->
    float_to_list(Value);
to_string(Value) when is_pid(Value)->
    pid_to_list(Value);
to_string(Value) when is_binary(Value)->
    binary_to_list(Value);
to_string(Value) ->
    lists:flatten(io_lib:format("~p", [Value])).


make_req(Req, [])->
    Req;
make_req(Req, [{Param, Value} | T])->

    case Param of
    "store" ->
        NewReq = Req#get_req{store_name = Value};

    "offset" ->
        NewReq = Req#get_req{offset = list_to_integer(Value)};

    "count" ->
        NewReq = Req#get_req{count = list_to_integer(Value)};

    _ ->
        NewReq = Req
    end,

    make_req(NewReq, T).




%EOF
