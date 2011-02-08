% $Id$
-module(ekvs_bdb_client_session).
-behavior(gen_server).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2
        ]).

%API functions
-export([
        start_link/4,
        stop/2,

        set/4,
        get/3,
        remove/3,
        bulk_get/4,

        set_bin/4,
        get_bin/3,
        remove_bin/3,
        bulk_get_bin/4,
 
        count/2,

        reconnect/2,

        flush/2
        ]).

-define(KEEP_ALIVE_TIMEOUT_MS,      30000).
-define(TCP_CONNECTION_TIMEOUT_MS,  60000).
-define(TCP_TX_TIMEOUT_MS,          60000).
-define(TCP_RX_TIMEOUT_MS,          60000).
-define(REQUEST_TIMEOUT,            infinity).

-record(server_state, {store_name, session_id, ip = "127.0.0.1", port = 22122, fd}).

% Interface functions
start_link(StoreName, SessionId, Ip, Port) ->
    gen_server:start_link(make_name(StoreName, SessionId), ?MODULE, [StoreName, SessionId, Ip, Port], []).

stop(StoreName, SessionId) ->
    gen_server:cast(make_name(StoreName, SessionId), stop).

reconnect(StoreName, SessionId)->
    gen_server:call(make_name(StoreName, SessionId), reconnect, ?REQUEST_TIMEOUT).

set(StoreName, SessionId, Key, Value)->
     gen_server:call(make_name(StoreName, SessionId), {set, Key, Value, false}, ?REQUEST_TIMEOUT).

get(StoreName, SessionId, Key)->
     gen_server:call(make_name(StoreName, SessionId), {get, Key, false}, ?REQUEST_TIMEOUT).

remove(StoreName, SessionId, Key)->
     gen_server:call(make_name(StoreName, SessionId), {remove, Key, false}, ?REQUEST_TIMEOUT).

bulk_get(StoreName, SessionId, Offset, Count)->
     gen_server:call(make_name(StoreName, SessionId), {bulk_get, Offset, Count, false}, ?REQUEST_TIMEOUT).

count(StoreName, SessionId)->
     gen_server:call(make_name(StoreName, SessionId), count, ?REQUEST_TIMEOUT).

flush(StoreName, SessionId)->
     gen_server:call(make_name(StoreName, SessionId), flush, ?REQUEST_TIMEOUT).


%no term_to_binary / binary_to_term
set_bin(StoreName, SessionId, Key, Value) when is_binary(Key); is_binary(Value) ->
     gen_server:call(make_name(StoreName, SessionId), {set, Key, Value, true}, ?REQUEST_TIMEOUT).

get_bin(StoreName, SessionId, Key) when is_binary(Key) ->
     gen_server:call(make_name(StoreName, SessionId), {get, Key, true}, ?REQUEST_TIMEOUT).

remove_bin(StoreName, SessionId, Key) when is_binary(Key) ->
     gen_server:call(make_name(StoreName, SessionId), {remove, Key, true}, ?REQUEST_TIMEOUT).

bulk_get_bin(StoreName, SessionId, Offset, Count)->
     gen_server:call(make_name(StoreName, SessionId), {bulk_get, Offset, Count, true}, ?REQUEST_TIMEOUT).



% Callback functions
init([StoreName, SessionId, Ip, Port]) ->

    erlang:process_flag(trap_exit, true),

    ServerState = #server_state {
                                store_name  = StoreName,
                                session_id  = SessionId,
                                ip          = Ip,
                                port        = Port
                                },

    {ok, ServerState, 0}.

% handle_call callback
handle_call({set, Key, Value, Bin}, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_set(Fd, Key, Value, ?TCP_RX_TIMEOUT_MS, Bin) of
    ok ->
        {ok, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};

handle_call({get, Key, Bin}, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_get(Fd, Key, ?TCP_RX_TIMEOUT_MS, Bin) of
    {ok, {not_found, _}} ->
        {{error, not_found}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    {ok, {found, Data}} ->
        {{ok, Data}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};

handle_call({remove, Key, Bin}, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_remove(Fd, Key, ?TCP_RX_TIMEOUT_MS, Bin) of
    ok ->
        {ok, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    {error, remove_failed} ->
        {{error, remove_failed}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};

handle_call(count, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_count(Fd, ?TCP_RX_TIMEOUT_MS) of
    {ok, Count} ->
        {{ok, Count}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    {error, count_failed} ->
        {{error, count_failed}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};

handle_call(flush, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_flush(Fd, ?TCP_RX_TIMEOUT_MS) of
    ok ->
        {ok, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    {error, flush_failed} ->
        {{error, flush_failed}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};


handle_call({bulk_get, Offset, Count, Bin}, _From, #server_state {fd = Fd} = ServerState) ->

    {Result, NewFd, TimeoutMs} = 
    case do_bulk_get(Fd, Offset, Count, ?TCP_RX_TIMEOUT_MS, Bin) of
    {ok, KeyValuePairs} ->
        {{ok, KeyValuePairs}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    {error, bulk_get_failed} ->
        {{error, bulk_get_failed}, Fd, ?KEEP_ALIVE_TIMEOUT_MS};

    Err ->
        close_connection(Fd),
        {Err, undefined, 0}
    end,

    {reply, Result, ServerState#server_state{fd = NewFd}, TimeoutMs};


handle_call(reconnect, _From, #server_state {ip = Ip, port = Port} = ServerState) ->

    close_connection(ServerState#server_state.fd),

    {Result, Fd} = 
    case open_connection(Ip, Port) of
    {ok, NewFd} ->
        {ok, NewFd};
    Err ->
        {{error, Err}, undefined}
    end,

    {reply, Result, ServerState#server_state {fd = Fd}, ?KEEP_ALIVE_TIMEOUT_MS};

handle_call(Call, _From, ServerState) ->
    io:format("Unsupported call [~p] in ~p!~n", [Call, ?MODULE]),
    {reply, "not supported", ServerState, ?KEEP_ALIVE_TIMEOUT_MS}.
    
% handle_cast callback
handle_cast(stop, ServerState) ->
    {stop, normal, ServerState};

handle_cast(Cast, ServerState) ->
    io:format("Unsupported cast [~p] in ~p!~n", [Cast, ?MODULE]),
    {noreply, ServerState, ?KEEP_ALIVE_TIMEOUT_MS}.
        
% handle_info callback
handle_info(timeout, ServerState) ->

    NewServerState = test_and_reconnect(ServerState),

    {noreply, NewServerState, ?KEEP_ALIVE_TIMEOUT_MS};

handle_info(Info, ServerState) ->
    io:format("Unsupported info [~p] in ~p!~n", [Info, ?MODULE]),
    {noreply, ServerState, ?KEEP_ALIVE_TIMEOUT_MS}.
        
% code_change callback
code_change(_OldVsn, ServerState, _Extra) ->
    io:format("Unsupported code change in ~p!~n", [?MODULE]),
    {ok, ServerState}.

% terminate callback
terminate(Reason, #server_state {fd = Fd, store_name = StoreName, session_id = SessionId}) ->
    
    close_connection(Fd),
    
    io:format("[~p] ~p:~p Terminating [~p]~n", [?MODULE, StoreName, SessionId,  Reason]),
    ok.

%Worker piggies
make_name(StoreName, SessionId)->
    {global, {?MODULE, StoreName, SessionId}}.

test_and_reconnect(#server_state {ip = Ip, port = Port, fd = undefined} = ServerState)->

    case open_connection(Ip, Port) of
    {ok, Fd} ->
        ServerState#server_state{fd = Fd};

    _ ->
        timer:sleep(100),
        ServerState

    end;

test_and_reconnect(#server_state{fd = Fd, ip = Ip, port = Port} = ServerState)->

    case do_keepalive(Fd, ?TCP_RX_TIMEOUT_MS) of
    ok ->
        ServerState;

    _ ->
        close_connection(Fd),

        NewFd = 
        case open_connection(Ip, Port) of
        {ok, SockFd} ->
            SockFd;
        
        _ ->
            timer:sleep(100),
            undefined
        end,
 
        ServerState#server_state {fd = NewFd}

    end.

open_connection(Ip, Port)->

    case gen_tcp:connect(Ip, Port, [binary, {packet, 0}, {active, false}, {nodelay, true}, {send_timeout, ?TCP_TX_TIMEOUT_MS}], ?TCP_CONNECTION_TIMEOUT_MS) of
    {ok, Fd} ->

        {ok, Fd};

    Error ->
        Error

    end.

close_connection(undefined)->
    ok;
close_connection(Fd)->
    ok = gen_tcp:close(Fd).




do_set(undefined, _Key, _Value, _RxTimeoutMs, _)->
    {error, not_connected};
do_set(Socket, Key, Value, RxTimeoutMs, Bin)->


    if (Bin == false) ->

    BinKey = term_to_binary(Key),
    BinValue = term_to_binary(Value),

    BinKeySize = size(BinKey),
    BinValueSize = size(BinValue);

    true ->

    BinKey = Key,
    BinValue = Value,

    BinKeySize = size(BinKey),
    BinValueSize = size(BinValue)

    end,

    Cmd = $S,

    case gen_tcp:send(Socket, <<Cmd:8/unsigned-big-integer, BinKeySize:16/unsigned-big-integer, BinKey/binary, BinValueSize:16/unsigned-big-integer, BinValue/binary>>) of

    ok ->

        case gen_tcp:recv(Socket, 0, RxTimeoutMs) of
        {ok, <<"A">>} ->
            ok;

        {ok, Other}->
            {error, Other};

        Err ->
            Err

        end;

    Err ->
        Err

    end.

do_get(undefined, _Key, _RxTimeoutMs, _)->
    {error, not_connected};
do_get(Sock, Key, RxTimeoutMs, Bin)->

    if (Bin == false) ->

    BinKey = term_to_binary(Key),

    BinKeySize = size(BinKey);

    true ->

    BinKey = Key,

    BinKeySize = size(BinKey)

    end,

    Cmd = $G,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer, BinKeySize:16/unsigned-big-integer, BinKey/binary>>) of
    ok ->
  
        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"N">>} ->
            {ok, {not_found, undefined}};

        {ok, <<"A">>} ->

            case gen_tcp:recv(Sock, 2, RxTimeoutMs) of
            {ok, <<DataLen:16/unsigned-big-integer>>} ->

                case gen_tcp:recv(Sock, DataLen, RxTimeoutMs) of
                {ok, <<BinData/binary>>} ->

                    if (Bin == false) ->
                        {ok, {found, binary_to_term(BinData)}};
                    true ->
                        {ok, {found, BinData}}

                    end;

                Err ->
                    Err

                end;

            Err ->
                Err

            end;

        Err ->
            Err

        end;

    Err ->
        Err
    end.

do_remove(undefined, _Key, _RxTimeoutMs, _)->
    {error, not_connected};
do_remove(Sock, Key, RxTimeoutMs, Bin)->

    if (Bin == false) ->

    BinKey = term_to_binary(Key),

    BinKeySize = size(BinKey);

    true ->

    BinKey = Key,

    BinKeySize = size(BinKey)

    end,

    Cmd = $R,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer, BinKeySize:16/unsigned-big-integer, BinKey/binary>>) of
    ok ->

        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"N">>} ->
            {error, remove_failed};

        {ok, <<"A">>} ->
            ok;

        Err ->
            Err

        end;

    Err ->
        Err

    end.

do_count(undefined, _RxTimeoutMs)->
    {error, not_connected};
do_count(Sock, RxTimeoutMs)->
    Cmd = $C,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer>>) of
    ok ->

        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"N">>} ->
            {error, count_failed};

        {ok, <<"A">>} ->

            case gen_tcp:recv(Sock, 2, RxTimeoutMs) of
            {ok, <<DataLen:16/unsigned-big-integer>>} ->

                case gen_tcp:recv(Sock, DataLen, RxTimeoutMs) of
                {ok, BinStrLen} ->


                    {ok, list_to_integer(binary_to_list(BinStrLen))};

                Err ->
                    Err

                end;

            Err ->
                Err

            end;

        Err ->
            Err

        end;

    Err ->
        Err

    end.

do_keepalive(undefined, _RxTimeoutMs)->
    {error, not_connected};
do_keepalive(Sock, RxTimeoutMs)->
    Cmd = $K,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer>>) of
    ok ->

        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"A">>} ->
            ok;

        Err ->
            Err

        end;

    Err ->
        Err

    end.

do_flush(undefined, _RxTimeoutMs)->
    {error, not_connected};
do_flush(Sock, RxTimeoutMs)->
    Cmd = $F,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer>>) of
    ok ->

        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"A">>} ->
            ok;

        {ok, <<"N">>} ->
            {error, flush_failed};

        Err ->
            Err

        end;

    Err ->
        Err

    end.


do_bulk_get(undefined, _Start, _Count, _RxTimeoutMs, _)->
    {error, not_connected};
do_bulk_get(Sock, Start, Count, RxTimeoutMs, Bin)->
    Cmd = $B,

    case gen_tcp:send(Sock, <<Cmd:8/unsigned-big-integer, Start:32/unsigned-big-integer, Count:32/unsigned-big-integer>>) of
    ok ->

        case gen_tcp:recv(Sock, 1, RxTimeoutMs) of
        {ok, <<"A">>} -> 

            case gen_tcp:recv(Sock, 4, RxTimeoutMs) of
            {ok, <<ItemCount:32/unsigned-big-integer>>} ->
                loop_rx_bulk_get(ItemCount, Sock, RxTimeoutMs, Bin, []);

            Err ->
                Err

            end;

        {ok, <<"N">>} ->
            {error, bulk_get_failed};

        Err ->
            Err

        end;

    Err ->
        Err

    end.

loop_rx_bulk_get(0, _Sock, _RxTimeoutMs, _, KVP)->
    {ok, KVP};
loop_rx_bulk_get(N, Sock, RxTimeoutMs, Bin, KVP)->

    case read_key_and_data(Sock, RxTimeoutMs) of
    {ok, BinKey, BinData} ->

        Item =
        if (Bin == false) ->
            case catch(make_key_value_tuple( BinKey, BinData)) of
            {'EXIT', _} ->
                {BinKey, BinData};
            {A, B} ->
                {A, B}
            end;

        true ->
            {BinKey, BinData}
        end,

        loop_rx_bulk_get(N -1, Sock, RxTimeoutMs, Bin, lists:append(KVP, [Item]));

    Err ->
        Err
    end.

make_key_value_tuple(BinKey, BinData)->

    {binary_to_term(BinKey), binary_to_term(BinData)}.

read_key_and_data(Sock, RxTimeoutMs)->

    case gen_tcp:recv(Sock, 4, RxTimeoutMs) of
    {ok, <<KeyLen:32/unsigned-big-integer>>} ->

        case gen_tcp:recv(Sock, KeyLen, RxTimeoutMs) of
        {ok, BinKey} ->


            case gen_tcp:recv(Sock, 4, RxTimeoutMs) of
            {ok, <<DataLen:32/unsigned-big-integer>>} ->

                case gen_tcp:recv(Sock, DataLen, RxTimeoutMs) of
                {ok, BinData} ->

                    {ok, BinKey, BinData};

                Err ->
                    Err

                end;

            Err ->
                Err

            end;

        Err ->
            Err

        end;

    Err ->
        Err

    end.

% EOF
