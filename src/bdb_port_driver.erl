-module(bdb_port_driver).

-behaviour(gen_server).

-include("bdb_port_driver.hrl").

-export([start_link/3, set/3, get/2, del/2, count/1, sync/1, bulk_get/3, truncate/1, compact/1, fold/5, foldr/5, map/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define('DRIVER_NAME', 'mira_bdb_port_driver_drv').

-record(state, {port}).
-define(NAME(DbName), {global, {?MODULE, DbName}}).


start_link(DbName, DataDir, Options) ->
    gen_server:start_link(?NAME(DbName), ?MODULE, [DbName, DataDir, Options], []).

set(DbName, Key, Value)->
    gen_server:call(?NAME(DbName), {set, Key, Value}, infinity).

get(DbName, Key)->
    gen_server:call(?NAME(DbName), {get, Key}, infinity).

del(DbName, Key)->
    gen_server:call(?NAME(DbName), {del, Key}, infinity).

count(DbName)->
    gen_server:call(?NAME(DbName), count, infinity).

sync(DbName)->
    gen_server:call(?NAME(DbName), sync, infinity).

bulk_get(DbName, Offset, Count)->
    gen_server:call(?NAME(DbName), {bulk_get, Offset, Count}, infinity).

truncate(DbName)->
    gen_server:call(?NAME(DbName), truncate, infinity).

compact(DbName)->
    gen_server:call(?NAME(DbName), compact, infinity).

fold(DbName, Fun, Acc, Start, BatchSize)->
    gen_server:call(?NAME(DbName), {fold, Fun, Acc, Start, BatchSize}, infinity).

foldr(DbName, Fun, Acc, Start, BatchSize)->
    gen_server:call(?NAME(DbName), {foldr, Fun, Acc, Start, BatchSize}, infinity).

map(DbName, Key, Fun)->
    gen_server:call(?NAME(DbName), {map, Key, Fun}, infinity).


%Options: List of {option, value} pairs where option can be:
%   
%   sync        -> integer() > 0 (Will call bdb_store:sync at "sync" milliseconds intervals)
%   txn_enabled -> true | false
%   db_type     -> btree | hash
%   cache_size  -> integer() (Size of the in memory cahc in bytes - must be a multiple of 1024 !!!)
%   page_size   -> integer() (B-Tree page size in bytes)
%   buffer_size -> integer() (Size of the bulk_get buffer in bytes - must new a multiple of 1024 !!!!) 
%

init([DbName, DataDir, Options]) ->
 
    process_flag(trap_exit, true),

    ok = filelib:ensure_dir(DataDir ++ "/"),

    TxnEnabled                 = option(txn_enabled, Options, true),
    DbType                     = option(db_type,     Options, btree),
    CacheSizeBytes             = option(cache_size,  Options, trunc(5 * 1024 * 1024)),
    PageSizeBytes              = option(page_size,   Options, 4096),
    BulkGetBufferSizeBytes     = option(buffer_size, Options, trunc(1 * 1024 * 1024)),

    true = is_integer(CacheSizeBytes),
    true = is_integer(PageSizeBytes),
    true = is_integer(BulkGetBufferSizeBytes),

    SearchDir = filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]),
    case erl_ddll:load(SearchDir, atom_to_list(?DRIVER_NAME)) of
	ok ->

        Port = open_port({spawn, ?DRIVER_NAME}, [binary]),

        BinDataDir = list_to_binary(DataDir),
        BinDbName = list_to_binary(DbName),

        BinDataDirSize = size(BinDataDir),
        BinDbNameSize = size(BinDbName),
        
        Txn =
        if (TxnEnabled == true) ->
            1;
        true ->
            0
        end,

        Dbt = 
        if (DbType == btree) ->
            $B;
        true ->
            $H
        end,

        Cmd = $O,

        Spare0 = 0,

        Message =  <<   Cmd:8/unsigned-big-integer, 

                        Txn:8/unsigned-big-integer, 

                        Dbt:8/unsigned-big-integer, 

                        Spare0:8/unsigned-big-integer, 

                        CacheSizeBytes:32/unsigned-big-integer,
                        PageSizeBytes:32/unsigned-big-integer,
                        BulkGetBufferSizeBytes:32/unsigned-big-integer,
 
                        BinDbNameSize:32/unsigned-big-integer, 
                        BinDbName/binary, 

                        BinDataDirSize:32/unsigned-big-integer, 
                        BinDataDir/binary
                        
                        >>,

        case send_command(Port, Message) of
        ok ->

    	    {ok, #state{port=Port}};

        Error ->
            ?warn({?MODULE, DbName, Error}),
            Error
        end;

	Error ->
	    Error
    end.

handle_call(truncate, _From, State) ->

    Cmd = $T,

    Message =  <<Cmd:8/unsigned-big-integer>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};


handle_call(compact, _From, State) ->

    Cmd = $Z,

    Message =  <<Cmd:8/unsigned-big-integer>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};


handle_call(sync, _From, State) ->

    Cmd = $F,

    Message =  <<Cmd:8/unsigned-big-integer>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call(count, _From, State) ->

    Cmd = $C,

    Message =  <<Cmd:8/unsigned-big-integer>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call({bulk_get, Offset, Count}, _From, State)
  when is_integer(Offset) and is_integer(Count) ->

    Cmd = $B,

    Message =  <<Cmd:8/unsigned-big-integer, Offset:32/unsigned-big-integer, Count:32/unsigned-big-integer>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};


handle_call({fold, Fun, Acc, Start, BatchSize}, _From, State) ->

    Reply =
    case catch(traverse_fold(State#state.port, Fun, Acc, Start, BatchSize)) of
    {'EXIT', _} = Ex ->
        {error, Ex};

    NewAcc ->
        NewAcc

    end,

    {reply, Reply, State};

handle_call({foldr, Fun, Acc, Start, BatchSize}, _From, State) ->

    Reply =
    case catch(traverse_foldr(State#state.port, Fun, Acc, Start, BatchSize)) of
    {'EXIT', _} = Ex ->
        {error, Ex};

    NewAcc ->
        NewAcc

    end,

    {reply, Reply, State};


handle_call({set, Key, Value}, _From, State)
  when is_binary(Key) and is_binary(Value) ->

    KeySize   = size(Key),
    ValueSize = size(Value),

    Cmd = $S,

    Message =  <<Cmd:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary, ValueSize:32/unsigned-big-integer, Value/binary>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call({get, Key}, _From, State)
  when is_binary(Key) ->

    KeySize   = size(Key),

    Cmd = $G,

    Message =  <<Cmd:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call({map, Key, Fun}, _From, State) when is_binary(Key) and is_function(Fun, 1) ->
    KeySize   = size(Key),
    GetMessage =  <<$G:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary>>,
    case send_command(State#state.port, GetMessage) of
    {ok, LCurrentValue} ->
        CurrentBinValue = list_to_binary(LCurrentValue),
        Reply =
        case catch(Fun(CurrentBinValue)) of
        {update, NewValue} when is_binary(NewValue) ->
            ValueSize = size(NewValue),
            SetMessage = <<$S:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary, ValueSize:32/unsigned-big-integer, NewValue/binary>>,
            case send_command(State#state.port, SetMessage) of
            ok ->
                {ok, {updated, NewValue}};
            Error ->
                Error
            end;
        ignore ->
            {ok, {ignored, CurrentBinValue}};
        delete ->
            DelMessage = <<$D:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary>>,
            case send_command(State#state.port, DelMessage) of
            ok ->
                {ok, {deleted, CurrentBinValue}};
            Error ->
                Error
            end;
        Error ->
            {error, Error}
        end,
        {reply, Reply, State};
    Reply ->    
        {reply, Reply, State}
    end;

handle_call({del, Key}, _From, State)
  when is_binary(Key) ->
    KeySize   = size(Key),

    Cmd = $D,

    Message =  <<Cmd:8/unsigned-big-integer, KeySize:32/unsigned-big-integer, Key/binary>>,

    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call(Request, _From, State) ->
    ?warn({unexpected, {handle_call, Request}}),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?warn({unexpected, {handle_cast, Msg}}),
    {noreply, State}.

handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
    ?warn({port_died, Reason}),
    {stop, {port_terminated, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate({port_terminated, _Reason}, _State) ->
    ok;

terminate(_Reason, State) ->
    port_close(State#state.port),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_command(Port, Command) ->
    port_command(Port, Command),
    receive
	Data ->
	    Data
%    after 25000 ->
%	    {error, timeout}
    end.


option(Name, List, Default)->

    case lists:keysearch(Name, 1, List) of
    {value, {_, Value}} ->
        Value;

    _ ->
        Default
    end.


traverse_fold(Port, Fun, Acc, Offset, Count) ->

    Cmd = $B,

    Message =  <<Cmd:8/unsigned-big-integer, Offset:32/unsigned-big-integer, Count:32/unsigned-big-integer>>,

    case send_command(Port, Message) of
    {ok, []} ->
        %EOS
        {ok, Acc};

    {ok, Data} ->

        NewAcc = traverse_fold_batch(Fun, Acc, Data),

        traverse_fold(Port, Fun, NewAcc, Offset + length(Data), Count);


    Error ->
        {error, Error}

    end.

traverse_foldr(Port, Fun, Acc, Offset, Count) when Offset >= 1 ->

    Cmd = $B,

    Message =  <<Cmd:8/unsigned-big-integer, Offset:32/unsigned-big-integer, Count:32/unsigned-big-integer>>,

    case send_command(Port, Message) of
    {ok, []} ->
        %EOS
        {ok, Acc};

    {ok, Data} ->

        NewAcc = traverse_fold_batch(Fun, Acc, lists:reverse(Data)),

        traverse_foldr(Port, Fun, NewAcc, Offset - length(Data), Count);


    Error ->
        {error, Error}

    end;
traverse_foldr(_Port, _Fun, Acc, _Offset, _Count) ->
    {ok, Acc}.


traverse_fold_batch(Fun, Acc, [{LKey, LData} | T]) ->

    NewAcc = Fun(list_to_binary(LKey), list_to_binary(LData), Acc),

    traverse_fold_batch(Fun, NewAcc, T);

traverse_fold_batch(_, Acc, []) -> Acc.


