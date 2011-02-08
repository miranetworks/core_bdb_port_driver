%$Id$
-module(bdb_port_driver_proxy).

-behaviour(gen_server).

-export([start_link/1, stop/1, set/3, get/2, del/2, count/1, sync/1, bulk_get/3, truncate/1, compact/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

start_link(DbName) ->
    gen_server:start_link(make_name(DbName), ?MODULE, [DbName], []).

stop(DbName)->
    gen_server:cast(make_name(DbName), stop). 

set(DbName, Key, Value)->
    gen_server:call(make_name(DbName), {set, Key, Value}, infinity).

get(DbName, Key)->
    gen_server:call(make_name(DbName), {get, Key}, infinity).

del(DbName, Key)->
    gen_server:call(make_name(DbName), {del, Key}, infinity).

count(DbName)->
    gen_server:call(make_name(DbName), count, infinity).

sync(DbName)->
    gen_server:call(make_name(DbName), sync, infinity).

bulk_get(DbName, Offset, Count)->
    gen_server:call(make_name(DbName), {bulk_get, Offset, Count}, infinity).

truncate(DbName)->
    gen_server:call(make_name(DbName), truncate, infinity).

compact(DbName)->
    gen_server:call(make_name(DbName), compact, infinity).



make_name(DbName)->
    {global, {?MODULE, DbName}}.


init([DbName]) ->
    {ok, DbName}.

handle_call(truncate, _From, DbName) ->

    Reply =

    case catch(bdb_port_driver:truncate(DbName)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call(compact, _From, DbName) ->

    Reply =

    case catch(bdb_port_driver:compact(DbName)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};



handle_call(sync, _From, DbName) ->

    Reply =

    case catch(bdb_port_driver:sync(DbName)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call(count, _From, DbName) ->

    Reply =

    case catch(bdb_port_driver:count(DbName)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call({bulk_get, Offset, Count}, _From, DbName)
  when is_integer(Offset) and is_integer(Count) ->

    Reply =

    case catch(bdb_port_driver:bulk_get(DbName, Offset, Count)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};


handle_call({set, Key, Value}, _From, DbName)
  when is_binary(Key) and is_binary(Value) ->

    Reply =

    case catch(bdb_port_driver:set(DbName, Key, Value)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call({get, Key}, _From, DbName)
  when is_binary(Key) ->

    Reply =

    case catch(bdb_port_driver:get(DbName, Key)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call({del, Key}, _From, DbName)
  when is_binary(Key) ->

    Reply =

    case catch(bdb_port_driver:del(DbName, Key)) of
    {'EXIT', Err} ->
        {error, Err};

    Rsp ->
        Rsp

    end,

    {reply, Reply, DbName};

handle_call(_Request, _From, DbName) ->
    Reply = {error, unkown_call},
    {reply, Reply, DbName}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%EOF
