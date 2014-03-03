-module(bdb_store_benchmark).

-export([start/0]).

%-define(ts, now()).
-define(ts, os:timestamp()).

-define(info(X), error_logger:info_report({?MODULE, X})).
-define(warn(X), error_logger:warning_report({?MODULE, X})).

-define(diffms(T1, T0), trunc(timer:now_diff(T1, T0)/1000)).
-define(diffus(T1, T0), trunc(timer:now_diff(T1, T0))).

-define(insert_data, <<"01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\n">>).

start() ->

    error_logger:tty(true),

    ok = setup(),

    F1 = fun() -> loop_do_some_computations(0) end,
    F2 = fun(Id) -> {ok, Fd} = file:open("benchmark/benchmark_worker.log" ++ integer_to_list(Id), [write, append, raw, binary]), loop_log_some_lines(Fd, 0) end,

    T0 = ?ts,
%    OtherWorkerPids1 = [],
%    OtherWorkerPids2 = [],

    OtherWorkerPids1 = [erlang:spawn(F1) || _ <- lists:seq(1, 5)],
    OtherWorkerPids2 = [erlang:spawn(fun() -> F2(I) end) || I <- lists:seq(1, 5)],

    ok = test_insert_many(1000000),

    ok = test_lookup_many(1000000),

    ok = test_del_many(1000000),

    [Pid ! {stop, self()} || Pid <- OtherWorkerPids1],
    [Pid ! {stop, self()} || Pid <- OtherWorkerPids2],

    FD1 = fun() ->
        receive {done1, Count1, Done1Ts} ->
            DiffUs1 = ?diffus(Done1Ts, T0),
            ?info({other_work_done_per_us, Count1, Count1/DiffUs1}),
            Count1
        end
    end,

    FD2 = fun() ->
        receive {done2, Count2, Done2Ts} ->
            DiffMs2 = ?diffms(Done2Ts, T0),
            ?info({records_logged_per_ms, Count2, trunc(Count2/DiffMs2)}),
            Count2
        end
    end,

    TotalCalcs = lists:sum([FD1() || _ <- OtherWorkerPids1]),
    TotalWrites = lists:sum([FD2() || _ <- OtherWorkerPids2]),

    TotalMs = ?diffms(?ts, T0),

    ?info({total_time, TotalMs, work_done, TotalCalcs, records_logged, TotalWrites, work_per_ms, TotalCalcs/ TotalMs, records_logged_per_ms, TotalWrites/TotalMs}),

    ?info({memory, erlang:memory()}),

    [erlang:garbage_collect(P) || P <- erlang:processes()],

    timer:sleep(2000),

    ?info({memory, erlang:memory()}),

    ok = shutdown(),

    init:stop().

setup() ->

    os:cmd("rm -fr ./benchmark"),

    {ok, _} = bdb_store:start_link("test", "./benchmark", [{txn_enable, true}, {cache_size, 4 * 1024 * 1024}, {page_size, 4096}, {buffer_size, 1024 * 1024}, {db_type, btree}]),

    ok.

shutdown() ->
    kill({global, {bdb_store, "test"}}, shutdown).


test_insert_many(N) ->

    T0 = ?ts,

    ok = loop_insert(N),

    T1 = ?ts,

    ok = bdb_store:sync("test"),

    T2 = ?ts,

    ?info({inserting, N, took, ?diffms(T1, T0)}),   
    ?info({sync, N, took, ?diffms(T2, T1)}),

    ok.
    
test_lookup_many(N) ->

    T0 = ?ts,

    ok = loop_lookup(1, N),

    T1 = ?ts,

    ?info({lookup, N, took, ?diffms(T1, T0)}),   

    ok.

test_del_many(N) ->

    T0 = ?ts,

    ok = loop_del(1, N),

    T1 = ?ts,

    ok = bdb_store:sync("test"),

    T2 = ?ts,

    ?info({del, N, took, ?diffms(T1, T0)}),   
    ?info({sync, N, took, ?diffms(T2, T1)}),

    ok.
  
loop_lookup(P, N) when P =< N ->
    Key = <<P:64/unsigned-big-integer>>,
    {ok, _} = bdb_store:get("test", Key),
    loop_lookup(P+1, N);
loop_lookup(_,_) -> ok.

loop_del(P, N) when P =< N ->
    Key = <<P:64/unsigned-big-integer>>,
    ok = bdb_store:del("test", Key),
    loop_del(P+1, N);
loop_del(_,_) -> ok.


loop_insert(N) when N > 0 ->
    Key = <<N:64/unsigned-big-integer>>,
    ok = bdb_store:set("test", Key, ?insert_data),
    loop_insert(N-1);
loop_insert(0) -> ok.

kill({global, Name}, How) when ((How =:= shutdown) or (How =:= kill)) ->
    kill(global:whereis_name(Name), How);

kill(Pid, How) when is_pid(Pid) and ((How =:= shutdown) or (How =:= kill)) ->
    unlink(Pid),
    monitor(process, Pid),
    exit(Pid, How),
    receive {'DOWN', _, process, Pid, _} -> ok end.


loop_do_some_computations(N) ->

    receive {stop, Pid} ->
        Pid ! {done1, N, ?ts}

    after 0 ->
        random:seed(?ts),

        V1 = random:uniform(1 + N),

        V2 = random:uniform(V1 + N),

        NewVal = V1 + V2,

%        bdb_store:get("test", <<"somekey">>),

        loop_do_some_computations(N+1)

    end.

loop_log_some_lines(Fd, N) ->

    receive {stop, Pid} ->

        ok = file:close(Fd),

        Pid ! {done2, N, ?ts}

    after 0 ->

        ok = file:write(Fd, ?insert_data),

%        bdb_store:get("test", <<"somekey">>),

        loop_log_some_lines(Fd, N+1)

    end.

  

%EOF
