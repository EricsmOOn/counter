%%----------------------------------------------------
%% @doc 好一点的计数器
%% 该计数器提供新建/增加/减少/查询功能 不可删除
%% [内存泄漏]该计数器组内存不会释放
%% @author Eric Wong
%% @end
%% Created : 2021-11-03 16:10 Wednesday
%%----------------------------------------------------
-module(counter).
-export([
        make/1, make/2, make/3
        ,add/1, add/2, add/3
        ,sub/1, sub/2, sub/3
        ,get/1, get/2
    ]).

-type result() :: ok | {false, bitstring()}.

%%----------------------------------------------------
%% 外部接口
%%----------------------------------------------------
%% @doc 创建一个名为Key的计数器
-spec make(pos_integer()) -> result().
make(Key) ->
    make(Key, 1).

%% @doc 创建一个名称为Key的拥有Size位置的计数器组
-spec make(term(), pos_integer()) -> result().
make(Key, Size) ->
    make(safe, Key, Size).

%% @doc 创建一个指定类型名称为Key的拥有Size位置的计数器组
%% safe类型在并发下表现为Sequential consistency
%% unsafe类型在并发下表现为最终一致性但并发写性能更好读性能更差
%% 例如：
%% 在并发情况下 W1 W2依次分别写入 与此同时R进行读取
%% 在safe情况下R可能读取到的结果是：
%% 初始状态/W1完成结果/W1和W2完成的结果
%% 在unsafe情况下R可能读取到的结果是：
%% 初始状态/W1完成结果/W2完成结果/W1和W2完成的结果
-spec make(safe | unsafe, term(), pos_integer()) -> result().
make(Type, Key, Size) ->
    case exist(Key) of
        false ->
            make_counter(Type, Key, Size);
        _ ->
            {false, <<"key_already_exist">>}
    end.

%% @doc 名称为Key的计数器自增一
-spec add(term()) -> result().
add(Key) ->
    add(Key, 1).

%% @doc 名称为Key的计数器自增Incr
-spec add(term(), pos_integer()) -> result().
add(Key, Incr) ->
    add(Key, 1, Incr).

%% @doc 名称为Key的计数器组中的第Ix个自增Incr
-spec add(term(), pos_integer(), pos_integer()) -> result().
add(Key, Ix, Incr) when is_integer(Ix) andalso is_integer(Incr) andalso Ix > 0 andalso Incr > 0 ->
    case read(Key) of
        Ref = {_, R} when is_reference(R) ->
            counters:add(Ref, Ix, Incr);
        _ ->
            {false, <<"key_not_exist">>}
    end;
add(_, _, _) ->
    {false, <<"args_error">>}.

%% @doc 名称为Key的计数器自减一
-spec sub(term()) -> result().
sub(Key) ->
    sub(Key, 1).

%% @doc 名称为Key的计数器自减Decr
-spec sub(term(), pos_integer()) -> result().
sub(Key, Decr) ->
    sub(Key, 1, Decr).

%% @doc 名称为Key的计数器组中的第Ix个自增Incr
-spec sub(term(), pos_integer(), pos_integer()) -> result().
sub(Key, Ix, Decr) when is_integer(Ix) andalso is_integer(Decr) andalso Ix > 0 andalso Decr > 0 ->
    case read(Key) of
        Ref = {_, R} when is_reference(R) ->
            counters:sub(Ref, Ix, Decr);
        _ ->
            {false, <<"key_not_exist">>}
    end;
sub(_, _, _) ->
    {false, <<"args_error">>}.

%% @doc 名称为Key的计数器组中的第Ix个当前计数
-spec get(term()) -> integer().
get(Key) ->
    get(Key, 1).

%% @doc 名称为Key的计数器组中的第Ix个当前计数
-spec get(term(), pos_integer()) -> integer().
get(Key, Ix) when is_integer(Ix) andalso Ix > 0 ->
    case read(Key) of
        Ref = {_, R} when is_reference(R) ->
            counters:get(Ref, Ix);
        _ ->
            {false, <<"key_not_exist">>}
    end;
get(_, _) ->
    {false, <<"args_error">>}.

%%----------------------------------------------------
%% 内部私有
%%----------------------------------------------------
make_counter(safe, Key, Size) when is_integer(Size) andalso Size > 0 ->
    Ref = counters:new(Size, []),
    save(Key, Ref);
make_counter(unsafe, Key, Size) when is_integer(Size) andalso Size > 0 ->
    Ref = counters:new(Size, [write_concurrency]),
    save(Key, Ref);
make_counter(_, _, _) ->
    {false, <<"args_error">>}.

save(Key, Ref = {_, R}) when is_reference(R) ->
    persistent_term:put({?MODULE, Key}, Ref).

read(Key) ->
    case persistent_term:get({?MODULE, Key}, undefined) of
        Ref = {_, R} when is_reference(R) -> Ref;
        _ -> undefined
    end.

exist(Key) ->
    read(Key) =/= undefined.

%%----------------------------------------------------
%% 测试用例
%%----------------------------------------------------
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

normal_test() ->
    persistent_term:erase({?MODULE, t_counter}),
    ok = make(safe, t_counter, 9999),
    {false, <<"key_already_exist">>} = make(unsafe, t_counter, 9999),
    {false, <<"args_error">>} = make(err, err_counter, 9999),

    ok = add(t_counter, 9989, 10086),
    {false, <<"args_error">>} = add(err_counter, <<>>, 10086),
    {false, <<"key_not_exist">>} = add(err_counter, 9989, 10086),

    ok = sub(t_counter, 9989, 86),

    {false, <<"args_error">>} = get(t_counter, 0),
    0 = get(t_counter, 1),
    10000 = get(t_counter, 9989).

% unsafe_test() ->
%     persistent_term:erase({?MODULE, un_counter}),
%     ok = make(un_counter),
%     F = fun() -> [begin add(un_counter, X), sub(un_counter, X) end || X <- lists:seq(1, 100000)] end,
%     spawn(F),
%     0 = counter:get(un_counter).

% safe_test() ->
%     persistent_term:erase(s_counter),
%     ok = make(s_counter),
%     Add = fun() -> add(s_counter, 2) end,
%     Sub = fun() -> sub(s_counter, 2) end,
%     [spawn(Add) || _ <- lists:seq(1, 100000)],
%     [spawn(Sub) || _ <- lists:seq(1, 100000)],
%     0 =/= counter:get(s_counter).

-endif.
