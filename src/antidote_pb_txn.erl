%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_pb_txn).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/antidote_pb.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #fpbatomicupdatetxnreq{} ->
            {ok, Msg, {"antidote.atomicupdate", <<>>}};
        #fpbsnapshotreadtxnreq{} ->
            {ok, Msg, {"antidote.snapshotread",<<>>}};
        #fpbgeneraltxnreq{} ->
            {ok, Msg, {"antidote.generaltxn",<<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbatomicupdatetxnreq{ops = Ops}, State) ->
    Updates = decode_au_txn_ops(Ops),
    case antidote:clocksi_bulk_update(Updates) of
        {error, _Reason} ->
            {reply, #fpbatomicupdatetxnresp{success = false}, State};
        {ok, {_Txid, _ReadSet, CommitTime}} ->
            {reply, #fpbatomicupdatetxnresp{success = true,
                                            clock=term_to_binary(CommitTime)},
             State}
    end;

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbgeneraltxnreq{ops = Ops}, State) ->
    Updates = decode_general_txn(Ops),
    case antidote:clocksi_execute_g_tx(Updates) of
        {error, _Reason} ->
            {reply, #fpbsnapshotreadtxnresp{success = false}, State};
        {ok, {_Txid, ReadSet, CommitTime}} ->
            FlattenedList = lists:flatten(Updates),
            ReadReqs = lists:filter(fun(Op) -> case Op of 
                            {update, _, _, _} -> false; {read, _, _} -> true end end, FlattenedList),
            Zipped = lists:zip(ReadReqs, ReadSet), 
            Reply = encode_snapshot_read_response(Zipped),
            {reply, #fpbsnapshotreadtxnresp{success=true,
                                            clock= term_to_binary(CommitTime),
                                            results=Reply}, State}
    end;

process(#fpbsnapshotreadtxnreq{ops = Ops}, State) ->
    ReadReqs = decode_snapshot_read_ops(Ops),
    %%TODO: change this to interactive reads
    case antidote:clocksi_execute_tx(ReadReqs) of
        {ok, {_TxId, ReadSet, CommitTime}} ->
            Zipped = lists:zip(ReadReqs, ReadSet),
            Reply = encode_snapshot_read_response(Zipped),
            {reply, #fpbsnapshotreadtxnresp{success=true,
                                            clock= term_to_binary(CommitTime),
                                            results=Reply}, State};
        Other ->
            lager:error("Clocksi execute received ~p",[Other]),
            {reply, #fpbsnapshotreadtxnresp{success=false}, State}
    end.


%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

decode_general_txn(Ops) ->
    TList = lists:foldl(fun(#fpbgeneraltxnlist{op=OpList}, Acc) -> 
            [lists:map(fun(Op) -> decode_general_txn_op(Op) end, OpList)|Acc] 
            end, [], Ops),
    lists:reverse(TList).
    
%% Counter
decode_general_txn_op(#fpbgeneraltxnop{counterinc=#fpbincrementreq{key=Key, amount=Amount}}) ->
    {update, Key, riak_dt_pncounter, {{increment, Amount}, node()}};
decode_general_txn_op(#fpbgeneraltxnop{counterdec=#fpbdecrementreq{key=Key, amount=Amount}}) ->
    {update, Key, riak_dt_pncounter, {{decrement, Amount}, node()}};
%% Set
decode_general_txn_op(#fpbgeneraltxnop{setupdate=#fpbsetupdatereq{key=Key, adds=AddElems, rems=RemElems}}) ->
    Adds = lists:map(fun(X) ->
                              binary_to_term(X)
                      end, AddElems),
    _Rems = lists:map(fun(X) ->
                              binary_to_term(X)
                      end, RemElems),
    Op = case length(Adds) of
             0 -> [];
             1 -> {update, Key, riak_dt_orset, {{add,Adds}, node()}};
             _ -> {update, Key, riak_dt_orset, {{add_all, Adds},node()}}
         end,
    Op;
    %case length(Rems) of
    %    0 -> Op;
    %    1 -> [{update, Key, riak_dt_orset, {{remove,Adds}, ignore}}] ++ Op;
    %    _ -> [{update, Key, riak_dt_orset, {{remove_all, Adds},ignore}}] ++ Op
    %end;
decode_general_txn_op(#fpbgeneraltxnop{counter=#fpbgetcounterreq{key=Key}}) ->
    {read, Key, riak_dt_pncounter};
decode_general_txn_op(#fpbgeneraltxnop{set=#fpbgetsetreq{key=Key}}) ->
    {read, Key, riak_dt_orset}.

decode_au_txn_ops(Ops) ->
    lists:foldl(fun(Op, Acc) ->
                     Acc ++ decode_au_txn_op(Op)
                end, [], Ops).

%% Counter
decode_au_txn_op(#fpbatomicupdatetxnop{counterinc=#fpbincrementreq{key=Key, amount=Amount}}) ->
    [{update, Key, riak_dt_pncounter, {{increment, Amount}, node()}}];
decode_au_txn_op(#fpbatomicupdatetxnop{counterdec=#fpbdecrementreq{key=Key, amount=Amount}}) ->
    [{update, Key, riak_dt_pncounter, {{decrement, Amount}, node()}}];
%% Set
decode_au_txn_op(#fpbatomicupdatetxnop{setupdate=#fpbsetupdatereq{key=Key, adds=AddElems, rems=RemElems}}) ->
    Adds = lists:map(fun(X) ->
                              binary_to_term(X)
                      end, AddElems),
    Rems = lists:map(fun(X) ->
                              binary_to_term(X)
                      end, RemElems),
    Op = case length(Adds) of
             0 -> [];
             1 -> [{update, Key, riak_dt_orset, {{add,Adds}, node()}}];
             _ -> [{update, Key, riak_dt_orset, {{add_all, Adds},node()}}]
         end,
    case length(Rems) of
        0 -> Op;
        1 -> [{update, Key, riak_dt_orset, {{remove,Adds}, ignore}}] ++ Op;
        _ -> [{update, Key, riak_dt_orset, {{remove_all, Adds},ignore}}] ++ Op
    end.

decode_snapshot_read_ops(Ops) ->
    lists:map(fun(Op) ->
                      decode_snapshot_txn_op(Op)
              end, Ops).

decode_snapshot_txn_op(#fpbsnapshotreadtxnop{counter=#fpbgetcounterreq{key=Key}}) ->
    {read, Key, riak_dt_pncounter};
decode_snapshot_txn_op(#fpbsnapshotreadtxnop{set=#fpbgetsetreq{key=Key}}) ->
    {read, Key, riak_dt_orset}.

encode_snapshot_read_response(Zipped) ->
    lists:map(fun(Resp) ->
                      encode_snapshot_read_resp(Resp)
              end, Zipped).
encode_snapshot_read_resp({{read, Key, riak_dt_pncounter}, Result}) ->
    #fpbsnapshotreadtxnrespvalue{key=Key,counter=#fpbgetcounterresp{value =Result}};
encode_snapshot_read_resp({{read,Key,riak_dt_orset}, Result}) ->
    #fpbsnapshotreadtxnrespvalue{key=Key,set=#fpbgetsetresp{value = term_to_binary(Result)}}.
