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
%% @doc The coordinator for a given Clock SI general tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_general_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-endif.

-define(HONEYPOT, true).

%% API
-export([start_link/3, start_link/2]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([execute_batch_ops/2,
         finish_op/3,
         receive_prepared/2,
         single_committing/2,
         committing/2,
         receive_committed/2,
         abort/2,
         reply_to_client/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
	  from :: {pid(), term()},
	  tx_id :: txid(),
      operations :: [],
	  num_to_ack :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
	  commit_time :: non_neg_integer(),
      updated_partitions :: dict(),
      final_read_set :: [],
      read_set :: [],
      causal_clock :: non_neg_integer(),
	  state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    random:seed(now()),
    SD = #state{
            causal_clock = ClientClock,
            operations = Operations,
            updated_partitions = dict:new(),
            final_read_set=[],
            read_set = [],
            from = From,
            prepare_time=0
           },
    {ok, execute_batch_ops, SD, 0}.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_batch_ops(abort, SD) ->
    execute_batch_ops(timeout, SD);

execute_batch_ops({prepared, _}, SD) ->
    execute_batch_ops(timeout, SD);

execute_batch_ops(timeout, 
           SD=#state{causal_clock=CausalClock,
                    operations=Operations
		      }) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    [CurrentOps|_RestOps] = Operations, 
    
    ProcessOp = fun(Operation, {UpdatedParts, RSet}) ->
        case Operation of  
            {read, Key, Type} ->
                {UpdatedParts, [{Key, Type}|RSet]};
                    % {ok, {Type, Snapshot}}-> %returned by cache_serv:read(hd(Preflist), Key, Type, TxId); 
                    %      {UpdatedParts, [Type:value(Snapshot)|RSet], dict:store(Key, Snapshot, Buffer)};
            {update, Key, Type, Op} ->
                Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                IndexNode = hd(Preflist),
                UpdatedParts1 = case dict:is_key(IndexNode, UpdatedParts) of
                                    false ->
                                        dict:store(IndexNode, [{Key, Type, Op}], UpdatedParts);
                                    true ->
                                        dict:append(IndexNode, {Key, Type, Op}, UpdatedParts)
                                end,
                {UpdatedParts1, RSet}
        end
    end,

    
    %%ReadSet1 is a list of values [0,1,2,3,]
    {WriteSet1, ReadSet1} = lists:foldl(ProcessOp, {dict:new(), []}, CurrentOps),
    %lager:info("Operations are ~w, WriteSet is ~w, ReadSet is ~w",[CurrentOps, WriteSet1, ReadSet1]),
    io:format("opeations: ~p ~n", [WriteSet1]),
    %%cache_serv:read(ReadSet1, TxId), %get keys consistently 

cache_serv:update(dict:to_list(WriteSet1), TxId, self()),
reply_to_client(SD#state{state=committed, tx_id=TxId, read_set=fetch_data(ReadSet1, TxId), commit_time=clocksi_vnode:now_microsec(now())}).

fetch_data(ReadSet, TxId) ->
    case ReadSet of
        [] -> [empty];
        _ -> FromRs = cache_serv:read(ReadSet, TxId), 
            io:format("result: ~p~n", [FromRs]),
            FromRs
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                            prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 
        1 ->
            {next_state, committing,
                S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}, 0};
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
    
single_committing(abort, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, SD0=#state{tx_id = TxId,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    case dict:size(UpdatedPartitions) of
        0 ->
            reply_to_client(SD0#state{state=committed});
        N ->
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, TxId, Commit_time),
            {next_state, receive_committed,
             SD0#state{num_to_ack=N, state=committing}}
    end.


%% @doc the fsm waits for acks indicating that each partition has successfully
%%	committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(S0#state{state=committed});
        _ ->
           {next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.

%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{tx_id = TxId,
                          updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort(abort, SD0=#state{tx_id = TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort({prepared, _}, SD0=#state{tx_id=TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
reply_to_client(SD=#state{from=From, tx_id=TxId, state=TxState, read_set=ReadSet,
                final_read_set=FinalReadSet, commit_time=CommitTime, operations=Operations}) ->
    case TxState of
        committed ->
            case length(Operations) of 
                1 ->
                    RRSet = lists:reverse(lists:flatten([ReadSet|FinalReadSet])),
                    From ! {ok, {TxId, RRSet, CommitTime}},
                    {stop, normal, SD};
                _ ->
                    [_ExecutedOps|RestOps] = Operations,
                    {next_state, execute_batch_ops, SD#state{operations=RestOps,
                        final_read_set=[ReadSet|FinalReadSet], causal_clock=CommitTime}, 0}
            end;
        aborted ->
            timer:sleep(random:uniform(10)),
            {next_state, execute_batch_ops, SD, 0}
            %From ! {error, commit_fail},
            %{stop, normal, SD}
    end.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    {stop,normal,ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

