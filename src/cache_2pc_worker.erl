

-module(cache_2pc_worker).

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
-export([start_link/3]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([ 
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
    tx_id,
    num_to_ack :: non_neg_integer(),
    prepare_time :: non_neg_integer(),
    commit_time :: non_neg_integer(),
    updated_partitions :: dict(),
    state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Tx, WriteSet, From) ->
    gen_fsm:start_link(?MODULE, [Tx, WriteSet, From], []).

finish_op(From, Key, Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([TxId, WriteSet, From]) ->
  random:seed(now()),
  SD = #state{
    updated_partitions = WriteSet,
    from = From,
    prepare_time=0,
    tx_id = TxId
  },
  case dict:size(WriteSet) > 1 of 
    false ->
      % [{Node,WriteSet}]
      ?CLOCKSI_VNODE:single_commit(WriteSet, TxId),  %%writeset should be [{Node, {Key, Type[{Op, Act}...]}}}]
      {ok, single_committing, SD#state{state=committing, num_to_ack=1, tx_id=TxId}}.  
    true -> 
      %%this 
      ?CLOCKSI_VNODE:prepare(WriteSet, TxId),
      {ok, receive_prepared, SD#state{num_to_ack=dict:size(WriteSet), state=prepared,
        updated_partitions=WriteSet, tx_id=TxId}}
  %end.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.


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
%%  committed the tx and finishes operation.
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
reply_to_client(SD=#state{from=From, tx_id=TxId, state=TxState, commit_time=CommitTime}) ->
  case TxState of
    committed ->
      From ! {ok, {TxId, CommitTime}},
      {stop, normal, SD};
    aborted ->
      From ! {error, {TxId, commit_fail}},
      {stop, normal, SD}
  end.

%% =============================================================================

handle_info(Info, _StateName, StateData) ->
    io:format("clock si vnode handle info ~p, ~n",[Info]),
    {stop,badmsg,StateData}.

handle_event(Event, _StateName, StateData) ->
    io:format("clock si vnode  handle event ~p, ~n",[Event]),
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    {stop,normal,ok, StateData};

handle_sync_event(Event, _From, _StateName, StateData) ->
    io:format("clock si vnode  handle SYNC event ~p, ~n",[Event]),
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    io:format("worker terminating~n"),
    ok.

