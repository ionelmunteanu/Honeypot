-module(cache_timer_serv).
-behaviour(gen_server).
-include("antidote.hrl").

-define(STORE, key_timer_store).

-export([
  start_link/0,
  stop/0,
  init/1]).

-export([ 
    handle_call/3,
    terminate/2,
    code_change/3,
    handle_info/2,
    handle_cast/2 ]).

-export([start_counter/5]).

-define(IF(Cond,Then,Else), (case (Cond) of true -> (Then); false -> (Else) end)).

%% Graph keeps track of relations between keys of multi-key transactions
%% Table is a key-value store where key denotes a crdt and the value is its timer reference
%% it is used to cancel timers for keys in multi-key transction. 
%%
-record(state, {graph :: term(), timer_store :: term()}).
  
%% ===================================================================
%% API
%% ===================================================================


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:cast(?MODULE, stop).

start_counter(KeyList, TxId, HitCount, Lease, From) ->
  gen_server:call( gen_timer_serv_name(), {start_counter,TxId, KeyList, Lease, HitCount, From} ).


%% ===================================================================
%% CallBacks 
%% ===================================================================

init([]) ->
  Graph = digraph:new(), 
  Store = ets:new(?STORE, [set, named_table]), 
  timer:start(),
  State=#state{graph = Graph, timer_store = Store},
  {ok, State}.

%% ================================================================

handle_call({start_counter,_TxId, Keys, Lease, HitCount, Sender}, _From, State=#state{graph = Graph}) ->
  %%Reply = case  timer:send_after(Lease, Sender, {lease_expired, Keys, [time_now()]}) of
  insert_dependencies(Keys, Graph),
  io:format("connected components: ~p, ~n ", [digraph_utils:strong_components(Graph)]), 
  Reply = case HitCount > 0 of
    false ->
      [Key| _ ] = Keys, 
      io:format("activating trigger for ~p ~n ",[Key]),
      {ok, TRef} = timer:send_after(Lease, self(), {send_lease_expired, [Key, Sender]}),
      insert_dependencies([Key, {tref, TRef}], Graph);
    true ->
      ok
    end,
  {reply, Reply, State};

handle_call({cancel_timer, TRef}, _From, State) ->
  Reply = timer:cancel(TRef),
  {reply, Reply, State}.

%% ================================================================

handle_info({send_lease_expired, [Key, Sender]},  State=#state{graph = Graph}) ->
  ListOfVertices = digraph_utils:reaching([{Key}], Graph),

  ListOfKeys = lists:foldl(
    fun(Val,A1) -> 
      case Val of 
        {tref, TRef} -> timer:cancel(TRef); 
        {V} -> [V|A1] 
      end 
    end, [], ListOfVertices),
  
  digraph:del_vertices(Graph, ListOfVertices),
  %% [JustKey || {JustKey} <- ListOfVertices],
  io:format("list of expired keys: ~p~n", [ListOfKeys] ),
  io:format("remaining vertices: ~p~n",[digraph:vertices(Graph)]),
  Sender ! {lease_expired, ListOfKeys},

  {noreply, State};

handle_info(_Msg, State) ->
  {noreply, State}.

%% ================================================================

handle_cast(_Msg, State) ->
  {noreply, State}.

%% ================================================================

insert_dependencies(Keys, Graph) ->
  io:format("inserting dependendcies among keys: ~p, ~n",[Keys]),
  InsertDependency = fun (CurrentKey, PrevKey) ->
    case PrevKey of
      [] ->
        digraph:add_vertex(Graph, {CurrentKey}), 
        CurrentKey;
      _ ->
        digraph:add_vertex(Graph, {CurrentKey}),
        digraph:add_edge(Graph, {CurrentKey}, {PrevKey}),
        digraph:add_edge(Graph, {PrevKey}, {CurrentKey}),
        CurrentKey
    end
  end,
  lists:foldl(InsertDependency,[], Keys),
  ok.


%% ================================================================

terminate(Reason, _State) ->
    io:format("Cache Timer Server has craches, REason:~p, ~n",[Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% ================================================================
gen_timer_serv_name() ->
  cache_timer_serv.

