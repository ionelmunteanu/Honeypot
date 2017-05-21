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

-export([
  start_counter/4,
  evict_sync/1
  ]).


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

start_counter(KeyList,  HitCount, Lease, From) ->
  gen_server:call( gen_timer_serv_name(), {start_counter, KeyList, Lease, HitCount, From} ).

evict_sync(Key) ->
  gen_server:call(gen_timer_serv_name(), {evict_now, Key} ).


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

handle_call({evict_now, Key },_From, State=#state{graph = Graph}) ->
  {reply, pop_reaching_vertices(Key, Graph) , State};



handle_call({start_counter, Keys, Lease, HitCount, Sender}, _From, State=#state{graph = Graph}) ->
  %%Reply = case  timer:send_after(Lease, Sender, {lease_expired, Keys, [time_now()]}) of
  %io:format("list sent to these fuckers:~p~n ", [Keys]),
  insert_dependencies(Keys, Graph),
  %io:format("connected components: ~p, ~n ", [digraph_utils:strong_components(Graph)]), 
  Reply = case HitCount > 0 of
    false ->
      [Key| _ ] = Keys, 

      {ok, TRef} = timer:send_after(Lease, self(), {send_lease_expired, {Key, Sender}}),
      %io:format("activating trigger for ~p ~n ",[[Key, {tref, TRef}]]),
      
      %%todo if timer expires before inser_dependencies finishes .... put a boundary on lease???
      ?IF(Lease > 0,
        insert_dependencies([Key, {tref, TRef}], Graph),    %% if lease is not 0 link a TRef to these keys 
        {ok, quick_evict});                                 %% if lease is 0 there is no need to add Tref to graph. 
    true ->
      ok
    end,
  {reply, Reply, State};

handle_call({cancel_timer, TRef}, _From, State) ->
  Reply = timer:cancel(TRef),
  {noreply, Reply, State}.

%% ================================================================

handle_info({send_lease_expired, {Key, Sender}},  State=#state{graph = Graph}) ->
  %% [JustKey || {JustKey} <- ListOfVertices],
  Cacat = pop_reaching_vertices(Key, Graph),
  Sender ! {lease_expired, Cacat },
  {noreply, State};


handle_info(_Msg, State) ->
  {noreply, State}.

%% ================================================================

handle_cast(_Msg, State) ->
  {noreply, State}.

%% ================================================================

%% todo add lock on graph to insure no keys are inserted during eviction => bad clash inconsistent state
insert_dependencies(Keys, Graph) ->
  %io:format("inserting dependendcies among keys: ~p, ~n",[Keys]),
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

%% remove all nodes reaching Key and return them as list. 
pop_reaching_vertices(Key, Graph) ->
  ListOfVertices = digraph_utils:reaching([{Key}], Graph),
  %io:format("removing all keys linked to <<~p>> from graph:~p ~n", [Key,ListOfVertices]),
  digraph:del_vertices(Graph, ListOfVertices),
  
  
  %io:format("remaining vertices: ~p~n",[digraph:vertices(Graph)]),
  
  Pla = lists:foldl(
    fun(Val,A1) -> 
      case Val of 
        {{tref, TRef}} -> timer:cancel(TRef),
          A1; 
        {V} -> [V|A1] 
      end 
    end, [], ListOfVertices),
  %io:format("all the keys that will be  evicted: ~p~n", [Pla] ),
  Pla.


%% ================================================================

terminate(Reason, _State) ->
    io:format("Cache Timer Server has craches, REason:~p, ~n",[Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% ================================================================
gen_timer_serv_name() ->
  cache_timer_serv.