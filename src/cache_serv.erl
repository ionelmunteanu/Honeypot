-module(cache_serv).
-include("antidote.hrl").
-behaviour(gen_server).

-define(CACHE, crdt_cache).
-define(BKUP_FILE, "cache_restore_file").
-define(MAX_TABLE_SIZE, 10000).
-define(MAX_INT, 65535).
-define(LEASE, 5000).
%%  Eviction strategy based on: 
%%    - hit_count - least number of demands
%%    - last_accessed - oldest to be accessed
%%    - time_created  - oldest item -> cache will soon expire
-define(EVICT_STRAT_FIELD, hit_count).

-record (state,{table_name :: term(),
        tmr_serv_id :: term(),
        backup_file :: term(),
        max_table_size :: integer(),
        prepared :: term(),
        partition :: term(),
        node :: term()}).

-record (crdt, {
        key :: term(),
        snapshot :: term()|[term()],
        hit_count :: integer(),
        last_accessed :: term(),
        time_created :: term(),
        type :: type(),
        stable_version :: term(),
        borrow_time :: term(),
        ops :: [term()],
        txId:: term()}).

%% API
-export([start_link/0,
  stop/0,
  init/1]).

%% Server CallBacks
-export([
    handle_call/3, 
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2]).

-export([read/4,
    update/3,
    update_multi/3, 
    cache_info/1,
    start_cache_serv/1,
    simple_lookup/4]).



%% =============================================================================
%% API
%% =============================================================================

start_link() ->
  gen_server:start_link({global, ?MODULE}, ?MODULE, [],[]).

stop() ->
    gen_server:cast(?MODULE, stop).

%% Calls a read callback which returns the crdt with key Key. If the data item 
%% is not already cached it will be borrowed from owner.
%% Params:
%%  {Partition, Node} -> Tuple describing the location of the key
%%  Key               -> key of the crdt
%%  Type              -> crdt type used to update the crdt's value
%%                       reflectively
%%  TxId              -> transaction's id 
%% Returns:
%%  {ok, {Type, Value}}

read({Partition, Node}, Key, Type, TxId) ->
  gen_server:call({global, cache_serv}, {read, {{Partition, Node}, Key, Type, TxId}}).


%% Calls a callback responsable with updating items in the cache, similar to 
%% a ClockSI tx coordinator's "sigle_commit" function.
%% Params: 
%%  {Partition, Node} -> Tuple describing address of list in WriteSet
%%  WriteSet          -> List of tuples {Key, Type, {Operation, Actor}}
%%  TxId              -> Transaction ID
%% Returns: - 

update([{{Partition, Node}, WriteSet}], TxId,OriginalSender) ->
  Answer = gen_server:call({global,gen_cache_name()}, {update, [{{Partition, Node}, WriteSet}], TxId, OriginalSender}),
  io:format("returtning from update gen_serv call: ~p, ~n", [Answer]),
  Answer.



update_multi([{{Partition, Node}, WriteSet}|Rest], TxId, OriginalSender) ->
    gen_server:call({global,gen_cache_name()}, {update_multi, [{{Partition, Node}, WriteSet}|Rest], TxId, OriginalSender}).

cache_info(Item) ->
  ets:info(?CACHE, Item).   

simple_lookup(_Node, Key, Type, _TxId) ->
  gen_server:call({global,gen_cache_name()}, {simple_lookup,Key, Type}).

start_cache_serv(Node) ->
  cache_serv_sup:start_cache(Node).


%% =============================================================================
%% CallBacks 
%% =============================================================================

init([]) ->
  %% do i need to generate a table name as well? don't think so...to do: check
  Table = ets:new(gen_table_name(), [set, named_table, {keypos, #crdt.key}]),
  Prepared = ets:new(gen_prepare_name(), [set, named_table]),
  {ok, #state{table_name = Table,
        tmr_serv_id = gen_timer_serv_name(),
        backup_file = ?BKUP_FILE,
        max_table_size = ?MAX_TABLE_SIZE,
        prepared = Prepared,
        node= 1}}.


%% =============================================================================

handle_call({simple_lookup, Key, _Type, _TxId}, _From, State=#state{table_name=Table}) ->
  Reply = case ets:lookup(Table, Key) of
    [] -> 
      {none};
    [Object] ->
      UpdatedObject = Object#crdt{last_accessed = time_now(),
                                  hit_count = Object#crdt.hit_count + 1},
      ets:insert(Table, UpdatedObject),
      {ok, Object#crdt.snapshot}
    end,
  {reply, Reply, State};


%% The list argument is of form [{{Partition,Node}, WriteSet}]
%% and                          WriteSet = {Key, Type, {Op, Actor}}
%% 

handle_call({read, {{Partition, Node}, Key, Type, TxId}}, _From, State=#state{table_name = Table, max_table_size = _SizeLimit}) ->
  
  Reply = case fetch_cachable_crdt({Partition, Node},Key, Type,TxId, Table) of
    {error, Reason} ->
      io:format("Error has occured while fetching crdt from Vnode.~nReason: ~w~n", [Reason]),
      {error, Reason};
    {ok,Object} ->
      ets:insert(Table, Object),
      trigger_counter(Object), %% will trigger counter if object has just been cached(i.e. hit count is -1)
      {ok,{Type, Object#crdt.snapshot}}
  end,
  {reply, Reply, State};


%% Verifies if object is already in cache. If not, fetch if from its vnode, 
%%{[{Partition,Node}, WriteSet], TxId}
%% WriteSet = [{Key, Type, {Operation, Actor}}]
%% TO DO : case of abort! 
handle_call({update, [{{Partition, Node}, WriteSet}], TxId, _OriginalSender}, _From, State=#state{table_name = Table, max_table_size = _SizeLimit, prepared = Prepared}) ->

  UpdateVal = fun(FObject, FType, FOp, FActor) ->  
                {ok, Result} = FType:update(FOp, FActor, FObject#crdt.snapshot),
                Result
              end,

  UpdateItemFun = fun(UpdateOp, Acc) ->
    {Key, Type, {Op, Actor}} = UpdateOp,
    ets:insert(Prepared, {Key}),
    %%hit count is updated automatically by fetch_catchable_crdt (might break separation of concerns )
    case fetch_cachable_crdt({Partition, Node},Key, Type,TxId, Table) of
      {error, Reason} ->
        io:format("Error has occured while fetching crdt from Vnode.~nReason: ~w~n", [Reason]),
        {error, Reason};

      {ok, Object} ->
        UpdatedObject = Object#crdt{snapshot = UpdateVal(Object, Type, Op, Actor),ops= (Object#crdt.ops ++ [{Op, Actor}])},
        
        ets:insert(Table, UpdatedObject),
        %% triggerring will be done only newly inserted key (i.e. hit_count = 0 )and only one
        %% one key will trigger a chain reaction. 
        %%trigger_counter(Object),
        %%case lists:member(Key, Acc) of
        case Object#crdt.hit_count =:= 0 of
          true -> lists:append(Acc, [Object]);
          false -> Acc
        end
       
      end %fetch_cachable_crdt
      
    end, %UpdateItemFun

  UpdatedKeySet = lists:foldl(UpdateItemFun,[],WriteSet),
  io:format("newly inserted:~p, ~n ", [UpdatedKeySet]),
  case UpdatedKeySet of
    [Head| _DontCare] ->
      trigger_counter(Head);
    _ ->
      no_need_to_trigger
  end, 
  {reply, ok, State};


% [
%   {{0,'antidote@127.0.0.1'}, [{c,riak_dt_gcounter,{increment,c}}, {a,riak_dt_gcounter,{increment,}}]},            
%   {{730750818665451459101842416358141509827966271488,'antidote@127.0.0.1'},[{m,riak_dt_gcounter,{increment,1}}]}
% ]

handle_call({update_multi, WriteSetList, TxId,OriginalSender}, _From, 
            State=#state{table_name = Table, max_table_size = SizeLimit, 
            prepared = Prepared, node= Node}) ->
  Ppfun = fun(A, B) -> pp(A,B) end, 

  {_,KeyMulte, HitMulte,_,_,_} = lists:foldl(Ppfun, {TxId, [], 0, Table, Prepared ,SizeLimit},  WriteSetList),

  cache_timer_serv:start_counter(Node, KeyMulte, ?LEASE, HitMulte, self()),
  lists:map(fun(_X) ->  riak_core_vnode:reply(OriginalSender, {prepared, time_now()}) end, WriteSetList),
  {reply, ok, State}.


pp({_Node,WriteSet}, {TxId, Keys, Hit, Table, Prepared,SizeLimit}) ->
  HitCount = lists:foldl(fun( {K, _Type, {_Op, _Actor}}, Acc) -> 
                            case ets:lookup(Table,K) of 
                              [] -> Acc ;
                               _-> Acc +1 
                            end 
                          end, 0, WriteSet),
  UpdateItemFun = fun(REcv, Acc) ->
    {Key, Type, {Op, Actor}} = REcv,
    ets:insert(Prepared, {Key}),
    %case get_crdt(Table, {{Partition,Node}, Key, Type,TxId}, SizeLimit) of 
    case etc:lookup(Table, Key) of
      [{_Key, Object}] ->
        GtSnpShotVal = fun(FObject, FType, FOp, FActor) ->  
                          {ok, Result} = FType:update(FOp, FActor, FObject#crdt.snapshot),
                          Result
                        end,
        UpdatedObject = Object#crdt{
                          last_accessed = time_now(),
                          snapshot = GtSnpShotVal(Object, Type, Op, Actor),
                          ops= (Object#crdt.ops ++ [{Op, Actor}])},
        ets:insert(Table, UpdatedObject),
        case lists:member(Key, Acc) of
          false -> lists:append(Acc, [Key]);
          true -> Acc
        end;
      [] ->
        {error, not_found}
      % {error, Reason} ->
      %   {error, Reason}
    end
  end,
  UpdatedKeySet = lists:foldl(UpdateItemFun,[],WriteSet),
  {TxId,UpdatedKeySet ++ Keys , Hit+HitCount, Table, Prepared, SizeLimit}.


%% =============================================================================

handle_cast(Msg, State) ->
  io:format("received in cast:~p  ~n", [Msg]),
  {noreply, State}.

%% =============================================================================

handle_info({lease_expired, Keys}, State=#state{table_name = Table}) ->
  Evict = fun(Key) ->
    case ets:lookup(Table, Key) of
      [] ->
        ok; %% crdt not found. maybe deleted by make_room
      [Result] ->
        case length(Result#crdt.ops) > 0 of 
          true ->
            return_to_owner(Result);
          _ -> ok
        end, 
        ets:delete_object(Table, Result)
    end
  end,
  lists:map(Evict, Keys),
  {noreply, State};

handle_info(Msg, State) ->
  io:format("received in info :~p  ~n", [Msg]),
  {noreply, State}.

%% =============================================================================

terminate(Reason,  _State) ->
  io:format("terminating due to :~p ~n", [Reason]),
  %%ets:tab2file(Table, ?BKUP_FILE),
  %%io:format("cache safely stored to ~w~n", [?BKUP_FILE]),
  ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% Internals
%% =============================================================================


%% to do: check if the object has been involved in a multikey transaction
%% if so send all the implicated nodes home at once 
%% params to send to vnode is this: 
%% [{{Partition,Node}, [{Key,Type,{Op,Actor/Amount}}]}]
return_to_owner(Object) ->
  io:format("returning: ~p  ~n", [Object]),  
  {Key, Type, [{Op, Actor}|_], TxId} = {Object#crdt.key, Object#crdt.type, Object#crdt.ops, Object#crdt.txId},
  Node = hd(log_utilities:get_preflist_from_key(Key)),
  clocksi_vnode:single_commit([{Node, [{Key,Type, {Op, Actor}}] }], TxId).


%% Evicts cache CRDTs using defined strategy until ExtraSizeAmount 
%% space is cleared from the cache
% make_room(Table, SizeLimit, Object) ->
%   ExtraSizeAmount = case ets:lookup(Table, Object#crdt.key) of
%     [] ->
%       %%need to store the entire object
%       size(term_to_binary(Object));   
%     _ ->
%       %%need to store only the ops
%       size(term_to_binary(Object#crdt.ops))   
%   end,
%   io:format("ExtraSizeAmount:~B;SizeLimit: ~B~n",[ExtraSizeAmount,SizeLimit]),
%   case ((ets:info(Table, memory) + ExtraSizeAmount) < SizeLimit) of
%     true ->
%       io:format("there is enough space~n") ,
%       ok; %% to do make rcusive and return list ??
%     false -> 
%       io:format ("there is not enough space~n") ,
%       evict(Table),
%       make_room(Table, SizeLimit, Object)
%   end.






% cache_crdt(Table, Node, Key, Type, TxId, SizeLimit) ->
%   Result = case ets:lookup(Table, Key) of 
%     [] ->
%       case fetch_cachable_crdt(Node, Key, Type,TxId) of
%         {ok, FetchedObj} ->
%           io:format("sending start counter data~n "),
%           cache_timer_serv:start_counter(Partition, TxId, [Key], ?LEASE, self()),
%           {ok,FetchedObj};
%         {error, Reason} ->
%           {error, Reason}
%       end;
%     [Answer] ->
%        {ok,Answer};
%     ListOfAnsweres ->
%       ListOfAnsweres  
%     end,
%   case Result of
%     {ok, Obj} ->
%       %make_room(Table, SizeLimit, Obj),
%       {ok, Obj};
%     {error, ErrMsg} ->
%       {error, ErrMsg};
%     Rest ->
%       [Head|_] = Rest, 
%       {ok, Head}
%   end.



%% not cache's 

fetch_cachable_crdt({Partition, Node},Key, Type, TxId, Table) ->
  case ets:lookup(Table, Key) of
    [] ->
      case clocksi_vnode:read_data_item({Partition, Node},Key,Type,TxId) of
        {ok, {_CrdtType, CrdtSnapshot}} -> 
          Object = #crdt{ key =  Key, snapshot = CrdtSnapshot, hit_count  =  0, last_accessed = 0,
                          time_created  = 0, type = Type, stable_version = -1, borrow_time = -1, ops = [], txId = TxId}, 
          %% start counter only if element has was not previously cached.
          %% not a good ideea since it might trigger a counter for a key linked to a previous cachef one by a transaction id
          %cache_timer_serv:start_counter(Node, TxId, [Key], ?LEASE, -1, self()),
          {ok, Object};
        {error, Reason} -> 
          {error, Reason}
      end;

      %%object found in cache. update last_accessdd and increse hit_count
      [Object] ->
        UpdatedObject = Object#crdt{hit_count = Object#crdt.hit_count+1, last_accessed = time_now()},
        {ok, UpdatedObject}
   end.


%%Caled in case of a read or single update. If object has just been cached, its hitcount is -1. This means a counter has never been trigger for this key
%%

trigger_counter(Object) ->
  case Object#crdt.hit_count =:= 0 of
    true -> 
      cache_timer_serv:start_counter( hd(log_utilities:get_preflist_from_key(Object#crdt.key)),
                                        Object#crdt.txId, [Object#crdt.key], ?LEASE, -1, self());
    _ ->
      ok
  end.

% %% TO DO: test this function
% evict(Table) ->
%   GetMin = fun (Obj1, Obj2) -> 
%     case Obj1#crdt.?EVICT_STRAT_FIELD < Obj2#crdt.?EVICT_STRAT_FIELD of 
%       true -> Obj1;
%       false -> Obj2
%     end
%   end,
%   Object = ets:foldl(GetMin, #crdt{hit_count = ?MAX_INT, 
%             last_accessed = time_now(),
%             time_created  = time_now()}, 
%             Table ),
%   io:format("evicting ~p ~n",[Object]),
%   ets:delete(Table, Object#crdt.key),
%   if Object =/= []  ->
%     return_to_owner(Object) %% to do optimization, do not return Object if ops is empty. but needed for buffer => causality
%   end,
%   ok.



time_now() ->
  time_to_ms(erlang:now()).

%% get Erang time to
time_to_ms({Mega, Sec, Mili}) ->
  (Mega * 1000000 + Sec) * 1000000  + Mili.
  
gen_cache_name() ->
  cache_serv.

gen_prepare_name() ->
  prepare.

gen_timer_serv_name() ->
  cache_timer_serv.

gen_table_name() ->
  cache_timer .