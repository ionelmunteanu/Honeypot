-module(cache_serv).
-include("antidote.hrl").
-behaviour(gen_server).

-define(CACHE, crdt_cache).
-define(BKUP_FILE, "cache_restore_file").
-define(MAX_TABLE_SIZE, 50000).  %% words 
-define(MAX_INT, 65535).

-define(MAX_RETRIES, 5).

-define(EVICT_RATIO, 1).


%%  Eviction strategy based on: 
%%    - hit_count - least number of demands
%%    - last_accessed - oldest to be accessed
%%    - time_created  - oldest item -> cache will soon expire
-define(EVICT_STRAT_FIELD, hit_count).

-record (state,{table_name :: term(),
        tmr_serv_id :: term(),
        backup_file :: term(),
        max_table_size :: integer(),
        keys_with_hit_count :: term(),
        partition :: term(),
        node :: term()}).
%% Honeypot specific crdt record used to wrap a CRDT information
%%  key - name of the crdt
%%  timestamp - the version (the crdt's commit ts)
%%  snapshot  - data
%%  ops       - list of operations performed on the key since it has been cached
%%  type      - crdt's type. 
-record (crdt,{
        key :: term(),
        timestamp :: term(),
        aux_timestamp :: term(),
        snapshot :: term()|[term()],
        hit_count :: integer(),
        %last_accessed :: term(),
        time_created :: term(),
        type :: type(),
        metadata :: term(),
        ops :: [term()]}).

%% API
-export([start_link/0,
  stop/0,
  init/1]).

%% Server CallBacks
-export([
    handle_call/3, 
    evict/0,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2]).

-export([
    read/2,
    update/3,
    update_multi/3, 
    cache_info/1,
    start_cache_serv/1,
    simple_lookup/4]).

%%TODO MAKE COMMUNICATION ASYNCHRONOUS BETWEEN CACHE SERV AND TIMESERV

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
  %io:format("cache serve start link "),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [],[]).

stop() ->
    gen_server:cast(?MODULE, stop).


evict() ->
  gen_server:call(gen_cache_name(), evict).

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

% read({Partition, Node}, Key, Type, TxId) ->
%   gen_server:call( cache_serv, {read, {{Partition, Node}, Key, Type, TxId}}).

read(KeyTypeList, TxId) ->
  gen_server:call( cache_serv, {make_view, {KeyTypeList, TxId}}).


%% Calls a callback responsable with updating items in the cache, similar to 
%% a ClockSI tx coordinator's "sigle_commit" function.
%% Params: 
%%  {Partition, Node} -> Tuple describing address of list in WriteSet
%%  WriteSet          -> List of tuples {Key, Type, {Operation, Actor}}
%%  TxId              -> Transaction ID
%% Returns: - 

%%update([{{Partition, Node}, WriteSet}], TxId,OriginalSender) ->
update(ListOfOps, TxId,OriginalSender) ->
  Answer = gen_server:call(gen_cache_name(), {update, ListOfOps, TxId, OriginalSender}),
  %io:format("returtning from update gen_serv call: ~p, ~n", [Answer]),
  Answer.


update_multi([{{Partition, Node}, WriteSet}|Rest], TxId, OriginalSender) ->
    gen_server:call(gen_cache_name(), {update_multi, [{{Partition, Node}, WriteSet}|Rest], TxId, OriginalSender}).

cache_info(Item) ->
  ets:info(?CACHE, Item).   

simple_lookup(_Node, Key, Type, _TxId) ->
  gen_server:call(gen_cache_name(), {simple_lookup,Key, Type}).

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
        max_table_size = ?MAX_TABLE_SIZE,
        keys_with_hit_count = Prepared,
        node = 1}}.


%% =============================================================================

handle_call({simple_lookup, Key, _Type, _TxId}, _From, State=#state{table_name=Table}) ->
  Reply = case ets:lookup(Table, Key) of
    [] -> 
      {none};
    [Object] ->
      UpdatedObject = Object#crdt{%last_accessed = time_now(),
                                  hit_count = Object#crdt.hit_count + 1},
      ets:insert(Table, UpdatedObject),
      {ok, Object#crdt.snapshot}
    end,
  {reply, Reply, State};


%% The list argument is of form [{{Partition,Node}, WriteSet}]
%% and                          WriteSet = {Key, Type, {Op, Actor}}
%% 
%% todo read on key at a time is innefficient 
handle_call({read, {{_Partition, _Node}, Key, Type, TxId}}, _From, State=#state{table_name = Table, max_table_size = _SizeLimit}) ->
  
  Reply = case fetch_cachable_crdt(Key, Type,TxId, Table) of
    {error, Reason} ->
      %io:format("Error has occured while fetching crdt from Vnode.~nReason: ~w~n", [Reason]),
      {error, Reason};
    {ok,Object} ->
      ets:insert(Table, Object),
      trigger_counter([Object#crdt.key]),
      {ok,{Type, Object#crdt.snapshot}}
  end,
  {reply, Reply, State};


handle_call(evict, _From, State=#state{table_name = Table, keys_with_hit_count = Prepared}) ->
  evict(Table, Prepared),
  {noreply, State};





handle_call({make_view, {KeyTypeList, TxId}}, _From, State=#state{table_name = Table}) ->
  %% todo put lock in case of lease expiration during TX
  %% timestamp of -1 symbolies cache miss. 
  %% todo remove 

  %%io:format("KeyTypeList: ~p  TxId: ~p~n",[KeyTypeList,TxId]),



  % KeysByTS = getKeyVersions(KeyTypeList, Table), 

  % Versions = lists:reverse(orddict:fetch_keys(KeysByTS)), %%lists:reverse(orddict:to_list(OrderedByTS))

  % % Precondition: VersionList is ordered. 
  % PartitionedVersions = fun(VersionList) ->
  %   case VersionList of 
  %     [-1]          -> {[],[-1]};
  %     [Version]     -> {[Version], []};
  %     [Head|Tail]   -> {[Head], Tail} %%?IF( (time_now() - Head) < (?EVICT_RATIO * ?LEASE), {Head, Tail}, {[], VersionList})
  %   end
  % end,

  % {VersionsToKeep, ToEvictOrUncached} = PartitionedVersions(Versions),


  % %% Evict older (yet cached) versions and assmble new list of key/type tuples to be retrieved. 
  % KeysToFetch = lists:flatmap( fun(K) -> 
  %     KT = orddict:fetch(K, KeysByTS),
  %     ?IF(K =/= -1, quick_evict(lists:map(fun({Key,_}) -> Key end, KT)), nothing), %%todo take only one key from each version? 
  %     KT
  % end, ToEvictOrUncached),

  % {ObjectList, Errors} = lists:foldl(
  %   fun({Key, Type}, {AccL, ErrAcc}) -> 
  %     case  fetch_cachable_crdt(Key, Type,TxId, Table) of
  %       {ok, Object}    ->  {AccL ++ [Object], ErrAcc}; 
  %       {error, Reason} ->  {AccL, ErrAcc ++ [Reason]}
  %     end                                          
  %   end,{[], []}, KeysToFetch),

  % lists:foreach(fun(Obj) -> ets:insert(Table, Obj#crdt{hit_count = Obj#crdt.hit_count + 1}) end, ObjectList),
  % trigger_counter(ObjectList),
  
  

  % Reply = case Errors of
  %   [] -> 
  %       KeptVals = lists:flatmap( fun(K) -> orddict:fetch(K, KeysByTS) end, VersionsToKeep),
  %       KeptObjects = lists:map( 
  %         fun({K1, T1}) ->
  %           {ok, Obj} = fetch_cachable_crdt(K1, T1, TxId, Table),
  %           Obj 
  %       end, KeptVals),
  %       lists:map(fun(Obj) -> Type = Obj#crdt.type, {Obj#crdt.key, Type:value(Obj#crdt.snapshot)} end, lists:merge(ObjectList, KeptObjects));
  %    _ -> Errors
  % end,



 Reply = lists:map(
    fun({Key, Type}) ->
      case clocksi_vnode:read_data_item(get_location(Key), Key, Type, TxId) of
        {ok, {_CrdtType, CrdtSnapshot, TS}} -> {Key, Type:value(CrdtSnapshot)};
        {error, Reason} -> {error, Reason}
      end
    end, KeyTypeList
  ),

{reply, Reply , State};






%% Verifies if object is already in cache. If not, fetch if from its vnode, 
%%{[{Partition,Node}, WriteSet], TxId}
%% WriteSet = [{Key, Type, {Operation, Actor}}]
%% TO DO : case of abort! 
%%handle_call({update, [{{Partition, Node}, WriteSet}], TxId, _OriginalSender}, _From, State=#state{table_name = Table, max_table_size = _SizeLimit, prepared = Prepared}) ->
handle_call({update, ListOfOperations, TxId, _OriginalSender}, _From, State=#state{table_name = Table, max_table_size = _SizeLimit, 
  keys_with_hit_count = Prepared}) ->
  
  UpdateVal = fun(FObject, FType, FOp, FActor) ->  
      {ok, Result} = FType:update(FOp, FActor, FObject#crdt.snapshot),
      Result
  end,

  UpdateItemFun = fun(UpdateOp, {Acc, TotalHitCount, MaxTx}) ->
      {Key, Type, {Op, Actor}} = UpdateOp,
      %{Partition, Node} = hd(log_utilities:get_preflist_from_key(Key)),
      %% bug: this stores the operation only if the key is not fetched before hand.
      %% a subsequent read will show only if this hasn't been yet fetched for
      %% todo add flag for this situation. a read will check if the flag is true(has been fetched from the owner) and if not fetches and merges 
      %% TODO: check to see if timer is started (timmer should be started for this type of operation)
      %% TODO: send to the timer server the keys tht have just been fetched + 1 older key (to link to correct component)
      Object = case ets:lookup(Table, Key) of 
        %% if the key is not cached yet just store the operations and fetch the version from the owner just in case of a read. 
        [] -> #crdt{ key =  Key, snapshot = Type:new(), hit_count = 0, %last_accessed = 0,
                            time_created = 0, type = Type, timestamp = 0, aux_timestamp = 0,  ops = []};
        [Obj] -> Obj#crdt{hit_count = Obj#crdt.hit_count+1 }%, last_accessed = time_now()}
      end,

      UpdatedObject = Object#crdt {
        snapshot = UpdateVal(Object, Type, Op, Actor), 
        ops = (Object#crdt.ops ++ [{Op, Actor}])
      },
      
      make_room(Table, Prepared, ?MAX_TABLE_SIZE, UpdatedObject),
      
      ets:insert(Table, UpdatedObject), 

      %% returns 
      { ?IF(lists:member(Key, Acc), Acc, Acc ++ [Key]),                                     %% accumulate keys
        ?IF(lists:member(Key, Acc), TotalHitCount, TotalHitCount + Object#crdt.hit_count),  %% accumulate total hit_count for distinct keys 
        ?IF(UpdatedObject#crdt.aux_timestamp > MaxTx, UpdatedObject#crdt.aux_timestamp, MaxTx)}     %% accumulate max timestamp
  end, %UpdateItemFun

  UpdateWriteset = fun({_,WSs}, {Ks, Hc, Mt}) ->
    {UKS, THC, MxTs} = lists:foldl(UpdateItemFun,{[],0, -1},WSs),
    {Ks++UKS, Hc+THC, ?IF(MxTs > Mt, MxTs, Mt)}
  end,



  {UpdatedKeySet, TotalHitCount, MaxTs} = lists:foldl(UpdateWriteset, {[],0, -1}, ListOfOperations),
  
    Reply = case UpdatedKeySet of
    [] ->
      {error,'Read servers not ready yet'};
    NotEmpty ->
      _TxId1 = TxId#tx_id{snapshot_time = MaxTs},
      %io:format("newly inserted:~p, with total hitcount: ~p  and new txid:~p~n ", [NotEmpty, TotalHitCount, TxId1]),
      %trigger_counter(NotEmpty,TxId, TotalHitCount)   
      cache_timer_serv:start_counter(NotEmpty, TotalHitCount, ?LEASE, self()),
      ok
    end,

  {reply, Reply, State}.



%% =============================================================================

handle_cast(Msg, State) ->
  io:format("received in cast:~p  ~n", [Msg]),
  {noreply, State}.

%% =============================================================================


%%{[{Partition,Node}, [{Key, Type, {Operation, Actor}}]], TxId}
%% WriteSet = 
handle_info({lease_expired, Keys}, State=#state{table_name = Table, keys_with_hit_count = Prepared}) ->
  %io:format("lease expired on keys: ~p~n ",[Keys]),
  key_handover(Keys, Table, Prepared),
  {noreply, State};



%%
%%
%%
handle_info(Msg, State=#state{ keys_with_hit_count = Prepared}) ->
  case Msg of
    {ok, {TxId, _CommitTime}} ->
      %io:format("TxId has commited :~p  ~n", [{TxId,CommitTime}]),
      ets:delete(Prepared, TxId),
      ok;
    {error, {TxId, commit_fail}} ->
      [{Tx, ND, Retries}] = ets:lookup(Prepared, TxId),
      case Retries of 
        0 ->
          %io:format("no more retries for ~p~n",[Tx]), 
          ets:delete(Prepared,TxId);
        N ->
          %io:format("failed delivering : ~p, ~nretrying~n",[TxId]), 
          cache_2pc_sup:start_worker( Tx, ND, self()),
          ets:insert(Prepared, {TxId, ND, N-1})
        end;
    Else ->
      io:format("received in info :~p  ~n", [Else])
    end,
  {noreply, State}.

%% =============================================================================

terminate(Reason,  _State) ->
  io:format("terminating due to :~p ~n", [Reason]),
  %%ets:tab2file(Table, ?BKUP_FILE),
  %%%io:format("cache safely stored to ~w~n", [?BKUP_FILE]),
  ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% Internals
%% =============================================================================


get_location(Key) -> hd(log_utilities:get_preflist_from_key(Key)).

%% to do: check if the object has been involved in a multikey transaction
%% if so send all the implicated nodes home at once 
%% params to send to vnode is this: 
%% [{{Partition,Node}, [{Key,Type,{Op,Actor/Amount}}]}]




%% not cache's 

fetch_cachable_crdt(Key, Type, TxId, Table) ->
  case ets:lookup(Table, Key) of
    [] ->
      case clocksi_vnode:read_data_item(get_location(Key), Key, Type, TxId) of
        {ok, {_CrdtType, CrdtSnapshot, TS}} -> 
          Object = #crdt{ key =  Key, snapshot = CrdtSnapshot, hit_count  =  0  , %last_accessed = 0,
                          time_created = 0, type = Type, aux_timestamp = TxId#tx_id.snapshot_time, timestamp = TS, ops = []}, 
          %% start counter only if element has was not previously cached.
          %% not a good ideea since it might trigger a counter for a key linked to a previous cachef one by a transaction id
          % cache_timer_serv:start_counter(Node, TxId, [Key], ?LEASE, -1, self()),
          {ok, Object};
        {error, Reason} -> 
          {error, Reason}
      end;

      %%object found in cache. update last_accessdd and increse hit_count
      [Object] ->
        %%todo persist this update in ets as well 
        UpdatedObject = Object#crdt{hit_count = Object#crdt.hit_count+1 %, last_accessed = time_now()
        },
        {ok, UpdatedObject}
   end.

%% Get the biggest timestamp 
%%
% max_timestamp(KeyList, Table) ->
%   StoredKeysMaxTs = lists:foldl(
%     fun (K , Ts) -> 
%       case ets:lookup(Table, K) of
%         [] -> Ts;
%         [Object] -> ?IF(Object#crdt.timestamp > Ts, Object#crdt.timestamp  , Ts)
%       end
%     end, -1, KeyList ),
%   case StoredKeysMaxTs of
%     -1 -> time_now();
%     _  -> StoredKeysMaxTs
%   end.


%% Caled in case of a read or single update. If object has just been cached, its hitcount is -1. This means a counter has never been trigger for this key
%% When an object is cached, its hit_count is 0. In this situation, a trigger is created for this one object. When the timer expires, an event is 
%% generated and the updated object is sent back to its owner. 
%% If a multi-key transaction involves an already cached object,for which a trigger has already been activated, (and, without loosing generality, we can 
%% suppose it's the earliest cached object in this multi-key transaction), once the trigger is fired, all the objects involved must be sent back (to maintain 
%% isolation). The oldest object in this transaction, say X, will have a hit_count of 0. Upon calling it the second time, X's hit count will be 1 and all the 
%% other objects will have 0. This is how we can detect that at least one trigger from the whole chain has been activated. When X will be handed back, all 
%% the chain will be sent along to its owners.
%% 
%% @param KeyList - list of keys involved in this transaction
%% @param TotalHitCount - if it is 0 a trigger will be set
%%    

quick_evict(KeyList) -> 
  case KeyList of 
    [] -> ok;
    _ -> cache_timer_serv:start_counter(KeyList, 0, 0, self())
  end.


trigger_counter(CrdtList) ->
  trigger_counter(CrdtList, ?LEASE).


%% Partition the keylist accoding to hit counts. All about-to-be-cached CRDTs will have a zero hitcount. Send only these keys to the time sever
%% to eliminate inserting duplicates 
trigger_counter(CrdtList, Duration) ->
  case CrdtList of 
    [] -> ok;
    _ ->
      {PreCached, JustCached} = lists:foldl( 
        fun(Crdt, {PC,JC}) ->
          K = Crdt#crdt.key,
          ?IF(Crdt#crdt.hit_count > 0, {PC ++ [K], JC}, {PC, [K] ++ JC})
        end, {[], []}, CrdtList),
      %io:format("trigger_counter  precached: ~p; just cached: ~p ~n", [PreCached, JustCached]),
      case PreCached of
        [] -> cache_timer_serv:start_counter( JustCached , 0, Duration, self()); %% make new connected component
        [H | _] -> cache_timer_serv:start_counter( [H] ++ JustCached , 1, Duration, self()) %% link to existing connected component
      end      
  end. 





key_handover(Keys, Table, Prepared) ->
  %% get all entries from ets by key from expired set and create one transaction.  
  FlatmapOps = fun(Key, Dict) ->
    case ets:lookup(Table, Key) of
      [] ->
        %io:format("key: ~p not found~n", [Key]), 
        Dict;
      [Result] ->
        Bla = case length(Result#crdt.ops) > 0 of 
          true -> %%todo putt this in worker and make it async 
            Answ = ?IF( ?COMPRESS , 
              compress_crdt(Result), %% op compression on 
              [ {Op, Actor} || {Op, Actor, _Tx} <- Result#crdt.ops]), %% no op compression 
              dict:store(get_location(Key), [{Result#crdt.key, Result#crdt.type, Answ}], Dict);
          false -> 
            %io:format("no op found, returning nothing~n"),
            Dict
        end,
        true = ets:delete_object(Table,  Result),
        [] = ets:lookup(Table, Key),
        Bla
    end
  end,

  Fm = lists:foldl(FlatmapOps, dict:new(), Keys), 
  case dict:size(Fm) > 0  of
    true ->
      Tx = tx_utilities:create_transaction_record(ignore),     
      ets:insert(Prepared, {Tx, Fm, ?MAX_RETRIES}),
      %%TODO replace whit a pool of workers
      cache_2pc_sup:start_worker( Tx, Fm, self());
    false ->
      ok
    end.
%% key_handover





% %% Evicts cache CRDTs using defined strategy until ExtraSizeAmount 
% %% space is cleared from the cache
make_room(Table, Prepared, SizeLimit, Object) ->
  ExtraSizeAmount = case ets:lookup(Table, Object#crdt.key) of
    [] ->
      %%need to store the entire object
      size(term_to_binary(Object));   
    _ ->
      %%need to store only the ops
      size(term_to_binary(Object#crdt.ops))   
  end,
  %io:format("ExtraSizeAmount:~B;SizeLimit: ~B, Table size:~B ~n",[ExtraSizeAmount,SizeLimit, ets:info(Table, memory)]),
  case ((ets:info(Table, memory) + ExtraSizeAmount) < SizeLimit) of
    true ->
      %io:format("there is enough space~n") ,
      ok; %% to do make rcusive and return list ??
    false -> 
      %io:format ("there is not enough space~n") ,
      evict(Table, Prepared),
      make_room(Table,Prepared, SizeLimit, Object)
  end.




% % %% TO DO: test this function
evict(Table, Prepared) ->
  GetMin = fun (Obj1, Obj2) -> 
    case Obj1#crdt.?EVICT_STRAT_FIELD < Obj2#crdt.?EVICT_STRAT_FIELD of 
      true -> Obj1;
      false -> Obj2
    end
  end,

  Object = ets:foldl(GetMin, #crdt{hit_count = ?MAX_INT, 
            %last_accessed = time_now(), 
            time_created  = time_now()}, 
            Table ),
  %io:format("evicting ~p ~n",[Object#crdt.key]),
  ConnectedKeys = cache_timer_serv:evict_sync(Object#crdt.key),
  key_handover(ConnectedKeys, Table, Prepared), 
  %io:format("evicted keys ~p~n", [ConnectedKeys]),
  %io:format("table memory is now: ~B ~n", [ets:info(Table, memory)]),
  ok.



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

%% Get every timestamp of the stored keys. Missing keys are flagged with a timestamp of value -1.
getKeyVersions(Keys, Table) ->
  lists:foldl(
    fun({K, Type}, OrdDict) ->
      case ets:lookup(Table, K) of
        [] -> 
          case orddict:is_key(-1, OrdDict) of 
            false -> orddict:store(-1, [{K, Type}], OrdDict);
            true  -> orddict:append(-1, {K, Type}, OrdDict)
          end;
        [Result] -> 
          case orddict:is_key(Result#crdt.aux_timestamp, OrdDict) of 
            false -> orddict:store(Result#crdt.aux_timestamp, [{K, Type}],OrdDict);
            true  -> orddict:append(Result#crdt.aux_timestamp, {K, Type}, OrdDict)
          end
      end 
    end, orddict:new(), Keys).



%% adapter/visitor to handle un-uniform crdt implementation and reduce the operation count 
%% to the equivalent minimum (i.e. {inc, 1}, {inc, 2},..{inc, 100} to {inc, 5050})

compress_crdt(Crdt) ->
  Tp = Crdt#crdt.type, 
   %io:format("compressing key ~p from ~p to => ",[Crdt#crdt.key,Crdt#crdt.ops] ),
  Pl = case Tp of
    riak_dt_gcounter->
    [{{increment, lists:foldl( fun({Op, _Acc}, Sum) -> 
      Sum + case Op of 
        {increment, Am} -> Am;
        increment -> 1 
      end 
    end, 0, Crdt#crdt.ops)}, compressed}];
    riak_dt_gset   -> {error, "not implemented"};
    riak_dt_lwwreg -> {error, "not implemented"}; %% lrr wins is the same thing 
    riak_dt_map    -> {error, "not implemented"};
    riak_dt_orset  -> {error, "not implemented"};
    riak_dt_orswot -> {error, "not implemented"};
    riak_dt_emcntr -> {error, "not implemented"};
    riak_dt_vclock -> {error, "not implemented"}
  end,
  %io:format("compressed crdt:~p ~n", [Pl]),
  Pl.