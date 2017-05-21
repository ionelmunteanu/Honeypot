

-module(cache_2pc_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([start_worker/3]).
-export([init/1]).

start_link() -> 
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_worker(TxId, WriteSet, From  )->
	%io:format("starting worker ~n"),
	supervisor:start_child(?MODULE, [TxId, WriteSet, From]).


init([]) ->
    Worker = {cache_2pc_worker,
              {cache_2pc_worker, start_link, []},
               transient, 5000, worker, [cache_2pc_worker]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.

