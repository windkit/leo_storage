-module(leo_storage_worker).

-behaviour(gen_server).

-include_lib("leo_logger/include/leo_logger.hrl").

%% API
-export([start_link/1]).
-export([enqueue/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(PENDING_LENGTH,   200).

-record(state, {
         }).

%%====================================================================
%% API
%%====================================================================

start_link(Options) ->
    Name = proplists:get_value(name, Options),
    PoolOpts = [
                {worker, {?MODULE, []}}
               ],
    Ret = wpool:start_pool(Name, PoolOpts),
    ?debug("start_link/1", "Ret: ~p", [Ret]),
    Ret.

enqueue(Name, MFA) ->
    Stats = wpool_pool:stats(Name),
    case proplists:get_value(total_message_queue_len, Stats) of
        Len when Len =< ?PENDING_LENGTH ->
            wpool:call(Name, {execute, MFA}, available_worker);
        _ ->
            {error, unavailable}
    end.

%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================

init(_Args) ->
    {ok, #state{}}.

handle_call({execute, {M,F,A}}, _From, State) ->
    Ret = (catch erlang:apply(M, F, A)),
    {reply, Ret, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.
