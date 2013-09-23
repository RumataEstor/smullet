-module(smullet_session).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([new/2, ensure_started/2]).
-export([find/1, send/2, recv/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% define smullet_session behaviour with following callbacks
-type state() :: term().
-callback init(Key) -> {ok, State} | ignore | {stop, Reason}
                           when Key :: term(),
                                State :: state(),
                                Reason :: term().
-callback handle_info(Msg, State) -> {noreply, State}
                                         | {noreply, State, Timeout}
                                         | {stop, Reason, State}
                                         when Msg :: term(),
                                              State :: state(),
                                              Timeout :: timeout(),
                                              Reason :: term().
-callback terminate(Reason, State) -> ok when Reason :: term(),
                                              State :: state().

-opaque session() :: pid().
-export_type([session/0, state/0]).

-record(state, {handler, module, state, messages, timeout, ref}).
-define(ERROR(Format, Params), error_logger:error_msg("[~p:~p ~p] " ++ Format,
                                                      [?MODULE, ?LINE, self()] ++ Params)).
-define(gproc_key(Key), {n, l, {smullet, Key}}).
-define(inactive_message(Timer), {timeout, Timer, inactive_message}).


%% @doc Creates a new session under specified supervisor using SessionKey.
%%      If the key is not unique returns an error.
-spec new(Supervisor, SessionKey) -> {ok, session()} | {error, _}
                                         when Supervisor :: atom() | pid(),
                                              SessionKey :: term().
new(Supervisor, SessionKey) ->
    GProcKey = ?gproc_key(SessionKey),
    case smullet_sup:start_child(Supervisor, GProcKey) of
        {error, {shutdown, Reason}} ->
            {error, Reason};
        Other ->
            Other
    end.


%% @doc Returns an existing session associated with SessionKey
%%      or undefined if no such session exists.
-spec find(SessionKey) -> undefined | session() when SessionKey :: term().
find(SessionKey) ->
    Key = ?gproc_key(SessionKey),
    gproc:where(Key).


%% @doc Returns an existing session associated with SessionKey
%%      or creates a new session under specified supervisor.
-spec ensure_started(Supervisor, SessionKey) -> session()
                                                    when Supervisor :: atom() | pid(),
                                                         SessionKey :: term().
ensure_started(Supervisor, SessionKey) ->
    GProcKey = ?gproc_key(SessionKey),
    case gproc:where(GProcKey) of
        undefined ->
            case smullet_sup:start_child(Supervisor, GProcKey) of
                {ok, Pid} ->
                    Pid;
                {error, {already_registered, OtherPid}} ->
                    OtherPid
            end;
        Pid ->
            Pid
    end.


%% @doc Delivers a message to a subscribed handler or stores until
%%      such handler subscribes. If no handler appears until session
%%      is terminated due to inactivity ALL SUCH MESSAGES ARE LOST!
send(undefined, _) ->
    not_found;
send(Session, Msg) when is_pid(Session) ->
    gen_server:call(Session, {send, Msg}).


%% @doc Subscribes calling process for a message delivery.
-spec recv(undefined | session()) -> reference() | not_found.
recv(undefined) ->
    not_found;
recv(Session) when is_pid(Session) ->
    gen_server:call(Session, recv).


%% @doc Starts a session.
start_link(Timeout, Module, GProcKey) ->
    gen_server:start_link(?MODULE, {GProcKey, Timeout, Module}, []).


%% @private
init({GProcKey, Timeout, Module}) ->
    case gproc:reg_or_locate(GProcKey) of
        {Pid, _} when Pid =:= self() ->
            ?gproc_key(SessionKey) = GProcKey,
            case Module:init(SessionKey) of
                {ok, State} ->
                    {ok, start_timer(#state{module=Module, state=State,
                                            messages=queue:new(), timeout=Timeout})};
                Other ->
                    Other
            end;
        {OtherPid, _} ->
            {stop, {shutdown, {already_registered, OtherPid}}}
    end.


start_timer(#state{timeout=Timeout} = State) ->
    State#state{ref=erlang:start_timer(Timeout, self(), inactive_message)}.


%% @private
%%
%% Store a message to be delivered later.
handle_call({send, Msg}, _, #state{handler=undefined, messages=Messages} = State) ->
    Msgs = queue:in(Msg, Messages),
    {reply, ok, State#state{messages=Msgs}};

%% Send a message to already subscribed handler, start inactivity timer.
handle_call({send, Msg}, _, #state{handler=Pid, ref=Ref} = State) ->
    %% Subscription only happens when there are no messages in the queue.
    %% So the queue must be empty here, let's ensure that.
    true = queue:is_empty(State#state.messages),
    erlang:demonitor(Ref),
    send_message(Pid, Ref, Msg),
    {reply, ok, start_timer(State#state{handler=undefined})};

%% Stop inactivity timer. Subscribe if no messages to deliver,
%% else send one message and start inactivity timer again.
handle_call(recv, {Pid, _}, #state{handler=undefined, messages=Messages} = State) ->
    cancel_timer(State#state.ref),
    NewState = case queue:out(Messages) of
                   {empty, _} ->
                       Ref = erlang:monitor(process, Pid),
                       State#state{handler=Pid, ref=Ref};
                   {{value, Msg}, Msgs} ->
                       Ref = make_ref(),
                       send_message(Pid, Ref, Msg),
                       start_timer(State#state{messages=Msgs})
               end,
    {reply, Ref, NewState};

%% Only one subscriber is allowed!
handle_call(recv, {Pid, _}, #state{handler=Handler} = State) ->
    ?ERROR("~p subscribes to subscribed by ~p session", [Pid, Handler]),
    {reply, error, State};

handle_call(Request, {Pid, _}, State) ->
    ?ERROR("unexpected message ~p from ~p\n", [Request, Pid]),
    {reply, error, State}.


cancel_timer(Timer) ->
    erlang:cancel_timer(Timer),
    receive
        ?inactive_message(Timer) ->
            ok
    after 0 ->
            ok
    end.


send_message(Pid, Ref, Msg) ->
    Pid ! {Ref, Msg}.


%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.


%% @private
handle_info(?inactive_message(Timer), #state{ref=Timer} = State) ->
    {stop, {shutdown, inactive}, State};
handle_info({'DOWN', Ref, _, _, _}, #state{ref=Ref} = State) ->
    {noreply, start_timer(State#state{handler=undefined})};
handle_info(Info, #state{module=Module, state=MState} = State) ->
    case Module:handle_info(Info, MState) of
        {noreply, NState} ->
            {noreply, State#state{state=NState}};
        {noreply, NState, Timeout} ->
            {noreply, State#state{state=NState}, Timeout};
        {stop, Reason, NState} ->
            {stop, Reason, State#state{state=NState}}
    end.


%% @private
terminate(Reason, #state{module=Module, state=MState}) ->
    Module:terminate(Reason, MState).


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
