-module(smullet_session).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([new/2, ensure_started/2]).
-export([find/2, send/3, recv/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% define smullet_session behaviour with following callbacks
-type state() :: term().
-callback init(Group, Key) -> {ok, State} | ignore | {stop, Reason}
                                  when Group :: term(),
                                       Key :: term(),
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
-define(gproc_key(Group, Key), {n, l, {smullet, Group, Key}}).
-define(inactive_message(Timer), {timeout, Timer, inactive_message}).


%% @doc Creates a new session under specified supervisor using SessionKey.
%%      If the key is not unique returns an error.
-spec new(SessionGroup, SessionKey) -> {ok, session()} | {error, _}
                                          when SessionGroup :: term(),
                                               SessionKey :: term().
new(SessionGroup, SessionKey) ->
    GProcKey = ?gproc_key(SessionGroup, SessionKey),
    case smullet_sup:start_child(SessionGroup, GProcKey) of
        {error, {shutdown, Reason}} ->
            {error, Reason};
        Other ->
            Other
    end.


%% @doc Returns an existing session associated with SessionKey
%%      or undefined if no such session exists.
-spec find(SessionGroup, SessionKey) -> undefined | session()
                                           when SessionGroup :: term(),
                                                SessionKey :: term().
find(SessionGroup, SessionKey) ->
    Key = ?gproc_key(SessionGroup, SessionKey),
    gproc:where(Key).


%% @doc Returns an existing session associated with SessionKey
%%      or creates a new session under specified supervisor.
-spec ensure_started(SessionGroup, SessionKey) -> session()
                                                     when SessionGroup :: term(),
                                                          SessionKey :: term().
ensure_started(SessionGroup, SessionKey) ->
    GProcKey = ?gproc_key(SessionGroup, SessionKey),
    case gproc:where(GProcKey) of
        undefined ->
            case smullet_sup:start_child(SessionGroup, GProcKey) of
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
%%      If `Timeout' is `async' then function returnes immediately
%%      after the message is stored for the delivery. Otherwise
%%      function returns after the message is delivered to the handler
%%      or fails if it is not delivered in that amount of time.
-spec send(session(), Msg, Timeout) -> ok when Msg :: term(),
                                               Timeout :: timeout() | async | infinity.
send(Session, Msg, async) when is_pid(Session) ->
    gen_server:call(Session, {send, Msg, async});
send(Session, Msg, infinity) when is_pid(Session) ->
    gen_server:call(Session, {send, Msg, infinity}, infinity);
send(Session, Msg, Timeout) when is_pid(Session) ->
    WaitTill = milliseconds() + Timeout,
    gen_server:call(Session, {send, Msg, WaitTill}, Timeout).


milliseconds() ->
    {Megas, Secs, Millis} = os:timestamp(),
    (Megas * 1000000 + Secs) * 1000000 + Millis.


%% @doc Subscribes calling process for a message delivery.
%%      The message will be asynchronously delivered to the caller
%%      message queue as `{Tag, Msg}'.
-spec recv(session()) -> Tag when Tag :: reference().
recv(Session) when is_pid(Session) ->
    gen_server:call(Session, recv).


%% @doc Starts a session.
start_link(Timeout, Module, GProcKey) ->
    gen_server:start_link(?MODULE, {GProcKey, Timeout, Module}, []).


%% @private
init({GProcKey, Timeout, Module}) ->
    case gproc:reg_or_locate(GProcKey) of
        {Pid, _} when Pid =:= self() ->
            ?gproc_key(SessionGroup, SessionKey) = GProcKey,
            case Module:init(SessionGroup, SessionKey) of
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


send_ack(async, _) ->
    ok;
send_ack(infinity, From) ->
    gen_server:reply(From, ok);
send_ack(WaitUntill, From) ->
    case milliseconds() of
        Milliseconds when Milliseconds < WaitUntill ->
            gen_server:reply(From, ok);
        _ ->
            ok
    end.


%% @private
%%
%% Store a message to be delivered later.
handle_call({send, Msg, async}, _, #state{handler=undefined, messages=Messages} = State) ->
    Msgs = queue:in({async, undefined, Msg}, Messages),
    {reply, ok, State#state{messages=Msgs}};

%% Store a message to be delivered and acknowledged later.
handle_call({send, Msg, Type}, From, #state{handler=undefined, messages=Messages} = State) ->
    Msgs = queue:in({Type, From, Msg}, Messages),
    {noreply, State#state{messages=Msgs}};

%% Send a message to already subscribed handler, start inactivity timer.
handle_call({send, Msg, Type}, From, #state{handler=Pid, ref=Ref} = State) ->
    %% Subscription only happens when there are no messages in the queue.
    %% So the queue must be empty here, let's ensure that.
    true = queue:is_empty(State#state.messages),
    erlang:demonitor(Ref),
    send_message(Pid, Ref, Msg, Type, From),
    {noreply, start_timer(State#state{handler=undefined})};

%% Stop inactivity timer. Subscribe if no messages to deliver,
%% else send one message and start inactivity timer again.
handle_call(recv, {Pid, _}, #state{handler=undefined, messages=Messages} = State) ->
    cancel_timer(State#state.ref),
    NewState = case queue:out(Messages) of
                   {empty, _} ->
                       Ref = erlang:monitor(process, Pid),
                       State#state{handler=Pid, ref=Ref};
                   {{value, {Type, From, Msg}}, Msgs} ->
                       Ref = make_ref(),
                       send_message(Pid, Ref, Msg, Type, From),
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


send_message(Pid, Ref, Msg, Type, From) ->
    Pid ! {Ref, Msg},
    send_ack(Type, From).


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
