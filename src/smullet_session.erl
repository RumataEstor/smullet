-module(smullet_session).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([new/2, ensure_started/2]).
-export([find/2, send/3, recv/1]).
-export([send/1]).
-export([call/2, call/3, cast/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% define smullet_session behaviour with following callbacks
-callback init(Group :: term(), Key :: term()) -> {ok, State :: term()} |
                                                  {stop, Reason :: term()} |
                                                  ignore.

-type gen_noreply() :: {noreply, NewState :: term()} |
                       {noreply, NewState :: term(), Timeout :: timeout() | hibernate} |
                       {stop, Reason :: term(), NewState :: term()}.

-callback handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), Timeout :: timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    gen_noreply().

-callback handle_cast(Request :: term(), State :: term()) -> gen_noreply().

-callback handle_info(Info :: term(), State :: term()) -> gen_noreply().

-callback terminate(Reason :: term(), Messages :: [term()], State :: term()) -> term().

-opaque session() :: pid().
-export_type([session/0]).

-record(state, {handler, module, state, messages, timeout, ref}).
-define(ERROR(Format, Params), error_logger:error_msg("[~p:~p ~p] " ++ Format,
                                                      [?MODULE, ?LINE, self()] ++ Params)).
-define(gproc_key(Group, Key), {n, l, {smullet, Group, Key}}).
-define(inactive_message(Timer), {timeout, Timer, inactive_message}).
-define(send(Msg, Type), {'$smullet_send', Msg, Type}).
-define(send(Msg), {'$smullet_send', Msg}).
-define(recv, '$smullet_recv').


%% @doc Creates a new session under specified supervisor using SessionKey.
%%      If the key is not unique returns an error.
-spec new(SessionGroup, SessionKey) -> {ok, session()} | {error, _}
                                          when SessionGroup :: term(),
                                               SessionKey :: term().
new(SessionGroup, SessionKey) ->
    GProcKey = ?gproc_key(SessionGroup, SessionKey),
    case smullet_group:start_child(SessionGroup, GProcKey) of
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
            case smullet_group:start_child(SessionGroup, GProcKey) of
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
%%      If `Timeout' is `async' then function returns immediately
%%      after the message is stored for the delivery. Otherwise
%%      function returns after the message is delivered to the handler
%%      or fails if it is not delivered in that amount of time.
-spec send(session(), Msg, Timeout) -> ok when Msg :: term(),
                                               Timeout :: timeout() | async | infinity.
send(Session, Msg, async) when is_pid(Session) ->
    gen_server:call(Session, ?send(Msg, async));
send(Session, Msg, infinity) when is_pid(Session) ->
    gen_server:call(Session, ?send(Msg, infinity), infinity);
send(Session, Msg, Timeout) when is_pid(Session) ->
    ReplyUntil = timestamp:add_micros(os:timestamp(), Timeout * 1000),
    gen_server:call(Session, ?send(Msg, ReplyUntil), Timeout).


%% @doc The same as `send(self(), Msg, async)' but doesn't hang.
send(Msg) ->
    gen_server:cast(self(), ?send(Msg)).


%% @doc Subscribes calling process for a message delivery.
%%      The message will be asynchronously delivered to the caller
%%      message queue as `{Tag, Msg}'.
-spec recv(session()) -> Tag when Tag :: reference().
recv(Session) when is_pid(Session) ->
    gen_server:call(Session, ?recv).


%% @doc The same as `gen_server:call/2'.
call(Session, Request) ->
    gen_server:call(Session, Request).

%% @doc The same as `gen_server:call/3'.
call(Session, Request, Timeout) ->
    gen_server:call(Session, Request, Timeout).

%% @doc The same as `gen_server:cast/2'.
cast(Session, Request) ->
    gen_server:cast(Session, Request).


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
send_ack(ReplyUntil, From) ->
    case os:timestamp() of
        Now when Now < ReplyUntil ->
            gen_server:reply(From, ok);
        _ ->
            ok
    end.


%% Store a message to be delivered and acknowledged later.
send(Msg, Type, From, #state{handler=undefined, messages=Messages} = State) ->
    Msgs = queue:in({Type, From, Msg}, Messages),
    State#state{messages=Msgs};

%% Send a message to already subscribed handler, start inactivity timer.
send(Msg, Type, From, #state{handler=Pid, ref=Ref} = State) ->
    %% Subscription only happens when there are no messages in the queue.
    %% So the queue must be empty here, let's ensure that.
    true = queue:is_empty(State#state.messages),
    erlang:demonitor(Ref),
    send_message(Pid, Ref, Msg, Type, From),
    start_timer(State#state{handler=undefined}).


%% @private
%%
%% Async send is acknowledged immediately.
handle_call(?send(Msg, async), _, State) ->
    {reply, ok, send(Msg, async, undefined, State)};

%% Synchronous send is acknowledged on delivery.
handle_call(?send(Msg, Type), From, State) ->
    {noreply, send(Msg, Type, From, State)};

%% Stop inactivity timer. Subscribe if no messages to deliver,
%% else send one message and start inactivity timer again.
handle_call(?recv, {Pid, _}, #state{handler=undefined, messages=Messages} = State) ->
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
handle_call(?recv, {Pid, _}, #state{handler=Handler} = State) ->
    ?ERROR("~p subscribes to subscribed by ~p session", [Pid, Handler]),
    {reply, error, State};

handle_call(Msg, From, #state{module=Module, state=MState} = State) ->
    gen_reply(Module:handle_call(Msg, From, MState), State).


gen_reply(Result, State) ->
    case Result of
        {reply, Reply, NState} ->
            {reply, Reply, State#state{state=NState}};
        {reply, Reply, NState, Timeout} ->
            {reply, Reply, State#state{state=NState}, Timeout};
        {noreply, NState} ->
            {noreply, State#state{state=NState}};
        {noreply, NState, Timeout} ->
            {noreply, State#state{state=NState}, Timeout};
        {stop, Reason, Reply, NState} ->
            {stop, Reason, Reply, State#state{state=NState}};
        {stop, Reason, NState} ->
            {stop, Reason, State#state{state=NState}}
    end.


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
handle_cast(?send(Msg), State) ->
    {noreply, send(Msg, async, undefined, State)};
handle_cast(Msg, #state{module=Module, state=MState} = State) ->
    gen_reply(Module:handle_cast(Msg, MState), State).


%% @private
handle_info(?inactive_message(Timer), #state{ref=Timer} = State) ->
    {stop, {shutdown, inactive}, State};
handle_info({'DOWN', Ref, _, _, _}, #state{ref=Ref} = State) ->
    {noreply, start_timer(State#state{handler=undefined})};
handle_info(Info, #state{module=Module, state=MState} = State) ->
    gen_reply(Module:handle_info(Info, MState), State).


%% @private
terminate(Reason, #state{module=Module, messages=Messages, state=MState}) ->
    Module:terminate(Reason, Messages, MState).


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
