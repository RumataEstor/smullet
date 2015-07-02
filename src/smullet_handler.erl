-module(smullet_handler).
%% -behaviour(bullet_handler). % Sadly there is no such behaviour.

%% bullet handler callbacks
-export([init/4, stream/3, info/3, terminate/2]).


-callback session_id(Req) -> {ok, SessionId, Req} | {shutdown, Req}
                                 when Req :: cowboy_req:req(),
                                      SessionId :: term().
-callback session(SessionId) -> {ok, Session} | not_found
                                    when SessionId :: term(),
                                         Session :: smullet_session:session().
-callback info(Msg, SessionId) -> {reply, Data} | ok
                           when Msg :: term(),
				 SessionId :: term(),
                                Data :: iodata().
-callback stream(Data, SessionId) -> {reply, Data} | ok
                                         when Data :: iodata(),
                                              SessionId :: term().

-record(state, {active, handler, session_id, session, ref}).


init(_Transport, Req, Opts, Active) ->
    {_, Handler} = lists:keyfind(smullet_handler, 1, Opts),
    case Handler:session_id(Req) of
        {ok, SessionId, Req1} when Active =:= false ->
            {ok, Req1, #state{active = false, handler = Handler,
                              session_id = SessionId}};
        {ok, SessionId, Req1} when Active =:= once; Active =:= true ->
            case Handler:session(SessionId) of
                {ok, Session} ->
                    Ref = smullet_session:recv(Session),
                    {ok, Req, #state{active = Active, handler = Handler,
                                     session_id = SessionId,
                                     session = Session, ref = Ref}};
                not_found ->
                    {shutdown, Req1, undefined}
            end;
        {shutdown, Req1} ->
            {shutdown, Req1, undefined}
    end.


stream(Data, Req, #state{handler=Handler, session_id=SessionId} = State) ->
    case Handler:stream(Data, SessionId) of
        {reply, Msg} ->
            {reply, Msg, Req, State};
        ok ->
            {ok, Req, State}
    end.


info({Ref, Info}, Req, #state{ref=Ref, handler=Handler, session_id=SessionId} = State)
  when Ref =/= undefined ->
    case Handler:info(Info, SessionId) of
        {reply, Msg} ->
            {reply, Msg, Req, resubscribe(State)};
        ok ->
            {ok, Req, resubscribe(State)}
    end;
info(Info, Req, #state{handler=Handler, session_id=SessionId}=State) ->
    case Handler:info(Info, SessionId) of
        {reply, Msg} ->
            {reply, Msg, Req, State};
        ok ->
            {ok, Req, State}
    end.


resubscribe(#state{active=true, session=Session} = State) ->
    State#state{ref = smullet_session:recv(Session)};
resubscribe(State) ->
    State.


terminate(_Req, State) ->
    error_logger:info_msg("terminating: ~p", [State]).
