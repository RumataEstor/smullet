-module(smullet_session_test).
-behavoiur(smullet_session).

%% smullet_session callbacks
-export([init/2, handle_call/3, handle_cast/2, handle_info/2, terminate/3]).

-include_lib("eunit/include/eunit.hrl").
-include("assert_ex.hrl").

-define(t, 200).
-define(GROUP, {?MODULE, 1}).
-define(last_state, {p, l, ?MODULE}).
-define(assertRef(Expression), ?assertMatchR(Ref, is_reference(Ref), Expression)).


sessions_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {inorder, [fun sessions/0,
                {inparallel, [fun undelivered_fail/0,
                              fun dead_session/0
                             ]}
               ]}
    }.


undelivered_fail() ->
    ?assertMatchEx({ok, S}, smullet_session:new(?GROUP, 3)),
    ?assertExit({timeout, _}, smullet_session:send(S, undelivered, 100)).


dead_session() ->
    ?assertMatchEx({ok, S}, smullet_session:new(?GROUP, 4)),
    ?assertExit({{shutdown, inactive}, _}, smullet_session:send(S, undelivered, infinity)).


sessions() ->
    %% create sessions
    ?assertMatchEx({ok, S1}, smullet_session:new(?GROUP, 1)),
    ?assertMatch({init, 1}, st()),
    ?assertMatch({error, {already_registered, S1}}, smullet_session:new(?GROUP, 1)),
    ?assertMatchEx({ok, S2}, smullet_session:new(?GROUP, 2)),
    ?assertMatch({init, 2}, st()),

    %% subscribe for messages
    Ref1 = ?assertRef(smullet_session:recv(S1)),
    Ref2 = ?assertRef(smullet_session:recv(S2)),

    %% impossible to subscribe if someone is already subscribed to session
    ?assertMatch(error, smullet_session:recv(S1)),

    %% this message is immediately sent to the subscriber
    ?assertMatch(ok, smullet_session:send(S1, m1, 1000)),
    ?assertMatch({msg, {Ref1, m1}}, msg()),

    %% this message is saved for next delivery
    ?assertMatch(ok, smullet_session:send(S1, m2, async)),
    ?assertMatch(no_msg, msg()),

    %% now it is delivered
    ?assertMatchEx(Ref3, is_reference(Ref3), smullet_session:recv(S1)),
    ?assertMatch({msg, {Ref3, m2}}, msg()),

    %% session is terminated due to inactivity
    ?assertMatch(ok, timer:sleep(?t + ?t)),
    ?assertMatch(undefined, smullet_session:find(?GROUP, 1)),
    ?assertExit({noproc, _}, smullet_session:send(S1, m3, infinity)),

    %% but second is alive because was subscribed to
    ?assertMatchEx(ok, smullet_session:send(smullet_session:find(?GROUP, 2), n1, infinity)),
    ?assertMatch({msg, {Ref2, n1}}, msg()).


init(?GROUP, Key) ->
    gproc:set_value_shared(?last_state, {init, Key}),
    {ok, undefined}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.


st() ->
    gproc:get_value(?last_state, shared).


setup() ->
    ok = application:start(gproc),
    ok = application:start(smullet),
    gproc:reg_shared(?last_state),
    {ok, Pid} = smullet_group:start(?GROUP, ?MODULE, ?t, 1000),
    Pid.


cleanup(Pid) ->
    gproc:unreg_shared(?last_state),
    unlink(Pid),
    exit(Pid, kill).


msg() ->
        receive
            Msg ->
                {msg, Msg}
        after 0 ->
                no_msg
        end.
