-module(smullet_group).
-behaviour(supervisor).

%% API
-export([start/4, start/3, stop/1]).
-export([start_child/3]).
-export([start_link/4]).

%% Supervisor callbacks
-export([init/1]).


start(Module, SessionTimeout, ShutdownTimeout) ->
    start(Module, Module, SessionTimeout, ShutdownTimeout).


start(Group, Module, SessionTimeout, ShutdownTimeout) ->
    supervisor:start_child(smullet_sup, [Group, Module, SessionTimeout, ShutdownTimeout]).


stop(Group) ->
    supervisor:terminate_child(smullet_sup, whereis(group_to_sup(Group))).


start_link(Group, Module, SessionTimeout, ShutdownTimeout) ->
    GroupId = group_to_sup(Group),
    supervisor:start_link({local, GroupId}, ?MODULE, {Module, SessionTimeout, ShutdownTimeout}).


group_to_sup(Group) ->
    GroupId = lists:flatten(io_lib:format("smullet_group_~p", [Group])),
    list_to_atom(GroupId).


start_child(Group, Key, Init) ->
    supervisor:start_child(group_to_sup(Group), [Key, Init]).


init({Module, SessionTimeout, ShutdownTimeout}) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 1,

    {ok, {{simple_one_for_one, MaxRestarts, MaxSecondsBetweenRestarts},
          [{smullet_session, {smullet_session, start_link, [SessionTimeout, Module]},
            temporary, ShutdownTimeout, worker, [smullet_session]}]}}.
