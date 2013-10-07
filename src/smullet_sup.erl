-module(smullet_sup).

-behaviour(supervisor).

%% API
-export([start_link/3, start_child/2, spec/3]).

%% Supervisor callbacks
-export([init/1]).


spec(Group, Timeout, Module) ->
    {group_to_sup(Group),
     {?MODULE, start_link, [Group, Timeout, Module]},
     permanent, infinity, supervisor, [smullet_sup]}.


start_link(Group, Timeout, Module) ->
    supervisor:start_link({local, group_to_sup(Group)}, ?MODULE, [Timeout, Module]).


group_to_sup(Group) ->
    GroupId = lists:flatten(io_lib:format("smullet_group_~p", [Group])),
    list_to_atom(GroupId).


start_child(Group, Key) ->
    supervisor:start_child(group_to_sup(Group), [Key]).


init(Params) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 1,

    {ok, {{simple_one_for_one, MaxRestarts, MaxSecondsBetweenRestarts},
          [{smullet, {smullet_session, start_link, Params},
            temporary, 1000, worker, [smullet_session]}]}}.
