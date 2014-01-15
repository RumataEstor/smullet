-module(smullet_sup).
-behaviour(supervisor).


-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


start_link() ->
    supervisor:start_link({local, smullet_sup}, ?MODULE, []).


init([]) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 1,

    {ok, {{simple_one_for_one, MaxRestarts, MaxSecondsBetweenRestarts},
          [{smullet_group, {smullet_group, start_link, []},
            permanent, infinity, supervisor, [smullet_group]}]}}.
