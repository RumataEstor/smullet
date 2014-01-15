-module(smullet_app).
-behaviour(application).


%% application callbacks
-export([start/2, stop/1]).


start(_StartType, _StartArgs) ->
    smullet_sup:start_link().


stop(_) ->
    ok.
