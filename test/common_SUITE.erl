-module(common_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-import(ct_helper, [config/2]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        {group, common_app_checks}
    ].

groups() ->
    [
        {common_app_checks,
            [parallel, shuffle],
                [app_module_load, sup_module_load]}
    ].


%% =============================================================================
%% init
%% =============================================================================
init_per_group(_Group, Config) ->
    ok = application:load(rocker),
    {ok, _} = application:ensure_all_started(rocker, temporary),
    [{init, true} | Config].


%% =============================================================================
%% end
%% =============================================================================
end_per_group(_Group, _Config) ->
    ok = application:stop(rocker),
    ok = application:unload(rocker),
    ok.


%% =============================================================================
%% group: common_app_checks
%% =============================================================================
app_module_load(_)->
    {module,rocker_app} = code:load_file(rocker_app).

sup_module_load(_)->
    {module,rocker_sup} = code:load_file(rocker_sup).
