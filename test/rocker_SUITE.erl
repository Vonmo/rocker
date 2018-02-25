-module(rocker_SUITE).
-compile(export_all).
-import(ct_helper, [config/2]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        {group, init},
        {group, atomic}
    ].

groups() ->
    [
        {init,
            [parallel, shuffle],
                [lxcode, open, open_default, destroy, repair, path]},
        {atomic,
            [parallel, shuffle],
                [put_get, delete, write_batch]}
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
%% group: init
%% =============================================================================
lxcode(_)->
    {ok, vn1} = rocker:lxcode().

open(_)->
    {ok, Db} = rocker:open(<<"/project/priv/db_path">>, #{
        create_if_missing => true,
        set_max_open_files => 1000,
        set_use_fsync => false,
        set_bytes_per_sync => 8388608,
        optimize_for_point_lookup => 1024,
        set_table_cache_num_shard_bits => 6,
        set_max_write_buffer_number => 32,
        set_write_buffer_size => 536870912,
        set_target_file_size_base => 1073741824,
        set_min_write_buffer_number_to_merge => 4,
        set_level_zero_stop_writes_trigger => 2000,
        set_level_zero_slowdown_writes_trigger => 0,
        set_max_background_compactions => 4,
        set_max_background_flushes => 4,
        set_disable_auto_compactions => true,
        set_compaction_style => universal
    }),
    true = is_reference(Db),
    ok.

open_default(_)->
    {ok, Db} = rocker:open_default(<<"/project/priv/db_default_path">>),
    true = is_reference(Db),
    ok.

destroy(_)->
    Path = <<"/project/priv/db_destr">>,
    Test = self(),
    spawn(fun()->
        {ok, Db} = rocker:open_default(Path),
        true = is_reference(Db),
        Test ! ok
    end),
    receive
        ok ->
            ok = rocker:destroy(Path)
    end,
    ok.

repair(_)->
    Path = <<"/project/priv/db_repair">>,
    Test = self(),
    spawn(fun()->
        {ok, Db} = rocker:open_default(Path),
        true = is_reference(Db),
        Test ! ok
    end),
    receive
        ok ->
            ok = rocker:repair(Path)
    end,
    ok.

path(_)->
    Path = <<"/project/priv/db_get_path">>,
    {ok, Db} = rocker:open_default(Path),
    {ok, Path} = rocker:path(Db),
    ok.

%% =============================================================================
%% group: atomic
%% =============================================================================
put_get(_)->
    Path = <<"/project/priv/db_put">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"key">>, <<"value">>),
    {ok, <<"value">>} = rocker:get(Db, <<"key">>),
    ok = rocker:put(Db, <<"key">>, <<"value1">>),
    {ok, <<"value1">>} = rocker:get(Db, <<"key">>),
    ok = rocker:put(Db, <<"key">>, <<"value2">>),
    {ok, <<"value2">>} = rocker:get(Db, <<"key">>),
    notfound = rocker:get(Db, <<"unknown">>),
    ok.

delete(_)->
    Path = <<"/project/priv/db_delete">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"key">>, <<"value">>),
    {ok, <<"value">>} = rocker:get(Db, <<"key">>),
    ok = rocker:delete(Db, <<"key">>),
    notfound = rocker:get(Db, <<"key">>),
    ok.

write_batch(_)->
    Path = <<"/project/priv/db_bath">>,
    {ok, Db} = rocker:open_default(Path),
    ?debugVal(rocker:write_batch(Db, [
        {put, <<"k1">>, <<"v1">>},
        {put, <<"k2">>, <<"v2">>},
        {put, <<"k3">>, <<"v3">>}
    ])),
    ok.
