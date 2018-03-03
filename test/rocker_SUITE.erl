-module(rocker_SUITE).
-compile(export_all).
-import(ct_helper, [config/2]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        {group, init},
        {group, atomic},
        {group, iterator},
        {group, cf}
    ].

groups() ->
    [
        {init,
            [parallel, shuffle],
                [lxcode, open, open_default, destroy, repair, path]},

        {atomic,
            [parallel, shuffle],
                [put_get, delete, write_batch]},

        {iterator,
            [parallel, shuffle],
                [create_iterator, next_start, next_end,
                 next_from_forward, next_from_reverse,
                 prefix_iterator]},

        {cf,
            [parallel, shuffle],
                [create_default, open_cf_default, list_cf, drop_cf,
                 put_cf_get_cf, put_cf_get_cf_multi, delete_cf,
                 create_iterator_cf, next_end_cf,
                 next_from_forward_cf, next_from_reverse_cf,
                 prefix_iterator_cf]}

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
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    {ok, 4} = rocker:tx(Db, [
        {put, <<"k1">>, <<"v1">>},
        {put, <<"k2">>, <<"v2">>},
        {delete, <<"k0">>, <<"v0">>},
        {put, <<"k3">>, <<"v3">>}
    ]),
    notfound = rocker:get(Db, <<"k0">>),
    {ok, <<"v1">>} = rocker:get(Db, <<"k1">>),
    {ok, <<"v2">>} = rocker:get(Db, <<"k2">>),
    {ok, <<"v3">>} = rocker:get(Db, <<"k3">>),
    ok.

%% =============================================================================
%% group: iterator
%% =============================================================================
create_iterator(_)->
    Path = <<"/project/priv/db_iter1">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    {ok, StartRef} = rocker:iterator(Db, {'start'}),
    true = is_reference(StartRef),
    {ok, true} = rocker:iterator_valid(StartRef),

    {ok, EndRef} = rocker:iterator(Db, {'end'}),
    true = is_reference(EndRef),
    {ok, true} = rocker:iterator_valid(EndRef),

    {ok, FromRef1} = rocker:iterator(Db, {'from', <<"k0">>, forward}),
    true = is_reference(FromRef1),
    {ok, true} = rocker:iterator_valid(FromRef1),

    {ok, FromRef2} = rocker:iterator(Db, {'from', <<"k0">>, reverse}),
    true = is_reference(FromRef2),
    {ok, true} = rocker:iterator_valid(FromRef2),

    {ok, FromRef3} = rocker:iterator(Db, {'from', <<"k1">>, forward}),
    true = is_reference(FromRef3),
    {ok, false} = rocker:iterator_valid(FromRef3),

    {ok, FromRef4} = rocker:iterator(Db, {'from', <<"k1">>, reverse}),
    true = is_reference(FromRef4),
    {ok, false} = rocker:iterator_valid(FromRef4),

    ok.

next_start(_)->
    Path = <<"/project/priv/db_iter_start">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    ok = rocker:put(Db, <<"k1">>, <<"v1">>),
    ok = rocker:put(Db, <<"k2">>, <<"v2">>),
    {ok, Iter} = rocker:iterator(Db, {'start'}),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_end(_)->
    Path = <<"/project/priv/db_iter_end">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    ok = rocker:put(Db, <<"k1">>, <<"v1">>),
    ok = rocker:put(Db, <<"k2">>, <<"v2">>),
    {ok, Iter} = rocker:iterator(Db, {'end'}),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_from_forward(_)->
    Path = <<"/project/priv/db_iter_next_forward">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    ok = rocker:put(Db, <<"k1">>, <<"v1">>),
    ok = rocker:put(Db, <<"k2">>, <<"v2">>),
    {ok, Iter} = rocker:iterator(Db, {'from', <<"k1">>, forward}),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_from_reverse(_)->
    Path = <<"/project/priv/db_iter_next_reverse">>,
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:put(Db, <<"k0">>, <<"v0">>),
    ok = rocker:put(Db, <<"k1">>, <<"v1">>),
    ok = rocker:put(Db, <<"k2">>, <<"v2">>),
    {ok, Iter} = rocker:iterator(Db, {'from', <<"k1">>, reverse}),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

prefix_iterator(_)->
    Path = <<"/project/priv/db_iter_prefix">>,
    {ok, Db} = rocker:open(Path, #{
        prefix_length => 3
    }),
    ok = rocker:put(Db, <<"aaa1">>, <<"va1">>),
    ok = rocker:put(Db, <<"bbb1">>, <<"vb1">>),
    ok = rocker:put(Db, <<"aaa2">>, <<"va2">>),
    {ok, Iter} = rocker:prefix_iterator(Db, <<"aaa">>),
    true = is_reference(Iter),
    {ok, <<"aaa1">>, <<"va1">>} = rocker:next(Iter),
    {ok, <<"aaa2">>, <<"va2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),

    {ok, Iter2} = rocker:prefix_iterator(Db, <<"bbb">>),
    true = is_reference(Iter2),
    {ok, <<"bbb1">>, <<"vb1">>} = rocker:next(Iter2),
    ok = rocker:next(Iter2),

    ok.

%% =============================================================================
%% group: cf
%% =============================================================================
create_default(_)->
    Path = <<"/project/priv/db_cf">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:create_cf_default(Db, <<"testcf">>),
    ok.

open_cf_default(_)->
    Path = <<"/project/priv/db_cf_open_default">>,
    rocker:destroy(Path),
    Self = self(),
    spawn(fun()->
        {ok, Db} = rocker:open_default(Path),
        ok = rocker:create_cf_default(Db, <<"testcf1">>),
        ok = rocker:create_cf_default(Db, <<"testcf2">>),
        ok = rocker:create_cf_default(Db, <<"testcf3">>),
        Self ! ok
    end),
    receive
        ok ->
            {err, _} = rocker:open_default(Path),
            {ok, Db} = rocker:open_cf_default(
                Path, [<<"testcf1">>,<<"testcf2">>,<<"testcf3">>]
            ),
            true = is_reference(Db)
    end,
    ok.


list_cf(_)->
    Path = <<"/project/priv/db_list_cf">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:create_cf_default(Db, <<"testcf">>),
    {ok,[<<"default">>,<<"testcf">>]} = rocker:list_cf(Path),
    ok.

drop_cf(_)->
    Path = <<"/project/priv/db_drop_cf">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:create_cf_default(Db, <<"testcf">>),
    {ok,[<<"default">>,<<"testcf">>]} = rocker:list_cf(Path),
    ok = rocker:drop_cf(Db, <<"testcf">>),
    {ok,[<<"default">>]} = rocker:list_cf(Path),
    {err, _} = rocker:drop_cf(Db, <<"testcf">>),
    ok.

put_cf_get_cf(_)->
    Path = <<"/project/priv/db_put_cf">>,
    rocker:destroy(Path),
    Self = self(),
    spawn(fun()->
        {ok, Db} = rocker:open_default(Path),
        ok = rocker:create_cf_default(Db, <<"testcf">>),
        Self ! ok
    end),
    receive
        ok ->
            {ok, Db} = rocker:open_cf_default(
                Path, [<<"testcf">>]
            ),
            ok = rocker:put_cf(Db, <<"testcf">>, <<"key">>, <<"value">>),
            {ok, <<"value">>} = rocker:get_cf(Db, <<"testcf">>, <<"key">>),
            notfound = rocker:get_cf(Db, <<"testcf">>, <<"unknown">>)
    end,
    ok.

put_cf_get_cf_multi(_)->
    Path = <<"/project/priv/db_put_cf_multi">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:create_cf_default(Db, <<"testcf">>),
    ok = rocker:put_cf(Db, <<"testcf">>, <<"key">>, <<"value">>),
    {ok, <<"value">>} = rocker:get_cf(Db, <<"testcf">>, <<"key">>),
    notfound = rocker:get_cf(Db, <<"testcf">>, <<"unknown">>),
    ok.

delete_cf(_)->
    Path = <<"/project/priv/db_delete_cf">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    ok = rocker:create_cf_default(Db, <<"testcf">>),
    ok = rocker:put_cf(Db, <<"testcf">>, <<"key">>, <<"value">>),
    {ok, <<"value">>} = rocker:get_cf(Db, <<"testcf">>, <<"key">>),
    ok = rocker:delete_cf(Db, <<"testcf">>, <<"key">>),
    notfound = rocker:get_cf(Db, <<"testcf">>, <<"key">>),
    ok.

create_iterator_cf(_)->
    Path = <<"/project/priv/db_iter_cf1">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf_default(Db, Cf),
    ok = rocker:put_cf(Db, Cf, <<"k0">>, <<"v0">>),

    {ok, StartRef} = rocker:iterator_cf(Db, Cf, {'start'}),
    true = is_reference(StartRef),
    {ok, true} = rocker:iterator_valid(StartRef),

    {ok, EndRef} = rocker:iterator_cf(Db, Cf, {'end'}),
    true = is_reference(EndRef),
    {ok, true} = rocker:iterator_valid(EndRef),

    {ok, FromRef1} = rocker:iterator_cf(Db, Cf, {'from', <<"k0">>, forward}),
    true = is_reference(FromRef1),
    {ok, true} = rocker:iterator_valid(FromRef1),

    {ok, FromRef2} = rocker:iterator_cf(Db, Cf, {'from', <<"k0">>, reverse}),
    true = is_reference(FromRef2),
    {ok, true} = rocker:iterator_valid(FromRef2),

    {ok, FromRef3} = rocker:iterator_cf(Db, Cf, {'from', <<"k1">>, forward}),
    true = is_reference(FromRef3),
    {ok, false} = rocker:iterator_valid(FromRef3),

    {ok, FromRef4} = rocker:iterator_cf(Db, Cf, {'from', <<"k1">>, reverse}),
    true = is_reference(FromRef4),
    {ok, false} = rocker:iterator_valid(FromRef4),

    ok.

next_start_cf(_)->
    Path = <<"/project/priv/db_iter_cf2">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf_default(Db, Cf),
    ok = rocker:put_cf(Db, Cf, <<"k0">>, <<"v0">>),
    ok = rocker:put_cf(Db, Cf, <<"k1">>, <<"v1">>),
    ok = rocker:put_cf(Db, Cf, <<"k2">>, <<"v2">>),

    {ok, Iter} = rocker:iterator_cf(Db, Cf, {'start'}),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_end_cf(_)->
    Path = <<"/project/priv/db_iter_cf3">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf_default(Db, Cf),

    ok = rocker:put_cf(Db, Cf, <<"k0">>, <<"v0">>),
    ok = rocker:put_cf(Db, Cf, <<"k1">>, <<"v1">>),
    ok = rocker:put_cf(Db, Cf, <<"k2">>, <<"v2">>),

    {ok, Iter} = rocker:iterator_cf(Db, Cf, {'end'}),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_from_forward_cf(_)->
    Path = <<"/project/priv/db_iter_cf4">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf_default(Db, Cf),

    ok = rocker:put_cf(Db, Cf, <<"k0">>, <<"v0">>),
    ok = rocker:put_cf(Db, Cf, <<"k1">>, <<"v1">>),
    ok = rocker:put_cf(Db, Cf, <<"k2">>, <<"v2">>),

    {ok, Iter} = rocker:iterator_cf(Db, Cf, {'from', <<"k1">>, forward}),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k2">>, <<"v2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

next_from_reverse_cf(_)->
    Path = <<"/project/priv/db_iter_cf5">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf_default(Db, Cf),

    ok = rocker:put_cf(Db, Cf, <<"k0">>, <<"v0">>),
    ok = rocker:put_cf(Db, Cf, <<"k1">>, <<"v1">>),
    ok = rocker:put_cf(Db, Cf, <<"k2">>, <<"v2">>),

    {ok, Iter} = rocker:iterator_cf(Db, Cf, {'from', <<"k1">>, reverse}),
    {ok, <<"k1">>, <<"v1">>} = rocker:next(Iter),
    {ok, <<"k0">>, <<"v0">>} = rocker:next(Iter),
    ok = rocker:next(Iter),
    ok.

prefix_iterator_cf(_)->
    Path = <<"/project/priv/db_iter_cf6">>,
    rocker:destroy(Path),
    {ok, Db} = rocker:open_default(Path),
    Cf = <<"test_cf">>,
    ok = rocker:create_cf(Db, Cf, #{
        prefix_length => 3
    }),

    ok = rocker:put_cf(Db, Cf, <<"aaa1">>, <<"va1">>),
    ok = rocker:put_cf(Db, Cf, <<"bbb1">>, <<"vb1">>),
    ok = rocker:put_cf(Db, Cf, <<"aaa2">>, <<"va2">>),
    {ok, Iter} = rocker:prefix_iterator_cf(Db, Cf, <<"aaa">>),
    true = is_reference(Iter),
    {ok, <<"aaa1">>, <<"va1">>} = rocker:next(Iter),
    {ok, <<"aaa2">>, <<"va2">>} = rocker:next(Iter),
    ok = rocker:next(Iter),

    {ok, Iter2} = rocker:prefix_iterator_cf(Db, Cf, <<"bbb">>),
    true = is_reference(Iter2),
    {ok, <<"bbb1">>, <<"vb1">>} = rocker:next(Iter2),
    ok = rocker:next(Iter2),

    ok.
