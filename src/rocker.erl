-module(rocker).

%% API
-export([
    lxcode/0,
    latest_sequence_number/1,
    open/2,
    open/1,
    destroy/2,
    destroy/1,
    repair/2,
    repair/1,
    get_db_path/1,
    put/3,
    get/2,
    get/3,
    delete/2,
    tx/2,
    iterator/2,
    next/1,
    prefix_iterator/2,
    create_cf/2,
    create_cf/3,
    open_cf/2,
    open_cf/3,
    list_cf/2,
    list_cf/1,
    drop_cf/2,
    put_cf/4,
    get_cf/4,
    get_cf/3,
    delete_cf/3,
    iterator_cf/3,
    prefix_iterator_cf/3,
    delete_range/3,
    delete_range_cf/4,
    multi_get/2,
    multi_get_cf/2,
    key_may_exist/2,
    key_may_exist_cf/3,
    snapshot/1,
    snapshot_get/2,
    snapshot_get/3,
    snapshot_get_cf/4,
    snapshot_get_cf/3,
    snapshot_multi_get/2,
    snapshot_multi_get_cf/2,
    snapshot_iterator/2,
    snapshot_iterator_cf/3,
    create_checkpoint/2
]).

%% Native library support
-export([load/0]).
-on_load(load/0).

%%==============================================================================
%% api
%%==============================================================================

lxcode() ->
    not_loaded(?LINE).

latest_sequence_number(_DbRef) ->
    not_loaded(?LINE).

open(_Path, _Options) ->
    not_loaded(?LINE).

open(Path) ->
    open(Path, #{}).

destroy(_Path, _Options) ->
    not_loaded(?LINE).

destroy(Path) ->
    destroy(Path, #{}).

repair(_Path, _Options) ->
    not_loaded(?LINE).

repair(Path) ->
    repair(Path, #{}).

get_db_path(_DbRef) ->
    not_loaded(?LINE).

put(_DbRef, _Key, _Value) ->
    not_loaded(?LINE).

get(_DbRef, _Key) ->
    not_loaded(?LINE).

get(DbRef, Key, Default) ->
    case get(DbRef, Key) of
        undefined ->
            {ok, Default};
        Some ->
            Some
    end.

delete(_DbRef, _Key) ->
    not_loaded(?LINE).

tx(_DbRef, _Txs) ->
    not_loaded(?LINE).

iterator(_DbRef, _Mode) ->
    not_loaded(?LINE).

next(_IterRef) ->
    not_loaded(?LINE).

prefix_iterator(_DbRef, _Prefix) ->
    not_loaded(?LINE).

create_cf(DbRef, CfName) ->
    create_cf(DbRef, CfName, #{}).

create_cf(_DbRef, _CfName, _Options) ->
    not_loaded(?LINE).

open_cf(_Path, _CfNames, _Options) ->
    not_loaded(?LINE).

open_cf(Path, CfNames) ->
    open_cf(Path, CfNames, #{}).

list_cf(_Path, _Options) ->
    not_loaded(?LINE).

list_cf(Path) ->
    list_cf(Path, #{}).

drop_cf(_DbRef, _CfName) ->
    not_loaded(?LINE).

put_cf(_DbRef, _CfName, _Key, _Value) ->
    not_loaded(?LINE).

get_cf(_DbRef, _CfName, _Key) ->
    not_loaded(?LINE).

get_cf(DbRef, CfName, Key, Default) ->
    case get_cf(DbRef, CfName, Key) of
        undefined ->
            {ok, Default};
        Some ->
            Some
    end.

delete_cf(_DbRef, _CfName, _Key) ->
    not_loaded(?LINE).

iterator_cf(_DbRef, _CfName, _Mode) ->
    not_loaded(?LINE).

prefix_iterator_cf(_DbRef, _CfName, _Prefix) ->
    not_loaded(?LINE).

delete_range(_DbRef, _KeyFrom, _KeyTo) ->
    not_loaded(?LINE).

delete_range_cf(_DbRef, _CfName, _KeyFrom, _KeyTo) ->
    not_loaded(?LINE).

multi_get(_DbRef, _Keys) ->
    not_loaded(?LINE).

multi_get_cf(_DbRef, _Keys) ->
    not_loaded(?LINE).

key_may_exist(_DfRef, _Key) ->
    not_loaded(?LINE).

key_may_exist_cf(_DbRef, _CfName, _Key) ->
    not_loaded(?LINE).

snapshot(_DbRef) ->
    not_loaded(?LINE).

snapshot_get(_SnapRef, _Key) ->
    not_loaded(?LINE).

snapshot_get(SnapRef, Key, Default) ->
    case snapshot_get(SnapRef, Key) of
        undefined ->
            {ok, Default};
        Some ->
            Some
    end.

snapshot_get_cf(_SnapRef, _CfName, _Key) ->
    not_loaded(?LINE).

snapshot_get_cf(SnapRef, CfName, Key, Default) ->
    case snapshot_get_cf(SnapRef, CfName, Key) of
        undefined ->
            {ok, Default};
        Some ->
            Some
    end.

snapshot_multi_get(_SnapRef, _Keys) ->
    not_loaded(?LINE).

snapshot_multi_get_cf(_SnapRef, _Keys) ->
    not_loaded(?LINE).

snapshot_iterator(_SnapRef, _Mode) ->
    not_loaded(?LINE).

snapshot_iterator_cf(_SnapRef, _CfName, _Mode) ->
    not_loaded(?LINE).

create_checkpoint(_DbRef, _Path) ->
    not_loaded(?LINE).

%%==============================================================================
%% helpers
%%==============================================================================

load() ->
    erlang:load_nif(filename:join(priv(), "librocker"), none).

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv()->
  case code:priv_dir(?MODULE) of
      {error, _} ->
          EbinDir = filename:dirname(code:which(?MODULE)),
          AppPath = filename:dirname(EbinDir),
          filename:join(AppPath, "priv");
      Path ->
          Path
  end.
