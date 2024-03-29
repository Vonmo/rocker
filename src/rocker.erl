-module(rocker).

%% API
-export([
    lxcode/0,
    latest_sequence_number/1,
    open/2,
    open/1,
    open_for_read_only/2,
    open_for_read_only/1,
    destroy/2,
    destroy/1,
    repair/2,
    repair/1,
    get_db_path/1,
    put/3,
    get/2,
    get/3,
    delete/2,
    write_batch/2,
    iterator/2,
    iterator_range/5,
    iterator_range/4,
    next/1,
    prefix_iterator/2,
    create_cf/2,
    create_cf/3,
    open_cf/2,
    open_cf/3,
    open_cf_for_read_only/3,
    open_cf_for_read_only/2,
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
    create_checkpoint/2,
    create_backup/2,
    get_backup_info/1,
    purge_old_backups/2,
    restore_from_backup/3,
    restore_from_backup/2
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

open_for_read_only(_Path, _Options) ->
    not_loaded(?LINE).

open_for_read_only(Path) ->
    open_for_read_only(Path, #{}).

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

write_batch(_DbRef, _Ops) ->
    not_loaded(?LINE).

iterator(_DbRef, _Mode) ->
    not_loaded(?LINE).

iterator_range(_DbRef, _Mode, _From, _To, _ReadOptions) ->
    not_loaded(?LINE).

iterator_range(DbRef, Mode, From, To) ->
    iterator_range(DbRef, Mode, From, To, #{}).

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

open_cf_for_read_only(_Path, _CfNames, _Options) ->
    not_loaded(?LINE).

open_cf_for_read_only(Path, CfNames) ->
    open_cf_for_read_only(Path, CfNames, #{}).

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

create_backup(_DbRef, _Path) ->
    not_loaded(?LINE).

get_backup_info(_BackupPath) ->
    not_loaded(?LINE).

purge_old_backups(_BackupPath, _NumBackupsToKeep) ->
    not_loaded(?LINE).

restore_from_backup(_BackupPath, _RestorePath, _BackupId) ->
    not_loaded(?LINE).

restore_from_backup(BackupPath, RestorePath) ->
    restore_from_backup(BackupPath, RestorePath, -1).

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
