-module(rocker).

%% API
-export([lxcode/0,
         open/2,
         open_default/1,
         destroy/1,
         repair/1,
         path/1,
         put/3,
         get/2,
         delete/2,
         write_batch/2
         ]).

%% Native library support
-export([load/0]).
-on_load(load/0).

%%==============================================================================
%% api
%%==============================================================================

lxcode() ->
    not_loaded(?LINE).

open(_Path, _Options) ->
    not_loaded(?LINE).

open_default(_Path) ->
    not_loaded(?LINE).

destroy(_Path) ->
    not_loaded(?LINE).

repair(_Path) ->
    not_loaded(?LINE).

path(_Db) ->
    not_loaded(?LINE).

put(_Db, _Key, _Value) ->
    not_loaded(?LINE).

get(_Db, _Key) ->
    not_loaded(?LINE).

delete(_Db, _Key) ->
    not_loaded(?LINE).

write_batch(_Db, _Operations) ->
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
