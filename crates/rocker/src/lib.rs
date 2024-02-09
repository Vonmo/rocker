extern crate core;
extern crate libc;
extern crate rocksdb;
extern crate rustler;
extern crate serde;

mod atoms;
mod nif;
mod options;

rustler::init!(
    "rocker",
    [
        nif::lxcode,
        nif::latest_sequence_number,
        nif::open,
        nif::open_for_read_only,
        nif::destroy,
        nif::repair,
        nif::get_db_path,
        nif::put,
        nif::get,
        nif::delete,
        nif::tx,
        nif::iterator,
        nif::next,
        nif::prefix_iterator,
        nif::create_cf,
        nif::open_cf,
        nif::open_cf_for_read_only,
        nif::list_cf,
        nif::drop_cf,
        nif::put_cf,
        nif::get_cf,
        nif::delete_cf,
        nif::iterator_cf,
        nif::prefix_iterator_cf,
        nif::delete_range,
        nif::delete_range_cf,
        nif::multi_get,
        nif::multi_get_cf,
        nif::key_may_exist,
        nif::key_may_exist_cf,
        nif::snapshot,
        nif::snapshot_get,
        nif::snapshot_get_cf,
        nif::snapshot_multi_get,
        nif::snapshot_multi_get_cf,
        nif::snapshot_iterator,
        nif::snapshot_iterator_cf,
        nif::create_checkpoint,
        nif::create_backup,
        nif::get_backup_info,
        nif::purge_old_backups,
        nif::restore_from_backup,
    ],
    load = nif::on_load
);
