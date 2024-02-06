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
        nif::open,
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
    ],
    load = nif::on_load
);
