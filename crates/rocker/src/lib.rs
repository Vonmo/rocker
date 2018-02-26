#[macro_use]
extern crate lazy_static;
extern crate rocksdb;
#[macro_use]
extern crate rustler;

use rocksdb::{DB, DBCompactionStyle, Direction, IteratorMode, Options, WriteBatch};
use rocksdb::DBIterator;
use rustler::{NifEncoder, NifEnv, NifResult, NifTerm};
use rustler::resource::ResourceArc;
use rustler::schedule::NifScheduleFlags;
use rustler::types::list::NifListIterator;
use rustler::types::map::NifMapIterator;
use std::sync::RwLock;

mod atoms {
    rustler_atoms! {
        atom ok;
        atom vn1;
        atom err;
        atom notfound;
    }
}

struct DbResource {
    db: RwLock<DB>,
    path: String,
}

struct IteratorResource {
    iter: RwLock<DBIterator>,
}

rustler_export_nifs!(
    "rocker",
    [
        ("lxcode", 0, lxcode), // library version code
        ("open", 2, open), // open db with options
        ("open_default", 1, open_default), // open db with defaults
        ("open_cf_default", 2, open_cf_default), // open db with default options and cfs
        ("destroy", 1 , destroy, NifScheduleFlags::DirtyIo), //destroy db and data
        ("repair", 1 , repair, NifScheduleFlags::DirtyIo), //repair db
        ("path", 1, path), //get fs path
        ("put", 3, put), //put key payload
        ("get", 2, get), //get key payload
        ("delete", 2, delete), //delete key
        ("tx", 2, tx), //atomic write batch
        ("iterator", 2, iterator), // get db iterator
        ("prefix_iterator", 2, prefix_iterator), // get prefix iterator
        ("iterator_valid", 1, iterator_valid), // validate iterator
        ("next", 1, next), // go to next element in iterator
        ("create_cf_default", 2, create_cf_default), // create cf with default options
        ("create_cf", 3, create_cf), // create cf with options
        ("list_cf", 1, list_cf), // list db cfs
        ("drop_cf", 2, drop_cf, NifScheduleFlags::DirtyIo), // drop cf from db
    ],
    Some(on_load)
);

fn on_load<'a>(env: NifEnv<'a>, _load_info: NifTerm<'a>) -> bool {
    resource_struct_init!(DbResource, env);
    resource_struct_init!(IteratorResource, env);
    true
}

fn lxcode<'a>(env: NifEnv<'a>, _args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    Ok((atoms::ok(), atoms::vn1()).encode(env))
}

fn open<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;
    let iter: NifMapIterator = args[1].decode()?;

    let mut opts = Options::default();
    for (key, value) in iter {
        let param = key.atom_to_string()?;
        match param.as_str() {
            "create_if_missing" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_if_missing(true);
                }
            }
            "create_missing_column_families" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_missing_column_families(true);
                }
            }
            "set_max_open_files" => {
                let limit: i32 = value.decode()?;
                opts.set_max_open_files(limit);
            }
            "set_use_fsync" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_use_fsync(true);
                }
            }
            "set_bytes_per_sync" => {
                let limit: u64 = value.decode()?;
                opts.set_bytes_per_sync(limit);
            }
            "optimize_for_point_lookup" => {
                let limit: u64 = value.decode()?;
                opts.optimize_for_point_lookup(limit);
            }
            "set_table_cache_num_shard_bits" => {
                let limit: i32 = value.decode()?;
                opts.set_table_cache_num_shard_bits(limit);
            }
            "set_max_write_buffer_number" => {
                let limit: i32 = value.decode()?;
                opts.set_max_write_buffer_number(limit);
            }
            "set_write_buffer_size" => {
                let limit: usize = value.decode()?;
                opts.set_write_buffer_size(limit);
            }
            "set_target_file_size_base" => {
                let limit: u64 = value.decode()?;
                opts.set_target_file_size_base(limit);
            }
            "set_min_write_buffer_number_to_merge" => {
                let limit: i32 = value.decode()?;
                opts.set_min_write_buffer_number_to_merge(limit);
            }
            "set_level_zero_stop_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_stop_writes_trigger(limit);
            }
            "set_level_zero_slowdown_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_slowdown_writes_trigger(limit);
            }
            "set_max_background_compactions" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_compactions(limit);
            }
            "set_max_background_flushes" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_flushes(limit);
            }
            "set_disable_auto_compactions" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_disable_auto_compactions(true);
                }
            }
            "set_compaction_style" => {
                let style = try!(value.atom_to_string());
                if style == "level" {
                    opts.set_compaction_style(DBCompactionStyle::Level);
                } else if style == "universal" {
                    opts.set_compaction_style(DBCompactionStyle::Universal);
                } else if style == "fifo" {
                    opts.set_compaction_style(DBCompactionStyle::Fifo);
                }
            }
            "prefix_length" => {
                let limit: usize = value.decode()?;
                let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(limit);
                opts.set_prefix_extractor(prefix_extractor);
            }
            _ => {}
        }
    }

    match DB::open(&opts,path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(
                    db
                ),
                path: path.clone(),
            });
            Ok((atoms::ok(), resource.encode(env)).encode(env))
        }
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn open_default<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;

    match DB::open_default(path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(
                    db
                ),
                path: path.clone(),
            });
            Ok((atoms::ok(), resource.encode(env)).encode(env))
        }
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn open_cf_default<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;
    let iter: NifListIterator = args[1].decode()?;
    let mut cfs: Vec<String> = Vec::new();
    for elem in iter {
        let name: String = elem.decode()?;
        cfs.push(name);
    }
    let cfs2: Vec<&str> = cfs.iter().map(|s| &**s).collect();
    let resource = ResourceArc::new(DbResource {
        db: RwLock::new(
            DB::open_cf(&Options::default(), path.clone(), &cfs2).unwrap()
        ),
        path: path.clone(),
    });

    Ok((atoms::ok(), resource.encode(env)).encode(env))
}


fn destroy<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;
    match DB::destroy(&Options::default(), path) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn repair<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;
    match DB::repair(Options::default(), path) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn path<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let path = resource.path.to_string();
    Ok((atoms::ok(), path).encode(env))
}


fn put<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let key: String = args[1].decode()?;
    let value: String = args[2].decode()?;
    let key_bin: Vec<u8> = key.into_bytes();
    let value_bin: Vec<u8> = value.into_bytes();
    let db = resource.db.write().unwrap();
    match db.put(&key_bin, &value_bin) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn get<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let key: String = args[1].decode()?;
    let key_bin: Vec<u8> = key.into_bytes();
    let db = resource.db.read().unwrap();
    match db.get(&key_bin) {
        Ok(Some(v)) => {
            let res = std::str::from_utf8(&v[..]).unwrap();
            Ok((atoms::ok(), res.to_string()).encode(env))
        }
        Ok(None) => Ok((atoms::notfound()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn delete<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let key: String = args[1].decode()?;
    let key_bin: Vec<u8> = key.into_bytes();
    let db = resource.db.write().unwrap();
    match db.delete(&key_bin) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn tx<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let iter: NifListIterator = args[1].decode()?;
    let db = resource.db.write().unwrap();
    let mut batch = WriteBatch::default();
    for elem in iter {
        let terms: Vec<NifTerm> = ::rustler::types::tuple::get_tuple(elem)?;
        if terms.len() >= 2 {
            let op: String = terms[0].atom_to_string()?;
            let key: String = terms[1].decode()?;
            let key_bin: Vec<u8> = key.into_bytes();
            match op.as_str() {
                "put" => {
                    let val: String = terms[2].decode()?;
                    let val_bin: Vec<u8> = val.into_bytes();
                    let _ = batch.put(&key_bin, &val_bin);
                }
                "delete" => {
                    let _ = batch.delete(&key_bin);
                }
                _ => {}
            }
        }
    }
    if batch.len() > 0 {
        let applied = batch.len();
        match db.write(batch) {
            Ok(_) => Ok((atoms::ok(), applied).encode(env)),
            Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
        }
    } else {
        Ok((atoms::ok(), 0).encode(env))
    }
}


fn iterator<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let mode_terms: Vec<NifTerm> = ::rustler::types::tuple::get_tuple(args[1])?;
    let db = resource.db.read().unwrap();
    let mut iterator = db.iterator(IteratorMode::Start);
    if mode_terms.len() >= 1 {
        let mode: String = mode_terms[0].atom_to_string()?;
        match mode.as_str() {
            "end" => iterator = db.iterator(IteratorMode::End),
            "from" => {
                let from: String = mode_terms[1].decode()?;
                let from_bin: Vec<u8> = from.into_bytes();
                if mode_terms.len() == 3 {
                    let direction: String = mode_terms[2].atom_to_string()?;
                    iterator = match direction.as_str() {
                        "reverse" => db.iterator(IteratorMode::From(&from_bin, Direction::Reverse)),
                        _ => db.iterator(IteratorMode::From(&from_bin, Direction::Forward)),
                    }
                } else {
                    iterator = db.iterator(IteratorMode::From(&from_bin, Direction::Forward));
                }
            }
            _ => {}
        }
    }

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            iterator,
        ),
    });

    Ok((atoms::ok(), resource.encode(env)).encode(env))
}


fn prefix_iterator<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let prefix: String = args[1].decode()?;
    let prefix_bin: Vec<u8> = prefix.into_bytes();

    let db = resource.db.read().unwrap();
    let iterator = db.prefix_iterator(&prefix_bin);

    let resource = ResourceArc::new(IteratorResource {
        iter: RwLock::new(
            iterator,
        ),
    });

    Ok((atoms::ok(), resource.encode(env)).encode(env))
}


fn iterator_valid<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<IteratorResource> = args[0].decode()?;
    let iter = resource.iter.read().unwrap();
    Ok((atoms::ok(), iter.valid()).encode(env))
}


fn next<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<IteratorResource> = args[0].decode()?;
    let mut iter = resource.iter.write().unwrap();
    match iter.next() {
        None => Ok((atoms::ok()).encode(env)),
        Some((k, v)) => {
            let key = std::str::from_utf8(&k[..]).unwrap();
            let value = std::str::from_utf8(&v[..]).unwrap();
            Ok((atoms::ok(), key.to_string(), value.to_string()).encode(env))
        }
    }
}


fn create_cf_default<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let name: String = args[1].decode()?;

    let mut db = resource.db.write().unwrap();
    let opts = Options::default();

    match db.create_cf(name.as_str(), &opts) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn create_cf<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let name: String = args[1].decode()?;
    let iter: NifMapIterator = args[2].decode()?;

    let mut db = resource.db.write().unwrap();

    let mut opts = Options::default();
    for (key, value) in iter {
        let param = key.atom_to_string()?;
        match param.as_str() {
            "create_if_missing" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_if_missing(true);
                }
            }
            "create_missing_column_families" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_missing_column_families(true);
                }
            }
            "set_max_open_files" => {
                let limit: i32 = value.decode()?;
                opts.set_max_open_files(limit);
            }
            "set_use_fsync" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_use_fsync(true);
                }
            }
            "set_bytes_per_sync" => {
                let limit: u64 = value.decode()?;
                opts.set_bytes_per_sync(limit);
            }
            "optimize_for_point_lookup" => {
                let limit: u64 = value.decode()?;
                opts.optimize_for_point_lookup(limit);
            }
            "set_table_cache_num_shard_bits" => {
                let limit: i32 = value.decode()?;
                opts.set_table_cache_num_shard_bits(limit);
            }
            "set_max_write_buffer_number" => {
                let limit: i32 = value.decode()?;
                opts.set_max_write_buffer_number(limit);
            }
            "set_write_buffer_size" => {
                let limit: usize = value.decode()?;
                opts.set_write_buffer_size(limit);
            }
            "set_target_file_size_base" => {
                let limit: u64 = value.decode()?;
                opts.set_target_file_size_base(limit);
            }
            "set_min_write_buffer_number_to_merge" => {
                let limit: i32 = value.decode()?;
                opts.set_min_write_buffer_number_to_merge(limit);
            }
            "set_level_zero_stop_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_stop_writes_trigger(limit);
            }
            "set_level_zero_slowdown_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_slowdown_writes_trigger(limit);
            }
            "set_max_background_compactions" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_compactions(limit);
            }
            "set_max_background_flushes" => {
                let limit: i32 = value.decode()?;
                opts.set_max_background_flushes(limit);
            }
            "set_disable_auto_compactions" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_disable_auto_compactions(true);
                }
            }
            "set_compaction_style" => {
                let style = try!(value.atom_to_string());
                if style == "level" {
                    opts.set_compaction_style(DBCompactionStyle::Level);
                } else if style == "universal" {
                    opts.set_compaction_style(DBCompactionStyle::Universal);
                } else if style == "fifo" {
                    opts.set_compaction_style(DBCompactionStyle::Fifo);
                }
            }
            "prefix_length" => {
                let limit: usize = value.decode()?;
                let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(limit);
                opts.set_prefix_extractor(prefix_extractor);
            }
            _ => {}
        }
    }

    match db.create_cf(name.as_str(), &opts) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn list_cf<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let path: String = args[0].decode()?;
    match DB::list_cf(&Options::default(), path) {
        Ok(cfs) => Ok((atoms::ok(), cfs).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}


fn drop_cf<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let resource: ResourceArc<DbResource> = args[0].decode()?;
    let name: String = args[1].decode()?;

    let mut db = resource.db.write().unwrap();

    match db.drop_cf(name.as_str()) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}
