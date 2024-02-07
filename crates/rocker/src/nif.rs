use atoms::{end_of_iterator, error, ok, snap, undefined, unknown_cf, vsn1};
use options::RockerOptions;
use rocksdb::{
    ColumnFamily, DBIterator, Direction, IteratorMode, Options, Snapshot, WriteBatch, DB,
};
use rustler::resource::ResourceArc;
use rustler::types::list::ListIterator;
use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, Term};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

// =================================================================================================
// resource
// =================================================================================================

#[repr(transparent)]
struct DbResource {
    db: RwLock<DB>,
}

impl DbResource {
    fn read(&self) -> RwLockReadGuard<'_, DB> {
        self.db.read().unwrap()
    }

    fn write(&self) -> RwLockWriteGuard<'_, DB> {
        self.db.write().unwrap()
    }
}

impl From<DB> for DbResource {
    fn from(other: DB) -> Self {
        DbResource {
            db: RwLock::new(other),
        }
    }
}

// ---------------------------------------------------------------------

#[repr(transparent)]
struct SnapshotResource {
    snap: Mutex<Snapshot<'static>>,
}

impl SnapshotResource {
    fn lock(&self) -> MutexGuard<'_, Snapshot<'static>> {
        self.snap.lock().unwrap()
    }
}

// ---------------------------------------------------------------------

#[repr(transparent)]
struct IteratorResource {
    iter: Mutex<DBIterator<'static>>,
}

impl IteratorResource {
    fn lock(&self) -> MutexGuard<'_, DBIterator<'static>> {
        self.iter.lock().unwrap()
    }
}

// ---------------------------------------------------------------------

pub fn on_load(env: Env, _load_info: Term) -> bool {
    rustler::resource!(DbResource, env);
    rustler::resource!(SnapshotResource, env);
    rustler::resource!(IteratorResource, env);
    true
}

// =================================================================================================
// api
// =================================================================================================

#[rustler::nif]
fn lxcode(env: Env) -> NifResult<Term> {
    Ok((ok(), vsn1()).encode(env))
}

#[rustler::nif]
fn latest_sequence_number(env: Env, resource: ResourceArc<DbResource>) -> NifResult<Term> {
    let db_guard = resource.read();
    Ok((ok(), db_guard.latest_sequence_number() as u64).encode(env))
}

#[rustler::nif]
fn open(env: Env, path: String, opts: RockerOptions) -> NifResult<Term> {
    let db_opts = Options::from(opts);
    match DB::open(&db_opts, path) {
        Ok(db) => Ok((ok(), ResourceArc::new(DbResource::from(db))).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn destroy(env: Env, path: String, opts: RockerOptions) -> NifResult<Term> {
    let db_opts = Options::from(opts);
    match DB::destroy(&db_opts, path) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn repair(env: Env, path: String, opts: RockerOptions) -> NifResult<Term> {
    let db_opts = Options::from(opts);
    match DB::repair(&db_opts, path) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn get_db_path(env: Env, resource: ResourceArc<DbResource>) -> NifResult<Term> {
    let db_guard = resource.read();
    Ok((ok(), db_guard.path().display().to_string()).encode(env))
}

#[rustler::nif]
fn put<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    key: LazyBinary<'a>,
    val: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    match db_guard.put(&key.as_ref(), &val.as_ref()) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn get<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    match db_guard.get(&key.as_ref()) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok((ok(), value.release(env)).encode(env))
        }
        Ok(None) => Ok((undefined()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn delete<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    match db_guard.delete(&key.as_ref()) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn tx<'a>(env: Env<'a>, resource: ResourceArc<DbResource>, txs: Term<'a>) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    let iter: ListIterator = txs.decode()?;
    let mut batch = WriteBatch::default();

    for elem in iter {
        let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(elem)?;
        if terms.len() >= 2 {
            let op: String = terms[0].atom_to_string()?;
            match op.as_str() {
                "put" => {
                    let key: Binary = terms[1].decode()?;
                    let val: Binary = terms[2].decode()?;
                    let _ = batch.put(&key.as_ref(), &val.as_ref());
                }
                "put_cf" => {
                    let cf: String = terms[1].decode()?;
                    let key: Binary = terms[2].decode()?;
                    let value: Binary = terms[3].decode()?;
                    let cf_handler = db_guard.cf_handle(&cf.as_str()).unwrap();
                    let _ = batch.put_cf(cf_handler, &key.as_ref(), &value.as_ref());
                }
                "delete" => {
                    let key: Binary = terms[1].decode()?;
                    let _ = batch.delete(&key.as_ref());
                }
                "delete_cf" => {
                    let cf: String = terms[1].decode()?;
                    let key: Binary = terms[2].decode()?;
                    let cf_handler = db_guard.cf_handle(&cf.as_str()).unwrap();
                    let _ = batch.delete_cf(cf_handler, &key.as_ref());
                }
                _ => {}
            }
        }
    }

    if batch.len() > 0 {
        let applied = batch.len();
        match db_guard.write(batch) {
            Ok(_) => Ok((ok(), applied).encode(env)),
            Err(e) => Ok((error(), e.to_string()).encode(env)),
        }
    } else {
        Ok((ok(), 0).encode(env))
    }
}

#[rustler::nif]
fn iterator<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    mode: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let mut iterator = db_guard.iterator(IteratorMode::Start);
    let mode_terms: Vec<Term> = ::rustler::types::tuple::get_tuple(mode)?;
    if mode_terms.len() >= 1 {
        let mode: String = mode_terms[0].atom_to_string()?;
        match mode.as_str() {
            "end" => iterator = db_guard.iterator(IteratorMode::End),
            "from" => {
                let from: Binary = mode_terms[1].decode()?;
                if mode_terms.len() == 3 {
                    let direction: String = mode_terms[2].atom_to_string()?;
                    iterator = match direction.as_str() {
                        "reverse" => {
                            db_guard.iterator(IteratorMode::From(&from, Direction::Reverse))
                        }
                        _ => db_guard.iterator(IteratorMode::From(&from, Direction::Forward)),
                    }
                } else {
                    iterator = db_guard.iterator(IteratorMode::From(&from, Direction::Forward));
                }
            }
            _ => {}
        }
    }

    let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(iterator) };
    let resource = ResourceArc::new(IteratorResource {
        iter: Mutex::new(eternal_iter),
    });
    Ok((ok(), resource.encode(env)).encode(env))
}

#[rustler::nif]
fn next<'a>(env: Env<'a>, resource: ResourceArc<IteratorResource>) -> NifResult<Term<'a>> {
    let mut iter = resource.lock();
    match iter.next() {
        None => Ok((end_of_iterator()).encode(env)),
        Some(Ok((k, v))) => {
            let mut key = OwnedBinary::new(k[..].len()).unwrap();
            key.clone_from_slice(&k[..]);

            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);

            Ok((ok(), key.release(env), value.release(env)).encode(env))
        }
        Some(Err(e)) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn prefix_iterator<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    prefix: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let iter = db_guard.prefix_iterator(&prefix.as_ref());
    let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(iter) };
    let resource = ResourceArc::new(IteratorResource {
        iter: Mutex::new(eternal_iter),
    });
    Ok((ok(), resource.encode(env)).encode(env))
}

#[rustler::nif]
fn create_cf(
    env: Env,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    opts: RockerOptions,
) -> NifResult<Term> {
    let mut db_guard = resource.write();
    let db_opts = Options::from(opts);

    match db_guard.create_cf(cf_name, &db_opts) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn open_cf<'a>(
    env: Env<'a>,
    path: String,
    cf_names: Term<'a>,
    opts: RockerOptions,
) -> NifResult<Term<'a>> {
    let db_opts = Options::from(opts);
    let cf_names_iter: ListIterator = cf_names.decode()?;
    let mut cfs: Vec<String> = Vec::new();
    for elem in cf_names_iter {
        let name: String = elem.decode()?;
        cfs.push(name);
    }

    match DB::open_cf(&db_opts, path, &cfs) {
        Ok(db) => Ok((ok(), ResourceArc::new(DbResource::from(db))).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn list_cf(env: Env, path: String, opts: RockerOptions) -> NifResult<Term> {
    let db_opts = Options::from(opts);
    match DB::list_cf(&db_opts, path) {
        Ok(cfs) => Ok((ok(), cfs).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn drop_cf(env: Env, resource: ResourceArc<DbResource>, cf_name: String) -> NifResult<Term> {
    let mut db_guard = resource.write();

    match db_guard.drop_cf(&cf_name.as_ref()) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn put_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    key: LazyBinary<'a>,
    val: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    let cf_handler = db_guard.cf_handle(&cf_name.as_ref()).unwrap();
    match db_guard.put_cf(cf_handler, &key.as_ref(), &val.as_ref()) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn get_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let cf_handler = db_guard.cf_handle(&cf_name.as_ref()).unwrap();
    match db_guard.get_cf(cf_handler, &key.as_ref()) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok((ok(), value.release(env)).encode(env))
        }
        Ok(None) => Ok((undefined()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn delete_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let cf_handler = db_guard.cf_handle(&cf_name.as_ref()).unwrap();
    match db_guard.delete_cf(cf_handler, &key.as_ref()) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn iterator_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    mode: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    match db_guard.cf_handle(&cf_name.as_ref()) {
        None => Ok((error(), unknown_cf()).encode(env)),
        Some(cf_handler) => {
            let mut cf_iterator = db_guard.iterator_cf(cf_handler, IteratorMode::Start);
            let mode_terms: Vec<Term> = ::rustler::types::tuple::get_tuple(mode)?;
            if mode_terms.len() >= 1 {
                let mode: String = mode_terms[0].atom_to_string()?;
                match mode.as_str() {
                    "end" => cf_iterator = db_guard.iterator_cf(cf_handler, IteratorMode::End),
                    "from" => {
                        let from: Binary = mode_terms[1].decode()?;
                        if mode_terms.len() == 3 {
                            let direction: String = mode_terms[2].atom_to_string()?;
                            cf_iterator = match direction.as_str() {
                                "reverse" => db_guard.iterator_cf(
                                    cf_handler,
                                    IteratorMode::From(&from, Direction::Reverse),
                                ),
                                _ => db_guard.iterator_cf(
                                    cf_handler,
                                    IteratorMode::From(&from, Direction::Forward),
                                ),
                            }
                        } else {
                            cf_iterator = db_guard.iterator_cf(
                                cf_handler,
                                IteratorMode::From(&from, Direction::Forward),
                            );
                        }
                    }
                    _ => {}
                }
            }

            let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(cf_iterator) };
            let resource = ResourceArc::new(IteratorResource {
                iter: Mutex::new(eternal_iter),
            });
            Ok((ok(), resource.encode(env)).encode(env))
        }
    }
}

#[rustler::nif]
fn prefix_iterator_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    prefix: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    match db_guard.cf_handle(&cf_name.as_ref()) {
        None => Ok((error(), unknown_cf()).encode(env)),
        Some(cf_handler) => {
            let iter = db_guard.prefix_iterator_cf(cf_handler, &prefix.as_ref());
            let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(iter) };
            let resource = ResourceArc::new(IteratorResource {
                iter: Mutex::new(eternal_iter),
            });
            Ok((ok(), resource.encode(env)).encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete_range<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    key_from: LazyBinary<'a>,
    key_to: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    let mut batch = WriteBatch::default();
    batch.delete_range(&key_from.as_ref(), &key_to.as_ref());
    match db_guard.write(batch) {
        Ok(_) => Ok((ok()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete_range_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    cf_name: String,
    key_from: LazyBinary<'a>,
    key_to: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.write();
    match db_guard.cf_handle(&cf_name.as_ref()) {
        None => Ok((error(), unknown_cf()).encode(env)),
        Some(cf_handler) => {
            match db_guard.delete_range_cf(cf_handler, &key_from.as_ref(), &key_to.as_ref()) {
                Ok(_) => Ok((ok()).encode(env)),
                Err(e) => Ok((error(), e.to_string()).encode(env)),
            }
        }
    }
}

#[rustler::nif]
fn multi_get<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    keys: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let keys_iter: ListIterator = keys.decode()?;
    let mut keys_list: Vec<String> = Vec::new();
    for elem in keys_iter {
        let k: String = elem.decode()?;
        keys_list.push(k);
    }
    let values = db_guard.multi_get(keys_list);
    let mut result: Vec<Term> = Vec::new();
    for v in values {
        match v {
            Ok(Some(item)) => {
                let mut value = OwnedBinary::new(item[..].len()).unwrap();
                value.clone_from_slice(&item[..]);
                result.push((ok(), value.release(env)).encode(env))
            }
            Ok(None) => result.push((undefined()).encode(env)),
            Err(e) => result.push((error(), e.to_string()).encode(env)),
        }
    }
    Ok((ok(), result).encode(env))
}

#[rustler::nif]
fn multi_get_cf<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    keys: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = resource.read();
    let keys_iter: ListIterator = keys.decode()?;
    let mut keys_list: Vec<(&ColumnFamily, Vec<u8>)> = Vec::new();
    for elem in keys_iter {
        let x = elem.decode()?;
        let xs: Vec<Term> = ::rustler::types::tuple::get_tuple(x)?;
        let cf_name: String = xs[0].decode()?;
        let key: Binary = xs[1].decode()?;
        let cf_handler = db_guard.cf_handle(&cf_name.as_ref());
        keys_list.push((cf_handler.unwrap(), key.to_vec()))
    }
    let values = db_guard.multi_get_cf(keys_list);
    let mut result: Vec<Term> = Vec::new();
    for v in values {
        match v {
            Ok(Some(item)) => {
                let mut value = OwnedBinary::new(item[..].len()).unwrap();
                value.clone_from_slice(&item[..]);
                result.push((ok(), value.release(env)).encode(env))
            }
            Ok(None) => result.push((undefined()).encode(env)),
            Err(e) => result.push((error(), e.to_string()).encode(env)),
        }
    }
    Ok((ok(), result).encode(env))
}

#[rustler::nif]
fn snapshot(env: Env, resource: ResourceArc<DbResource>) -> NifResult<Term> {
    let db_guard = resource.read();
    let snapshot = db_guard.snapshot();
    let eternal_snap: Snapshot<'static> = unsafe { std::mem::transmute(snapshot) };
    let snap_resource = ResourceArc::new(SnapshotResource {
        snap: Mutex::new(eternal_snap),
    });
    Ok((
        ok(),
        (snap(), resource.encode(env), snap_resource.encode(env)).encode(env),
    )
        .encode(env))
}

#[rustler::nif]
fn snapshot_get<'a>(env: Env<'a>, resource: Term<'a>, key: LazyBinary<'a>) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let snap_guard = resource.lock();
    match snap_guard.get(&key.as_ref()) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok((ok(), value.release(env)).encode(env))
        }
        Ok(None) => Ok((undefined()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn snapshot_get_cf<'a>(
    env: Env<'a>,
    resource: Term,
    cf_name: String,
    key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let db_resource: ResourceArc<DbResource> = terms[1].decode()?;
    let snap_resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let db_guard = db_resource.read();
    let snap_guard = snap_resource.lock();

    let cf_handler = db_guard.cf_handle(&cf_name.as_ref()).unwrap();
    match snap_guard.get_cf(cf_handler, &key.as_ref()) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok((ok(), value.release(env)).encode(env))
        }
        Ok(None) => Ok((undefined()).encode(env)),
        Err(e) => Ok((error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn snapshot_multi_get<'a>(env: Env<'a>, resource: Term, keys: Term<'a>) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let snap_guard = resource.lock();
    let keys_iter: ListIterator = keys.decode()?;
    let mut keys_list: Vec<String> = Vec::new();
    for elem in keys_iter {
        let k: String = elem.decode()?;
        keys_list.push(k);
    }
    let values = snap_guard.multi_get(keys_list);
    let mut result: Vec<Term> = Vec::new();
    for v in values {
        match v {
            Ok(Some(item)) => {
                let mut value = OwnedBinary::new(item[..].len()).unwrap();
                value.clone_from_slice(&item[..]);
                result.push((ok(), value.release(env)).encode(env))
            }
            Ok(None) => result.push((undefined()).encode(env)),
            Err(e) => result.push((error(), e.to_string()).encode(env)),
        }
    }
    Ok((ok(), result).encode(env))
}

#[rustler::nif]
fn snapshot_multi_get_cf<'a>(env: Env<'a>, resource: Term, keys: Term<'a>) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let db_resource: ResourceArc<DbResource> = terms[1].decode()?;
    let snap_resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let db_guard = db_resource.read();
    let snap_guard = snap_resource.lock();

    let keys_iter: ListIterator = keys.decode()?;
    let mut keys_list: Vec<(&ColumnFamily, Vec<u8>)> = Vec::new();
    for elem in keys_iter {
        let x = elem.decode()?;
        let xs: Vec<Term> = ::rustler::types::tuple::get_tuple(x)?;
        let cf_name: String = xs[0].decode()?;
        let key: Binary = xs[1].decode()?;
        let cf_handler = db_guard.cf_handle(&cf_name.as_ref());
        keys_list.push((cf_handler.unwrap(), key.to_vec()))
    }
    let values = snap_guard.multi_get_cf(keys_list);
    let mut result: Vec<Term> = Vec::new();
    for v in values {
        match v {
            Ok(Some(item)) => {
                let mut value = OwnedBinary::new(item[..].len()).unwrap();
                value.clone_from_slice(&item[..]);
                result.push((ok(), value.release(env)).encode(env))
            }
            Ok(None) => result.push((undefined()).encode(env)),
            Err(e) => result.push((error(), e.to_string()).encode(env)),
        }
    }
    Ok((ok(), result).encode(env))
}

#[rustler::nif]
fn snapshot_iterator<'a>(
    env: Env<'a>,
    resource: Term,
    mode: Term<'a>,
) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let snap_resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let snap_guard = snap_resource.lock();

    let mut snap_iterator = snap_guard.iterator(IteratorMode::Start);
    let mode_terms: Vec<Term> = ::rustler::types::tuple::get_tuple(mode)?;
    if mode_terms.len() >= 1 {
        let mode: String = mode_terms[0].atom_to_string()?;
        match mode.as_str() {
            "end" => snap_iterator = snap_guard.iterator(IteratorMode::End),
            "from" => {
                let from: Binary = mode_terms[1].decode()?;
                if mode_terms.len() == 3 {
                    let direction: String = mode_terms[2].atom_to_string()?;
                    snap_iterator = match direction.as_str() {
                        "reverse" => {
                            snap_guard.iterator(IteratorMode::From(&from, Direction::Reverse))
                        }
                        _ => snap_guard.iterator(IteratorMode::From(&from, Direction::Forward)),
                    }
                } else {
                    snap_iterator = snap_guard.iterator(IteratorMode::From(&from, Direction::Forward));
                }
            }
            _ => {}
        }
    }

    let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(snap_iterator) };
    let resource = ResourceArc::new(IteratorResource {
        iter: Mutex::new(eternal_iter),
    });
    Ok((ok(), resource.encode(env)).encode(env))
}

#[rustler::nif]
fn snapshot_iterator_cf<'a>(
    env: Env<'a>,
    resource: Term,
    cf_name: String,
    mode: Term<'a>,
) -> NifResult<Term<'a>> {
    let terms: Vec<Term> = ::rustler::types::tuple::get_tuple(resource)?;
    let db_resource: ResourceArc<DbResource> = terms[1].decode()?;
    let snap_resource: ResourceArc<SnapshotResource> = terms[2].decode()?;
    let db_guard = db_resource.read();
    let snap_guard = snap_resource.lock();

    match db_guard.cf_handle(&cf_name.as_ref()) {
        None => Ok((error(), unknown_cf()).encode(env)),
        Some(cf_handler) => {
            let mut cf_iterator = snap_guard.iterator_cf(cf_handler, IteratorMode::Start);
            let mode_terms: Vec<Term> = ::rustler::types::tuple::get_tuple(mode)?;
            if mode_terms.len() >= 1 {
                let mode: String = mode_terms[0].atom_to_string()?;
                match mode.as_str() {
                    "end" => cf_iterator = snap_guard.iterator_cf(cf_handler, IteratorMode::End),
                    "from" => {
                        let from: Binary = mode_terms[1].decode()?;
                        if mode_terms.len() == 3 {
                            let direction: String = mode_terms[2].atom_to_string()?;
                            cf_iterator = match direction.as_str() {
                                "reverse" => snap_guard.iterator_cf(
                                    cf_handler,
                                    IteratorMode::From(&from, Direction::Reverse),
                                ),
                                _ => snap_guard.iterator_cf(
                                    cf_handler,
                                    IteratorMode::From(&from, Direction::Forward),
                                ),
                            }
                        } else {
                            cf_iterator = snap_guard.iterator_cf(
                                cf_handler,
                                IteratorMode::From(&from, Direction::Forward),
                            );
                        }
                    }
                    _ => {}
                }
            }

            let eternal_iter: DBIterator<'static> = unsafe { std::mem::transmute(cf_iterator) };
            let resource = ResourceArc::new(IteratorResource {
                iter: Mutex::new(eternal_iter),
            });
            Ok((ok(), resource.encode(env)).encode(env))
        }
    }
}

// =================================================================================================
// helpers
// =================================================================================================

/// Represents either a borrowed `Binary` or `OwnedBinary`.
///
/// `LazyBinary` allows for the most efficient conversion from an
/// Erlang term to a byte slice. If the term is an actual Erlang
/// binary, constructing `LazyBinary` is essentially
/// zero-cost. However, if the term is any other Erlang type, it is
/// converted to an `OwnedBinary`, which requires a heap allocation.
enum LazyBinary<'a> {
    Owned(OwnedBinary),
    Borrowed(Binary<'a>),
}

impl<'a> std::ops::Deref for LazyBinary<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(owned) => owned.as_ref(),
            Self::Borrowed(borrowed) => borrowed.as_ref(),
        }
    }
}

impl<'a> rustler::Decoder<'a> for LazyBinary<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if term.is_binary() {
            Ok(Self::Borrowed(Binary::from_term(term)?))
        } else {
            Ok(Self::Owned(term.to_binary()))
        }
    }
}