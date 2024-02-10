use rocksdb::ReadOptions;
use rustler::{Decoder, NifResult, Term};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct RockerReadOptions {
    pub iterate_upper_bound: Option<String>,
    pub iterate_lower_bound: Option<String>,
}

impl Default for RockerReadOptions {
    fn default() -> RockerReadOptions {
        RockerReadOptions {
            iterate_upper_bound: None,
            iterate_lower_bound: None,
        }
    }
}

impl<'a> Decoder<'a> for RockerReadOptions {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        use rustler::{Error, MapIterator};
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "iterate_upper_bound" => opts.iterate_upper_bound = Some(value.decode()?),
                "iterate_lower_bound" => opts.iterate_lower_bound = Some(value.decode()?),
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl From<RockerReadOptions> for ReadOptions {
    fn from(opts: RockerReadOptions) -> Self {
        let mut read_opts = ReadOptions::default();

        if !opts.iterate_upper_bound.is_none() {
            read_opts.set_iterate_upper_bound(opts.iterate_upper_bound.unwrap());
        }
        if !opts.iterate_lower_bound.is_none() {
            read_opts.set_iterate_upper_bound(opts.iterate_lower_bound.unwrap());
        }

        read_opts
    }
}
