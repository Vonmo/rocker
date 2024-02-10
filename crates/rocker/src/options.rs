use libc::{c_double, c_int, c_uint, size_t};
use rocksdb::{DBCompactionStyle, DBCompressionType, DBRecoveryMode, LogLevel, Options};
use rustler::{Decoder, NifResult, Term};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct RockerOptions {
    pub create_if_missing: Option<bool>,
    pub create_missing_column_families: Option<bool>,
    pub set_error_if_exists: Option<bool>,
    pub set_paranoid_checks: Option<bool>,
    pub increase_parallelism: Option<i32>,
    pub optimize_level_style_compaction: Option<usize>,
    pub optimize_universal_style_compaction: Option<usize>,
    pub set_compression_type: Option<String>, // None, Snappy, Zlib, Bz2, Lz4, Lz4hc, Zstd
    // pub set_compression_options_parallel_threads: Option<i32>,
    // pub set_wal_compression_type: Option<String>, // None, Snappy, Zlib, Bz2, Lz4, Lz4hc, Zstd
    pub set_bottommost_compression_type: Option<String>, // None, Snappy, Zlib, Bz2, Lz4, Lz4hc, Zstd
    pub set_zstd_max_train_bytes: Option<c_int>,
    pub set_compaction_readahead_size: Option<usize>,
    pub set_level_compaction_dynamic_level_bytes: Option<bool>,
    // pub set_periodic_compaction_seconds: Option<u64>,
    pub set_optimize_filters_for_hits: Option<bool>,
    pub set_delete_obsolete_files_period_micros: Option<u64>,
    pub set_max_open_files: Option<c_int>,
    pub set_max_file_opening_threads: Option<c_int>,
    pub set_use_fsync: Option<bool>,
    pub set_db_log_dir: Option<String>,
    pub set_log_level: Option<String>, // Debug, Info, Warn, Error, Fatal, Header
    pub set_bytes_per_sync: Option<u64>,
    pub set_wal_bytes_per_sync: Option<u64>,
    pub set_writable_file_max_buffer_size: Option<u64>,
    pub set_allow_concurrent_memtable_write: Option<bool>,
    pub set_enable_write_thread_adaptive_yield: Option<bool>,
    pub set_max_sequential_skip_in_iterations: Option<u64>,
    pub set_use_direct_reads: Option<bool>,
    pub set_use_direct_io_for_flush_and_compaction: Option<bool>,
    pub set_is_fd_close_on_exec: Option<bool>,
    pub set_table_cache_num_shard_bits: Option<c_int>,
    pub set_target_file_size_multiplier: Option<i32>,
    pub set_min_write_buffer_number: Option<c_int>,
    pub set_max_write_buffer_number: Option<c_int>,
    pub set_write_buffer_size: Option<usize>,
    pub set_db_write_buffer_size: Option<usize>,
    pub set_max_bytes_for_level_base: Option<u64>,
    pub set_max_bytes_for_level_multiplier: Option<f64>,
    pub set_max_manifest_file_size: Option<usize>,
    pub set_target_file_size_base: Option<u64>,
    pub set_min_write_buffer_number_to_merge: Option<c_int>,
    pub set_level_zero_file_num_compaction_trigger: Option<c_int>,
    pub set_level_zero_slowdown_writes_trigger: Option<c_int>,
    pub set_level_zero_stop_writes_trigger: Option<c_int>,
    pub set_compaction_style: Option<String>, // Level, Universal, Fifo
    pub set_unordered_write: Option<bool>,
    pub set_max_subcompactions: Option<u32>,
    pub set_max_background_jobs: Option<c_int>,
    pub set_disable_auto_compactions: Option<bool>,
    pub set_memtable_huge_page_size: Option<size_t>,
    pub set_max_successive_merges: Option<usize>,
    pub set_bloom_locality: Option<u32>,
    pub set_inplace_update_support: Option<bool>,
    pub set_inplace_update_locks: Option<usize>,
    pub set_max_bytes_for_level_multiplier_additional: Option<String>,
    pub set_skip_checking_sst_file_sizes_on_db_open: Option<bool>,
    pub set_max_write_buffer_size_to_maintain: Option<i64>,
    pub set_enable_pipelined_write: Option<bool>,
    pub set_min_level_to_compress: Option<c_int>,
    pub set_report_bg_io_stats: Option<bool>,
    pub set_max_total_wal_size: Option<u64>,
    pub set_wal_recovery_mode: Option<String>, // TolerateCorruptedTailRecords, AbsoluteConsistency, PointInTime, SkipAnyCorruptedRecord
    pub enable_statistics: Option<bool>,
    pub set_stats_dump_period_sec: Option<c_uint>,
    pub set_stats_persist_period_sec: Option<c_uint>,
    pub set_advise_random_on_open: Option<bool>,
    // pub set_access_hint_on_compaction_start: Option<String>, //None, Normal, Sequential, WillNeed
    pub set_use_adaptive_mutex: Option<bool>,
    pub set_num_levels: Option<c_int>,
    pub set_memtable_prefix_bloom_ratio: Option<f64>,
    pub set_max_compaction_bytes: Option<u64>,
    pub set_wal_dir: Option<String>,
    pub set_wal_ttl_seconds: Option<u64>,
    pub set_wal_size_limit_mb: Option<u64>,
    pub set_manifest_preallocation_size: Option<usize>,
    pub set_skip_stats_update_on_db_open: Option<bool>,
    pub set_keep_log_file_num: Option<usize>,
    pub set_allow_mmap_writes: Option<bool>,
    pub set_allow_mmap_reads: Option<bool>,
    pub set_manual_wal_flush: Option<bool>,
    pub set_atomic_flush: Option<bool>,
    pub set_ratelimiter: Option<String>,
    pub set_max_log_file_size: Option<usize>,
    pub set_log_file_time_to_roll: Option<usize>,
    pub set_recycle_log_file_num: Option<usize>,
    pub set_soft_pending_compaction_bytes_limit: Option<usize>,
    pub set_hard_pending_compaction_bytes_limit: Option<usize>,
    pub set_arena_block_size: Option<usize>,
    pub set_dump_malloc_stats: Option<bool>,
    pub set_memtable_whole_key_filtering: Option<bool>,
    pub set_enable_blob_files: Option<bool>,
    pub set_min_blob_size: Option<u64>,
    pub set_blob_file_size: Option<u64>,
    pub set_blob_compression_type: Option<String>, // None, Snappy, Zlib, Bz2, Lz4, Lz4hc, Zstd
    pub set_enable_blob_gc: Option<bool>,
    pub set_blob_gc_age_cutoff: Option<c_double>,
    pub set_blob_gc_force_threshold: Option<c_double>,
    pub set_blob_compaction_readahead_size: Option<u64>,
    // pub set_allow_ingest_behind: Option<bool>,
    // pub add_compact_on_deletion_collector_factory: Option<String>,
    pub set_prefix_extractor_prefix_length: Option<usize>,
}

impl Default for RockerOptions {
    fn default() -> RockerOptions {
        RockerOptions {
            create_if_missing: Some(true),
            create_missing_column_families: None,
            set_error_if_exists: None,
            set_paranoid_checks: None,
            increase_parallelism: None,
            optimize_level_style_compaction: None,
            optimize_universal_style_compaction: None,
            set_compression_type: None,
            // set_compression_options_parallel_threads: None,
            // set_wal_compression_type: None,
            set_bottommost_compression_type: None,
            set_zstd_max_train_bytes: None,
            set_compaction_readahead_size: None,
            set_level_compaction_dynamic_level_bytes: None,
            // set_periodic_compaction_seconds: None,
            set_optimize_filters_for_hits: None,
            set_delete_obsolete_files_period_micros: None,
            set_max_open_files: None,
            set_max_file_opening_threads: None,
            set_use_fsync: None,
            set_db_log_dir: None,
            set_log_level: None,
            set_bytes_per_sync: None,
            set_wal_bytes_per_sync: None,
            set_writable_file_max_buffer_size: None,
            set_allow_concurrent_memtable_write: None,
            set_enable_write_thread_adaptive_yield: None,
            set_max_sequential_skip_in_iterations: None,
            set_use_direct_reads: None,
            set_use_direct_io_for_flush_and_compaction: None,
            set_is_fd_close_on_exec: None,
            set_table_cache_num_shard_bits: None,
            set_target_file_size_multiplier: None,
            set_min_write_buffer_number: None,
            set_max_write_buffer_number: None,
            set_write_buffer_size: None,
            set_db_write_buffer_size: None,
            set_max_bytes_for_level_base: None,
            set_max_bytes_for_level_multiplier: None,
            set_max_manifest_file_size: None,
            set_target_file_size_base: None,
            set_min_write_buffer_number_to_merge: None,
            set_level_zero_file_num_compaction_trigger: None,
            set_level_zero_slowdown_writes_trigger: None,
            set_level_zero_stop_writes_trigger: None,
            set_compaction_style: None,
            set_unordered_write: None,
            set_max_subcompactions: None,
            set_max_background_jobs: None,
            set_disable_auto_compactions: None,
            set_memtable_huge_page_size: None,
            set_max_successive_merges: None,
            set_bloom_locality: None,
            set_inplace_update_support: None,
            set_inplace_update_locks: None,
            set_max_bytes_for_level_multiplier_additional: None,
            set_skip_checking_sst_file_sizes_on_db_open: None,
            set_max_write_buffer_size_to_maintain: None,
            set_enable_pipelined_write: None,
            set_min_level_to_compress: None,
            set_report_bg_io_stats: None,
            set_max_total_wal_size: None,
            set_wal_recovery_mode: None,
            enable_statistics: None,
            set_stats_dump_period_sec: None,
            set_stats_persist_period_sec: None,
            set_advise_random_on_open: None,
            // set_access_hint_on_compaction_start: None,
            set_use_adaptive_mutex: None,
            set_num_levels: None,
            set_memtable_prefix_bloom_ratio: None,
            set_max_compaction_bytes: None,
            set_wal_dir: None,
            set_wal_ttl_seconds: None,
            set_wal_size_limit_mb: None,
            set_manifest_preallocation_size: None,
            set_skip_stats_update_on_db_open: None,
            set_keep_log_file_num: None,
            set_allow_mmap_writes: None,
            set_allow_mmap_reads: None,
            set_manual_wal_flush: None,
            set_atomic_flush: None,
            set_ratelimiter: None,
            set_max_log_file_size: None,
            set_log_file_time_to_roll: None,
            set_recycle_log_file_num: None,
            set_soft_pending_compaction_bytes_limit: None,
            set_hard_pending_compaction_bytes_limit: None,
            set_arena_block_size: None,
            set_dump_malloc_stats: None,
            set_memtable_whole_key_filtering: None,
            set_enable_blob_files: None,
            set_min_blob_size: None,
            set_blob_file_size: None,
            set_blob_compression_type: None,
            set_enable_blob_gc: None,
            set_blob_gc_age_cutoff: None,
            set_blob_gc_force_threshold: None,
            set_blob_compaction_readahead_size: None,
            // set_allow_ingest_behind: None,
            // add_compact_on_deletion_collector_factory: None,
            set_prefix_extractor_prefix_length: None,
        }
    }
}

impl<'a> Decoder<'a> for RockerOptions {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        use rustler::{Error, MapIterator};
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "create_if_missing" => opts.create_if_missing = Some(value.decode()?),
                "create_missing_column_families" => {
                    opts.create_missing_column_families = Some(value.decode()?)
                }
                "set_error_if_exists" => opts.set_error_if_exists = Some(value.decode()?),
                "set_paranoid_checks" => opts.set_paranoid_checks = Some(value.decode()?),
                "increase_parallelism" => opts.increase_parallelism = Some(value.decode()?),
                "optimize_level_style_compaction" => {
                    opts.optimize_level_style_compaction = Some(value.decode()?)
                }
                "optimize_universal_style_compaction" => {
                    opts.optimize_universal_style_compaction = Some(value.decode()?)
                }
                "set_compression_type" => opts.set_compression_type = Some(value.decode()?),
                // "set_compression_options_parallel_threads" => {
                //     opts.set_compression_options_parallel_threads = Some(value.decode()?)
                // }
                // "set_wal_compression_type" => opts.set_wal_compression_type = Some(value.decode()?),
                "set_bottommost_compression_type" => {
                    opts.set_bottommost_compression_type = Some(value.decode()?)
                }
                "set_zstd_max_train_bytes" => opts.set_zstd_max_train_bytes = Some(value.decode()?),
                "set_compaction_readahead_size" => {
                    opts.set_compaction_readahead_size = Some(value.decode()?)
                }
                "set_level_compaction_dynamic_level_bytes" => {
                    opts.set_level_compaction_dynamic_level_bytes = Some(value.decode()?)
                }
                // "set_periodic_compaction_seconds" => {
                //     opts.set_periodic_compaction_seconds = Some(value.decode()?)
                // }
                "set_optimize_filters_for_hits" => {
                    opts.set_optimize_filters_for_hits = Some(value.decode()?)
                }
                "set_delete_obsolete_files_period_micros" => {
                    opts.set_delete_obsolete_files_period_micros = Some(value.decode()?)
                }
                "set_max_open_files" => opts.set_max_open_files = Some(value.decode()?),
                "set_max_file_opening_threads" => {
                    opts.set_max_file_opening_threads = Some(value.decode()?)
                }
                "set_use_fsync" => opts.set_use_fsync = Some(value.decode()?),
                "set_db_log_dir" => opts.set_db_log_dir = Some(value.decode()?),
                "set_log_level" => opts.set_log_level = Some(value.decode()?),
                "set_bytes_per_sync" => opts.set_bytes_per_sync = Some(value.decode()?),
                "set_wal_bytes_per_sync" => opts.set_wal_bytes_per_sync = Some(value.decode()?),
                "set_writable_file_max_buffer_size" => {
                    opts.set_writable_file_max_buffer_size = Some(value.decode()?)
                }
                "set_allow_concurrent_memtable_write" => {
                    opts.set_allow_concurrent_memtable_write = Some(value.decode()?)
                }
                "set_enable_write_thread_adaptive_yield" => {
                    opts.set_enable_write_thread_adaptive_yield = Some(value.decode()?)
                }
                "set_max_sequential_skip_in_iterations" => {
                    opts.set_max_sequential_skip_in_iterations = Some(value.decode()?)
                }
                "set_use_direct_reads" => opts.set_use_direct_reads = Some(value.decode()?),
                "set_use_direct_io_for_flush_and_compaction" => {
                    opts.set_use_direct_io_for_flush_and_compaction = Some(value.decode()?)
                }
                "set_is_fd_close_on_exec" => opts.set_is_fd_close_on_exec = Some(value.decode()?),
                "set_table_cache_num_shard_bits" => {
                    opts.set_table_cache_num_shard_bits = Some(value.decode()?)
                }
                "set_target_file_size_multiplier" => {
                    opts.set_target_file_size_multiplier = Some(value.decode()?)
                }
                "set_min_write_buffer_number" => {
                    opts.set_min_write_buffer_number = Some(value.decode()?)
                }
                "set_max_write_buffer_number" => {
                    opts.set_max_write_buffer_number = Some(value.decode()?)
                }
                "set_write_buffer_size" => opts.set_write_buffer_size = Some(value.decode()?),
                "set_db_write_buffer_size" => opts.set_db_write_buffer_size = Some(value.decode()?),
                "set_max_bytes_for_level_base" => {
                    opts.set_max_bytes_for_level_base = Some(value.decode()?)
                }
                "set_max_bytes_for_level_multiplier" => {
                    opts.set_max_bytes_for_level_multiplier = Some(value.decode()?)
                }
                "set_max_manifest_file_size" => {
                    opts.set_max_manifest_file_size = Some(value.decode()?)
                }
                "set_target_file_size_base" => {
                    opts.set_target_file_size_base = Some(value.decode()?)
                }
                "set_min_write_buffer_number_to_merge" => {
                    opts.set_min_write_buffer_number_to_merge = Some(value.decode()?)
                }
                "set_level_zero_file_num_compaction_trigger" => {
                    opts.set_level_zero_file_num_compaction_trigger = Some(value.decode()?)
                }
                "set_level_zero_slowdown_writes_trigger" => {
                    opts.set_level_zero_slowdown_writes_trigger = Some(value.decode()?)
                }
                "set_level_zero_stop_writes_trigger" => {
                    opts.set_level_zero_stop_writes_trigger = Some(value.decode()?)
                }
                "set_compaction_style" => opts.set_compaction_style = Some(value.decode()?),
                "set_unordered_write" => opts.set_unordered_write = Some(value.decode()?),
                "set_max_subcompactions" => opts.set_max_subcompactions = Some(value.decode()?),
                "set_max_background_jobs" => opts.set_max_background_jobs = Some(value.decode()?),
                "set_disable_auto_compactions" => {
                    opts.set_disable_auto_compactions = Some(value.decode()?)
                }
                "set_memtable_huge_page_size" => {
                    opts.set_memtable_huge_page_size = Some(value.decode()?)
                }
                "set_max_successive_merges" => {
                    opts.set_max_successive_merges = Some(value.decode()?)
                }
                "set_bloom_locality" => opts.set_bloom_locality = Some(value.decode()?),
                "set_inplace_update_support" => {
                    opts.set_inplace_update_support = Some(value.decode()?)
                }
                "set_inplace_update_locks" => opts.set_inplace_update_locks = Some(value.decode()?),
                "set_max_bytes_for_level_multiplier_additional" => {
                    opts.set_max_bytes_for_level_multiplier_additional = Some(value.decode()?)
                }
                "set_skip_checking_sst_file_sizes_on_db_open" => {
                    opts.set_skip_checking_sst_file_sizes_on_db_open = Some(value.decode()?)
                }
                "set_max_write_buffer_size_to_maintain" => {
                    opts.set_max_write_buffer_size_to_maintain = Some(value.decode()?)
                }
                "set_enable_pipelined_write" => {
                    opts.set_enable_pipelined_write = Some(value.decode()?)
                }
                "set_min_level_to_compress" => {
                    opts.set_min_level_to_compress = Some(value.decode()?)
                }
                "set_report_bg_io_stats" => opts.set_report_bg_io_stats = Some(value.decode()?),
                "set_max_total_wal_size" => opts.set_max_total_wal_size = Some(value.decode()?),
                "set_wal_recovery_mode" => opts.set_wal_recovery_mode = Some(value.decode()?),
                "enable_statistics" => opts.enable_statistics = Some(value.decode()?),
                "set_stats_dump_period_sec" => {
                    opts.set_stats_dump_period_sec = Some(value.decode()?)
                }
                "set_stats_persist_period_sec" => {
                    opts.set_stats_persist_period_sec = Some(value.decode()?)
                }
                "set_advise_random_on_open" => {
                    opts.set_advise_random_on_open = Some(value.decode()?)
                }
                // "set_access_hint_on_compaction_start" => {
                //     opts.set_access_hint_on_compaction_start = Some(value.decode()?)
                // }
                "set_use_adaptive_mutex" => opts.set_use_adaptive_mutex = Some(value.decode()?),
                "set_num_levels" => opts.set_num_levels = Some(value.decode()?),
                "set_memtable_prefix_bloom_ratio" => {
                    opts.set_memtable_prefix_bloom_ratio = Some(value.decode()?)
                }
                "set_max_compaction_bytes" => opts.set_max_compaction_bytes = Some(value.decode()?),
                "set_wal_dir" => opts.set_wal_dir = Some(value.decode()?),
                "set_wal_ttl_seconds" => opts.set_wal_ttl_seconds = Some(value.decode()?),
                "set_wal_size_limit_mb" => opts.set_wal_size_limit_mb = Some(value.decode()?),
                "set_manifest_preallocation_size" => {
                    opts.set_manifest_preallocation_size = Some(value.decode()?)
                }
                "set_skip_stats_update_on_db_open" => {
                    opts.set_skip_stats_update_on_db_open = Some(value.decode()?)
                }
                "set_keep_log_file_num" => opts.set_keep_log_file_num = Some(value.decode()?),
                "set_allow_mmap_writes" => opts.set_allow_mmap_writes = Some(value.decode()?),
                "set_allow_mmap_reads" => opts.set_allow_mmap_reads = Some(value.decode()?),
                "set_manual_wal_flush" => opts.set_manual_wal_flush = Some(value.decode()?),
                "set_atomic_flush" => opts.set_atomic_flush = Some(value.decode()?),
                "set_ratelimiter" => opts.set_ratelimiter = Some(value.decode()?),
                "set_max_log_file_size" => opts.set_max_log_file_size = Some(value.decode()?),
                "set_log_file_time_to_roll" => {
                    opts.set_log_file_time_to_roll = Some(value.decode()?)
                }
                "set_recycle_log_file_num" => opts.set_recycle_log_file_num = Some(value.decode()?),
                "set_soft_pending_compaction_bytes_limit" => {
                    opts.set_soft_pending_compaction_bytes_limit = Some(value.decode()?)
                }
                "set_hard_pending_compaction_bytes_limit" => {
                    opts.set_hard_pending_compaction_bytes_limit = Some(value.decode()?)
                }
                "set_arena_block_size" => opts.set_arena_block_size = Some(value.decode()?),
                "set_dump_malloc_stats" => opts.set_dump_malloc_stats = Some(value.decode()?),
                "set_memtable_whole_key_filtering" => {
                    opts.set_memtable_whole_key_filtering = Some(value.decode()?)
                }
                "set_enable_blob_files" => opts.set_enable_blob_files = Some(value.decode()?),
                "set_min_blob_size" => opts.set_min_blob_size = Some(value.decode()?),
                "set_blob_file_size" => opts.set_blob_file_size = Some(value.decode()?),
                "set_blob_compression_type" => {
                    opts.set_blob_compression_type = Some(value.decode()?)
                }
                "set_enable_blob_gc" => opts.set_enable_blob_gc = Some(value.decode()?),
                "set_blob_gc_age_cutoff" => opts.set_blob_gc_age_cutoff = Some(value.decode()?),
                "set_blob_gc_force_threshold" => {
                    opts.set_blob_gc_force_threshold = Some(value.decode()?)
                }
                "set_blob_compaction_readahead_size" => {
                    opts.set_blob_compaction_readahead_size = Some(value.decode()?)
                }
                // "set_allow_ingest_behind" => opts.set_allow_ingest_behind = Some(value.decode()?),
                // "add_compact_on_deletion_collector_factory" => {
                //     opts.add_compact_on_deletion_collector_factory = Some(value.decode()?)
                // }
                "set_prefix_extractor_prefix_length" => {
                    opts.set_prefix_extractor_prefix_length = Some(value.decode()?)
                }
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl From<RockerOptions> for Options {
    fn from(opts: RockerOptions) -> Self {
        let mut db_opts = Options::default();
        if !opts.create_if_missing.is_none() {
            db_opts.create_if_missing(opts.create_if_missing.unwrap());
        }
        if !opts.create_missing_column_families.is_none() {
            db_opts.create_missing_column_families(opts.create_missing_column_families.unwrap());
        }
        if !opts.set_error_if_exists.is_none() {
            db_opts.set_error_if_exists(opts.set_error_if_exists.unwrap());
        }
        if !opts.set_paranoid_checks.is_none() {
            db_opts.set_paranoid_checks(opts.set_paranoid_checks.unwrap());
        }
        if !opts.increase_parallelism.is_none() {
            db_opts.increase_parallelism(opts.increase_parallelism.unwrap());
        }
        if !opts.optimize_level_style_compaction.is_none() {
            db_opts.optimize_level_style_compaction(opts.optimize_level_style_compaction.unwrap());
        }
        if !opts.optimize_universal_style_compaction.is_none() {
            db_opts.optimize_universal_style_compaction(
                opts.optimize_universal_style_compaction.unwrap(),
            );
        }
        if !opts.set_compression_type.is_none() {
            let cs = match opts
                .set_compression_type
                .unwrap()
                .trim()
                .to_lowercase()
                .as_ref()
            {
                "none" => DBCompressionType::None,
                "snappy" => DBCompressionType::Snappy,
                "zlib" => DBCompressionType::Zlib,
                "bz2" => DBCompressionType::Bz2,
                "lz4" => DBCompressionType::Lz4,
                "lz4hc" => DBCompressionType::Lz4hc,
                "zstd" => DBCompressionType::Zstd,
                _ => DBCompressionType::None,
            };
            db_opts.set_compression_type(cs);
        }
        // THE FEATURE IS STILL EXPERIMENTAL
        // if !opts.set_compression_options_parallel_threads.is_none() {
        //     db_opts.set_compression_options_parallel_threads(
        //         opts.set_compression_options_parallel_threads.unwrap(),
        //     );
        // }
        // THE FEATURE IS STILL EXPERIMENTAL
        // if !opts.set_wal_compression_type.is_none() {
        //     let cs = match opts
        //         .set_wal_compression_type
        //         .unwrap()
        //         .trim()
        //         .to_lowercase()
        //         .as_ref()
        //     {
        //         "none" => DBCompressionType::None,
        //         "snappy" => DBCompressionType::Snappy,
        //         "zlib" => DBCompressionType::Zlib,
        //         "bz2" => DBCompressionType::Bz2,
        //         "lz4" => DBCompressionType::Lz4,
        //         "lz4hc" => DBCompressionType::Lz4hc,
        //         "zstd" => DBCompressionType::Zstd,
        //         _ => DBCompressionType::None,
        //     };
        //     db_opts.set_wal_compression_type(cs);
        // }

        if !opts.set_bottommost_compression_type.is_none() {
            let cs = match opts
                .set_bottommost_compression_type
                .unwrap()
                .trim()
                .to_lowercase()
                .as_ref()
            {
                "none" => DBCompressionType::None,
                "snappy" => DBCompressionType::Snappy,
                "zlib" => DBCompressionType::Zlib,
                "bz2" => DBCompressionType::Bz2,
                "lz4" => DBCompressionType::Lz4,
                "lz4hc" => DBCompressionType::Lz4hc,
                "zstd" => DBCompressionType::Zstd,
                _ => DBCompressionType::None,
            };
            db_opts.set_bottommost_compression_type(cs);
        }
        if !opts.set_zstd_max_train_bytes.is_none() {
            db_opts.set_zstd_max_train_bytes(opts.set_zstd_max_train_bytes.unwrap());
        }
        if !opts.set_compaction_readahead_size.is_none() {
            db_opts.set_compaction_readahead_size(opts.set_compaction_readahead_size.unwrap());
        }
        if !opts.set_level_compaction_dynamic_level_bytes.is_none() {
            db_opts.set_level_compaction_dynamic_level_bytes(
                opts.set_level_compaction_dynamic_level_bytes.unwrap(),
            );
        }
        if !opts.set_optimize_filters_for_hits.is_none() {
            db_opts.set_optimize_filters_for_hits(opts.set_optimize_filters_for_hits.unwrap());
        }
        if !opts.set_delete_obsolete_files_period_micros.is_none() {
            db_opts.set_delete_obsolete_files_period_micros(
                opts.set_delete_obsolete_files_period_micros.unwrap(),
            );
        }
        if !opts.set_max_open_files.is_none() {
            db_opts.set_max_open_files(opts.set_max_open_files.unwrap());
        }
        if !opts.set_max_file_opening_threads.is_none() {
            db_opts.set_max_file_opening_threads(opts.set_max_file_opening_threads.unwrap());
        }
        if !opts.set_use_fsync.is_none() {
            db_opts.set_use_fsync(opts.set_use_fsync.unwrap());
        }
        if !opts.set_db_log_dir.is_none() {
            db_opts.set_db_log_dir(opts.set_db_log_dir.unwrap());
        }
        if !opts.set_log_level.is_none() {
            let ll = match opts.set_log_level.unwrap().trim().to_lowercase().as_ref() {
                "debug" => LogLevel::Debug,
                "info" => LogLevel::Info,
                "warn" => LogLevel::Warn,
                "error" => LogLevel::Error,
                "fatal" => LogLevel::Fatal,
                "header" => LogLevel::Header,
                _ => LogLevel::Error,
            };
            db_opts.set_log_level(ll);
        }
        if !opts.set_bytes_per_sync.is_none() {
            db_opts.set_bytes_per_sync(opts.set_bytes_per_sync.unwrap());
        }
        if !opts.set_wal_bytes_per_sync.is_none() {
            db_opts.set_wal_bytes_per_sync(opts.set_wal_bytes_per_sync.unwrap());
        }
        if !opts.set_writable_file_max_buffer_size.is_none() {
            db_opts
                .set_writable_file_max_buffer_size(opts.set_writable_file_max_buffer_size.unwrap());
        }
        if !opts.set_allow_concurrent_memtable_write.is_none() {
            db_opts.set_allow_concurrent_memtable_write(
                opts.set_allow_concurrent_memtable_write.unwrap(),
            );
        }
        if !opts.set_enable_write_thread_adaptive_yield.is_none() {
            db_opts.set_enable_write_thread_adaptive_yield(
                opts.set_enable_write_thread_adaptive_yield.unwrap(),
            );
        }
        if !opts.set_max_sequential_skip_in_iterations.is_none() {
            db_opts.set_max_sequential_skip_in_iterations(
                opts.set_max_sequential_skip_in_iterations.unwrap(),
            );
        }
        if !opts.set_use_direct_reads.is_none() {
            db_opts.set_use_direct_reads(opts.set_use_direct_reads.unwrap());
        }
        if !opts.set_use_direct_io_for_flush_and_compaction.is_none() {
            db_opts.set_use_direct_io_for_flush_and_compaction(
                opts.set_use_direct_io_for_flush_and_compaction.unwrap(),
            );
        }
        if !opts.set_is_fd_close_on_exec.is_none() {
            db_opts.set_is_fd_close_on_exec(opts.set_is_fd_close_on_exec.unwrap());
        }
        if !opts.set_table_cache_num_shard_bits.is_none() {
            db_opts.set_table_cache_num_shard_bits(opts.set_table_cache_num_shard_bits.unwrap());
        }
        if !opts.set_target_file_size_multiplier.is_none() {
            db_opts.set_target_file_size_multiplier(opts.set_target_file_size_multiplier.unwrap());
        }
        if !opts.set_min_write_buffer_number.is_none() {
            db_opts.set_min_write_buffer_number(opts.set_min_write_buffer_number.unwrap());
        }
        if !opts.set_max_write_buffer_number.is_none() {
            db_opts.set_max_write_buffer_number(opts.set_max_write_buffer_number.unwrap());
        }
        if !opts.set_write_buffer_size.is_none() {
            db_opts.set_write_buffer_size(opts.set_write_buffer_size.unwrap());
        }
        if !opts.set_db_write_buffer_size.is_none() {
            db_opts.set_db_write_buffer_size(opts.set_db_write_buffer_size.unwrap());
        }
        if !opts.set_max_bytes_for_level_base.is_none() {
            db_opts.set_max_bytes_for_level_base(opts.set_max_bytes_for_level_base.unwrap());
        }
        if !opts.set_max_bytes_for_level_multiplier.is_none() {
            db_opts.set_max_bytes_for_level_multiplier(
                opts.set_max_bytes_for_level_multiplier.unwrap(),
            );
        }
        if !opts.set_max_manifest_file_size.is_none() {
            db_opts.set_max_manifest_file_size(opts.set_max_manifest_file_size.unwrap());
        }
        if !opts.set_target_file_size_base.is_none() {
            db_opts.set_target_file_size_base(opts.set_target_file_size_base.unwrap());
        }
        if !opts.set_min_write_buffer_number_to_merge.is_none() {
            db_opts.set_min_write_buffer_number_to_merge(
                opts.set_min_write_buffer_number_to_merge.unwrap(),
            );
        }
        if !opts.set_level_zero_file_num_compaction_trigger.is_none() {
            db_opts.set_level_zero_file_num_compaction_trigger(
                opts.set_level_zero_file_num_compaction_trigger.unwrap(),
            );
        }
        if !opts.set_level_zero_slowdown_writes_trigger.is_none() {
            db_opts.set_level_zero_slowdown_writes_trigger(
                opts.set_level_zero_slowdown_writes_trigger.unwrap(),
            );
        }
        if !opts.set_level_zero_stop_writes_trigger.is_none() {
            db_opts.set_level_zero_stop_writes_trigger(
                opts.set_level_zero_stop_writes_trigger.unwrap(),
            );
        }
        if !opts.set_compaction_style.is_none() {
            let cs = match opts
                .set_compaction_style
                .unwrap()
                .trim()
                .to_lowercase()
                .as_ref()
            {
                "level" => DBCompactionStyle::Level,
                "universal" => DBCompactionStyle::Universal,
                "fifo" => DBCompactionStyle::Fifo,
                _ => DBCompactionStyle::Level,
            };
            db_opts.set_compaction_style(cs);
        }
        if !opts.set_unordered_write.is_none() {
            db_opts.set_unordered_write(opts.set_unordered_write.unwrap());
        }
        if !opts.set_max_subcompactions.is_none() {
            db_opts.set_max_subcompactions(opts.set_max_subcompactions.unwrap());
        }
        if !opts.set_max_background_jobs.is_none() {
            db_opts.set_max_background_jobs(opts.set_max_background_jobs.unwrap());
        }
        if !opts.set_disable_auto_compactions.is_none() {
            db_opts.set_disable_auto_compactions(opts.set_disable_auto_compactions.unwrap());
        }
        if !opts.set_memtable_huge_page_size.is_none() {
            db_opts.set_memtable_huge_page_size(opts.set_memtable_huge_page_size.unwrap());
        }
        if !opts.set_max_successive_merges.is_none() {
            db_opts.set_max_successive_merges(opts.set_max_successive_merges.unwrap());
        }
        if !opts.set_bloom_locality.is_none() {
            db_opts.set_bloom_locality(opts.set_bloom_locality.unwrap());
        }
        if !opts.set_inplace_update_support.is_none() {
            db_opts.set_inplace_update_support(opts.set_inplace_update_support.unwrap());
        }
        if !opts.set_inplace_update_locks.is_none() {
            db_opts.set_inplace_update_locks(opts.set_inplace_update_locks.unwrap());
        }
        if !opts.set_max_bytes_for_level_multiplier_additional.is_none() {
            let mut v: Vec<i32> = Vec::new();
            for part in opts
                .set_max_bytes_for_level_multiplier_additional
                .unwrap()
                .split(",")
            {
                v.push(part.trim().parse::<i32>().unwrap())
            }
            db_opts.set_max_bytes_for_level_multiplier_additional(v.as_slice());
        }
        if !opts.set_skip_checking_sst_file_sizes_on_db_open.is_none() {
            db_opts.set_skip_checking_sst_file_sizes_on_db_open(
                opts.set_skip_checking_sst_file_sizes_on_db_open.unwrap(),
            );
        }
        if !opts.set_max_write_buffer_size_to_maintain.is_none() {
            db_opts.set_max_write_buffer_size_to_maintain(
                opts.set_max_write_buffer_size_to_maintain.unwrap(),
            );
        }
        if !opts.set_enable_pipelined_write.is_none() {
            db_opts.set_enable_pipelined_write(opts.set_enable_pipelined_write.unwrap());
        }
        if !opts.set_min_level_to_compress.is_none() {
            db_opts.set_min_level_to_compress(opts.set_min_level_to_compress.unwrap());
        }
        if !opts.set_report_bg_io_stats.is_none() {
            db_opts.set_report_bg_io_stats(opts.set_report_bg_io_stats.unwrap());
        }
        if !opts.set_max_total_wal_size.is_none() {
            db_opts.set_max_total_wal_size(opts.set_max_total_wal_size.unwrap());
        }

        if !opts.set_wal_recovery_mode.is_none() {
            let wrm = match opts
                .set_wal_recovery_mode
                .unwrap()
                .trim()
                .to_lowercase()
                .as_ref()
            {
                "toleratecorruptedtailrecords" => DBRecoveryMode::TolerateCorruptedTailRecords,
                "absoluteconsistency" => DBRecoveryMode::AbsoluteConsistency,
                "pointintime" => DBRecoveryMode::PointInTime,
                "skipanycorruptedrecord" => DBRecoveryMode::SkipAnyCorruptedRecord,
                _ => DBRecoveryMode::PointInTime,
            };
            db_opts.set_wal_recovery_mode(wrm);
        }
        if !opts.enable_statistics.is_none() && opts.enable_statistics.unwrap() {
            db_opts.enable_statistics();
        }
        if !opts.set_stats_dump_period_sec.is_none() {
            db_opts.set_stats_dump_period_sec(opts.set_stats_dump_period_sec.unwrap());
        }
        if !opts.set_stats_persist_period_sec.is_none() {
            db_opts.set_stats_persist_period_sec(opts.set_stats_persist_period_sec.unwrap());
        }
        if !opts.set_advise_random_on_open.is_none() {
            db_opts.set_advise_random_on_open(opts.set_advise_random_on_open.unwrap());
        }
        // if !opts.set_access_hint_on_compaction_start.is_none() {
        //     let ah = match opts
        //         .set_access_hint_on_compaction_start
        //         .unwrap()
        //         .trim()
        //         .to_lowercase()
        //         .as_ref()
        //     {
        //         "none" => AccessHint::None,
        //         "normal" => AccessHint::Normal,
        //         "sequential" => AccessHint::Sequential,
        //         "willneed" => AccessHint::WillNeed,
        //         _ => AccessHint::Normal,
        //     };
        //     db_opts.set_access_hint_on_compaction_start(ah);
        // }
        if !opts.set_use_adaptive_mutex.is_none() {
            db_opts.set_use_adaptive_mutex(opts.set_use_adaptive_mutex.unwrap());
        }
        if !opts.set_num_levels.is_none() {
            db_opts.set_num_levels(opts.set_num_levels.unwrap());
        }
        if !opts.set_memtable_prefix_bloom_ratio.is_none() {
            db_opts.set_memtable_prefix_bloom_ratio(opts.set_memtable_prefix_bloom_ratio.unwrap());
        }
        if !opts.set_max_compaction_bytes.is_none() {
            db_opts.set_max_compaction_bytes(opts.set_max_compaction_bytes.unwrap());
        }
        if !opts.set_wal_dir.is_none() {
            db_opts.set_wal_dir(opts.set_wal_dir.unwrap());
        }
        if !opts.set_wal_ttl_seconds.is_none() {
            db_opts.set_wal_ttl_seconds(opts.set_wal_ttl_seconds.unwrap());
        }
        if !opts.set_wal_size_limit_mb.is_none() {
            db_opts.set_wal_size_limit_mb(opts.set_wal_size_limit_mb.unwrap());
        }
        if !opts.set_manifest_preallocation_size.is_none() {
            db_opts.set_manifest_preallocation_size(opts.set_manifest_preallocation_size.unwrap());
        }
        if !opts.set_skip_stats_update_on_db_open.is_none() {
            db_opts
                .set_skip_stats_update_on_db_open(opts.set_skip_stats_update_on_db_open.unwrap());
        }
        if !opts.set_keep_log_file_num.is_none() {
            db_opts.set_keep_log_file_num(opts.set_keep_log_file_num.unwrap());
        }
        if !opts.set_allow_mmap_writes.is_none() {
            db_opts.set_allow_mmap_writes(opts.set_allow_mmap_writes.unwrap());
        }
        if !opts.set_allow_mmap_reads.is_none() {
            db_opts.set_allow_mmap_reads(opts.set_allow_mmap_reads.unwrap());
        }
        if !opts.set_manual_wal_flush.is_none() {
            db_opts.set_manual_wal_flush(opts.set_manual_wal_flush.unwrap());
        }
        if !opts.set_atomic_flush.is_none() {
            db_opts.set_atomic_flush(opts.set_atomic_flush.unwrap());
        }
        if !opts.set_ratelimiter.is_none() {
            let mut v: Vec<i64> = Vec::new();
            for part in opts.set_ratelimiter.unwrap().split(",") {
                v.push(part.trim().parse::<i64>().unwrap())
            }
            db_opts.set_ratelimiter(v[0], v[1], v[2] as i32);
        }
        if !opts.set_max_log_file_size.is_none() {
            db_opts.set_max_log_file_size(opts.set_max_log_file_size.unwrap());
        }
        if !opts.set_log_file_time_to_roll.is_none() {
            db_opts.set_log_file_time_to_roll(opts.set_log_file_time_to_roll.unwrap());
        }
        if !opts.set_recycle_log_file_num.is_none() {
            db_opts.set_recycle_log_file_num(opts.set_recycle_log_file_num.unwrap());
        }
        if !opts.set_soft_pending_compaction_bytes_limit.is_none() {
            db_opts.set_soft_pending_compaction_bytes_limit(
                opts.set_soft_pending_compaction_bytes_limit.unwrap(),
            );
        }
        if !opts.set_hard_pending_compaction_bytes_limit.is_none() {
            db_opts.set_hard_pending_compaction_bytes_limit(
                opts.set_hard_pending_compaction_bytes_limit.unwrap(),
            );
        }
        if !opts.set_arena_block_size.is_none() {
            db_opts.set_arena_block_size(opts.set_arena_block_size.unwrap());
        }
        if !opts.set_dump_malloc_stats.is_none() {
            db_opts.set_dump_malloc_stats(opts.set_dump_malloc_stats.unwrap());
        }
        if !opts.set_memtable_whole_key_filtering.is_none() {
            db_opts
                .set_memtable_whole_key_filtering(opts.set_memtable_whole_key_filtering.unwrap());
        }
        if !opts.set_enable_blob_files.is_none() {
            db_opts.set_enable_blob_files(opts.set_enable_blob_files.unwrap());
        }
        if !opts.set_min_blob_size.is_none() {
            db_opts.set_min_blob_size(opts.set_min_blob_size.unwrap());
        }
        if !opts.set_blob_file_size.is_none() {
            db_opts.set_blob_file_size(opts.set_blob_file_size.unwrap());
        }
        if !opts.set_blob_compression_type.is_none() {
            let bct = match opts
                .set_blob_compression_type
                .unwrap()
                .trim()
                .to_lowercase()
                .as_ref()
            {
                "none" => DBCompressionType::None,
                "snappy" => DBCompressionType::Snappy,
                "zlib" => DBCompressionType::Zlib,
                "bz2" => DBCompressionType::Bz2,
                "lz4" => DBCompressionType::Lz4,
                "lz4hc" => DBCompressionType::Lz4hc,
                "zstd" => DBCompressionType::Zstd,
                _ => DBCompressionType::None,
            };
            db_opts.set_blob_compression_type(bct);
        }
        if !opts.set_enable_blob_gc.is_none() {
            db_opts.set_enable_blob_gc(opts.set_enable_blob_gc.unwrap());
        }
        if !opts.set_blob_gc_age_cutoff.is_none() {
            db_opts.set_blob_gc_age_cutoff(opts.set_blob_gc_age_cutoff.unwrap());
        }
        if !opts.set_blob_gc_force_threshold.is_none() {
            db_opts.set_blob_gc_force_threshold(opts.set_blob_gc_force_threshold.unwrap());
        }
        if !opts.set_blob_compaction_readahead_size.is_none() {
            db_opts.set_blob_compaction_readahead_size(
                opts.set_blob_compaction_readahead_size.unwrap(),
            );
        }
        if !opts.set_prefix_extractor_prefix_length.is_none() {
            let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(
                opts.set_prefix_extractor_prefix_length.unwrap(),
            );
            db_opts.set_prefix_extractor(prefix_extractor);
        }

        db_opts
    }
}
