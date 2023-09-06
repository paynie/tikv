// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use api_version::{match_template_api_version, KvFormat, RawValue};
use engine_traits::{raw_ttl::ttl_to_expire_ts, CfName};
use kvproto::kvrpcpb::ApiVersion;
use raw::RawStore;
use tikv_kv::Statistics;
use txn_types::{Key, Value};

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    raw,
    txn::{
        commands::{
            Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

// TODO: consider add `KvFormat` generic parameter.
command! {
    /// RawCompareAndSwap checks whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value. The bool indicates whether they are equal.
    /// The previous value is always returned regardless of whether the new value is set.
    RawCompareAndSwap:
        cmd_ty => (Option<Value>, bool),
        display => "kv::command::raw_compare_and_swap {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            ttl: u64,
            api_version: ApiVersion,
            enable_write_with_version: bool,
        }
}

impl CommandExt for RawCompareAndSwap {
    ctx!();
    tag!(raw_compare_and_swap);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

fn decode_u64(value: &Vec<u8>) -> u64 {
    let mut ret:u64 = 0;
    ret += value[0] as u64;
    ret += (value[1] as u64) << 8;
    ret += (value[2] as u64) << 16;
    ret += (value[3] as u64) << 24;
    ret += (value[4] as u64) << 32;
    ret += (value[5] as u64) << 40;
    ret += (value[6] as u64) << 48;
    ret += (value[7] as u64) << 56;
    return ret;
}

fn encode_u64(value: &u64) -> Vec<u8> {
    let mut ret: Vec<u8> = Vec::with_capacity(8);
    ret.push((value & 0xff) as u8);
    ret.push(((value >> 8) & 0xff) as u8);
    ret.push(((value >> 16) & 0xff) as u8);
    ret.push(((value >> 24) & 0xff) as u8);
    ret.push(((value >> 32) & 0xff) as u8);
    ret.push(((value >> 40) & 0xff) as u8);
    ret.push(((value >> 48) & 0xff) as u8);
    ret.push(((value >> 56) & 0xff) as u8);
    ret
}


impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawCompareAndSwap {
    fn process_write(self, snapshot: S, wctx: WriteContext<'_, L>) -> Result<WriteResult> {

        let (cf, mut key, value,  ctx, raw_ext) = (
            self.cf,
            self.key,
            self.value,
            self.ctx,
            wctx.raw_ext,
        );

        let mut data = vec![];
        let store = RawStore::new(snapshot, self.api_version);

        let old_value = store.raw_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        if self.enable_write_with_version {
            // Generate version key
            let mut version_key = key.get_version_key();
            let previous_version = self.previous_value;

            // Get old version
            let old_version = store.raw_get_key_value(
                cf,
                &version_key,
                &mut Statistics::default(),
            )?;

            let (pr, lock_guards) = match old_version {
                Some(ref v) => {
                    // Old version exist
                    // Decode the version to int64
                    let old_version_u64 = decode_u64(v);

                    info!("Old version is not null"; "old_version_u64" => old_version_u64);

                    match previous_version{
                        Some(ref pv) => {
                            // Previous version set
                            // Decode previous version to u64
                            let previous_version_u64 = decode_u64(pv);

                            info!("Use set version is not null"; "previous_version_u64" => previous_version_u64);

                            if old_version_u64 == previous_version_u64 {
                                // CAS success, update value and version
                                // Generate new value
                                let raw_value = RawValue {
                                    user_value: value,
                                    expire_ts: ttl_to_expire_ts(self.ttl),
                                    is_delete: false,
                                };

                                let encoded_raw_value = match_template_api_version!(
                                    API,
                                    match self.api_version {
                                        ApiVersion::API => API::encode_raw_value_owned(raw_value),
                                    }
                                );

                                if let Some(ref raw_ext) = raw_ext {
                                    key = key.append_ts(raw_ext.ts);
                                }

                                let m = Modify::Put(cf, key, encoded_raw_value);
                                data.push(m);

                                // Generate new version
                                let new_version_64 = previous_version_u64 + 1;
                                let raw_version_value = RawValue {
                                    user_value: encode_u64(&new_version_64),
                                    expire_ts: ttl_to_expire_ts(self.ttl),
                                    is_delete: false,
                                };

                                let encoded_raw_version_value = match_template_api_version!(
                                    API,
                                    match self.api_version {
                                        ApiVersion::API => API::encode_raw_value_owned(raw_version_value),
                                    }
                                );

                                if let Some(ref raw_ext) = raw_ext {
                                    version_key = version_key.append_ts(raw_ext.ts);
                                }

                                let m_version = Modify::Put(cf, version_key, encoded_raw_version_value);
                                data.push(m_version);
                                (
                                    // Return success and new version value
                                    ProcessResult::RawCompareAndSwapRes {
                                        previous_value: Some(encode_u64(&new_version_64)),
                                        succeed: true,
                                    },

                                    raw_ext.into_iter().map(|r| r.key_guard).collect(),
                                )
                            } else {
                                (
                                    // CAS failed, just return false and current version
                                    ProcessResult::RawCompareAndSwapRes {
                                        previous_value: old_version,
                                        succeed: false,
                                    },
                                    vec![],
                                )
                            }
                        }

                        None => {
                            info!("Use set version is null");
                            // User not set previous version, just return false and current version
                            (
                                ProcessResult::RawCompareAndSwapRes {
                                    previous_value: old_version,
                                    succeed: false,
                                },
                                vec![],
                            )
                        }
                    }
                }

                None => {
                    // Can not find old version
                    info!("Old version is null");

                    match previous_version {
                        Some(ref _pv) => {
                            // User set previous version, just return false
                            info!("Use set version is not null");
                            (
                                ProcessResult::RawCompareAndSwapRes {
                                    previous_value: old_version,
                                    succeed: false,
                                }, 
                                vec![],
                            )
                        }

                        None => {
                            info!("Use set version is null");
                            // Version key does not exist
                            // Has not previous version
                            let new_version_u64 = 0;
                            // CAS success, update value and version
                            // Generate new value
                            let raw_value = RawValue {
                                user_value: value,
                                expire_ts: ttl_to_expire_ts(self.ttl),
                                is_delete: false,
                            };

                            let encoded_raw_value = match_template_api_version!(
                                API,
                                match self.api_version {
                                    ApiVersion::API => API::encode_raw_value_owned(raw_value),
                                }
                            );

                            if let Some(ref raw_ext) = raw_ext {
                                key = key.append_ts(raw_ext.ts);
                            }

                            let m = Modify::Put(cf, key, encoded_raw_value);
                            data.push(m);

                            // Generate new version
                            let raw_version_value = RawValue {
                                user_value: encode_u64(&new_version_u64),
                                expire_ts: ttl_to_expire_ts(self.ttl),
                                is_delete: false,
                            };

                            let encoded_raw_version_value = match_template_api_version!(
                                API,
                                match self.api_version {
                                    ApiVersion::API => API::encode_raw_value_owned(raw_version_value),
                                }
                            );

                            if let Some(ref raw_ext) = raw_ext {
                                version_key = version_key.append_ts(raw_ext.ts);
                            }
                            let m_version = Modify::Put(cf, version_key, encoded_raw_version_value);
                            data.push(m_version);

                            // Return current version
                            (
                                ProcessResult::RawCompareAndSwapRes {
                                    previous_value: Some(encode_u64(&new_version_u64)),
                                    succeed: true,
                                },
                                raw_ext.into_iter().map(|r| r.key_guard).collect(),
                            )
                        }
                    }
                }
            };

            fail_point!("txn_commands_compare_and_swap");
            let rows = data.len();
            let mut to_be_write = WriteData::from_modifies(data);
            to_be_write.set_allowed_on_disk_almost_full();
            Ok(WriteResult {
                ctx,
                to_be_write,
                rows,
                pr,
                lock_info: vec![],
                released_locks: ReleasedLocks::new(),
                new_acquired_locks: vec![],
                lock_guards,
                response_policy: ResponsePolicy::OnApplied,
            })
        } else {
            let previous_value = self.previous_value;
            let (pr, lock_guards) = if old_value == previous_value {
                let raw_value = RawValue {
                    user_value: value,
                    expire_ts: ttl_to_expire_ts(self.ttl),
                    is_delete: false,
                };
                let encoded_raw_value = match_template_api_version!(
                    API,
                    match self.api_version {
                        ApiVersion::API => API::encode_raw_value_owned(raw_value),
                    }
                );
    
                if let Some(ref raw_ext) = raw_ext {
                    key = key.append_ts(raw_ext.ts);
                }
    
                let m = Modify::Put(cf, key, encoded_raw_value);
                data.push(m);
                (
                    ProcessResult::RawCompareAndSwapRes {
                        previous_value: old_value,
                        succeed: true,
                    },
                    raw_ext.into_iter().map(|r| r.key_guard).collect(),
                )
            } else {
                (
                    ProcessResult::RawCompareAndSwapRes {
                        previous_value: old_value,
                        succeed: false,
                    },
                    vec![],
                )
            };

            fail_point!("txn_commands_compare_and_swap");
            let rows = data.len();
            let mut to_be_write = WriteData::from_modifies(data);
            to_be_write.set_allowed_on_disk_almost_full();
            Ok(WriteResult {
                ctx,
                to_be_write,
                rows,
                pr,
                lock_info: vec![],
                released_locks: ReleasedLocks::new(),
                new_acquired_locks: vec![],
                lock_guards,
                response_policy: ResponsePolicy::OnApplied,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api_version::{test_kv_format_impl, ApiV2};
    use causal_ts::CausalTsProviderImpl;
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use futures::executor::block_on;
    use kvproto::kvrpcpb::Context;

    use super::*;
    use crate::storage::{
        lock_manager::MockLockManager, txn::scheduler::get_raw_ext, Engine, Statistics,
        TestEngineBuilder,
    };

    #[test]
    fn test_cas_basic() {
        test_kv_format_impl!(test_cas_basic_impl);
    }

    /// Note: for API V2, TestEngine don't support MVCC reading, so
    /// `pre_propose` observer is ignored, and no timestamp will be append
    /// to key. The full test of `RawCompareAndSwap` is in
    /// `src/storage/mod.rs`.
    fn test_cas_basic_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"rk";

        let encoded_key = F::encode_raw_key(key, None);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v1".to_vec(),
            0,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v2".to_vec(),
            1,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key,
            Some(b"v1".to_vec()),
            b"v3".to_vec(),
            2,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&mut engine, cm, cmd, ts_provider).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);
    }

    pub fn sched_command<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
        ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = engine.snapshot(Default::default())?;
        use kvproto::kvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();

        let raw_ext = block_on(get_raw_ext(ts_provider, cm.clone(), true, &cmd.cmd)).unwrap();
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
            raw_ext,
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        match ret.pr {
            ProcessResult::RawCompareAndSwapRes {
                previous_value,
                succeed,
            } => {
                if succeed {
                    let ctx = Context::default();
                    engine.write(&ctx, ret.to_be_write).unwrap();
                }
                Ok((previous_value, succeed))
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_cas_process_write() {
        test_kv_format_impl!(test_cas_process_write_impl);
    }

    fn test_cas_process_write_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);

        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let raw_key = b"rk";
        let raw_value = b"valuek";
        let ttl = 30;
        let encode_value = RawValue {
            user_value: raw_value.to_vec(),
            expire_ts: ttl_to_expire_ts(ttl),
            is_delete: false,
        };
        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, None),
            None,
            raw_value.to_vec(),
            ttl,
            F::TAG,
            Context::default(),
        );
        let mut statistic = Statistics::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        let raw_ext = block_on(get_raw_ext(ts_provider, cm.clone(), true, &cmd.cmd)).unwrap();
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: kvproto::kvrpcpb::ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
            raw_ext,
        };
        let cmd: Command = cmd.into();
        let write_result = cmd.process_write(snap, context).unwrap();
        let modifies_with_ts = vec![Modify::Put(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, Some(101.into())),
            F::encode_raw_value_owned(encode_value),
        )];
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts);
        if F::TAG == ApiVersion::V2 {
            assert_eq!(write_result.lock_guards.len(), 1);
            let raw_key = vec![api_version::api_v2::RAW_KEY_PREFIX];
            let encoded_key = ApiV2::encode_raw_key(&raw_key, Some(100.into()));
            assert_eq!(
                write_result.lock_guards.first().unwrap().key(),
                &encoded_key
            );
        }
    }
}
