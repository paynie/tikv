use api_version::{match_template_api_version, KvFormat, RawValue};
use engine_traits::{CfName, raw_ttl::ttl_to_expire_ts};
use kvproto::kvrpcpb::ApiVersion;
//use raw::RawStore;
use tikv_kv::Statistics;
use txn_types::Key;
use crate::storage::raw::RawStore;

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    txn::{
        commands::{
            CommandExt, ReleasedLocks, ResponsePolicy, WriteCommand,
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
    RawSetKeyTTL:
        cmd_ty => (),
        display => {"kv::command::raw_set_key_ttl {:?}", (ctx),}
        content => {
            cf: CfName,
            key: Key,
            ttl: u64,
            api_version: ApiVersion,
        }
        in_heap => {
            key,
            ttl,
        }
}

impl CommandExt for RawSetKeyTTL {
    ctx!();
    tag!(raw_set_key_ttl);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        // Key len + value len + ttl
        self.key.as_encoded().len() + (256 as usize)
    }
}


impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawSetKeyTTL {
    fn process_write(self, snapshot: S, wctx: WriteContext<'_, L>) -> Result<WriteResult> {

        let (cf, mut key, ttl, ctx, raw_ext) = (self.cf, self.key, self.ttl, self.ctx, wctx.raw_ext);

        let mut data = vec![];

        let store = RawStore::new(snapshot, self.api_version);

        // Get old value, already remove ttl
        let old_value = store.raw_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        match old_value {
            Some(v) => {
                // Value exist, append ttl at the end of value

                // Generate new value
                let raw_value = RawValue {
                    user_value: v,
                    expire_ts: ttl_to_expire_ts(ttl),
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

                // Generate Modify
                let m = Modify::Put(
                    cf,
                    key,
                    encoded_raw_value,
                );

                data.push(m);
            }

            None => {
                // Value not exist, just return
            }
        }

        fail_point!("txn_commands_set_key_ttl");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: vec![],
            lock_guards: raw_ext.into_iter().map(|r| r.key_guard).collect(),
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}
