use api_version::{match_template_api_version, KvFormat, RawValue};
use engine_traits::{raw_ttl::ttl_to_expire_ts, CfName, CF_DEFAULT};
use kvproto::kvrpcpb::ApiVersion;
use kvproto::kvrpcpb::Op;
use kvproto::kvrpcpb::Op::Put;
use kvproto::kvrpcpb::Op::Del;
use raw::RawStore;
use tikv_kv::Statistics;
use txn_types::{Key, Value, KvPair, KvWithOp};

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    raw,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
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
    RawWriteWithOpVersion:
        cmd_ty => (),
        display => "kv::command::raw_batch_put {:?}", (ctx),
        content => {
            cf: CfName,
            write_ops: Vec<(Key, Value, Op)>,
            ttls: Vec<u64>,
            api_version: ApiVersion,
        }
}

impl CommandExt for RawWriteWithOpVersion {
    ctx!();
    tag!(raw_batch_write);
    gen_lock!(write_ops: multiple(|x| &x.0));

    fn write_bytes(&self) -> usize {
        //self.mutations.iter().map(|x| x.size() + x.key().len() + 4 + 8).sum()
        // CF length
        let cf_size = if self.cf == CF_DEFAULT { 0 } else { self.cf.len() };

        // Key len + value len + key len + ttl length
        self.write_ops.iter().map(|kv_with_op| 
            {
                if kv_with_op.op == Del {
                    cf_size + kv.0.len() + kv.0.len() + 4
                } else {
                    cf_size + kv.0.len() + kv.1.len() + kv.0.len() + 4 + 8
                }
            }).sum()
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawWriteWithOpVersion {
    fn process_write(self, snapshot: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, write_ops, ttls, ctx, api_version) = (self.cf, self.write_ops, self.ttls, self.ctx, self.api_version);
        let mut data = vec![];

        let store = RawStore::new(snapshot, self.api_version);

        write_ops.into_iter().zip(ttls).for_each(|((key, value, op), ttl)| {
            if op == Put {
                //info!("Write with version ttl"; "new ttl" => ttl, "key str = " => key.to_string());
                // Generate version key
                let version_key = key.get_version_key();


                // Generate key/value update
                let m = Modify::Delete(
                    cf,
                    key,
                );

                data.push(m);

                // Generate version key/value update
                let m_version = Modify::Delete(
                    cf,
                    version_key,
                );
                
                data.push(m_version);
            } else {
             //info!("Write with version ttl"; "new ttl" => ttl, "key str = " => key.to_string());
                // Generate version key
                let version_key = key.get_version_key();

                // Generate value update
                let raw_value = RawValue {
                    user_value: value,
                    expire_ts: ttl_to_expire_ts(ttl),
                    is_delete: false,
                };

                // Encode value
                let encoded_raw_value = match_template_api_version!(
                    API,
                    match api_version {
                        ApiVersion::API => API::encode_raw_value_owned(raw_value),
                    }
                );    

                // Generate key/value update
                let m = Modify::Put(
                    cf,
                    key,
                    encoded_raw_value,
                );

                data.push(m);

                // Get old version
                let old_version = store.raw_get_key_value(
                    cf,
                    &version_key,
                    &mut Statistics::default(),
                ).unwrap();

                // Calculate new version
                let new_version_u64 = match old_version {
                    Some(ref version_value) => {
                        // Update version by + 1
                        decode_u64(version_value) + 1
                    }

                    None => {
                        // Not exist, just return 0
                        0
                    }
                };

                // Generate version value
                let raw_version_value = RawValue {
                    user_value: encode_u64(&new_version_u64),
                    expire_ts: ttl_to_expire_ts(ttl),
                    is_delete: false,
                };

                // Encode value
                let encoded_raw_version_value = match_template_api_version!(
                    API,
                    match api_version {
                        ApiVersion::API => API::encode_raw_value_owned(raw_version_value),
                    }
                );

                // Generate version key/value update
                let m_version = Modify::Put(
                    cf,
                    version_key,
                    encoded_raw_version_value,
                );
                
                data.push(m_version);
            }

 
        });

        fail_point!("txn_commands_raw_batch_write_atomic");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

