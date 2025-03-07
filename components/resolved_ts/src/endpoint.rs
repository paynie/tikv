// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use grpcio::Environment;
use kvproto::{metapb::Region, raft_cmdpb::AdminCmdType};
use online_config::{self, ConfigChange, ConfigManager, OnlineConfig};
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, ObserveId},
    router::CdcHandle,
    store::{
        fsm::store::StoreRegionMeta,
        util::{self, RegionReadProgress, RegionReadProgressRegistry},
    },
};
use security::SecurityManager;
use tikv::config::ResolvedTsConfig;
use tikv_util::{
    memory::MemoryQuota,
    warn,
    worker::{Runnable, RunnableWithTimer, Scheduler},
};
use tokio::sync::Notify;
use txn_types::{Key, TimeStamp};

use crate::{
    advance::{AdvanceTsWorker, LeadershipResolver, DEFAULT_CHECK_LEADER_TIMEOUT_DURATION},
    cmd::{ChangeLog, ChangeRow},
    metrics::*,
    resolver::Resolver,
    scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool},
    Error, Result,
};

/// grace period for logging safe-ts and resolved-ts gap in slow log
const SLOW_LOG_GRACE_PERIOD_MS: u64 = 1000;
const MEMORY_QUOTA_EXCEEDED_BACKOFF: Duration = Duration::from_secs(30);

enum ResolverStatus {
    Pending {
        tracked_index: u64,
        locks: Vec<PendingLock>,
        cancelled: Arc<AtomicBool>,
    },
    Ready,
}

#[allow(dead_code)]
enum PendingLock {
    Track {
        key: Key,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Key,
        start_ts: Option<TimeStamp>,
        commit_ts: Option<TimeStamp>,
    },
}

// Records information related to observed region.
// observe_id is used for avoiding ABA problems in incremental scan task,
// advance resolved ts task, and command observing.
struct ObserveRegion {
    meta: Region,
    handle: ObserveHandle,
    // TODO: Get lease from raftstore.
    // lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(meta: Region, rrp: Arc<RegionReadProgress>, memory_quota: Arc<MemoryQuota>) -> Self {
        ObserveRegion {
            resolver: Resolver::with_read_progress(meta.id, Some(rrp), memory_quota),
            meta,
            handle: ObserveHandle::new(),
            resolver_status: ResolverStatus::Pending {
                tracked_index: 0,
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        }
    }

    fn read_progress(&self) -> &Arc<RegionReadProgress> {
        self.resolver.read_progress().unwrap()
    }

    fn track_change_log(&mut self, change_logs: &[ChangeLog]) -> Result<()> {
        match &mut self.resolver_status {
            ResolverStatus::Pending {
                locks,
                tracked_index,
                ..
            } => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => {
                            debug!(
                                "skip change log error";
                                "region" => self.meta.id,
                                "error" => ?e,
                            );
                            continue;
                        }
                        ChangeLog::Admin(req_type) => {
                            // TODO: for admin cmd that won't change the region meta like peer list
                            // and key range (i.e. `CompactLog`, `ComputeHash`) we may not need to
                            // return error
                            return Err(box_err!(
                                "region met admin command {:?} while initializing resolver",
                                req_type
                            ));
                        }
                        ChangeLog::Rows { rows, index } => {
                            rows.iter().for_each(|row| match row {
                                ChangeRow::Prewrite { key, start_ts, .. } => {
                                    locks.push(PendingLock::Track {
                                        key: key.clone(),
                                        start_ts: *start_ts,
                                    })
                                }
                                ChangeRow::Commit {
                                    key,
                                    start_ts,
                                    commit_ts,
                                    ..
                                } => locks.push(PendingLock::Untrack {
                                    key: key.clone(),
                                    start_ts: *start_ts,
                                    commit_ts: *commit_ts,
                                }),
                                // One pc command do not contains any lock, so just skip it
                                ChangeRow::OnePc { .. } => {}
                                ChangeRow::IngestSsT => {}
                            });
                            assert!(
                                *tracked_index < *index,
                                "region {}, tracked_index: {}, incoming index: {}",
                                self.meta.id,
                                *tracked_index,
                                *index
                            );
                            *tracked_index = *index;
                        }
                    }
                }
            }
            ResolverStatus::Ready => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => {
                            debug!(
                                "skip change log error";
                                "region" => self.meta.id,
                                "error" => ?e,
                            );
                            continue;
                        }
                        ChangeLog::Admin(req_type) => match req_type {
                            AdminCmdType::Split
                            | AdminCmdType::BatchSplit
                            | AdminCmdType::PrepareMerge
                            | AdminCmdType::RollbackMerge
                            | AdminCmdType::CommitMerge => {
                                info!(
                                    "region met split/merge command, stop tracking since key range changed, wait for re-register";
                                    "req_type" => ?req_type,
                                );
                                // Stop tracking so that `tracked_index` larger than the split/merge
                                // command index won't be published until `RegionUpdate` event
                                // trigger the region re-register and re-scan the new key range
                                self.resolver.stop_tracking();
                            }
                            _ => {
                                debug!(
                                    "skip change log admin";
                                    "region" => self.meta.id,
                                    "req_type" => ?req_type,
                                );
                            }
                        },
                        ChangeLog::Rows { rows, index } => {
                            for row in rows {
                                match row {
                                    ChangeRow::Prewrite { key, start_ts, .. } => {
                                        if !self.resolver.track_lock(
                                            *start_ts,
                                            key.to_raw().unwrap(),
                                            Some(*index),
                                        ) {
                                            return Err(Error::MemoryQuotaExceeded);
                                        }
                                    }
                                    ChangeRow::Commit { key, .. } => self
                                        .resolver
                                        .untrack_lock(&key.to_raw().unwrap(), Some(*index)),
                                    // One pc command do not contains any lock, so just skip it
                                    ChangeRow::OnePc { .. } => {
                                        self.resolver.update_tracked_index(*index);
                                    }
                                    ChangeRow::IngestSsT => {
                                        self.resolver.update_tracked_index(*index);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Track locks in incoming scan entries.
    fn track_scan_locks(&mut self, entries: Vec<ScanEntry>, apply_index: u64) -> Result<()> {
        for es in entries {
            match es {
                ScanEntry::Lock(locks) => {
                    if let ResolverStatus::Ready = self.resolver_status {
                        panic!("region {:?} resolver has ready", self.meta.id)
                    }
                    for (key, lock) in locks {
                        if !self.resolver.track_lock(
                            lock.ts,
                            key.to_raw().unwrap(),
                            Some(apply_index),
                        ) {
                            return Err(Error::MemoryQuotaExceeded);
                        }
                    }
                }
                ScanEntry::None => {
                    // Update the `tracked_index` to the snapshot's `apply_index`
                    self.resolver.update_tracked_index(apply_index);
                    let pending_tracked_index =
                        match std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready) {
                            ResolverStatus::Pending {
                                locks,
                                tracked_index,
                                ..
                            } => {
                                for lock in locks {
                                    match lock {
                                        PendingLock::Track { key, start_ts } => {
                                            if !self.resolver.track_lock(
                                                start_ts,
                                                key.to_raw().unwrap(),
                                                Some(tracked_index),
                                            ) {
                                                return Err(Error::MemoryQuotaExceeded);
                                            }
                                        }
                                        PendingLock::Untrack { key, .. } => {
                                            self.resolver.untrack_lock(
                                                &key.to_raw().unwrap(),
                                                Some(tracked_index),
                                            )
                                        }
                                    }
                                }
                                tracked_index
                            }
                            ResolverStatus::Ready => {
                                panic!("region {:?} resolver has ready", self.meta.id)
                            }
                        };
                    info!(
                        "Resolver initialized";
                        "region" => self.meta.id,
                        "observe_id" => ?self.handle.id,
                        "snapshot_index" => apply_index,
                        "pending_data_index" => pending_tracked_index,
                    );
                }
                ScanEntry::TxnEntry(_) => panic!("unexpected entry type"),
            }
        }
        Ok(())
    }
}

pub struct Endpoint<T, E: KvEngine, S> {
    store_id: Option<u64>,
    cfg: ResolvedTsConfig,
    memory_quota: Arc<MemoryQuota>,
    advance_notify: Arc<Notify>,
    store_meta: Arc<Mutex<S>>,
    region_read_progress: RegionReadProgressRegistry,
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task>,
    advance_worker: AdvanceTsWorker,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E, S> Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    pub fn new(
        cfg: &ResolvedTsConfig,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        store_meta: Arc<Mutex<S>>,
        pd_client: Arc<dyn PdClient>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let (region_read_progress, store_id) = {
            let meta = store_meta.lock().unwrap();
            (meta.region_read_progress().clone(), meta.store_id())
        };
        let advance_worker = AdvanceTsWorker::new(
            cfg.advance_ts_interval.0,
            pd_client.clone(),
            scheduler.clone(),
            concurrency_manager,
        );
        let scanner_pool = ScannerPool::new(cfg.scan_lock_pool_size, cdc_handle);
        let store_resolver_gc_interval = Duration::from_secs(60);
        let leader_resolver = LeadershipResolver::new(
            store_id,
            pd_client.clone(),
            env,
            security_mgr,
            region_read_progress.clone(),
            store_resolver_gc_interval,
        );
        let ep = Self {
            store_id: Some(store_id),
            cfg: cfg.clone(),
            memory_quota: Arc::new(MemoryQuota::new(cfg.memory_quota.0 as usize)),
            advance_notify: Arc::new(Notify::new()),
            scheduler,
            store_meta,
            region_read_progress,
            advance_worker,
            scanner_pool,
            regions: HashMap::default(),
            _phantom: PhantomData::default(),
        };
        ep.handle_advance_resolved_ts(leader_resolver);
        ep
    }

    fn register_region(&mut self, region: Region, backoff: Option<Duration>) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let observe_region = {
            if let Some(read_progress) = self.region_read_progress.get(&region_id) {
                info!(
                    "register observe region";
                    "region" => ?region
                );
                ObserveRegion::new(region.clone(), read_progress, self.memory_quota.clone())
            } else {
                warn!(
                    "try register unexit region";
                    "region" => ?region,
                );
                return;
            }
        };
        let observe_handle = observe_region.handle.clone();
        let cancelled = match observe_region.resolver_status {
            ResolverStatus::Pending { ref cancelled, .. } => cancelled.clone(),
            ResolverStatus::Ready => panic!("resolved ts illeagal created observe region"),
        };
        observe_region
            .read_progress()
            .update_advance_resolved_ts_notify(self.advance_notify.clone());
        self.regions.insert(region_id, observe_region);

        let scan_task = self.build_scan_task(region, observe_handle, cancelled, backoff);
        self.scanner_pool.spawn_task(scan_task);
        RTS_SCAN_TASKS.with_label_values(&["total"]).inc();
    }

    fn build_scan_task(
        &self,
        region: Region,
        observe_handle: ObserveHandle,
        cancelled: Arc<AtomicBool>,
        backoff: Option<Duration>,
    ) -> ScanTask {
        let scheduler = self.scheduler.clone();
        let scheduler_error = self.scheduler.clone();
        let region_id = region.id;
        let observe_id = observe_handle.id;
        ScanTask {
            handle: observe_handle,
            tag: String::new(),
            mode: ScanMode::LockOnly,
            region,
            checkpoint_ts: TimeStamp::zero(),
            backoff,
            is_cancelled: Box::new(move || cancelled.load(Ordering::Acquire)),
            send_entries: Box::new(move |entries, apply_index| {
                scheduler
                    .schedule(Task::ScanLocks {
                        region_id,
                        observe_id,
                        entries,
                        apply_index,
                    })
                    .unwrap_or_else(|e| warn!("schedule resolved ts task failed"; "err" => ?e));
                RTS_SCAN_TASKS.with_label_values(&["finish"]).inc();
            }),
            on_error: Some(Box::new(move |observe_id, _region, e| {
                if let Err(e) = scheduler_error.schedule(Task::ReRegisterRegion {
                    region_id,
                    observe_id,
                    cause: e,
                }) {
                    warn!("schedule re-register task failed";
                        "region_id" => region_id,
                        "observe_id" => ?observe_id,
                        "error" => ?e);
                }
                RTS_SCAN_TASKS.with_label_values(&["abort"]).inc();
            })),
        }
    }

    fn deregister_region(&mut self, region_id: u64) {
        if let Some(observe_region) = self.regions.remove(&region_id) {
            let ObserveRegion {
                handle,
                resolver_status,
                ..
            } = observe_region;

            info!(
                "deregister observe region";
                "store_id" => ?self.get_or_init_store_id(),
                "region_id" => region_id,
                "observe_id" => ?handle.id
            );
            // Stop observing data
            handle.stop_observing();
            // Stop scanning data
            if let ResolverStatus::Pending { cancelled, .. } = resolver_status {
                cancelled.store(true, Ordering::Release);
            }
        } else {
            debug!("deregister unregister region"; "region_id" => region_id);
        }
    }

    fn region_updated(&mut self, incoming_region: Region) {
        let region_id = incoming_region.get_id();
        if let Some(obs_region) = self.regions.get_mut(&region_id) {
            if obs_region.meta.get_region_epoch().get_version()
                == incoming_region.get_region_epoch().get_version()
            {
                // only peer list change, no need to re-register region
                obs_region.meta = incoming_region;
                return;
            }
            // TODO: may not need to re-register region for some cases:
            // - `Split/BatchSplit`, which can be handled by remove out-of-range locks from
            //   the `Resolver`'s lock heap
            // - `PrepareMerge` and `RollbackMerge`, the key range is unchanged
            self.deregister_region(region_id);
            self.register_region(incoming_region, None);
        }
    }

    // This function is corresponding to RegionDestroyed event that can be only
    // scheduled by observer. To prevent destroying region for wrong peer, it
    // should check the region epoch at first.
    fn region_destroyed(&mut self, region: Region) {
        if let Some(observe_region) = self.regions.get(&region.id) {
            if util::compare_region_epoch(
                observe_region.meta.get_region_epoch(),
                &region,
                true,
                true,
                false,
            )
            .is_ok()
            {
                self.deregister_region(region.id);
            } else {
                warn!(
                    "resolved ts destroy region failed due to epoch not match";
                    "region_id" => region.id,
                    "current_epoch" => ?observe_region.meta.get_region_epoch(),
                    "request_epoch" => ?region.get_region_epoch(),
                )
            }
        }
    }

    // Deregister current observed region and try to register it again.
    fn re_register_region(
        &mut self,
        region_id: u64,
        observe_id: ObserveId,
        cause: Error,
        backoff: Option<Duration>,
    ) {
        if let Some(observe_region) = self.regions.get(&region_id) {
            if observe_region.handle.id != observe_id {
                warn!("resolved ts deregister region failed due to observe_id not match");
                return;
            }

            info!(
                "register region again";
                "region_id" => region_id,
                "observe_id" => ?observe_id,
                "cause" => ?cause
            );
            self.deregister_region(region_id);
            let region;
            {
                let meta = self.store_meta.lock().unwrap();
                match meta.reader(region_id) {
                    Some(r) => region = r.region.as_ref().clone(),
                    None => return,
                }
            }
            self.register_region(region, backoff);
        }
    }

    // Update advanced resolved ts.
    // Must ensure all regions are leaders at the point of ts.
    fn handle_resolved_ts_advanced(&mut self, regions: Vec<u64>, ts: TimeStamp) {
        if regions.is_empty() {
            return;
        }
        let now = tikv_util::time::Instant::now_coarse();
        for region_id in regions.iter() {
            if let Some(observe_region) = self.regions.get_mut(region_id) {
                if let ResolverStatus::Ready = observe_region.resolver_status {
                    let _ = observe_region.resolver.resolve(ts, Some(now));
                }
            }
        }
    }

    // Tracking or untracking locks with incoming commands that corresponding
    // observe id is valid.
    #[allow(clippy::drop_ref)]
    fn handle_change_log(&mut self, cmd_batch: Vec<CmdBatch>) {
        let size = cmd_batch.iter().map(|b| b.size()).sum::<usize>();
        RTS_CHANNEL_PENDING_CMD_BYTES.sub(size as i64);
        for batch in cmd_batch {
            if batch.is_empty() {
                continue;
            }
            if let Some(observe_region) = self.regions.get_mut(&batch.region_id) {
                let observe_id = batch.rts_id;
                let region_id = observe_region.meta.id;
                if observe_region.handle.id == observe_id {
                    let logs = ChangeLog::encode_change_log(region_id, batch);
                    if let Err(e) = observe_region.track_change_log(&logs) {
                        drop(observe_region);
                        let backoff = match e {
                            Error::MemoryQuotaExceeded => Some(MEMORY_QUOTA_EXCEEDED_BACKOFF),
                            Error::Other(_) => None,
                        };
                        self.re_register_region(region_id, observe_id, e, backoff);
                    }
                } else {
                    debug!("resolved ts CmdBatch discarded";
                        "region_id" => batch.region_id,
                        "observe_id" => ?batch.rts_id,
                        "current" => ?observe_region.handle.id,
                    );
                }
            }
        }
    }

    fn handle_scan_locks(
        &mut self,
        region_id: u64,
        observe_id: ObserveId,
        entries: Vec<ScanEntry>,
        apply_index: u64,
    ) {
        let mut is_memory_quota_exceeded = false;
        if let Some(observe_region) = self.regions.get_mut(&region_id) {
            if observe_region.handle.id == observe_id {
                if let Err(Error::MemoryQuotaExceeded) =
                    observe_region.track_scan_locks(entries, apply_index)
                {
                    is_memory_quota_exceeded = true;
                }
            }
        } else {
            debug!("scan locks region not exist";
                "region_id" => region_id,
                "observe_id" => ?observe_id);
        }
        if is_memory_quota_exceeded {
            let backoff = Some(MEMORY_QUOTA_EXCEEDED_BACKOFF);
            self.re_register_region(region_id, observe_id, Error::MemoryQuotaExceeded, backoff);
        }
    }

    fn handle_advance_resolved_ts(&self, leader_resolver: LeadershipResolver) {
        let regions = self.regions.keys().into_iter().copied().collect();
        self.advance_worker.advance_ts_for_regions(
            regions,
            leader_resolver,
            self.cfg.advance_ts_interval.0,
            self.advance_notify.clone(),
        );
    }

    fn handle_change_config(&mut self, change: ConfigChange) {
        let prev = format!("{:?}", self.cfg);
        if let Err(e) = self.cfg.update(change) {
            warn!("resolved-ts config fails"; "error" => ?e);
        } else {
            self.advance_notify.notify_waiters();
            self.memory_quota
                .set_capacity(self.cfg.memory_quota.0 as usize);
            info!(
                "resolved-ts config changed";
                "prev" => prev,
                "current" => ?self.cfg,
            );
        }
    }

    fn get_or_init_store_id(&mut self) -> Option<u64> {
        self.store_id.or_else(|| {
            let meta = self.store_meta.lock().unwrap();
            self.store_id = Some(meta.store_id());
            self.store_id
        })
    }

    fn handle_get_diagnosis_info(
        &self,
        region_id: u64,
        log_locks: bool,
        min_start_ts: u64,
        callback: tikv::server::service::ResolvedTsDiagnosisCallback,
    ) {
        if let Some(r) = self.regions.get(&region_id) {
            if log_locks {
                r.resolver.log_locks(min_start_ts);
            }
            callback(Some((
                r.resolver.stopped(),
                r.resolver.resolved_ts().into_inner(),
                r.resolver.tracked_index(),
                r.resolver.num_locks(),
                r.resolver.num_transactions(),
            )));
        } else {
            callback(None);
        }
    }
}

pub enum Task {
    RegionUpdated(Region),
    RegionDestroyed(Region),
    RegisterRegion {
        region: Region,
    },
    DeRegisterRegion {
        region_id: u64,
    },
    ReRegisterRegion {
        region_id: u64,
        observe_id: ObserveId,
        cause: Error,
    },
    AdvanceResolvedTs {
        leader_resolver: LeadershipResolver,
    },
    ResolvedTsAdvanced {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
    },
    ScanLocks {
        region_id: u64,
        observe_id: ObserveId,
        entries: Vec<ScanEntry>,
        apply_index: u64,
    },
    ChangeConfig {
        change: ConfigChange,
    },
    GetDiagnosisInfo {
        region_id: u64,
        log_locks: bool,
        min_start_ts: u64,
        callback: tikv::server::service::ResolvedTsDiagnosisCallback,
    },
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("ResolvedTsTask");
        match self {
            Task::RegionDestroyed(ref region) => de
                .field("name", &"region_destroyed")
                .field("region", &region)
                .finish(),
            Task::RegionUpdated(ref region) => de
                .field("name", &"region_updated")
                .field("region", &region)
                .finish(),
            Task::RegisterRegion { ref region } => de
                .field("name", &"register_region")
                .field("region", &region)
                .finish(),
            Task::DeRegisterRegion { ref region_id } => de
                .field("name", &"deregister_region")
                .field("region_id", &region_id)
                .finish(),
            Task::ReRegisterRegion {
                ref region_id,
                ref observe_id,
                ref cause,
            } => de
                .field("name", &"re_register_region")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("cause", &cause)
                .finish(),
            Task::ResolvedTsAdvanced {
                ref regions,
                ref ts,
            } => de
                .field("name", &"advance_resolved_ts")
                .field("regions", &regions)
                .field("ts", &ts)
                .finish(),
            Task::ChangeLog { .. } => de.field("name", &"change_log").finish(),
            Task::ScanLocks {
                ref region_id,
                ref observe_id,
                ref apply_index,
                ..
            } => de
                .field("name", &"scan_locks")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("apply_index", &apply_index)
                .finish(),
            Task::AdvanceResolvedTs { .. } => de.field("name", &"advance_resolved_ts").finish(),
            Task::ChangeConfig { ref change } => de
                .field("name", &"change_config")
                .field("change", &change)
                .finish(),
            Task::GetDiagnosisInfo { region_id, .. } => de
                .field("name", &"get_diagnosis_info")
                .field("region_id", &region_id)
                .field("callback", &"callback")
                .finish(),
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, E, S> Runnable for Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run resolved-ts task"; "task" => ?task);
        match task {
            Task::RegionDestroyed(region) => self.region_destroyed(region),
            Task::RegionUpdated(region) => self.region_updated(region),
            Task::RegisterRegion { region } => self.register_region(region, None),
            Task::DeRegisterRegion { region_id } => self.deregister_region(region_id),
            Task::ReRegisterRegion {
                region_id,
                observe_id,
                cause,
            } => self.re_register_region(region_id, observe_id, cause, None),
            Task::AdvanceResolvedTs { leader_resolver } => {
                self.handle_advance_resolved_ts(leader_resolver)
            }
            Task::ResolvedTsAdvanced { regions, ts } => {
                self.handle_resolved_ts_advanced(regions, ts)
            }
            Task::ChangeLog { cmd_batch } => self.handle_change_log(cmd_batch),
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
                apply_index,
            } => self.handle_scan_locks(region_id, observe_id, entries, apply_index),
            Task::ChangeConfig { change } => self.handle_change_config(change),
            Task::GetDiagnosisInfo {
                region_id,
                log_locks,
                min_start_ts,
                callback,
            } => self.handle_get_diagnosis_info(region_id, log_locks, min_start_ts, callback),
        }
    }
}

pub struct ResolvedTsConfigManager(Scheduler<Task>);

impl ResolvedTsConfigManager {
    pub fn new(scheduler: Scheduler<Task>) -> ResolvedTsConfigManager {
        ResolvedTsConfigManager(scheduler)
    }
}

impl ConfigManager for ResolvedTsConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        if let Err(e) = self.0.schedule(Task::ChangeConfig { change }) {
            error!("failed to schedule ChangeConfig task"; "err" => ?e);
        }
        Ok(())
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl<T, E, S> RunnableWithTimer for Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    fn on_timeout(&mut self) {
        let store_id = self.get_or_init_store_id();
        let (mut oldest_ts, mut oldest_region, mut zero_ts_count) = (u64::MAX, 0, 0);
        let (mut oldest_leader_ts, mut oldest_leader_region) = (u64::MAX, 0);
        let (mut oldest_safe_ts, mut oldest_safe_ts_region) = (u64::MAX, 0);
        let mut oldest_duration_to_last_update_ms = 0;
        let mut oldest_duration_to_last_consume_leader_ms = 0;
        self.region_read_progress.with(|registry| {
            for (region_id, read_progress) in registry {
                let safe_ts = read_progress.safe_ts();
                if safe_ts > 0 && safe_ts < oldest_safe_ts {
                    oldest_safe_ts = safe_ts;
                    oldest_safe_ts_region = *region_id;
                }

                let (leader_info, leader_store_id) = read_progress.dump_leader_info();
                // this is maximum resolved-ts pushed to region_read_progress, namely candidates
                // of safe_ts. It may not be the safe_ts yet
                let ts = leader_info.get_read_state().get_safe_ts();
                if ts == 0 {
                    zero_ts_count += 1;
                    continue;
                }
                if ts < oldest_ts {
                    oldest_ts = ts;
                    oldest_region = *region_id;
                    // use -1 to denote none.
                    oldest_duration_to_last_update_ms = read_progress
                        .get_core()
                        .last_instant_of_consume_leader()
                        .map(|t| t.saturating_elapsed().as_millis() as i64)
                        .unwrap_or(-1);
                    oldest_duration_to_last_consume_leader_ms = read_progress
                        .get_core()
                        .last_instant_of_consume_leader()
                        .map(|t| t.saturating_elapsed().as_millis() as i64)
                        .unwrap_or(-1);
                }

                if let (Some(store_id), Some(leader_store_id)) = (store_id, leader_store_id) {
                    if leader_store_id == store_id && ts < oldest_leader_ts {
                        oldest_leader_ts = ts;
                        oldest_leader_region = *region_id;
                    }
                }
            }
        });
        let mut lock_heap_size = 0;
        let (mut resolved_count, mut unresolved_count) = (0, 0);
        for observe_region in self.regions.values() {
            match &observe_region.resolver_status {
                ResolverStatus::Pending { locks, .. } => {
                    for l in locks {
                        match l {
                            PendingLock::Track { key, .. } => lock_heap_size += key.len(),
                            PendingLock::Untrack { key, .. } => lock_heap_size += key.len(),
                        }
                    }
                    unresolved_count += 1;
                }
                ResolverStatus::Ready { .. } => {
                    lock_heap_size += observe_region.resolver.approximate_heap_bytes();
                    resolved_count += 1;
                }
            }
        }
        // approximate a TSO from PD. It is better than local timestamp when clock skew
        // exists.
        let now: u64 = self
            .advance_worker
            .last_pd_tso
            .try_lock()
            .map(|opt| {
                opt.map(|(pd_ts, instant)| {
                    pd_ts.physical() + instant.saturating_elapsed().as_millis() as u64
                })
                .unwrap_or_else(|| TimeStamp::physical_now())
            })
            .unwrap_or_else(|_| TimeStamp::physical_now());

        RTS_MIN_SAFE_TS.set(oldest_safe_ts as i64);
        RTS_MIN_SAFE_TS_REGION.set(oldest_safe_ts_region as i64);
        let safe_ts_gap = now.saturating_sub(TimeStamp::from(oldest_safe_ts).physical());
        if safe_ts_gap
            > self.cfg.advance_ts_interval.as_millis()
                + DEFAULT_CHECK_LEADER_TIMEOUT_DURATION.as_millis() as u64
                + SLOW_LOG_GRACE_PERIOD_MS
        {
            let mut lock_num = None;
            let mut min_start_ts = None;
            if let Some(ob) = self.regions.get(&oldest_safe_ts_region) {
                min_start_ts = ob
                    .resolver
                    .locks()
                    .keys()
                    .next()
                    .cloned()
                    .map(TimeStamp::into_inner);
                lock_num = Some(ob.resolver.num_locks());
            }
            info!(
                "the max gap of safe-ts is large";
                "gap" => safe_ts_gap,
                "oldest_safe_ts" => ?oldest_safe_ts,
                "region_id" => oldest_safe_ts_region,
                "advance_ts_interval" => ?self.cfg.advance_ts_interval,
                "lock_num" => lock_num,
                "min_start_ts" => min_start_ts,
            );
        }
        RTS_MIN_SAFE_TS_GAP.set(safe_ts_gap as i64);
        RTS_MIN_SAFE_TS_DUATION_TO_UPDATE_SAFE_TS.set(oldest_duration_to_last_update_ms);
        RTS_MIN_SAFE_TS_DURATION_TO_LAST_CONSUME_LEADER
            .set(oldest_duration_to_last_consume_leader_ms);

        RTS_MIN_RESOLVED_TS_REGION.set(oldest_region as i64);
        RTS_MIN_RESOLVED_TS.set(oldest_ts as i64);
        RTS_ZERO_RESOLVED_TS.set(zero_ts_count as i64);
        RTS_MIN_RESOLVED_TS_GAP
            .set(now.saturating_sub(TimeStamp::from(oldest_ts).physical()) as i64);

        RTS_MIN_LEADER_RESOLVED_TS_REGION.set(oldest_leader_region as i64);
        RTS_MIN_LEADER_RESOLVED_TS.set(oldest_leader_ts as i64);
        RTS_MIN_LEADER_RESOLVED_TS_GAP
            .set(now.saturating_sub(TimeStamp::from(oldest_leader_ts).physical()) as i64);

        RTS_LOCK_HEAP_BYTES_GAUGE.set(lock_heap_size as i64);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["resolved"])
            .set(resolved_count as _);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["unresolved"])
            .set(unresolved_count as _);
    }

    fn get_interval(&self) -> Duration {
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}
