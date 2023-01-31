use crate::abort;
use crate::config::GeyserPluginPostgresConfig;
use crate::parallel_client_worker::LogTransactionRequest;
use crate::parallel_client_worker::ParallelClientWorker;
use crate::parallel_client_worker::UpdateAccountRequest;
use crate::parallel_client_worker::UpdateBlockMetadataRequest;
use crate::parallel_client_worker::UpdateSlotRequest;
use crate::parallel_client_worker::WorkRequest;
use crate::postgres_client::build_db_transaction;
use crate::postgres_client::DbAccountInfo;
use crate::postgres_client::DbBlockInfo;
use crate::postgres_client::ReadableAccountInfo;
use crossbeam_channel::bounded;
use crossbeam_channel::Sender;
use log::*;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use solana_sdk::timing::AtomicInterval;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::thread::{self};
use std::time::Duration;

const MAX_ASYNC_REQUESTS: usize = 40960;

#[warn(clippy::large_enum_variant)]
pub struct ParallelClient {
    workers: Vec<JoinHandle<Result<(), GeyserPluginError>>>,
    exit_worker: Arc<AtomicBool>,
    is_startup_done: Arc<AtomicBool>,
    startup_done_count: Arc<AtomicUsize>,
    initialized_worker_count: Arc<AtomicUsize>,
    sender: Sender<WorkRequest>,
    last_report: AtomicInterval,
    transaction_write_version: AtomicU64,
}

impl ParallelClient {
    pub fn new(config: &GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        info!("[ParallelClient] config=[{:?}]", config);
        let (sender, receiver) = bounded(MAX_ASYNC_REQUESTS);
        let exit_worker = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::default();
        let is_startup_done = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let worker_count = config.threads;
        let initialized_worker_count = Arc::new(AtomicUsize::new(0));
        for i in 0..worker_count {
            let cloned_receiver = receiver.clone();
            let exit_clone = exit_worker.clone();
            let is_startup_done_clone = is_startup_done.clone();
            let startup_done_count_clone = startup_done_count.clone();
            let initialized_worker_count_clone = initialized_worker_count.clone();
            let config = config.clone();
            let worker = Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || -> Result<(), GeyserPluginError> {
                    let panic_on_db_errors = config.panic_on_db_errors;
                    match ParallelClientWorker::new(config) {
                        Ok(mut worker) => {
                            initialized_worker_count_clone.fetch_add(1, Ordering::Relaxed);
                            worker.do_work(cloned_receiver, exit_clone, is_startup_done_clone, startup_done_count_clone, panic_on_db_errors)?;
                            Ok(())
                        }
                        Err(err) => {
                            error!("Error when making connection to database: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                            Err(err)
                        }
                    }
                })
                .unwrap();

            workers.push(worker);
        }

        Ok(Self {
            last_report: AtomicInterval::default(),
            workers,
            exit_worker,
            is_startup_done,
            startup_done_count,
            initialized_worker_count,
            sender,
            transaction_write_version: AtomicU64::default(),
        })
    }

    pub fn join(&mut self) -> thread::Result<()> {
        self.exit_worker.store(true, Ordering::Relaxed);
        while !self.workers.is_empty() {
            let worker = self.workers.pop();
            if worker.is_none() {
                break;
            }
            let worker = worker.unwrap();
            let result = worker.join().unwrap();
            if result.is_err() {
                error!("The worker thread has failed: {:?}", result);
            }
        }

        Ok(())
    }

    pub fn update_account(&mut self, account: &ReplicaAccountInfo, slot: u64, is_startup: bool) -> Result<(), GeyserPluginError> {
        if self.last_report.should_update(30000) {
            datapoint_debug!("postgres-plugin-stats", ("message-queue-length", self.sender.len() as i64, i64),);
        }
        let mut measure = Measure::start("geyser-plugin-posgres-create-work-item");
        let wrk_item = WorkRequest::UpdateAccount(Box::new(UpdateAccountRequest {
            account: DbAccountInfo::new(account, slot),
            is_startup,
        }));

        measure.stop();

        inc_new_counter_debug!("geyser-plugin-posgres-create-work-item-us", measure.as_us() as usize, 100000, 100000);

        let mut measure = Measure::start("geyser-plugin-posgres-send-msg");

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: format!("Failed to update the account {:?}, error: {:?}", bs58::encode(account.pubkey()).into_string(), err),
            });
        }

        measure.stop();
        inc_new_counter_debug!("geyser-plugin-posgres-send-msg-us", measure.as_us() as usize, 100000, 100000);

        Ok(())
    }

    pub fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<(), GeyserPluginError> {
        if let Err(err) = self.sender.send(WorkRequest::UpdateSlot(Box::new(UpdateSlotRequest { slot, parent, slot_status: status }))) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the slot {:?}, error: {:?}", slot, err),
            });
        }
        Ok(())
    }

    pub fn update_block_metadata(&mut self, block_info: &ReplicaBlockInfo) -> Result<(), GeyserPluginError> {
        if let Err(err) = self.sender.send(WorkRequest::UpdateBlockMetadata(Box::new(UpdateBlockMetadataRequest {
            block_info: DbBlockInfo::from(block_info),
        }))) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the block metadata at slot {:?}, error: {:?}", block_info.slot, err),
            });
        }
        Ok(())
    }

    pub fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        info!("[notify_end_of_startup]");
        // Ensure all items in the queue has been received by the workers
        while !self.sender.is_empty() {
            sleep(Duration::from_millis(100));
        }
        self.is_startup_done.store(true, Ordering::Relaxed);

        // Wait for all worker threads to be done with flushing
        while self.startup_done_count.load(Ordering::Relaxed) != self.initialized_worker_count.load(Ordering::Relaxed) {
            info!(
                "[notify_end_of_startup] {}/{}",
                self.startup_done_count.load(Ordering::Relaxed),
                self.initialized_worker_count.load(Ordering::Relaxed)
            );
            sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    pub fn log_transaction_info(&mut self, transaction_info: &ReplicaTransactionInfo, slot: u64) -> Result<(), GeyserPluginError> {
        self.transaction_write_version.fetch_add(1, Ordering::Relaxed);
        let wrk_item = WorkRequest::LogTransaction(Box::new(LogTransactionRequest {
            transaction_info: build_db_transaction(slot, transaction_info, self.transaction_write_version.load(Ordering::Relaxed)),
        }));

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the transaction, error: {:?}", err),
            });
        }
        Ok(())
    }
}
