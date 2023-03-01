use crate::accounts_selector::AccountsSelector;
use crate::config::GeyserPluginPostgresConfig;
use crate::parallel_client::ParallelClient;
use crate::postgres_client::PostgresClientBuilder;
use crate::transaction_selector::TransactionSelector;
use bs58;
use log::*;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::Result;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use thiserror::Error;

#[derive(Default)]
pub struct GeyserPluginPostgres {
    pub config: Option<GeyserPluginPostgresConfig>,
    client: Option<ParallelClient>,
    accounts_selector: Option<AccountsSelector>,
    transaction_selector: Option<TransactionSelector>,
    batch_starting_slot: Option<u64>,
}

impl std::fmt::Debug for GeyserPluginPostgres {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPluginPostgres {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Error, Debug)]
pub enum GeyserPluginPostgresError {
    #[error("Error connecting to the data store. Error message: ({msg})")]
    ConnectionError { msg: String },
    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },
}

fn client_err() -> Result<()> {
    Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConnectionError {
        msg: "Client not connected.".to_string(),
    })))
}

impl GeyserPlugin for GeyserPluginPostgres {
    fn name(&self) -> &'static str {
        "GeyserPluginPostgres"
    }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        solana_logger::setup_with_default("info");
        info!("[on_load] name=[{:?}] config_file=[{:?}]", self.name(), config_file);
        let config = GeyserPluginPostgresConfig::read_from(config_file)?;
        let (client, batch_starting_slot) = PostgresClientBuilder::build_pararallel_postgres_client(&config)?;
        self.client = Some(client);
        self.batch_starting_slot = batch_starting_slot;
        self.accounts_selector = config.accounts_selector.as_ref().map(AccountsSelector::new);
        self.transaction_selector = config.transaction_selector.as_ref().map(TransactionSelector::new);
        self.config = Some(config);
        Ok(())
    }

    fn on_unload(&mut self) {
        info!("[on_unload]");
        match &mut self.client {
            None => {}
            Some(client) => {
                client.join().unwrap();
            }
        }
    }

    fn update_account(&mut self, account: ReplicaAccountInfoVersions, slot: u64, is_startup: bool) -> Result<()> {
        info!("[update_account]");
        // skip updating account on startup of batch_starting_slot is configured
        if is_startup && self.batch_starting_slot.map(|slot_limit| slot < slot_limit).unwrap_or(false) {
            return Ok(());
        }

        let client = match &mut self.client {
            Some(client) => client,
            None => return client_err(),
        };

        let mut measure_all = Measure::start("geyser-plugin-postgres-update-account-main");
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let mut measure_select = Measure::start("geyser-plugin-postgres-update-account-select");
                if let Some(accounts_selector) = &self.accounts_selector {
                    if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
                measure_select.stop();
                inc_new_counter_debug!("geyser-plugin-postgres-update-account-select-us", measure_select.as_us() as usize, 100000, 100000);

                debug!(
                    "[update_account] pubkey=[{:?}] owner=[{:?}] slot=[{:?}] accounts_selector=[{:?}]",
                    bs58::encode(account.pubkey).into_string(),
                    bs58::encode(account.owner).into_string(),
                    slot,
                    self.accounts_selector.as_ref().unwrap()
                );

                let mut measure_update = Measure::start("geyser-plugin-postgres-update-account-client");
                let result = client.update_account(account, slot, is_startup);
                measure_update.stop();

                inc_new_counter_debug!("geyser-plugin-postgres-update-account-client-us", measure_update.as_us() as usize, 100000, 100000);
                if let Err(err) = result {
                    return Err(GeyserPluginError::AccountsUpdateError {
                        msg: format!("Failed to persist the update of account to the PostgreSQL database. Error: {:?}", err),
                    });
                }
            }
        }

        measure_all.stop();
        inc_new_counter_debug!("geyser-plugin-postgres-update-account-main-us", measure_all.as_us() as usize, 100000, 100000);
        Ok(())
    }

    fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<()> {
        info!("[update_slot_status] slot=[{:?}] status=[{:?}]", slot, status);
        let client = match &mut self.client {
            Some(client) => client,
            None => return client_err(),
        };
        if let Err(err) = client.update_slot_status(slot, parent, status) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to persist the update of slot to the PostgreSQL database. Error: {:?}", err),
            });
        }
        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> Result<()> {
        info!("[notify_end_of_startup]");
        let client = match &mut self.client {
            Some(client) => client,
            None => return client_err(),
        };
        let result = client.notify_end_of_startup();

        if let Err(err) = result {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to notify the end of startup for accounts notifications. Error: {:?}", err),
            });
        }
        Ok(())
    }

    fn notify_transaction(&mut self, transaction_info: ReplicaTransactionInfoVersions, slot: u64) -> Result<()> {
        info!("[notify_transaction]");
        let client = match &mut self.client {
            Some(client) => client,
            None => return client_err(),
        };

        match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(transaction_info) => {
                if let Some(transaction_selector) = &self.transaction_selector {
                    if !transaction_selector.is_transaction_selected(transaction_info.is_vote, Box::new(transaction_info.transaction.message().account_keys().iter())) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }

                let result = client.log_transaction_info(transaction_info, slot);

                if let Err(err) = result {
                    return Err(GeyserPluginError::SlotStatusUpdateError {
                        msg: format!("Failed to persist the transaction info to the PostgreSQL database. Error: {:?}", err),
                    });
                }
            }
        }

        Ok(())
    }

    fn notify_block_metadata(&mut self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        info!("[notify_block_metadata]");
        let client = match &mut self.client {
            Some(client) => client,
            None => return client_err(),
        };
        match block_info {
            ReplicaBlockInfoVersions::V0_0_1(block_info) => {
                let result = client.update_block_metadata(block_info);

                if let Err(err) = result {
                    return Err(GeyserPluginError::SlotStatusUpdateError {
                        msg: format!("Failed to persist the update of block metadata to the PostgreSQL database. Error: {:?}", err),
                    });
                }
            }
        }

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.accounts_selector.as_ref().map_or_else(|| false, |selector| selector.is_enabled())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        self.transaction_selector.as_ref().map_or_else(|| false, |selector| selector.is_enabled())
    }
}
