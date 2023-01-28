use crate::accounts_selector::AccountsSelector;
use crate::accounts_selector::AccountsSelectorConfig;
use crate::postgres_client::ParallelPostgresClient;
use crate::postgres_client::PostgresClientBuilder;
use crate::transaction_selector::TransactionSelector;
use crate::transaction_selector::TransactionSelectorConfig;
use bs58;
use log::*;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions;
use solana_geyser_plugin_interface::geyser_plugin_interface::Result;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use std::fs::File;
use std::io::Read;
use thiserror::Error;

#[derive(Default)]
pub struct GeyserPluginPostgres {
    pub config: Option<GeyserPluginPostgresConfig>,
    client: Option<ParallelPostgresClient>,
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

/// The Configuration for the PostgreSQL plugin
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GeyserPluginPostgresConfig {
    /// The host name or IP of the PostgreSQL server
    pub host: Option<String>,

    /// The user name of the PostgreSQL server.
    pub user: Option<String>,

    /// The port number of the PostgreSQL database, the default is 5432
    pub port: Option<u16>,

    /// The connection string of PostgreSQL database, if this is set
    /// `host`, `user` and `port` will be ignored.
    pub connection_str: Option<String>,

    /// Accounts to listen to
    pub accounts_selector: Option<AccountsSelectorConfig>,

    /// The connection string of PostgreSQL database, if this is set
    /// `host`, `user` and `port` will be ignored.
    pub transaction_selector: Option<TransactionSelectorConfig>,

    /// Controls the number of threads establishing connections to
    /// the PostgreSQL server. The default is 10.
    pub threads: Option<usize>,

    /// Controls the batch size when bulk loading accounts.
    /// The default is 10.
    pub batch_size: Option<usize>,

    /// Controls whether to panic the validator in case of errors
    /// writing to PostgreSQL server. The default is false
    pub panic_on_db_errors: Option<bool>,

    /// Indicates whether to store historical data for accounts
    pub store_account_historical_data: Option<bool>,

    /// Controls whether to use SSL based connection to the database server.
    /// The default is false
    pub use_ssl: Option<bool>,

    /// Specify the path to PostgreSQL server's certificate file
    pub server_ca: Option<String>,

    /// Specify the path to the local client's certificate file
    pub client_cert: Option<String>,

    /// Specify the path to the local client's private PEM key file.
    pub client_key: Option<String>,

    /// Controls whether to index the token owners. The default is false
    pub index_token_owner: Option<bool>,

    /// Controls whether to index the token mints. The default is false
    pub index_token_mint: Option<bool>,

    /// Controls if this plugin can read the database on_load() to find heighest slot
    /// and ignore upsert accounts (at_startup) that should already exist in DB
    #[serde(default)]
    pub skip_upsert_existing_accounts_at_startup: bool,
}

#[derive(Error, Debug)]
pub enum GeyserPluginPostgresError {
    #[error("Error connecting to the data store. Error message: ({msg})")]
    ConnectionError { msg: String },
    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },
    #[error("Error preparing data store schema. Error message: ({msg})")]
    ConfigurationError { msg: String },
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

    /// Do initialization for the PostgreSQL plugin.
    ///
    /// # Format of the config file:
    /// * The `accounts_selector` section allows the user to controls accounts selections.
    /// "accounts_selector" : {
    ///     "accounts" : \["pubkey-1", "pubkey-2", ..., "pubkey-n"\],
    /// }
    /// or:
    /// "accounts_selector" = {
    ///     "owners" : \["pubkey-1", "pubkey-2", ..., "pubkey-m"\]
    /// }
    /// Accounts either satisyfing the accounts condition or owners condition will be selected.
    /// When only owners is specified,
    /// all accounts belonging to the owners will be streamed.
    /// The accounts field supports wildcard to select all accounts:
    /// "accounts_selector" : {
    ///     "accounts" : \["*"\],
    /// }
    /// * "host", optional, specifies the PostgreSQL server.
    /// * "user", optional, specifies the PostgreSQL user.
    /// * "port", optional, specifies the PostgreSQL server's port.
    /// * "connection_str", optional, the custom PostgreSQL connection string.
    /// Please refer to https://docs.rs/postgres/0.19.2/postgres/config/struct.Config.html for the connection configuration.
    /// When `connection_str` is set, the values in "host", "user" and "port" are ignored. If `connection_str` is not given,
    /// `host` and `user` must be given.
    /// "store_account_historical_data", optional, set it to 'true', to store historical account data to account_audit
    /// table.
    /// * "threads" optional, specifies the number of worker threads for the plugin. A thread
    /// maintains a PostgreSQL connection to the server. The default is '10'.
    /// * "batch_size" optional, specifies the batch size of bulk insert when the AccountsDb is created
    /// from restoring a snapshot. The default is '10'.
    /// * "panic_on_db_errors", optional, contols if to panic when there are errors replicating data to the
    /// PostgreSQL database. The default is 'false'.
    /// * "transaction_selector", optional, controls if and what transaction to store. If this field is missing
    /// None of the transction is stored.
    /// "transaction_selector" : {
    ///     "mentions" : \["pubkey-1", "pubkey-2", ..., "pubkey-n"\],
    /// }
    /// The `mentions` field support wildcard to select all transaction or all 'vote' transactions:
    /// For example, to select all transactions:
    /// "transaction_selector" : {
    ///     "mentions" : \["*"\],
    /// }
    /// To select all vote transactions:
    /// "transaction_selector" : {
    ///     "mentions" : \["all_votes"\],
    /// }
    /// # Examples
    ///
    /// {
    ///    "libpath": "/home/solana/target/release/libsolana_geyser_plugin_postgres.so",
    ///    "host": "host_foo",
    ///    "user": "solana",
    ///    "threads": 10,
    ///    "accounts_selector" : {
    ///       "owners" : ["9oT9R5ZyRovSVnt37QvVoBttGpNqR3J7unkb567NP8k3"]
    ///    }
    /// }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        solana_logger::setup_with_default("info");
        info!("[on_load] name=[{:?}] config_file=[{:?}]", self.name(), config_file);
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config: GeyserPluginPostgresConfig = serde_json::from_str(&contents).map_err(|err| GeyserPluginError::ConfigFileReadError {
            msg: format!("[error] failed to parse config {:?}", err),
        })?;
        let (client, batch_optimize_by_skiping_older_slots) = PostgresClientBuilder::build_pararallel_postgres_client(&config)?;
        self.client = Some(client);
        self.batch_starting_slot = batch_optimize_by_skiping_older_slots;
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
        // skip updating account on startup of batch_optimize_by_skiping_older_slots is configured
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

    /// Check if the plugin is interested in account data
    /// Default is true -- if the plugin is not interested in
    /// account data, please return false.
    fn account_data_notifications_enabled(&self) -> bool {
        self.accounts_selector.as_ref().map_or_else(|| false, |selector| selector.is_enabled())
    }

    /// Check if the plugin is interested in transaction data
    fn transaction_notifications_enabled(&self) -> bool {
        self.transaction_selector.as_ref().map_or_else(|| false, |selector| selector.is_enabled())
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginPostgres pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserPluginPostgres::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
