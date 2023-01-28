use crate::accounts_selector::AccountsSelectorConfig;
use crate::transaction_selector::TransactionSelectorConfig;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::Result;
use std::fs::File;
use std::path::Path;

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

impl GeyserPluginPostgresConfig {
    /// Read plugin from JSON file.
    pub fn read_from<P: AsRef<Path>>(config_path: P) -> Result<Self> {
        let file = File::open(config_path)?;
        let this: Self = serde_json::from_reader(file).map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        Ok(this)
    }
}
