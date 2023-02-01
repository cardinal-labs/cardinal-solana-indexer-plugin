use crate::accounts_selector::AccountsSelectorConfig;
use crate::transaction_selector::TransactionSelectorConfig;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::Result;
use std::fs::File;
use std::path::Path;

/// Config for the PostgreSQL plugin
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
/// * "connection_str", the custom PostgreSQL connection string.
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GeyserPluginPostgresConfig {
    /// The connection string of PostgreSQL database, if this is set
    /// `host`, `user` and `port` will be ignored.
    pub connection_str: String,

    /// Accounts to listen to
    pub accounts_selector: Option<AccountsSelectorConfig>,

    /// The connection string of PostgreSQL database, if this is set
    /// `host`, `user` and `port` will be ignored.
    pub transaction_selector: Option<TransactionSelectorConfig>,

    /// Controls the number of threads establishing connections to
    /// the PostgreSQL server. The default is 10.
    pub threads: usize,

    /// Controls the batch size when bulk loading accounts.
    /// The default is 10.
    pub batch_size: usize,

    /// Controls whether to panic the validator in case of errors
    /// writing to PostgreSQL server. The default is false
    pub panic_on_db_errors: bool,

    /// Controls whether to use SSL based connection to the database server.
    /// The default is false
    pub use_ssl: Option<bool>,

    /// Specify the path to PostgreSQL server's certificate file
    pub server_ca: Option<String>,

    /// Specify the path to the local client's certificate file
    pub client_cert: Option<String>,

    /// Specify the path to the local client's private PEM key file.
    pub client_key: Option<String>,

    /// Controls if this plugin can read the database on_load() to find heighest slot
    /// and ignore upsert accounts (at_startup) that should already exist in DB
    pub skip_upsert_existing_accounts_at_startup: bool,

    /// The maximum asynchronous requests allowed in the channel to avoid excessive
    /// memory usage. The downside -- calls after this threshold is reached can get blocked.
    pub safe_batch_starting_slot_cushion: u64,
}

impl Default for GeyserPluginPostgresConfig {
    fn default() -> Self {
        Self {
            connection_str: "".to_string(),
            accounts_selector: None,
            transaction_selector: None,
            threads: 10,
            batch_size: 10,
            panic_on_db_errors: false,
            use_ssl: None,
            server_ca: None,
            client_cert: None,
            client_key: None,
            skip_upsert_existing_accounts_at_startup: false,
            safe_batch_starting_slot_cushion: 2 * 40960,
        }
    }
}

impl GeyserPluginPostgresConfig {
    /// Read plugin from JSON file.
    pub fn read_from<P: AsRef<Path>>(config_path: P) -> Result<Self> {
        let file = File::open(config_path)?;
        let this: Self = serde_json::from_reader(file).map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        Ok(this)
    }
}
