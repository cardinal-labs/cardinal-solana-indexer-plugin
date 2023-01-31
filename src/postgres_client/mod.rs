mod account_handler;
mod postgres_client_account_audit;
mod postgres_client_account_index;
mod postgres_client_block_metadata;
mod postgres_client_slot;
mod postgres_client_transaction;
mod token_account_handler;

use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;
use crate::parallel_client::ParallelClient;
use crate::postgres_client::postgres_client_account_audit::init_account_audit;
use crate::postgres_client::postgres_client_account_index::init_account;
use crate::postgres_client::postgres_client_block_metadata::init_block;
use crate::postgres_client::postgres_client_slot::init_slot;
use crate::postgres_client::postgres_client_transaction::init_transaction;
use crate::postgres_client::token_account_handler::TokenAccountHandler;
use log::*;
use openssl::ssl::SslConnector;
use openssl::ssl::SslFiletype;
use openssl::ssl::SslMethod;
use postgres::Client;
use postgres::NoTls;
use postgres::Statement;
use postgres_openssl::MakeTlsConnector;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use std::collections::HashSet;
use std::sync::Mutex;
use std::thread;

use self::account_handler::AccountHandler;
pub use self::postgres_client_account_index::DbAccountInfo;
pub use self::postgres_client_account_index::ReadableAccountInfo;
pub use self::postgres_client_block_metadata::DbBlockInfo;
pub use self::postgres_client_transaction::build_db_transaction;
pub use self::postgres_client_transaction::DbTransaction;

struct PostgresSqlClientWrapper {
    client: Client,
    update_account_stmt: Statement,
    bulk_account_insert_stmt: Statement,
    update_slot_with_parent_stmt: Statement,
    update_slot_without_parent_stmt: Statement,
    update_transaction_log_stmt: Statement,
    update_block_metadata_stmt: Statement,
    insert_account_audit_stmt: Option<Statement>,
}

pub struct SimplePostgresClient {
    batch_size: usize,
    slots_at_startup: HashSet<u64>,
    pending_account_updates: Vec<DbAccountInfo>,
    account_handlers: Vec<Box<dyn AccountHandler>>,
    client: Mutex<PostgresSqlClientWrapper>,
}

pub trait PostgresClient {
    fn join(&mut self) -> thread::Result<()> {
        Ok(())
    }

    fn update_account(&mut self, account: DbAccountInfo, is_startup: bool) -> Result<(), GeyserPluginError>;

    fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<(), GeyserPluginError>;

    fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError>;

    fn log_transaction(&mut self, transaction_info: DbTransaction) -> Result<(), GeyserPluginError>;

    fn update_block_metadata(&mut self, block_info: DbBlockInfo) -> Result<(), GeyserPluginError>;
}

impl SimplePostgresClient {
    pub fn new(config: &GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        info!("[SimplePostgresClient] creating");
        let mut client = Self::connect_to_db(config)?;

        let bulk_account_insert_stmt = Self::build_bulk_account_insert_statement(&mut client, config)?;
        let update_account_stmt = Self::build_single_account_upsert_statement(&mut client, config)?;
        let update_slot_with_parent_stmt = Self::build_slot_upsert_statement_with_parent(&mut client, config)?;
        let update_slot_without_parent_stmt = Self::build_slot_upsert_statement_without_parent(&mut client, config)?;
        let update_transaction_log_stmt = Self::build_transaction_info_upsert_statement(&mut client, config)?;
        let update_block_metadata_stmt = Self::build_block_metadata_upsert_statement(&mut client, config)?;

        let insert_account_audit_stmt = match config.store_account_historical_data {
            true => Some(Self::build_account_audit_insert_statement(&mut client, config)?),
            _ => None,
        };

        let batch_size = config.batch_size;
        info!("[SimplePostgresClient] created");
        Ok(Self {
            batch_size,
            pending_account_updates: Vec::with_capacity(batch_size),
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                update_account_stmt,
                bulk_account_insert_stmt,
                update_slot_with_parent_stmt,
                update_slot_without_parent_stmt,
                update_transaction_log_stmt,
                update_block_metadata_stmt,
                insert_account_audit_stmt,
            }),
            account_handlers: vec![Box::new(TokenAccountHandler {})],
            slots_at_startup: HashSet::default(),
        })
    }

    pub fn connect_to_db(config: &GeyserPluginPostgresConfig) -> Result<Client, GeyserPluginError> {
        let connection_str = match &config.connection_str {
            Some(connection_str) => connection_str.clone(),
            None => {
                if config.host.is_none() || config.user.is_none() {
                    let msg = format!(
                        "\"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?} must be specified",
                        config.connection_str, config.host, config.user
                    );
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                format!("host={} user={} port={}", config.host.as_ref().unwrap(), config.user.as_ref().unwrap(), config.port)
            }
        };

        let result = match config.use_ssl {
            Some(true) => {
                if config.server_ca.is_none() {
                    let msg = "\"server_ca\" must be specified when \"use_ssl\" is set".to_string();
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                if config.client_cert.is_none() {
                    let msg = "\"client_cert\" must be specified when \"use_ssl\" is set".to_string();
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                if config.client_key.is_none() {
                    let msg = "\"client_key\" must be specified when \"use_ssl\" is set".to_string();
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                if let Err(err) = builder.set_ca_file(config.server_ca.as_ref().unwrap()) {
                    let msg = format!(
                        "Failed to set the server certificate specified by \"server_ca\": {}. Error: ({})",
                        config.server_ca.as_ref().unwrap(),
                        err
                    );
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                if let Err(err) = builder.set_certificate_file(config.client_cert.as_ref().unwrap(), SslFiletype::PEM) {
                    let msg = format!(
                        "Failed to set the client certificate specified by \"client_cert\": {}. Error: ({})",
                        config.client_cert.as_ref().unwrap(),
                        err
                    );
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }
                if let Err(err) = builder.set_private_key_file(config.client_key.as_ref().unwrap(), SslFiletype::PEM) {
                    let msg = format!("Failed to set the client key specified by \"client_key\": {}. Error: ({})", config.client_key.as_ref().unwrap(), err);
                    return Err(GeyserPluginError::ConfigFileReadError { msg });
                }

                let mut connector = MakeTlsConnector::new(builder.build());
                connector.set_callback(|connect_config, _domain| {
                    connect_config.set_verify_hostname(false);
                    Ok(())
                });
                Client::connect(&connection_str, connector)
            }
            _ => Client::connect(&connection_str, NoTls),
        };

        match result {
            Err(err) => {
                let msg = format!("Error in connecting to the PostgreSQL database: {:?} connection_str: {:?}", err, connection_str);
                error!("{}", msg);
                Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConnectionError { msg })))
            }
            Ok(client) => Ok(client),
        }
    }

    /// Flush any left over accounts in batch which are not processed in the last batch
    fn flush_buffered_writes(&mut self) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        let insert_account_audit_stmt = &client.insert_account_audit_stmt;
        let statement = &client.update_account_stmt;
        let insert_slot_stmt = &client.update_slot_without_parent_stmt;
        let client = &mut client.client;

        for account in self.pending_account_updates.drain(..) {
            Self::upsert_account_internal(&account, statement, client, insert_account_audit_stmt)?;
        }

        let mut measure = Measure::start("geyser-plugin-postgres-flush-slots-us");

        for slot in &self.slots_at_startup {
            Self::upsert_slot_status_internal(*slot, None, SlotStatus::Rooted, client, insert_slot_stmt)?;
        }
        measure.stop();

        datapoint_info!(
            "geyser_plugin_notify_account_restore_from_snapshot_summary",
            ("flush_slots-us", measure.as_us(), i64),
            ("flush-slots-counts", self.slots_at_startup.len(), i64),
        );

        self.slots_at_startup.clear();
        Ok(())
    }
}

impl PostgresClient for SimplePostgresClient {
    fn update_account(&mut self, account: DbAccountInfo, is_startup: bool) -> Result<(), GeyserPluginError> {
        trace!(
            "[update_account] account=[{}] owner=[{}] slot=[{}]",
            bs58::encode(account.pubkey()).into_string(),
            bs58::encode(account.owner()).into_string(),
            account.slot,
        );
        if !is_startup {
            let account_match = self.account_handlers.iter().find(|h| h.account_match(&account));
            return match account_match {
                Some(a) => a.account_update(&mut self.client.get_mut().unwrap().client, &account),
                None => self.upsert_account(&account),
            };
        }

        self.slots_at_startup.insert(account.slot as u64);
        self.insert_accounts_in_batch(account)
    }

    fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<(), GeyserPluginError> {
        info!("[update_slot_status] slot=[{:?}] status=[{:?}]", slot, status);

        let client = self.client.get_mut().unwrap();

        let statement = match parent {
            Some(_) => &client.update_slot_with_parent_stmt,
            None => &client.update_slot_without_parent_stmt,
        };

        Self::upsert_slot_status_internal(slot, parent, status, &mut client.client, statement)
    }

    fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        self.flush_buffered_writes()
    }

    fn log_transaction(&mut self, transaction_info: DbTransaction) -> Result<(), GeyserPluginError> {
        self.log_transaction_impl(transaction_info)
    }

    fn update_block_metadata(&mut self, block_info: DbBlockInfo) -> Result<(), GeyserPluginError> {
        self.update_block_metadata_impl(block_info)
    }
}

pub struct PostgresClientBuilder {}

impl PostgresClientBuilder {
    pub fn build_pararallel_postgres_client(config: &GeyserPluginPostgresConfig) -> Result<(ParallelClient, Option<u64>), GeyserPluginError> {
        let mut client = SimplePostgresClient::connect_to_db(config)?;
        init_account(&mut client, config)?;
        init_slot(&mut client, config)?;
        init_block(&mut client, config)?;
        init_transaction(&mut client, config)?;
        init_account_audit(&mut client, config)?;

        let account_handlers = vec![Box::new(TokenAccountHandler {})];
        let init_query = account_handlers.iter().map(|a| a.init(&mut client, config)).collect::<Vec<String>>().join(",");
        if let Err(err) = client.batch_execute(&init_query) {
            return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
            })));
        };

        let batch_optimize_by_skiping_older_slots = match config.skip_upsert_existing_accounts_at_startup {
            true => {
                let mut on_load_client = SimplePostgresClient::new(config)?;
                // database if populated concurrently so we need to move some number of slots
                // below highest available slot to make sure we do not skip anything that was already in DB.
                let batch_slot_bound = on_load_client.get_highest_available_slot()?.saturating_sub(config.safe_batch_starting_slot_cushion);
                info!("Set batch_optimize_by_skiping_older_slots to {}", batch_slot_bound);
                Some(batch_slot_bound)
            }
            false => None,
        };

        ParallelClient::new(config).map(|v| (v, batch_optimize_by_skiping_older_slots))
    }
}
