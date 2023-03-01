mod account_handler;
mod block_handler;
mod slot_handler;
mod token_account_handler;
mod transaction_handler;
mod unknown_account_handler;

use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;
use crate::parallel_client::ParallelClient;
use crate::postgres_client::block_handler::BlockHandler;
use crate::postgres_client::slot_handler::SlotHandler;
use crate::postgres_client::token_account_handler::TokenAccountHandler;
use log::*;
use openssl::ssl::SslConnector;
use openssl::ssl::SslFiletype;
use openssl::ssl::SslMethod;
use postgres::Client;
use postgres::NoTls;
use postgres_openssl::MakeTlsConnector;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use std::collections::HashSet;
use std::sync::Mutex;
use std::thread;

use self::account_handler::AccountHandler;
pub use self::account_handler::DbAccountInfo;
pub use self::account_handler::ReadableAccountInfo;
pub use self::block_handler::DbBlockInfo;
pub use self::transaction_handler::build_db_transaction;
pub use self::transaction_handler::DbTransaction;
use self::transaction_handler::TransactionHandler;
use self::unknown_account_handler::UnknownAccountHandler;

pub struct SimplePostgresClient {
    batch_size: usize,
    slots_at_startup: HashSet<u64>,
    pending_account_updates: Vec<DbAccountInfo>,
    block_handler: BlockHandler,
    transaction_handler: TransactionHandler,
    account_handlers: Vec<Box<dyn AccountHandler>>,
    client: Mutex<Client>,
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
        let block_handler = BlockHandler::new(&mut client, config)?;
        let transaction_handler = TransactionHandler::new(&mut client, config)?;
        let batch_size = config.batch_size;
        info!("[SimplePostgresClient] created");
        Ok(Self {
            batch_size,
            client: Mutex::new(client),
            block_handler,
            transaction_handler,
            pending_account_updates: Vec::with_capacity(batch_size),
            account_handlers: vec![Box::new(TokenAccountHandler {}), Box::new(UnknownAccountHandler {})],
            slots_at_startup: HashSet::default(),
        })
    }

    pub fn connect_to_db(config: &GeyserPluginPostgresConfig) -> Result<Client, GeyserPluginError> {
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
                Client::connect(&config.connection_str, connector)
            }
            _ => Client::connect(&config.connection_str, NoTls),
        };
        match result {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConnectionError {
                msg: format!("[connect_to_db] connection_str={} error={}", config.connection_str, err),
            }))),
            Ok(client) => Ok(client),
        }
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
        let client = &mut self.client.get_mut().unwrap();
        if is_startup {
            self.slots_at_startup.insert(account.slot as u64);
            // flush if batch size
            if self.pending_account_updates.len() >= self.batch_size {
                let query = self
                    .pending_account_updates
                    .drain(..)
                    .map(|a| self.account_handlers.iter().map(|h| h.account_update(&a)).collect::<Vec<String>>().join(""))
                    .collect::<Vec<String>>()
                    .join("");

                if let Err(err) = client.batch_execute(&query) {
                    return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                        msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
                    })));
                };
            } else {
                self.pending_account_updates.push(account);
                info!("[update_account_batch] length={}/{}", self.pending_account_updates.len(), self.batch_size);
            }
            return Ok(());
        }

        let query = self.account_handlers.iter().map(|h| h.account_update(&account)).collect::<Vec<String>>().join("");
        if !query.is_empty() {
            return match client.batch_execute(&query) {
                Ok(_) => Ok(()),
                Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                    msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
                }))),
            };
        }
        Ok(())
    }

    fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<(), GeyserPluginError> {
        info!("[update_slot_status] slot=[{:?}] status=[{:?}]", slot, status);
        let client = &mut self.client.get_mut().unwrap();
        let query = SlotHandler::update(slot, parent, status);
        if !query.is_empty() {
            return match client.batch_execute(&query) {
                Ok(_) => Ok(()),
                Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                    msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
                }))),
            };
        }

        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        info!("[notify_end_of_startup]");
        // flush accounts
        let client = &mut self.client.get_mut().unwrap();
        let query = self
            .pending_account_updates
            .drain(..)
            .map(|a| self.account_handlers.iter().map(|h| h.account_update(&a)).collect::<Vec<String>>().join(""))
            .collect::<Vec<String>>()
            .join("");
        if let Err(err) = client.batch_execute(&query) {
            return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
            })));
        };

        // flush slots
        let mut measure = Measure::start("geyser-plugin-postgres-flush-slots-us");
        let query = &self
            .slots_at_startup
            .drain()
            .map(|s| SlotHandler::update(s, None, SlotStatus::Rooted))
            .collect::<Vec<String>>()
            .join("");
        if let Err(err) = client.batch_execute(&query) {
            return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
            })));
        };
        measure.stop();

        datapoint_info!(
            "geyser_plugin_notify_account_restore_from_snapshot_summary",
            ("flush_slots-us", measure.as_us(), i64),
            ("flush-slots-counts", self.slots_at_startup.len(), i64),
        );
        Ok(())
    }

    fn log_transaction(&mut self, transaction_info: DbTransaction) -> Result<(), GeyserPluginError> {
        self.transaction_handler.update(&mut self.client.get_mut().unwrap(), transaction_info)
    }

    fn update_block_metadata(&mut self, block_info: DbBlockInfo) -> Result<(), GeyserPluginError> {
        self.block_handler.update(&mut self.client.get_mut().unwrap(), block_info)
    }
}

pub struct PostgresClientBuilder {}

impl PostgresClientBuilder {
    pub fn build_pararallel_postgres_client(config: &GeyserPluginPostgresConfig) -> Result<(ParallelClient, Option<u64>), GeyserPluginError> {
        let mut client = SimplePostgresClient::connect_to_db(config)?;

        let account_handlers: Vec<Box<dyn AccountHandler>> = vec![Box::new(TokenAccountHandler {}), Box::new(UnknownAccountHandler {})];
        let mut init_query = account_handlers.iter().map(|a| a.init(config)).collect::<Vec<String>>().join("");
        init_query.push_str(&SlotHandler::init(config));
        init_query.push_str(&BlockHandler::init(config));
        init_query.push_str(&TransactionHandler::init(config));
        if let Err(err) = client.batch_execute(&init_query) {
            return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[build_pararallel_postgres_client] error=[{}]", err,),
            })));
        };

        let batch_starting_slot = match config.skip_upsert_existing_accounts_at_startup {
            true => {
                let batch_slot_bound = SlotHandler::get_highest_available_slot(&mut client)?.saturating_sub(config.safe_batch_starting_slot_cushion);
                info!("[batch_starting_slot] bound={}", batch_slot_bound);
                Some(batch_slot_bound)
            }
            false => None,
        };

        ParallelClient::new(config).map(|v| (v, batch_starting_slot))
    }
}
