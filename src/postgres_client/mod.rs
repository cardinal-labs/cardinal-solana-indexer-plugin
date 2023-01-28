#![allow(clippy::integer_arithmetic)]

mod postgres_client_account_index;
mod postgres_client_block_metadata;
mod postgres_client_transaction;

use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;
use crate::parallel_client::ParallelPostgresClient;
use crate::parallel_client_worker::DbWorkItem;
use crate::postgres_client::postgres_client_account_index::TokenSecondaryIndexEntry;
use chrono::Utc;
use log::*;
use openssl::ssl::SslConnector;
use openssl::ssl::SslFiletype;
use openssl::ssl::SslMethod;
use postgres::Client;
use postgres::NoTls;
use postgres::Statement;
use postgres_openssl::MakeTlsConnector;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_measure::measure::Measure;
use solana_metrics::*;
use std::collections::HashSet;
use std::sync::Mutex;
use std::thread::{self};
use tokio_postgres::types;

use self::postgres_client_transaction::DbReward;
use self::postgres_client_transaction::DbTransaction;

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const SAFE_BATCH_STARTING_SLOT_CUSHION: u64 = 2 * 40960;
const ACCOUNT_COLUMN_COUNT: usize = 10;

struct PostgresSqlClientWrapper {
    client: Client,
    update_account_stmt: Statement,
    bulk_account_insert_stmt: Statement,
    update_slot_with_parent_stmt: Statement,
    update_slot_without_parent_stmt: Statement,
    update_transaction_log_stmt: Statement,
    update_block_metadata_stmt: Statement,
    insert_account_audit_stmt: Option<Statement>,
    insert_token_owner_index_stmt: Option<Statement>,
    insert_token_mint_index_stmt: Option<Statement>,
    bulk_insert_token_owner_index_stmt: Option<Statement>,
    bulk_insert_token_mint_index_stmt: Option<Statement>,
}

pub struct SimplePostgresClient {
    batch_size: usize,
    slots_at_startup: HashSet<u64>,
    pending_account_updates: Vec<DbAccountInfo>,
    index_token_owner: bool,
    index_token_mint: bool,
    pending_token_owner_index: Vec<TokenSecondaryIndexEntry>,
    pending_token_mint_index: Vec<TokenSecondaryIndexEntry>,
    client: Mutex<PostgresSqlClientWrapper>,
}

impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
    pub write_version: i64,
    pub txn_signature: Option<Vec<u8>>,
}

pub(crate) fn abort() -> ! {
    #[cfg(not(test))]
    {
        // standard error is usually redirected to a log file, cry for help on standard output as well
        eprintln!("Validator process aborted. The validator log may contain further details");
        std::process::exit(1);
    }

    #[cfg(test)]
    panic!("process::exit(1) is intercepted for friendly test failure...");
}

impl DbAccountInfo {
    pub fn new<T: ReadableAccountInfo>(account: &T, slot: u64) -> DbAccountInfo {
        let data = account.data().to_vec();
        Self {
            pubkey: account.pubkey().to_vec(),
            lamports: account.lamports() as i64,
            owner: account.owner().to_vec(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch() as i64,
            data,
            slot: slot as i64,
            write_version: account.write_version(),
            txn_signature: account.txn_signature().map(|v| v.to_vec()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DbBlockInfo {
    pub slot: i64,
    pub blockhash: String,
    pub rewards: Vec<DbReward>,
    pub block_time: Option<i64>,
    pub block_height: Option<i64>,
}

impl<'a> From<&ReplicaBlockInfo<'a>> for DbBlockInfo {
    fn from(block_info: &ReplicaBlockInfo) -> Self {
        Self {
            slot: block_info.slot as i64,
            blockhash: block_info.blockhash.to_string(),
            rewards: block_info.rewards.iter().map(DbReward::from).collect(),
            block_time: block_info.block_time,
            block_height: block_info.block_height.map(|block_height| block_height as i64),
        }
    }
}

pub trait ReadableAccountInfo: Sized {
    fn pubkey(&self) -> &[u8];
    fn owner(&self) -> &[u8];
    fn lamports(&self) -> i64;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> i64;
    fn data(&self) -> &[u8];
    fn write_version(&self) -> i64;
    fn txn_signature(&self) -> Option<&[u8]>;
}

impl ReadableAccountInfo for DbAccountInfo {
    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn owner(&self) -> &[u8] {
        &self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn write_version(&self) -> i64 {
        self.write_version
    }

    fn txn_signature(&self) -> Option<&[u8]> {
        self.txn_signature.as_deref()
    }
}

impl<'a> ReadableAccountInfo for ReplicaAccountInfo<'a> {
    fn pubkey(&self) -> &[u8] {
        self.pubkey
    }

    fn owner(&self) -> &[u8] {
        self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports as i64
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch as i64
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn write_version(&self) -> i64 {
        self.write_version as i64
    }

    fn txn_signature(&self) -> Option<&[u8]> {
        None
    }
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
    pub fn connect_to_db(config: &GeyserPluginPostgresConfig) -> Result<Client, GeyserPluginError> {
        let connection_str = if let Some(connection_str) = &config.connection_str {
            connection_str.clone()
        } else {
            if config.host.is_none() || config.user.is_none() {
                let msg = format!(
                    "\"connection_str\": {:?}, or \"host\": {:?} \"user\": {:?} must be specified",
                    config.connection_str, config.host, config.user
                );
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            format!("host={} user={} port={}", config.host.as_ref().unwrap(), config.user.as_ref().unwrap(), config.port)
        };

        let result = if let Some(true) = config.use_ssl {
            if config.server_ca.is_none() {
                let msg = "\"server_ca\" must be specified when \"use_ssl\" is set".to_string();
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            if config.client_cert.is_none() {
                let msg = "\"client_cert\" must be specified when \"use_ssl\" is set".to_string();
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            if config.client_key.is_none() {
                let msg = "\"client_key\" must be specified when \"use_ssl\" is set".to_string();
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
            if let Err(err) = builder.set_ca_file(config.server_ca.as_ref().unwrap()) {
                let msg = format!(
                    "Failed to set the server certificate specified by \"server_ca\": {}. Error: ({})",
                    config.server_ca.as_ref().unwrap(),
                    err
                );
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            if let Err(err) = builder.set_certificate_file(config.client_cert.as_ref().unwrap(), SslFiletype::PEM) {
                let msg = format!(
                    "Failed to set the client certificate specified by \"client_cert\": {}. Error: ({})",
                    config.client_cert.as_ref().unwrap(),
                    err
                );
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }
            if let Err(err) = builder.set_private_key_file(config.client_key.as_ref().unwrap(), SslFiletype::PEM) {
                let msg = format!("Failed to set the client key specified by \"client_key\": {}. Error: ({})", config.client_key.as_ref().unwrap(), err);
                return Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::ConfigurationError { msg })));
            }

            let mut connector = MakeTlsConnector::new(builder.build());
            connector.set_callback(|connect_config, _domain| {
                connect_config.set_verify_hostname(false);
                Ok(())
            });
            Client::connect(&connection_str, connector)
        } else {
            Client::connect(&connection_str, NoTls)
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

    fn build_bulk_account_insert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let mut stmt = String::from("INSERT INTO account AS acct (pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on, txn_signature) VALUES");
        for j in 0..config.batch_size {
            let row = j * ACCOUNT_COLUMN_COUNT;
            let val_str = format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                row + 1,
                row + 2,
                row + 3,
                row + 4,
                row + 5,
                row + 6,
                row + 7,
                row + 8,
                row + 9,
                row + 10,
            );

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        let handle_conflict =
            "ON CONFLICT (pubkey) DO UPDATE SET slot=excluded.slot, owner=excluded.owner, lamports=excluded.lamports, executable=excluded.executable, rent_epoch=excluded.rent_epoch, \
            data=excluded.data, write_version=excluded.write_version, updated_on=excluded.updated_on, txn_signature=excluded.txn_signature WHERE acct.slot < excluded.slot OR (\
            acct.slot = excluded.slot AND acct.write_version < excluded.write_version)";

        stmt = format!("{} {}", stmt, handle_conflict);

        info!("{}", stmt);
        let bulk_stmt = client.prepare(&stmt);

        match bulk_stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the accounts update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    fn build_single_account_upsert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO account AS acct (pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on, txn_signature) \
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
        ON CONFLICT (pubkey) DO UPDATE SET slot=excluded.slot, owner=excluded.owner, lamports=excluded.lamports, executable=excluded.executable, rent_epoch=excluded.rent_epoch, \
        data=excluded.data, write_version=excluded.write_version, updated_on=excluded.updated_on, txn_signature=excluded.txn_signature  WHERE acct.slot < excluded.slot OR (\
        acct.slot = excluded.slot AND acct.write_version < excluded.write_version)";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the accounts update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    fn prepare_query_statement(client: &mut Client, config: &GeyserPluginPostgresConfig, stmt: &str) -> Result<Statement, GeyserPluginError> {
        let statement = client.prepare(stmt);

        match statement {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the statement {} for PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    stmt, err, config.host, config.user, config
                ),
            }))),
            Ok(statement) => Ok(statement),
        }
    }

    fn build_account_audit_insert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO account_audit (pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on, txn_signature) \
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the account_audit update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(stmt) => Ok(stmt),
        }
    }

    fn build_slot_upsert_statement_with_parent(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO slot (slot, parent, status, updated_on) \
        VALUES ($1, $2, $3, $4) \
        ON CONFLICT (slot) DO UPDATE SET parent=excluded.parent, status=excluded.status, updated_on=excluded.updated_on";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the slot update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(stmt) => Ok(stmt),
        }
    }

    fn build_slot_upsert_statement_without_parent(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO slot (slot, status, updated_on) \
        VALUES ($1, $2, $3) \
        ON CONFLICT (slot) DO UPDATE SET status=excluded.status, updated_on=excluded.updated_on";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the slot update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(stmt) => Ok(stmt),
        }
    }

    /// Internal function for inserting an account into account_audit table.
    fn insert_account_audit(account: &DbAccountInfo, statement: &Statement, client: &mut Client) -> Result<(), GeyserPluginError> {
        let lamports = account.lamports() as i64;
        let rent_epoch = account.rent_epoch() as i64;
        let updated_on = Utc::now().naive_utc();
        let result = client.execute(
            statement,
            &[
                &account.pubkey(),
                &account.slot,
                &account.owner(),
                &lamports,
                &account.executable(),
                &rent_epoch,
                &account.data(),
                &account.write_version(),
                &updated_on,
                &account.txn_signature(),
            ],
        );

        if let Err(err) = result {
            let msg = format!("Failed to persist the insert of account_audit to the PostgreSQL database. Error: {:?}", err);
            error!("{}", msg);
            return Err(GeyserPluginError::AccountsUpdateError { msg });
        }
        Ok(())
    }

    /// Internal function for updating or inserting a single account
    fn upsert_account_internal(
        account: &DbAccountInfo,
        statement: &Statement,
        client: &mut Client,
        insert_account_audit_stmt: &Option<Statement>,
        insert_token_owner_index_stmt: &Option<Statement>,
        insert_token_mint_index_stmt: &Option<Statement>,
    ) -> Result<(), GeyserPluginError> {
        let lamports = account.lamports() as i64;
        let rent_epoch = account.rent_epoch() as i64;
        let updated_on = Utc::now().naive_utc();
        let result = client.execute(
            statement,
            &[
                &account.pubkey(),
                &account.slot,
                &account.owner(),
                &lamports,
                &account.executable(),
                &rent_epoch,
                &account.data(),
                &account.write_version(),
                &updated_on,
                &account.txn_signature(),
            ],
        );

        if let Err(err) = result {
            let msg = format!("Failed to persist the update of account to the PostgreSQL database. Error: {:?}", err);
            error!("{}", msg);
            return Err(GeyserPluginError::AccountsUpdateError { msg });
        } else if result.unwrap() == 0 && insert_account_audit_stmt.is_some() {
            // If no records modified (inserted or updated), it is because the account is updated
            // at an older slot, insert the record directly into the account_audit table.
            let statement = insert_account_audit_stmt.as_ref().unwrap();
            Self::insert_account_audit(account, statement, client)?;
        }

        if let Some(insert_token_owner_index_stmt) = insert_token_owner_index_stmt {
            Self::update_token_owner_index(client, insert_token_owner_index_stmt, account)?;
        }

        if let Some(insert_token_mint_index_stmt) = insert_token_mint_index_stmt {
            Self::update_token_mint_index(client, insert_token_mint_index_stmt, account)?;
        }

        Ok(())
    }

    /// Update or insert a single account
    fn upsert_account(&mut self, account: &DbAccountInfo) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        let insert_account_audit_stmt = &client.insert_account_audit_stmt;
        let statement = &client.update_account_stmt;
        let insert_token_owner_index_stmt = &client.insert_token_owner_index_stmt;
        let insert_token_mint_index_stmt = &client.insert_token_mint_index_stmt;
        let client = &mut client.client;
        Self::upsert_account_internal(account, statement, client, insert_account_audit_stmt, insert_token_owner_index_stmt, insert_token_mint_index_stmt)?;

        Ok(())
    }

    /// Insert accounts in batch to reduce network overhead
    fn insert_accounts_in_batch(&mut self, account: DbAccountInfo) -> Result<(), GeyserPluginError> {
        self.queue_secondary_indexes(&account);
        self.pending_account_updates.push(account);

        self.bulk_insert_accounts()?;
        self.bulk_insert_token_owner_index()?;
        self.bulk_insert_token_mint_index()
    }

    fn bulk_insert_accounts(&mut self) -> Result<(), GeyserPluginError> {
        if self.pending_account_updates.len() == self.batch_size {
            let mut measure = Measure::start("geyser-plugin-postgres-prepare-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> = Vec::with_capacity(self.batch_size * ACCOUNT_COLUMN_COUNT);
            let updated_on = Utc::now().naive_utc();
            for j in 0..self.batch_size {
                let account = &self.pending_account_updates[j];

                values.push(&account.pubkey);
                values.push(&account.slot);
                values.push(&account.owner);
                values.push(&account.lamports);
                values.push(&account.executable);
                values.push(&account.rent_epoch);
                values.push(&account.data);
                values.push(&account.write_version);
                values.push(&updated_on);
                values.push(&account.txn_signature);
            }
            measure.stop();
            inc_new_counter_debug!("geyser-plugin-postgres-prepare-values-us", measure.as_us() as usize, 10000, 10000);

            let mut measure = Measure::start("geyser-plugin-postgres-update-account");
            let client = self.client.get_mut().unwrap();
            let result = client.client.query(&client.bulk_account_insert_stmt, &values);

            self.pending_account_updates.clear();

            if let Err(err) = result {
                let msg = format!("Failed to persist the update of account to the PostgreSQL database. Error: {:?}", err);
                error!("{}", msg);
                return Err(GeyserPluginError::AccountsUpdateError { msg });
            }

            measure.stop();
            inc_new_counter_debug!("geyser-plugin-postgres-update-account-us", measure.as_us() as usize, 10000, 10000);
            inc_new_counter_debug!("geyser-plugin-postgres-update-account-count", self.batch_size, 10000, 10000);
        }
        Ok(())
    }

    /// Flush any left over accounts in batch which are not processed in the last batch
    fn flush_buffered_writes(&mut self) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        let insert_account_audit_stmt = &client.insert_account_audit_stmt;
        let statement = &client.update_account_stmt;
        let insert_token_owner_index_stmt = &client.insert_token_owner_index_stmt;
        let insert_token_mint_index_stmt = &client.insert_token_mint_index_stmt;
        let insert_slot_stmt = &client.update_slot_without_parent_stmt;
        let client = &mut client.client;

        for account in self.pending_account_updates.drain(..) {
            Self::upsert_account_internal(&account, statement, client, insert_account_audit_stmt, insert_token_owner_index_stmt, insert_token_mint_index_stmt)?;
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
        self.clear_buffered_indexes();
        Ok(())
    }

    fn upsert_slot_status_internal(slot: u64, parent: Option<u64>, status: SlotStatus, client: &mut Client, statement: &Statement) -> Result<(), GeyserPluginError> {
        let slot = slot as i64; // postgres only supports i64
        let parent = parent.map(|parent| parent as i64);
        let updated_on = Utc::now().naive_utc();
        let status_str = status.as_str();

        let result = match parent {
            Some(parent) => client.execute(statement, &[&slot, &parent, &status_str, &updated_on]),
            None => client.execute(statement, &[&slot, &status_str, &updated_on]),
        };

        match result {
            Err(err) => {
                let msg = format!("Failed to persist the update of slot to the PostgreSQL database. Error: {:?}", err);
                error!("{:?}", msg);
                return Err(GeyserPluginError::SlotStatusUpdateError { msg });
            }
            Ok(rows) => {
                assert_eq!(1, rows, "Expected one rows to be updated a time");
            }
        }

        Ok(())
    }

    pub fn new(config: &GeyserPluginPostgresConfig) -> Result<Self, GeyserPluginError> {
        info!("Creating SimplePostgresClient...");
        let mut client = Self::connect_to_db(config)?;
        let bulk_account_insert_stmt = Self::build_bulk_account_insert_statement(&mut client, config)?;
        let update_account_stmt = Self::build_single_account_upsert_statement(&mut client, config)?;

        let update_slot_with_parent_stmt = Self::build_slot_upsert_statement_with_parent(&mut client, config)?;
        let update_slot_without_parent_stmt = Self::build_slot_upsert_statement_without_parent(&mut client, config)?;
        let update_transaction_log_stmt = Self::build_transaction_info_upsert_statement(&mut client, config)?;
        let update_block_metadata_stmt = Self::build_block_metadata_upsert_statement(&mut client, config)?;

        let batch_size = config.batch_size;

        let insert_account_audit_stmt = if config.store_account_historical_data {
            let stmt = Self::build_account_audit_insert_statement(&mut client, config)?;
            Some(stmt)
        } else {
            None
        };

        let bulk_insert_token_owner_index_stmt = if let Some(true) = config.index_token_owner {
            let stmt = Self::build_bulk_token_owner_index_insert_statement(&mut client, config)?;
            Some(stmt)
        } else {
            None
        };

        let bulk_insert_token_mint_index_stmt = if let Some(true) = config.index_token_mint {
            let stmt = Self::build_bulk_token_mint_index_insert_statement(&mut client, config)?;
            Some(stmt)
        } else {
            None
        };

        let insert_token_owner_index_stmt = if let Some(true) = config.index_token_owner {
            Some(Self::build_single_token_owner_index_upsert_statement(&mut client, config)?)
        } else {
            None
        };

        let insert_token_mint_index_stmt = if let Some(true) = config.index_token_mint {
            Some(Self::build_single_token_mint_index_upsert_statement(&mut client, config)?)
        } else {
            None
        };

        info!("Created SimplePostgresClient.");
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
                insert_token_owner_index_stmt,
                insert_token_mint_index_stmt,
                bulk_insert_token_owner_index_stmt,
                bulk_insert_token_mint_index_stmt,
            }),
            index_token_owner: config.index_token_owner.unwrap_or_default(),
            index_token_mint: config.index_token_mint.unwrap_or(false),
            pending_token_owner_index: Vec::with_capacity(batch_size),
            pending_token_mint_index: Vec::with_capacity(batch_size),
            slots_at_startup: HashSet::default(),
        })
    }

    pub fn get_highest_available_slot(&mut self) -> Result<u64, GeyserPluginError> {
        let client = self.client.get_mut().unwrap();

        let last_slot_query = "SELECT slot FROM slot ORDER BY slot DESC LIMIT 1;";

        let result = client.client.query_opt(last_slot_query, &[]);
        match result {
            Ok(opt_slot) => Ok(opt_slot
                .map(|row| {
                    let raw_slot: i64 = row.get(0);
                    raw_slot as u64
                })
                .unwrap_or(0)),
            Err(err) => {
                let msg = format!("Failed to receive last slot from PostgreSQL database. Error: {:?}", err);
                error!("{}", msg);
                Err(GeyserPluginError::AccountsUpdateError { msg })
            }
        }
    }
}

impl PostgresClient for SimplePostgresClient {
    fn update_account(&mut self, account: DbAccountInfo, is_startup: bool) -> Result<(), GeyserPluginError> {
        trace!(
            "Updating account {} with owner {} at slot {}",
            bs58::encode(account.pubkey()).into_string(),
            bs58::encode(account.owner()).into_string(),
            account.slot,
        );
        if !is_startup {
            return self.upsert_account(&account);
        }

        self.slots_at_startup.insert(account.slot as u64);
        self.insert_accounts_in_batch(account)
    }

    fn update_slot_status(&mut self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<(), GeyserPluginError> {
        info!("Updating slot {:?} at with status {:?}", slot, status);

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

pub struct UpdateAccountRequest {
    pub account: DbAccountInfo,
    pub is_startup: bool,
}

pub struct UpdateSlotRequest {
    pub slot: u64,
    pub parent: Option<u64>,
    pub slot_status: SlotStatus,
}

pub struct LogTransactionRequest {
    pub transaction_info: DbTransaction,
}

pub struct UpdateBlockMetadataRequest {
    pub block_info: DbBlockInfo,
}

pub struct PostgresClientBuilder {}

impl PostgresClientBuilder {
    pub fn build_pararallel_postgres_client(config: &GeyserPluginPostgresConfig) -> Result<(ParallelPostgresClient, Option<u64>), GeyserPluginError> {
        let batch_optimize_by_skiping_older_slots = match config.skip_upsert_existing_accounts_at_startup {
            true => {
                let mut on_load_client = SimplePostgresClient::new(config)?;

                // database if populated concurrently so we need to move some number of slots
                // below highest available slot to make sure we do not skip anything that was already in DB.
                let batch_slot_bound = on_load_client.get_highest_available_slot()?.saturating_sub(SAFE_BATCH_STARTING_SLOT_CUSHION);
                info!("Set batch_optimize_by_skiping_older_slots to {}", batch_slot_bound);
                Some(batch_slot_bound)
            }
            false => None,
        };

        ParallelPostgresClient::new(config).map(|v| (v, batch_optimize_by_skiping_older_slots))
    }
}
