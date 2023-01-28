use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;

use super::SimplePostgresClient;
use chrono::Utc;
use log::*;
use postgres::types;
use postgres::Client;
use postgres::Statement;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_measure::measure::Measure;
use solana_metrics::create_counter;
use solana_metrics::inc_counter;
use solana_metrics::inc_new_counter;
use solana_metrics::inc_new_counter_debug;

pub const ACCOUNT_COLUMN_COUNT: usize = 10;

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

impl SimplePostgresClient {
    pub(crate) fn build_bulk_account_insert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
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

    pub(crate) fn build_single_account_upsert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
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

    /// Internal function for updating or inserting a single account
    pub(crate) fn upsert_account_internal(
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
    pub(crate) fn upsert_account(&mut self, account: &DbAccountInfo) -> Result<(), GeyserPluginError> {
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
    pub(crate) fn insert_accounts_in_batch(&mut self, account: DbAccountInfo) -> Result<(), GeyserPluginError> {
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
}
