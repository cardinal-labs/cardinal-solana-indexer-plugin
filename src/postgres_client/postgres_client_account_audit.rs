use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;

use super::DbAccountInfo;
use super::ReadableAccountInfo;
use super::SimplePostgresClient;
use chrono::Utc;
use log::*;
use postgres::Client;
use postgres::Statement;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;

pub fn init_account_audit(client: &mut Client, _config: &GeyserPluginPostgresConfig) -> Result<(), GeyserPluginError> {
    let result = client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS account_audit (
            pubkey BYTEA,
            owner BYTEA,
            lamports BIGINT NOT NULL,
            slot BIGINT NOT NULL,
            executable BOOL NOT NULL,
            rent_epoch BIGINT NOT NULL,
            data BYTEA,
            write_version BIGINT NOT NULL,
            updated_on TIMESTAMP NOT NULL,
            txn_signature BYTEA
        );
        CREATE INDEX IF NOT EXISTS account_audit_account_key ON  account_audit (pubkey, write_version);
        CREATE INDEX IF NOT EXISTS account_audit_pubkey_slot ON account_audit (pubkey, slot);
        
        DO $$ BEGIN
            CREATE FUNCTION audit_account_update() RETURNS trigger AS $audit_account_update$
                BEGIN
                    INSERT INTO account_audit (pubkey, owner, lamports, slot, executable,
                                            rent_epoch, data, write_version, updated_on, txn_signature)
                        VALUES (OLD.pubkey, OLD.owner, OLD.lamports, OLD.slot,
                                OLD.executable, OLD.rent_epoch, OLD.data,
                                OLD.write_version, OLD.updated_on, OLD.txn_signature);
                    RETURN NEW;
                END;
            $audit_account_update$ LANGUAGE plpgsql;
            
            exception
                when duplicate_function then
                null;
        END $$;

        CREATE OR REPLACE TRIGGER account_update_trigger AFTER UPDATE OR DELETE ON account
            FOR EACH ROW EXECUTE PROCEDURE audit_account_update();

        ",
    );
    match result {
        Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
            msg: format!("[init_account_audit] error={:?}", err),
        }))),
        Ok(_) => Ok(()),
    }
}

impl SimplePostgresClient {
    pub(crate) fn build_account_audit_insert_statement(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
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

    /// Internal function for inserting an account into account_audit table.
    pub(crate) fn insert_account_audit(account: &DbAccountInfo, statement: &Statement, client: &mut Client) -> Result<(), GeyserPluginError> {
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
}
