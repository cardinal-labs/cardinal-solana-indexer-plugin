use super::account_handler::AccountHandler;
use super::DbAccountInfo;
use chrono::Utc;

#[derive(Clone, Copy)]
pub struct UnknownAccountHandler {}

impl AccountHandler for UnknownAccountHandler {
    fn init(&self, config: &crate::config::GeyserPluginPostgresConfig) -> String {
        if !self.enabled(config) {
            return "".to_string();
        };
        return "
            CREATE TABLE IF NOT EXISTS account (
                pubkey BYTEA PRIMARY KEY,
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
            CREATE INDEX IF NOT EXISTS account_owner ON account (owner);
            CREATE INDEX IF NOT EXISTS account_slot ON account (slot);
        "
        .to_string();
    }

    fn account_match(&self, _account: &DbAccountInfo) -> bool {
        true
    }

    fn account_update(&self, account: &DbAccountInfo) -> String {
        if !self.account_match(account) {
            return "".to_string();
        };
        format!(
            "
                INSERT INTO account AS acct (pubkey, slot, owner, lamports, executable, rent_epoch, data, write_version, updated_on, txn_signature) \
                VALUES ('\\x{0}', {1}, '\\x{2}', {3}, {4}, {5}, '\\x{6}', {7}, '{8}', {9}) \
                ON CONFLICT (pubkey) DO UPDATE SET
                    slot=excluded.slot, owner=excluded.owner, lamports=excluded.lamports, \
                    executable=excluded.executable, rent_epoch=excluded.rent_epoch, \
                    data=excluded.data, write_version=excluded.write_version, updated_on=excluded.updated_on, \
                    txn_signature=excluded.txn_signature \
                WHERE acct.slot < excluded.slot OR (acct.slot = excluded.slot AND acct.write_version < excluded.write_version);
            ",
            hex::encode(&account.pubkey),
            &account.slot,
            hex::encode(&account.owner),
            &account.lamports,
            &account.executable,
            &account.rent_epoch,
            hex::encode(&account.data),
            &account.write_version,
            &Utc::now().naive_utc(),
            account.txn_signature.as_deref().map_or("NULL".to_string(), |tx| format!("'\\x{}'", hex::encode(tx))),
        )
    }
}
