use super::account_handler::AccountHandler;
use super::DbAccountInfo;
use anchor_lang::idl::IdlAccount;
use anchor_lang::AnchorDeserialize;
use anchor_syn::idl::Idl;
use chrono::Utc;
use flate2::read::ZlibDecoder;
use futures::future::join_all;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;
use std::sync::Mutex;

pub struct IdlAccountHandler {
    idls: Mutex<HashMap<String, Idl>>,
}

async fn fetch_idl(client: RpcClient, program_id: Pubkey) -> Result<Idl, String> {
    let idl_addr = IdlAccount::address(&program_id);
    let response = match client.get_account_with_commitment(&idl_addr, CommitmentConfig::processed()) {
        Ok(a) => a,
        Err(e) => return Err(format!("[fetch_idl] Failed to fetch response error=[{}]", e)),
    };
    let account = match response.value {
        Some(a) => a,
        _ => return Err(format!("[fetch_idl] Failed to get account error=[{}]", e)),
    };

    // Cut off account discriminator.
    let mut d: &[u8] = &account.data[8..];
    let idl_account: IdlAccount = match AnchorDeserialize::deserialize(&mut d) {
        Ok(acc) => acc,
        Err(e) => return Err(format!("[fetch_idl] Failed to deserialize idl error=[{:?}]", e)),
    };

    let compressed_len: usize = idl_account.data.len().try_into().unwrap();
    let compressed_bytes = &account.data[44..44 + compressed_len];
    let mut z = ZlibDecoder::new(compressed_bytes);
    let mut s = Vec::new();

    match z.read_to_end(&mut s) {
        Ok(_) => {}
        Err(e) => return Err(format!("[fetch_idl] Failed to read to end error=[{}]", e)),
    };

    match serde_json::from_slice(&s[..]).map_err(Into::into) {
        Ok(idl) => idl,
        Err(e) => Err(format!("[fetch_idl] Failed to fetch idl error=[{}]", e)),
    }
}

impl AccountHandler for IdlAccountHandler {
    fn id(&self) -> String {
        "account".to_string()
    }

    fn init(&self, config: &crate::config::GeyserPluginPostgresConfig) -> String {
        if !self.enabled(config) {
            return "".to_string();
        };

        let idl_program_ids: [String; 2] = ["mgr99QFMYByTqGPWmNqunV7vBLmWWXdSrHUfV8Jf3JM".to_string(), "crcBwD7wUjzwsy8tJsVCzZvBTHeq5GoboGg84YraRyd".to_string()];
        let mut q = "".to_string();
        let rpc_client = RpcClient::new("url".to_string());

        return join_all(idl_program_ids.iter().map(|program_id| async {
            let idl = match fetch_idl(rpc_client, Pubkey::from_str(program_id).unwrap()).await {
                Ok(idl) => idl,
                Err(_) => return "".to_string(),
            };
            self.idls.get_mut().unwrap().insert(program_id.to_string(), idl);
            "".to_string()
        }))
        .await
        .join("");
    }

    fn account_match(&self, account: &DbAccountInfo) -> bool {
        self.idls.get_mut().unwrap().contains_key(&Pubkey::new(&account.owner).to_string())
    }

    fn account_update(&self, account: &DbAccountInfo) -> String {
        if !self.account_match(account) {
            return "".to_string();
        };
        return format!(
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
        );
    }
}
