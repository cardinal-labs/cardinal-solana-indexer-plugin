use std::collections::HashMap;

use crate::accounts_selector::AccountHandlerConfig;
use crate::accounts_selector::AccountsSelectorConfig;
use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;

use super::metadata_creators_account_handler::MetadataCreatorsAccountHandler;
use super::token_account_handler::TokenAccountHandler;
use super::token_manager_handler::TokenManagerAccountHandler;
use super::unknown_account_handler::UnknownAccountHandler;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum AccountHandlerId {
    TokenMetadataCreators,
    TokenAccount,
    TokenManager,
    UnknownAccount,
}

impl AccountHandlerId {
    pub fn from_str(input: &str) -> Result<AccountHandlerId, GeyserPluginError> {
        match input {
            "token_metadata_creators" => Ok(AccountHandlerId::TokenMetadataCreators),
            "token_account" => Ok(AccountHandlerId::TokenAccount),
            "token_manager" => Ok(AccountHandlerId::TokenManager),
            "unknown_account" => Ok(AccountHandlerId::UnknownAccount),
            _ => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[AccountHandlerId] error=[Invalid account handler id]"),
            }))),
        }
    }
}

pub fn all_account_handlers() -> HashMap<AccountHandlerId, Box<dyn AccountHandler>> {
    let mut account_handlers: HashMap<AccountHandlerId, Box<dyn AccountHandler>> = HashMap::default();
    account_handlers.insert(AccountHandlerId::TokenAccount, Box::new(TokenAccountHandler {}));
    account_handlers.insert(AccountHandlerId::TokenMetadataCreators, Box::new(MetadataCreatorsAccountHandler {}));
    account_handlers.insert(AccountHandlerId::TokenManager, Box::new(TokenManagerAccountHandler {}));
    account_handlers.insert(AccountHandlerId::UnknownAccount, Box::new(UnknownAccountHandler {}));
    return account_handlers;
}

pub fn select_account_handlers(account_selector: &Option<AccountsSelectorConfig>, account: &DbAccountInfo, is_startup: bool) -> Vec<AccountHandlerConfig> {
    let account_key = bs58::encode(&account.pubkey).into_string();
    let owner_key = bs58::encode(&account.owner).into_string();
    // get selected handlers from config
    let mut selected_handlers = Vec::new();
    if let Some(selector) = &account_selector {
        // add with any account specific handlers
        if let Some(accounts) = &selector.accounts {
            if let Some(handlers) = accounts.get(&account_key) {
                selected_handlers = handlers.to_vec();
            }
        }
        // get account owner handlers
        if let Some(owners) = &selector.owners {
            if let Some(handlers) = owners.get(&owner_key) {
                selected_handlers = handlers.to_vec();
            }
        }
    };
    selected_handlers.into_iter().filter(|h| !is_startup || !h.skip_on_startup.unwrap_or(false)).collect()
}

pub trait AccountHandler {
    fn enabled(&self, _config: &GeyserPluginPostgresConfig) -> bool {
        true
    }

    fn init(&self, config: &GeyserPluginPostgresConfig) -> String;

    fn account_match(&self, account: &DbAccountInfo) -> bool;

    fn account_update(&self, account: &DbAccountInfo) -> String;
}

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
    pub fn new(account: &ReplicaAccountInfo, slot: u64) -> DbAccountInfo {
        let data = account.data.to_vec();
        Self {
            pubkey: account.pubkey.to_vec(),
            lamports: account.lamports as i64,
            owner: account.owner.to_vec(),
            executable: account.executable,
            rent_epoch: account.rent_epoch as i64,
            data,
            slot: slot as i64,
            write_version: account.write_version as i64,
            txn_signature: None,
        }
    }
}
