use log::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountsSelectorConfig {
    accounts: Option<Vec<String>>,
    owners: Option<Vec<String>>,
}

#[derive(Debug, Default)]
pub(crate) struct AccountsSelector {
    pub accounts: HashSet<Vec<u8>>,
    pub owners: HashSet<Vec<u8>>,
    pub select_all_accounts: bool,
}

impl AccountsSelector {
    pub fn new(config: &AccountsSelectorConfig) -> Self {
        info!("[accounts_selector] accounts=[{:?}] owners=[{:?}]", config.accounts, config.owners);
        let select_all_accounts = match &config.accounts {
            Some(accounts) => accounts.iter().any(|key| key == "*"),
            None => false,
        };
        if select_all_accounts {
            return AccountsSelector {
                accounts: HashSet::default(),
                owners: HashSet::default(),
                select_all_accounts,
            };
        }
        let owners = match &config.owners {
            Some(owners) => owners.iter().map(|key| bs58::decode(key).into_vec().unwrap()).collect(),
            None => HashSet::default(),
        };
        let accounts = match &config.accounts {
            Some(accounts) => accounts.iter().map(|key| bs58::decode(key).into_vec().unwrap()).collect(),
            None => HashSet::default(),
        };
        AccountsSelector {
            accounts,
            owners,
            select_all_accounts,
        }
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.select_all_accounts || self.accounts.contains(account) || self.owners.contains(owner)
    }

    pub fn is_enabled(&self) -> bool {
        self.select_all_accounts || !self.accounts.is_empty() || !self.owners.is_empty()
    }
}
