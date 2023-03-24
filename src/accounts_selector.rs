use log::*;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;

/// * The `accounts_selector` section allows the user to controls accounts selections.
/// "accounts_selector" : {
///     "accounts" : \[{ handler_id: 'metadata-creators', skip_on_startup: true }, ..., { handler_id: 'metadata-attributes', skip_on_startup: false}],
/// }
/// or:
///
///
/// "accounts_selector" = {
///     "owners" : \ [{ handler_id: 'metadata-creators', skip_on_startup: true }, ..., { handler_id: 'metadata-attributes', skip_on_startup: false}]
/// }
/// Accounts either satisyfing the accounts condition or owners condition will be selected.
/// When only owners is specified,
/// all accounts belonging to the owners will be streamed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountsSelectorConfig {
    pub accounts: Option<HashMap<String, Vec<AccountHandlerConfig>>>,
    pub owners: Option<HashMap<String, Vec<AccountHandlerConfig>>>,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountHandlerConfig {
    pub handler_id: String,
    pub skip_on_startup: Option<bool>,
}

#[derive(Debug, Default)]
pub(crate) struct AccountsSelector {
    pub accounts: HashSet<Vec<u8>>,
    pub owners: HashSet<Vec<u8>>,
}

impl AccountsSelector {
    pub fn new(config: &AccountsSelectorConfig) -> Self {
        info!("[accounts_selector] accounts=[{:?}] owners=[{:?}]", config.accounts, config.owners);
        let owners = match &config.owners {
            Some(owners) => owners.iter().map(|(key, _)| bs58::decode(key).into_vec().unwrap()).collect(),
            None => HashSet::default(),
        };
        let accounts = match &config.accounts {
            Some(accounts) => accounts.iter().map(|(key, _)| bs58::decode(key).into_vec().unwrap()).collect(),
            None => HashSet::default(),
        };
        AccountsSelector { accounts, owners }
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.accounts.contains(account) || self.owners.contains(owner)
    }

    pub fn is_enabled(&self) -> bool {
        !self.accounts.is_empty() || !self.owners.is_empty()
    }
}
