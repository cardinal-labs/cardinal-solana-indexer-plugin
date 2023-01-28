use log::*;
use serde::Deserialize;
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

/// "transaction_selector" : {
///     "mentions" : \["pubkey-1", "pubkey-2", ..., "pubkey-n"\],
/// }
/// The `mentions` field support wildcard to select all transaction or all 'vote' transactions:
/// For example, to select all transactions:
/// "transaction_selector" : {
///     "mentions" : \["*"\],
/// }
/// To select all vote transactions:
/// "transaction_selector" : {
///     "mentions" : \["all_votes"\],
/// }
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionSelectorConfig {
    mentions: Vec<String>,
}

#[derive(Default, Debug)]
pub(crate) struct TransactionSelector {
    pub mentioned_addresses: HashSet<Vec<u8>>,
    pub select_all_transactions: bool,
    pub select_all_vote_transactions: bool,
}

#[allow(dead_code)]
impl TransactionSelector {
    pub fn new(config: &TransactionSelectorConfig) -> Self {
        info!("[transaction_selector] config=[{:?}]", config);

        let select_all_transactions = config.mentions.iter().any(|key| key == "*" || key == "all");
        if select_all_transactions {
            return Self {
                mentioned_addresses: HashSet::default(),
                select_all_transactions,
                select_all_vote_transactions: true,
            };
        }
        let select_all_vote_transactions = config.mentions.iter().any(|key| key == "all_votes");
        if select_all_vote_transactions {
            return Self {
                mentioned_addresses: HashSet::default(),
                select_all_transactions,
                select_all_vote_transactions: true,
            };
        }
        Self {
            mentioned_addresses: config.mentions.iter().map(|key| bs58::decode(key).into_vec().unwrap()).collect(),
            select_all_transactions: false,
            select_all_vote_transactions: false,
        }
    }

    /// Check if a transaction is of interest.
    pub fn is_transaction_selected(&self, is_vote: bool, mentioned_addresses: Box<dyn Iterator<Item = &Pubkey> + '_>) -> bool {
        if !self.is_enabled() {
            return false;
        }

        if self.select_all_transactions || (self.select_all_vote_transactions && is_vote) {
            return true;
        }
        for address in mentioned_addresses {
            if self.mentioned_addresses.contains(address.as_ref()) {
                return true;
            }
        }
        false
    }

    /// Check if any transaction is of interest at all
    pub fn is_enabled(&self) -> bool {
        self.select_all_transactions || self.select_all_vote_transactions || !self.mentioned_addresses.is_empty()
    }
}
