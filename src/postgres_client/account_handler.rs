use crate::config::GeyserPluginPostgresConfig;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;

pub trait AccountHandler {
    fn id(&self) -> String;

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
