use crate::config::GeyserPluginPostgresConfig;

use super::DbAccountInfo;

pub trait AccountHandler {
    fn id(&self) -> String;

    fn enabled(&self, _config: &GeyserPluginPostgresConfig) -> bool {
        true
    }

    fn init(&self, config: &GeyserPluginPostgresConfig) -> String;

    fn account_match(&self, account: &DbAccountInfo) -> bool;

    fn account_update(&self, account: &DbAccountInfo) -> String;
}
