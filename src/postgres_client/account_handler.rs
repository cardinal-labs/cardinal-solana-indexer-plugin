use postgres::Client;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;

use crate::config::GeyserPluginPostgresConfig;

use super::DbAccountInfo;

pub trait AccountHandler {
    fn id(&self) -> String;

    fn enabled(&self, _config: &GeyserPluginPostgresConfig) -> bool {
        true
    }

    fn init(&self, client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<(), GeyserPluginError>;

    fn account_match(&self, account: &DbAccountInfo) -> bool;

    fn account_update(&self, client: &mut Client, account: &DbAccountInfo) -> Result<(), GeyserPluginError>;
}
