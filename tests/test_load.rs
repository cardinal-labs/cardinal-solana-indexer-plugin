use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;

#[test]
fn test_load() {
    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_config.json")).unwrap();
}
