use std::thread::sleep;
use std::time::Duration;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_postgres::postgres_client::SimplePostgresClient;

#[test]
fn test_slot() {
    let slot_num: u32 = rand::random::<u32>();
    let slot = slot_num as i64;
    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_config.json")).unwrap();
    geyser_plugin.update_slot_status(slot as u64, None, SlotStatus::Confirmed).unwrap();

    sleep(Duration::from_secs(1));
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");
    let rows = client.query("SELECT * from slot where slot=$1", &[&slot]).expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect number of rows found");
    let first_row = rows.first().expect("No results found");

    let status: String = first_row.get("status");
    assert_eq!(status, SlotStatus::Confirmed.as_str(), "Incorrect status");
    let parent: Option<i64> = first_row.get("parent");
    assert_eq!(parent, None, "Incorrect parent");

    client.close().expect("Error disconnecting");
    geyser_plugin.on_unload();
}
