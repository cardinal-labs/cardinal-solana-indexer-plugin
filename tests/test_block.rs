use std::thread::sleep;
use std::time::Duration;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_postgres::postgres_client::SimplePostgresClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_transaction_status::Reward;
use solana_transaction_status::RewardType;

#[test]
fn test_block() {
    let address: Pubkey = Keypair::new().pubkey();
    let slot_num: u32 = rand::random::<u32>();
    let slot = slot_num as i64;
    let block_time: i64 = rand::random::<i64>();
    let block_height: u64 = rand::random::<u64>();

    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_config.json")).unwrap();
    geyser_plugin
        .notify_block_metadata(ReplicaBlockInfoVersions::V0_0_1(&ReplicaBlockInfo {
            slot: slot as u64,
            blockhash: "EEFdm1t3obBG5q2V7kwCs5HvHdfVAWbQs5dV1QZLqJJB",
            rewards: &[Reward {
                pubkey: address.to_string(),
                commission: Some(10),
                lamports: 10,
                post_balance: 10,
                reward_type: Some(RewardType::Fee),
            }],
            block_height: Some(block_height),
            block_time: Some(block_time),
        }))
        .unwrap();

    sleep(Duration::from_secs(1));
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");
    let rows = client.query("SELECT * from block where slot=$1", &[&slot]).expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect number of rows found");
    let first_row = rows.first().expect("No results found");

    let check_time: Option<i64> = first_row.get("block_time");
    assert_eq!(check_time.unwrap(), block_time, "Incorrect block time");
    let check_height: Option<i64> = first_row.get("block_height");
    assert_eq!(check_height.unwrap(), block_height as i64, "Incorrect block height");

    client.close().expect("Error disconnecting");
    geyser_plugin.on_unload();
}
