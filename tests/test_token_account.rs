use std::thread::sleep;
use std::time::Duration;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_postgres::postgres_client::SimplePostgresClient;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;

static ADDRESS: Pubkey = pubkey!("J7Gc9vhfyNAe44MDSKyo8BsFxGCF6qfwpt8xGK3JGBTF");
static OWNER: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
static TOKEN_ACCOUNT_OWNER: Pubkey = pubkey!("cpmaMZyBQiPxpeuxNsQhW7N8z1o9yaNdLgiPhWGUEiX");
static MINT: Pubkey = pubkey!("DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ");

#[test]
fn test_token_account() {
    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_config.json")).unwrap();

    geyser_plugin
        .update_account(
            ReplicaAccountInfoVersions::V0_0_1(&ReplicaAccountInfo {
                pubkey: ADDRESS.as_ref(),
                lamports: 2039280,
                owner: OWNER.as_ref(),
                executable: false,
                rent_epoch: 0,
                data: &[
                    0xb9, 0x53, 0xb5, 0xf8, 0xdd, 0x54, 0x57, 0xa2, 0xa0, 0xf0, 0xd4, 0x19, 0x03, 0x40, 0x97, 0x85, 0xb9, 0xd8, 0x4d, 0x40, 0x45, 0x61, 0x4f, 0xaa, 0x4f, 0x50, 0x5e, 0xe1, 0x32, 0xdc,
                    0xd7, 0x69, 0x09, 0x2d, 0x57, 0x22, 0xb4, 0x9f, 0xe7, 0xfa, 0x41, 0x86, 0x12, 0x8a, 0x41, 0x9a, 0x30, 0x13, 0x9f, 0x08, 0xc4, 0x0d, 0x81, 0x38, 0x97, 0x7c, 0x13, 0x4c, 0xaf, 0x56,
                    0xe3, 0x4c, 0x09, 0x84, 0xc8, 0xca, 0x35, 0x17, 0xd1, 0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                ],
                write_version: 0,
            }),
            0,
            false,
        )
        .unwrap();

    sleep(Duration::from_secs(1));
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");

    // check accounts
    let rows = client.query("SELECT * from account where pubkey=$1", &[&ADDRESS.as_ref()]).expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect number of rows found");
    let first_row = rows.first().expect("No results found");

    let pubkey: Vec<u8> = first_row.get("pubkey");
    assert!(Pubkey::new_from_array(pubkey[..].try_into().unwrap()) == ADDRESS, "Incorrect pubkey");

    let owner: Vec<u8> = first_row.get("owner");
    assert!(Pubkey::new_from_array(owner[..].try_into().unwrap()) == OWNER, "Incorrect pubkey");

    // check token owner
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");
    let rows = client
        .query("SELECT * from spl_token_owner_index where account_key=$1", &[&ADDRESS.as_ref()])
        .expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect number of rows found");
    let first_row = rows.first().expect("No results found");

    let pubkey: Vec<u8> = first_row.get("account_key");
    assert!(Pubkey::new_from_array(pubkey[..].try_into().unwrap()) == ADDRESS, "Incorrect pubkey");
    let owner: Vec<u8> = first_row.get("owner_key");
    assert!(Pubkey::new_from_array(owner[..].try_into().unwrap()) == TOKEN_ACCOUNT_OWNER, "Incorrect pubkey");

    // check mint
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");
    let rows = client
        .query("SELECT * from spl_token_mint_index where account_key=$1", &[&ADDRESS.as_ref()])
        .expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect number of rows found");
    let first_row = rows.first().expect("No results found");

    let pubkey: Vec<u8> = first_row.get("account_key");
    assert!(Pubkey::new_from_array(pubkey[..].try_into().unwrap()) == ADDRESS, "Incorrect pubkey");
    let mint: Vec<u8> = first_row.get("mint_key");
    assert!(Pubkey::new_from_array(mint[..].try_into().unwrap()) == MINT, "Incorrect pubkey");

    client.close().expect("Error disconnecting");
    geyser_plugin.on_unload();
}
