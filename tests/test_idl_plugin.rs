#![allow(clippy::integer_arithmetic)]

use std::fs::File;
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use serde_json::json;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_postgres::postgres_client::SimplePostgresClient;
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

fn temp_dir() -> TempDir {
    let dir: String = std::env::var("TEMP_DIR").unwrap_or_else(|_| "temp".to_string());
    fs::create_dir_all(dir.clone()).unwrap();
    let dir_path = PathBuf::from(dir);
    tempfile::tempdir_in(dir_path).unwrap()
}

fn load_test_plugin() -> GeyserPluginPostgres {
    let tmp_dir = temp_dir();
    let mut config_path = tmp_dir.path().to_path_buf();
    config_path.push("accounts_db_plugin.json");
    let mut config_file = File::create(config_path.clone()).unwrap();

    let lib_name = match std::env::consts::OS {
        "macos" => "libsolana_geyser_plugin.dylib",
        _ => "libsolana_geyser_plugin.so",
    };

    let mut lib_path = config_path.clone();
    lib_path.pop();
    lib_path.pop();
    lib_path.pop();
    lib_path.push("target");
    lib_path.push("debug");
    lib_path.push(lib_name);

    let lib_path = lib_path.as_os_str().to_str().unwrap();
    write!(
        config_file,
        "{}",
        json!({
            "libpath": lib_path,
            "connection_str": "host=localhost user=postgres password=postgres port=5432",
            "threads": 20,
            "batch_size": 20,
            "panic_on_db_errors": true,
            "accounts_selector" : {
                "accounts" : ["*"]
            },
            "transaction_selector" : {
                "mentions" : ["*"]
            }
        })
    )
    .unwrap();

    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(config_path.to_str().unwrap()).unwrap();

    geyser_plugin
}

#[test]
fn test_plugin() {
    let mut geyser_plugin = load_test_plugin();
    geyser_plugin
        .update_account(
            ReplicaAccountInfoVersions::V0_0_1(&ReplicaAccountInfo {
                pubkey: Pubkey::from_str("DWLt9b43ZbwoDdPrQMa9k7xTp2ASgQjKXEtFHWaz6xTU").unwrap().as_ref(),
                lamports: 2790960,
                owner: Pubkey::from_str("mgr99QFMYByTqGPWmNqunV7vBLmWWXdSrHUfV8Jf3JM").unwrap().as_ref(),
                executable: false,
                rent_epoch: 0,
                data: &[
                    0xb9, 0x61, 0x7c, 0xe7, 0x46, 0x4b, 0xe4, 0x2f, 0x00, 0xff, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xb7, 0xcd, 0xa6, 0x2e, 0xd1, 0x93, 0x4e, 0x14, 0x06, 0xbb, 0xc1,
                    0x96, 0x43, 0x60, 0x54, 0x5b, 0xdf, 0x13, 0x35, 0x4d, 0x89, 0x37, 0xdc, 0x43, 0x5c, 0x82, 0x6c, 0xb2, 0x33, 0x15, 0xe7, 0x43, 0x21, 0x30, 0x03, 0x8f, 0x5f, 0xc8, 0x8b, 0x0d, 0xc3,
                    0xd9, 0x80, 0xe1, 0xcc, 0x44, 0xa6, 0x75, 0xdd, 0xa1, 0x0e, 0x29, 0x6f, 0x38, 0x8d, 0xfa, 0x2f, 0x2d, 0x18, 0x4a, 0xda, 0x5a, 0xf9, 0x85, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x03, 0x01, 0x9f, 0x29, 0xcb, 0x63, 0x00, 0x00, 0x00, 0x00, 0x01, 0xf4, 0x98, 0xfc, 0x2b, 0x7d, 0xb7, 0x41, 0x56, 0xb8, 0x55, 0xb6, 0x6d, 0xe2, 0x27, 0x98, 0x32, 0x60, 0x6c,
                    0xcc, 0xda, 0xc0, 0x19, 0xb9, 0xa8, 0xbb, 0x3a, 0xba, 0x32, 0x12, 0x01, 0xe3, 0x83, 0x00, 0x00, 0x01, 0xb9, 0xd0, 0x91, 0x2b, 0xbb, 0x9e, 0xf2, 0x3a, 0xab, 0xf2, 0x29, 0x99, 0xac,
                    0xce, 0xbd, 0x51, 0x4e, 0x7c, 0xe4, 0x61, 0x5f, 0xeb, 0xcf, 0x41, 0x3e, 0xbf, 0x24, 0x07, 0x0b, 0x81, 0xea, 0xf3, 0x01, 0x00, 0x00, 0x00, 0x24, 0x8d, 0xa2, 0xf4, 0x00, 0xb9, 0xfb,
                    0xee, 0xac, 0xba, 0x03, 0x4f, 0xc6, 0x19, 0xcb, 0xb1, 0x11, 0x07, 0x18, 0x47, 0xbf, 0x5e, 0x81, 0x64, 0xe3, 0x8d, 0xee, 0x8f, 0x86, 0x8e, 0x59, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                write_version: 0,
            }),
            0,
            false,
        )
        .unwrap();

    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.expect("No plugin config found")).expect("Failed to connect");
    let rows = client.query("SELECT * from account", &[]).expect("Error selecting accounts");
    assert!(rows.len() == 1, "Incorrect rows found");
    let first_row = rows.first().expect("No results found");

    let pubkey: Vec<u8> = first_row.get("pubkey");
    assert!(
        Pubkey::new_from_array(pubkey[..].try_into().unwrap()) == Pubkey::from_str("DWLt9b43ZbwoDdPrQMa9k7xTp2ASgQjKXEtFHWaz6xTU").unwrap(),
        "Incorrect pubkey"
    );

    let owner: Vec<u8> = first_row.get("owner");
    assert!(
        Pubkey::new_from_array(owner[..].try_into().unwrap()) == Pubkey::from_str("mgr99QFMYByTqGPWmNqunV7vBLmWWXdSrHUfV8Jf3JM").unwrap(),
        "Incorrect pubkey"
    );
    client.close().expect("Error disconnecting");
}
