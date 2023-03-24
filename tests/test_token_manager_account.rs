use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfo;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
use solana_geyser_plugin_postgres::geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_postgres::postgres_client::SimplePostgresClient;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use std::thread::sleep;
use std::time::Duration;

static OWNER: Pubkey = pubkey!("mgr99QFMYByTqGPWmNqunV7vBLmWWXdSrHUfV8Jf3JM");
static MINT: Pubkey = pubkey!("4utZa12Q3j9J76JaNPncXfVyUxRA3uAjBkFS6Rp9z4Ek");
static TOKEN_MANAGER_ADDRESS: Pubkey = pubkey!("DxH9YVD9yafZ5vo8goKgxuMPR6zQtCC7uw3nnozArMcP");

#[test]
fn test_token_manager_account() {
    let mut geyser_plugin = GeyserPluginPostgres::default();
    geyser_plugin.on_load(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_config.json")).unwrap();

    geyser_plugin
        .update_account(
            ReplicaAccountInfoVersions::V0_0_1(&ReplicaAccountInfo {
                pubkey: TOKEN_MANAGER_ADDRESS.as_ref(),
                lamports: 2790960,
                owner: OWNER.as_ref(),
                executable: false,
                rent_epoch: 0,
                data: &[
                    0xb9, 0x61, 0x7c, 0xe7, 0x46, 0x4b, 0xe4, 0x2f, 0x00, 0xfb, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6b, 0xd3, 0x95, 0x2b, 0x7f, 0x86, 0x39, 0x67, 0x93, 0x89, 0x1a,
                    0x91, 0x4c, 0x34, 0x30, 0x05, 0xa4, 0xef, 0x9d, 0x3b, 0xad, 0xe4, 0x11, 0x8d, 0xdc, 0x97, 0xb4, 0xa4, 0x58, 0x60, 0x05, 0xcf, 0x3a, 0x1f, 0x90, 0x4c, 0x3d, 0x7a, 0x4d, 0xf3, 0x39,
                    0xf7, 0x65, 0x73, 0xcc, 0x2a, 0x95, 0xaf, 0x95, 0xc4, 0xbb, 0x53, 0x6a, 0x82, 0x05, 0xad, 0xfb, 0x6c, 0x9c, 0x55, 0xcc, 0x4b, 0xc9, 0xd1, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x03, 0x01, 0x95, 0xe8, 0x1a, 0x64, 0x00, 0x00, 0x00, 0x00, 0x04, 0xaa, 0xa5, 0xbb, 0x22, 0xe0, 0x66, 0xc7, 0x5c, 0x3e, 0x16, 0xe6, 0x48, 0xae, 0x7b, 0xae, 0x75, 0xcd, 0xe5,
                    0x02, 0xfe, 0xb3, 0x8c, 0xcd, 0xb7, 0x4d, 0x77, 0xd4, 0x82, 0x38, 0x27, 0x4f, 0x1a, 0x00, 0x00, 0x01, 0xc0, 0x75, 0x72, 0x06, 0xbc, 0xe0, 0x82, 0x9a, 0xb6, 0x13, 0xff, 0xa5, 0x1d,
                    0x3c, 0x6b, 0x0c, 0x99, 0x9c, 0x6a, 0x35, 0xfd, 0xab, 0x35, 0x47, 0xb0, 0x0c, 0x39, 0xa8, 0x6f, 0x9e, 0x47, 0xac, 0x01, 0x00, 0x00, 0x00, 0x34, 0x42, 0x31, 0x34, 0xc8, 0xbb, 0x12,
                    0x54, 0xf1, 0xbf, 0x2c, 0xb9, 0xb5, 0x8e, 0x9a, 0x8e, 0x53, 0x55, 0xb7, 0xca, 0x66, 0x47, 0x08, 0x12, 0x91, 0xf5, 0xad, 0xa0, 0x88, 0x10, 0x13, 0xe5, 0x00, 0x00, 0x00, 0x00, 0x00,
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

    sleep(Duration::from_secs(1));

    // check token owner
    let mut client = SimplePostgresClient::connect_to_db(&geyser_plugin.config.clone().expect("No plugin config found")).expect("Failed to connect");
    let rows = client.query("SELECT * from token_manager where mint=$1", &[&MINT.to_string()]).expect("Error selecting accounts");
    assert_eq!(rows.len(), 1, "Incorrect number of rows found (should be 1)");

    let token_manager = &rows[0];

    let id: String = token_manager.get("id");
    assert_eq!(id, TOKEN_MANAGER_ADDRESS.to_string(), "Incorrect token manager id");
    let mint: String = token_manager.get("mint");
    assert_eq!(mint, MINT.to_string(), "Incorrect mint pubkey");

    let id: String = token_manager.get("id");
    assert_eq!(id, TOKEN_MANAGER_ADDRESS.to_string(), "Incorrect token manager id");
    let mint: String = token_manager.get("mint");
    assert_eq!(mint, MINT.to_string(), "Incorrect mint pubkey");
    let version: i16 = token_manager.get("version");
    assert_eq!(version, 0, "Incorrect version");
    let bump: i16 = token_manager.get("bump");
    assert_eq!(bump, 251, "Incorrect bump");
    let count: i64 = token_manager.get("count");
    assert_eq!(count, 5, "Incorrect count");
    let num_invalidators: i16 = token_manager.get("num_invalidators");
    assert_eq!(num_invalidators, 1, "Incorrect num_invalidators");
    let issuer: String = token_manager.get("issuer");
    assert_eq!(issuer, "8FukQrBdS8fLXeJNmwtDMmLM2QQfX12oABS6DEuWgi78", "Incorrect issuer");
    let mint: String = token_manager.get("mint");
    assert_eq!(mint, MINT.to_string(), "Incorrect mint");
    let amount: i64 = token_manager.get("amount");
    assert_eq!(amount, 1, "Incorrect amount");
    let kind: i16 = token_manager.get("kind");
    assert_eq!(kind, 3, "Incorrect kind");
    let state: i16 = token_manager.get("state");
    assert_eq!(state, 1, "Incorrect state");
    let state_changed_at: i64 = token_manager.get("state_changed_at");
    assert_eq!(state_changed_at, 1679485077, "Incorrect state_changed_at");
    let invalidation_type: i16 = token_manager.get("invalidation_type");
    assert_eq!(invalidation_type, 4, "Incorrect invalidation_type");
    let recipient_token_account: String = token_manager.get("recipient_token_account");
    assert_eq!(recipient_token_account, "CV8t9uMQafrgvhJsDdsoNMqgM6sQfBeLrLmEbGWoW3Vs", "Incorrect recipient_token_account");
    let receipt_mint: Option<String> = token_manager.get("receipt_mint");
    assert_eq!(receipt_mint, None, "Incorrect receipt_mint");
    let claim_approver: Option<String> = token_manager.get("claim_approver");
    assert_eq!(claim_approver, None, "Incorrect claim_approver");
    let transfer_authority: Option<String> = token_manager.get("transfer_authority");
    assert_eq!(transfer_authority.unwrap(), "DxH9YVD9yafZ5vo8goKgxuMPR6zQtCC7uw3nnozArMcP", "Incorrect transfer_authority");
    let invalidators: Vec<String> = token_manager.get("invalidators");
    assert_eq!(invalidators, ["4WzjymAcLaGRSqEWqsfLYiL9BEZb7q1iJEDpzuTuiHhn"], "Incorrect invalidators");

    client.close().expect("Error disconnecting");
    geyser_plugin.on_unload();
}
