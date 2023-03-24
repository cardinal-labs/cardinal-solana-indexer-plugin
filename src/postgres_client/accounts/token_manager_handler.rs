use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use log::error;
use solana_program::hash::hash;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;

use super::account_handler::AccountHandler;
use super::DbAccountInfo;

pub static TOKEN_MANAGER_PROGRAM_ID: Pubkey = pubkey!("mgr99QFMYByTqGPWmNqunV7vBLmWWXdSrHUfV8Jf3JM");

#[repr(C)]
#[cfg_attr(feature = "serde-feature", derive(Serialize, Deserialize))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Eq, Hash)]
pub struct TokenManager {
    pub version: u8,
    pub bump: u8,
    pub count: u64,
    pub num_invalidators: u8,
    pub issuer: Pubkey,
    pub mint: Pubkey,
    pub amount: u64,
    pub kind: u8,
    pub state: u8,
    pub state_changed_at: i64,
    pub invalidation_type: u8,
    pub recipient_token_account: Pubkey,
    pub receipt_mint: Option<Pubkey>,
    pub claim_approver: Option<Pubkey>,
    pub transfer_authority: Option<Pubkey>,
    pub invalidators: Vec<Pubkey>,
}

pub struct TokenManagerAccountHandler {}

impl AccountHandler for TokenManagerAccountHandler {
    fn init(&self, config: &crate::config::GeyserPluginPostgresConfig) -> String {
        if !self.enabled(config) {
            return "".to_string();
        };
        return "
            CREATE TABLE IF NOT EXISTS token_manager (
                id VARCHAR(44) NOT NULL,
                version SMALLINT NOT NULL,
                bump SMALLINT NOT NULL,
                count BIGINT NOT NULL,
                num_invalidators SMALLINT NOT NULL,
                issuer VARCHAR(44) NOT NULL,
                mint VARCHAR(44) NOT NULL,
                amount BIGINT NOT NULL,
                kind SMALLINT NOT NULL,
                state SMALLINT NOT NULL,
                state_changed_at BIGINT NOT NULL,
                invalidation_type SMALLINT NOT NULL,
                recipient_token_account VARCHAR(44) NOT NULL,
                receipt_mint VARCHAR(44),
                claim_approver VARCHAR(44),
                transfer_authority VARCHAR(44),
                invalidators VARCHAR(44)[] NOT NULL,
                slot BIGINT NOT NULL,
                PRIMARY KEY(id)
            );
        "
        .to_string();
    }

    fn account_match(&self, account: &DbAccountInfo) -> bool {
        let discriminator_preimage = format!("account:{}", "TokenManager");
        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&hash(discriminator_preimage.as_bytes()).to_bytes()[..8]);
        account.owner == TOKEN_MANAGER_PROGRAM_ID.as_ref() && discriminator == *account.data.get(0..8).unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0])
    }

    fn account_update(&self, account: &DbAccountInfo) -> String {
        if !self.account_match(account) {
            return "".to_string();
        };

        let token_manager: TokenManager = match BorshDeserialize::deserialize(&mut account.data[8..].as_ref()) {
            Ok(c) => c,
            Err(e) => {
                error!("[account_update] Failed to deserialize token manager pubkey=[{:?}] error=[{:?}]", account.pubkey, e);
                return "".to_string();
            }
        };
        let token_manager_key: &Pubkey = bytemuck::from_bytes(&account.pubkey);
        let slot = account.slot;
        format!(
            "
            INSERT INTO token_manager AS acc (id, version, bump, count, num_invalidators, issuer, mint, amount, kind, state, state_changed_at, invalidation_type, recipient_token_account, receipt_mint, claim_approver, transfer_authority, invalidators, slot) \
            VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}', '{6}', {7}, {8}, {9}, {10}, {11}, '{12}', '{13}', '{14}', '{15}', '{16}', {17}) \
            ON CONFLICT (id) \
            DO UPDATE SET num_invalidators=excluded.num_invalidators, issuer=excluded.issuer, kind=excluded.kind, state=excluded.state, state_changed_at=excluded.state_changed_at, invalidation_type=excluded.invalidation_type, invalidators=excluded.invalidators \
            WHERE acc.slot < excluded.slot;
            ",
            &token_manager_key.to_string(),
            &token_manager.version,
            &token_manager.bump,
            &token_manager.count,
            &token_manager.num_invalidators,
            &token_manager.issuer.to_string(),
            &token_manager.mint.to_string(),
            &token_manager.amount,
            &token_manager.kind,
            &token_manager.state,
            &token_manager.state_changed_at,
            &token_manager.invalidation_type,
            &token_manager.recipient_token_account.to_string(),
            if token_manager.receipt_mint.is_none() {"NULL".to_string()} else {token_manager.receipt_mint.unwrap().to_string()},
            if token_manager.claim_approver.is_none() {"NULL".to_string()} else {token_manager.claim_approver.unwrap().to_string()},
            if token_manager.transfer_authority.is_none() {"NULL".to_string()} else {token_manager.transfer_authority.unwrap().to_string()},
            format!("{{{0}}}", token_manager.invalidators.iter().map(|inv| {
                inv.to_string()
            }).collect::<Vec<String>>()
            .join(",")),
            &slot
        )
    }
}
