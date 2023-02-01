use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::pubkey::PUBKEY_BYTES;

use super::account_handler::AccountHandler;
use super::DbAccountInfo;
use super::ReadableAccountInfo;

pub static TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub static TOKENZ_PROGRAM_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
/*
    /// The SPL token definition -- we care about only the mint and owner fields for now at offset 0 and 32 respectively
    spl_token::state::Account {
        mint: Pubkey,
        owner: Pubkey,
        amount: u64,
        delegate: COption<Pubkey>,
        state: AccountState,
        is_native: COption<u64>,
        delegated_amount: u64,
        close_authority: COption<Pubkey>,
    }
*/
const SPL_TOKEN_ACCOUNT_MINT_OFFSET: usize = 0;
const SPL_TOKEN_ACCOUNT_OWNER_OFFSET: usize = 32;
const SPL_TOKEN_ACCOUNT_LENGTH: usize = 165;
const SPL_TOKEN_ACCOUNT_DISCRIMINATOR: u8 = 2;

pub struct TokenAccountHandler {}

impl AccountHandler for TokenAccountHandler {
    fn id(&self) -> String {
        "spl_token_account".to_string()
    }

    fn init(&self, config: &crate::config::GeyserPluginPostgresConfig) -> String {
        if !self.enabled(config) {
            return "".to_string();
        };
        return "
            CREATE TABLE IF NOT EXISTS spl_token_account (
                pubkey VARCHAR(44) NOT NULL,
                owner VARCHAR(44) NOT NULL,
                mint VARCHAR(44) NOT NULL,
                slot BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS spl_token_account_owner ON spl_token_account (owner);
            CREATE INDEX IF NOT EXISTS spl_token_account_mint ON spl_token_account (mint);
            CREATE UNIQUE INDEX IF NOT EXISTS spl_token_account_owner_pair ON spl_token_account (pubkey, owner, mint);
        "
        .to_string();
    }

    fn account_match(&self, account: &DbAccountInfo) -> bool {
        account.owner() == TOKEN_PROGRAM_ID.as_ref() && account.data.len() == SPL_TOKEN_ACCOUNT_LENGTH
            || account.owner() == TOKENZ_PROGRAM_ID.as_ref() && SPL_TOKEN_ACCOUNT_DISCRIMINATOR == *account.data.get(SPL_TOKEN_ACCOUNT_LENGTH).unwrap_or(&0)
    }

    fn account_update(&self, account: &DbAccountInfo) -> String {
        if !self.account_match(account) {
            return "".to_string();
        };
        let mint: &Pubkey = bytemuck::from_bytes(&account.data[SPL_TOKEN_ACCOUNT_MINT_OFFSET..SPL_TOKEN_ACCOUNT_MINT_OFFSET + PUBKEY_BYTES]);
        let owner: &Pubkey = bytemuck::from_bytes(&account.data[SPL_TOKEN_ACCOUNT_OWNER_OFFSET..SPL_TOKEN_ACCOUNT_OWNER_OFFSET + PUBKEY_BYTES]);
        let pubkey = Pubkey::new(account.pubkey());
        let slot = account.slot;
        return format!(
            "
                INSERT INTO spl_token_account AS spl_token_entry (pubkey, owner, mint, slot) \
                VALUES ('{0}', '{1}', '{2}', {3}) \
                ON CONFLICT (pubkey, owner, mint) \
                DO UPDATE SET slot=excluded.slot \
                WHERE spl_token_entry.slot < excluded.slot;
            ",
            &bs58::encode(pubkey).into_string(),
            &bs58::encode(owner).into_string(),
            &bs58::encode(mint).into_string(),
            &slot,
        );
    }
}
