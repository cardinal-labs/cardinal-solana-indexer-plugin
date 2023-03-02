use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::pubkey::PUBKEY_BYTES;

use super::account_handler::AccountHandler;
use super::DbAccountInfo;

pub static METADATA_PROGRAM_ID: Pubkey = pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
const TOKEN_METADATA_MINT_OFFSET: usize = 33;
const TOKEN_METADATA_CREATORS_OFFSET: usize = 322;
const TOKEN_METADATA_DISCRIMINATOR: u8 = 4;

#[repr(C)]
#[cfg_attr(feature = "serde-feature", derive(Serialize, Deserialize))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Eq, Hash)]
pub struct Creator {
    #[cfg_attr(feature = "serde-feature", serde(with = "As::<DisplayFromStr>"))]
    pub address: Pubkey,
    pub verified: bool,
    pub share: u8,
}

pub struct MetadataCreatorsAccountHandler {}

impl AccountHandler for MetadataCreatorsAccountHandler {
    fn id(&self) -> String {
        "token_metadata_creators".to_string()
    }

    fn init(&self, config: &crate::config::GeyserPluginPostgresConfig) -> String {
        if !self.enabled(config) {
            return "".to_string();
        };
        return "
            CREATE TABLE IF NOT EXISTS token_metadata_creators (
                mint VARCHAR(44) NOT NULL,
                creator VARCHAR(44) NOT NULL,
                verified BOOL NOT NULL,
                share SMALLINT NOT NULL,
                position SMALLINT NOT NULL,
                slot BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS token_metadata_creators_creator ON token_metadata_creators (creator);
            CREATE UNIQUE INDEX IF NOT EXISTS token_metadata_creators_creator_mint ON token_metadata_creators (mint, creator);
        "
        .to_string();
    }

    fn account_match(&self, account: &DbAccountInfo) -> bool {
        account.owner == METADATA_PROGRAM_ID.as_ref() && TOKEN_METADATA_DISCRIMINATOR == *account.data.get(0).unwrap_or(&0)
    }

    fn account_update(&self, account: &DbAccountInfo) -> String {
        if !self.account_match(account) {
            return "".to_string();
        };

        let buf = &mut &account.data[TOKEN_METADATA_CREATORS_OFFSET..];
        let creators: Vec<Creator> = BorshDeserialize::deserialize(buf).expect("Failed to deserialize creators");
        let mint: &Pubkey = bytemuck::from_bytes(&account.data[TOKEN_METADATA_MINT_OFFSET..TOKEN_METADATA_MINT_OFFSET + PUBKEY_BYTES]);
        let slot = account.slot;
        return creators
            .iter()
            .enumerate()
            .map(|(index, c)| {
                format!(
                    "
                    INSERT INTO token_metadata_creators AS acc (mint, creator, verified, share, position, slot) \
                    VALUES ('{0}', '{1}', {2}, {3}, {4}, {5}) \
                    ON CONFLICT (mint, creator) \
                    DO UPDATE SET slot=excluded.slot, verified=excluded.verified \
                    WHERE acc.slot < excluded.slot;
                ",
                    &bs58::encode(mint).into_string(),
                    &bs58::encode(c.address).into_string(),
                    &c.verified,
                    &c.share,
                    &index,
                    &slot,
                )
            })
            .collect::<Vec<String>>()
            .join("");
    }
}
