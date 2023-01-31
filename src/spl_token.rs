use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::pubkey::PUBKEY_BYTES;

pub static TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub static TOKENZ_PROGRAM_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

/*
    /// The SPL token definition -- we care about only the mint and owner fields for now.
    /// at offset 0 and 32 respectively.
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

pub struct TokenAccount;

impl TokenAccount {
    pub fn valid_token_program(program_id: &[u8]) -> bool {
        program_id == TOKENZ_PROGRAM_ID.as_ref() || program_id == TOKEN_PROGRAM_ID.as_ref()
    }

    fn valid_account_data(account_data: &[u8]) -> bool {
        account_data.len() == SPL_TOKEN_ACCOUNT_LENGTH || SPL_TOKEN_ACCOUNT_DISCRIMINATOR == *account_data.get(SPL_TOKEN_ACCOUNT_LENGTH).unwrap_or(&0)
    }

    // Call after account length has already been verified
    fn unpack_account_owner_unchecked(account_data: &[u8]) -> &Pubkey {
        Self::unpack_pubkey_unchecked(account_data, SPL_TOKEN_ACCOUNT_OWNER_OFFSET)
    }

    // Call after account length has already been verified
    fn unpack_account_mint_unchecked(account_data: &[u8]) -> &Pubkey {
        Self::unpack_pubkey_unchecked(account_data, SPL_TOKEN_ACCOUNT_MINT_OFFSET)
    }

    // Call after account length has already been verified
    fn unpack_pubkey_unchecked(account_data: &[u8], offset: usize) -> &Pubkey {
        bytemuck::from_bytes(&account_data[offset..offset + PUBKEY_BYTES])
    }

    pub fn unpack_account_owner(account_data: &[u8]) -> Option<&Pubkey> {
        if Self::valid_account_data(account_data) {
            Some(Self::unpack_account_owner_unchecked(account_data))
        } else {
            None
        }
    }

    pub fn unpack_account_mint(account_data: &[u8]) -> Option<&Pubkey> {
        if Self::valid_account_data(account_data) {
            Some(Self::unpack_account_mint_unchecked(account_data))
        } else {
            None
        }
    }
}
