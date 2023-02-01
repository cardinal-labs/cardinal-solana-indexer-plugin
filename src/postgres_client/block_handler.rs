use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;
use chrono::Utc;
use log::*;
use postgres::Client;
use postgres::Statement;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfo;

use super::transaction_handler::DbReward;

#[derive(Clone, Debug)]
pub struct DbBlockInfo {
    pub slot: i64,
    pub blockhash: String,
    pub rewards: Vec<DbReward>,
    pub block_time: Option<i64>,
    pub block_height: Option<i64>,
}

impl<'a> From<&ReplicaBlockInfo<'a>> for DbBlockInfo {
    fn from(block_info: &ReplicaBlockInfo) -> Self {
        Self {
            slot: block_info.slot as i64,
            blockhash: block_info.blockhash.to_string(),
            rewards: block_info.rewards.iter().map(DbReward::from).collect(),
            block_time: block_info.block_time,
            block_height: block_info.block_height.map(|block_height| block_height as i64),
        }
    }
}

pub struct BlockHandler {
    pub upsert_statement: Statement,
}

impl BlockHandler {
    pub fn new(client: &mut Client, _config: &GeyserPluginPostgresConfig) -> Result<BlockHandler, GeyserPluginError> {
        let stmt = "INSERT INTO block (slot, blockhash, rewards, block_time, block_height, updated_on) \
        VALUES ($1, $2, $3, $4, $5, $6) \
        ON CONFLICT (slot) DO UPDATE SET blockhash=excluded.blockhash, rewards=excluded.rewards, \
        block_time=excluded.block_time, block_height=excluded.block_height, updated_on=excluded.updated_on";
        match client.prepare(stmt) {
            Ok(statement) => Ok(BlockHandler { upsert_statement: statement }),
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!("[block_handler::new] error={}", err),
            }))),
        }
    }

    pub fn init(_config: &crate::config::GeyserPluginPostgresConfig) -> String {
        return "
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'RewardType') THEN
                    CREATE TYPE \"RewardType\" AS ENUM (
                        'Fee',
                        'Rent',
                        'Staking',
                        'Voting'
                    );
                END IF;
            END $$;
            
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Reward') THEN
                    CREATE TYPE \"Reward\" AS (
                        pubkey VARCHAR(44),
                        lamports BIGINT,
                        post_balance BIGINT,
                        reward_type \"RewardType\",
                        commission SMALLINT
                    );
                END IF;
            END $$;     
            
            CREATE TABLE IF NOT EXISTS block (
                slot BIGINT PRIMARY KEY,
                blockhash VARCHAR(44),
                rewards \"Reward\"[],
                block_time BIGINT,
                block_height BIGINT,
                updated_on TIMESTAMP NOT NULL
            );
        "
        .to_string();
    }

    pub fn update(&self, client: &mut Client, block_info: DbBlockInfo) -> Result<(), GeyserPluginError> {
        let result = client.query(
            &self.upsert_statement,
            &[
                &block_info.slot,
                &block_info.blockhash,
                &block_info.rewards,
                &block_info.block_time,
                &block_info.block_height,
                &Utc::now().naive_utc(),
            ],
        );
        if let Err(err) = result {
            let msg = format!("Failed to persist the update of block metadata to the PostgreSQL database. Error: {:?}", err);
            error!("{}", msg);
            return Err(GeyserPluginError::AccountsUpdateError { msg });
        }

        Ok(())
    }
}
