use crate::config::GeyserPluginPostgresConfig;
use crate::geyser_plugin_postgres::GeyserPluginPostgresError;

use super::SimplePostgresClient;
use chrono::Utc;
use log::*;
use postgres::Client;
use postgres::Statement;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;

impl SimplePostgresClient {
    pub(crate) fn build_slot_upsert_statement_with_parent(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO slot (slot, parent, status, updated_on) \
        VALUES ($1, $2, $3, $4) \
        ON CONFLICT (slot) DO UPDATE SET parent=excluded.parent, status=excluded.status, updated_on=excluded.updated_on";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the slot update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(stmt) => Ok(stmt),
        }
    }

    pub(crate) fn build_slot_upsert_statement_without_parent(client: &mut Client, config: &GeyserPluginPostgresConfig) -> Result<Statement, GeyserPluginError> {
        let stmt = "INSERT INTO slot (slot, status, updated_on) \
        VALUES ($1, $2, $3) \
        ON CONFLICT (slot) DO UPDATE SET status=excluded.status, updated_on=excluded.updated_on";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                msg: format!(
                    "Error in preparing for the slot update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                    err, config.host, config.user, config
                ),
            }))),
            Ok(stmt) => Ok(stmt),
        }
    }

    pub(crate) fn upsert_slot_status_internal(slot: u64, parent: Option<u64>, status: SlotStatus, client: &mut Client, statement: &Statement) -> Result<(), GeyserPluginError> {
        let slot = slot as i64; // postgres only supports i64
        let parent = parent.map(|parent| parent as i64);
        let updated_on = Utc::now().naive_utc();
        let status_str = status.as_str();

        let result = match parent {
            Some(parent) => client.execute(statement, &[&slot, &parent, &status_str, &updated_on]),
            None => client.execute(statement, &[&slot, &status_str, &updated_on]),
        };

        match result {
            Err(err) => {
                let msg = format!("Failed to persist the update of slot to the PostgreSQL database. Error: {:?}", err);
                error!("{:?}", msg);
                return Err(GeyserPluginError::SlotStatusUpdateError { msg });
            }
            Ok(rows) => {
                assert_eq!(1, rows, "Expected one rows to be updated a time");
            }
        }

        Ok(())
    }

    pub(crate) fn get_highest_available_slot(&mut self) -> Result<u64, GeyserPluginError> {
        let client = self.client.get_mut().unwrap();

        let last_slot_query = "SELECT slot FROM slot ORDER BY slot DESC LIMIT 1;";

        let result = client.client.query_opt(last_slot_query, &[]);
        match result {
            Ok(opt_slot) => Ok(opt_slot
                .map(|row| {
                    let raw_slot: i64 = row.get(0);
                    raw_slot as u64
                })
                .unwrap_or(0)),
            Err(err) => {
                let msg = format!("Failed to receive last slot from PostgreSQL database. Error: {:?}", err);
                error!("{}", msg);
                Err(GeyserPluginError::AccountsUpdateError { msg })
            }
        }
    }
}
