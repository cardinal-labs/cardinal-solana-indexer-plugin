use chrono::Utc;
use postgres::Client;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;

pub struct SlotHandler {}

impl SlotHandler {
    pub fn init(_config: &crate::config::GeyserPluginPostgresConfig) -> String {
        return "
            CREATE TABLE IF NOT EXISTS slot (
                slot BIGINT PRIMARY KEY,
                parent BIGINT,
                status VARCHAR(16) NOT NULL,
                updated_on TIMESTAMP NOT NULL
            );
        "
        .to_string();
    }

    pub fn update(slot: u64, parent: Option<u64>, status: SlotStatus) -> String {
        format!(
            "
                INSERT INTO slot (slot, parent, status, updated_on) \
                VALUES ({0}, {1}, '{2}', '{3}') \
                ON CONFLICT (slot) DO UPDATE SET parent=excluded.parent, status=excluded.status, updated_on=excluded.updated_on;
            ",
            &slot,
            parent.map_or("NULL".to_string(), |p| p.to_string()),
            &status.as_str(),
            &Utc::now().naive_utc()
        )
    }

    pub fn get_highest_available_slot(client: &mut Client) -> Result<u64, GeyserPluginError> {
        match client.query_opt("SELECT slot FROM slot ORDER BY slot DESC LIMIT 1;", &[]) {
            Ok(opt_slot) => Ok(opt_slot
                .map(|row| {
                    let raw_slot: i64 = row.get(0);
                    raw_slot as u64
                })
                .unwrap_or(0)),
            Err(err) => Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to receive last slot from PostgreSQL database. Error: {:?}", err),
            }),
        }
    }
}
