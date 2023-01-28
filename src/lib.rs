use geyser_plugin_postgres::GeyserPluginPostgres;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

pub mod accounts_selector;
pub mod config;
pub mod geyser_plugin_postgres;
pub mod inline_spl_token;
pub mod inline_spl_token_2022;
pub mod parallel_client;
pub mod parallel_client_worker;
pub mod postgres_client;
pub mod transaction_selector;

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns a pointer to the Kafka Plugin box implementing trait GeyserPlugin.
///
/// The Solana validator and this plugin must be compiled with the same Rust compiler version and Solana core version.
/// Loading this plugin with mismatching versions is undefined behavior and will likely cause memory corruption.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserPluginPostgres::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
