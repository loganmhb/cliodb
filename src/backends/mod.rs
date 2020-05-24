pub mod sqlite;
pub mod mysql;

use std::marker::{Send, Sync};

use db::DbMetadata;
use tx::TxRaw;
use super::Result;

/// Abstracts over various backends; all that's required for a ClioDB
/// backend is the ability to add a key, retrieve a key, and
/// atomically set/get the DbMetadata.
pub trait KVStore: Send + Sync {
    /// Set a value in the store. This method implies only eventual consistency;
    /// use `compare_and_set` when consistency is required.
    // FIXME: This is currently used for setting db_metadata as well as index segments,
    // which isn't ACID-safe.
    fn set(&self, key: &str, value: &[u8]) -> Result<()>;

    // TODO: implement: fn compare_and_set for db metadata

    /// Get a value out of the store.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    // FIXME: return a Result<Option<DbMetadata>>
    fn get_metadata(&self) -> Result<DbMetadata> {
        let serialized = self.get("db_metadata")?;
        let metadata: DbMetadata = rmp_serde::from_read_ref(&serialized)?;
        Ok(metadata)
    }

    fn set_metadata(&self, metadata: &DbMetadata) -> Result<()> {
        let buf = rmp_serde::to_vec(metadata)?;

        self.set("db_metadata", &buf)
    }

    fn add_tx(&self, raw_tx: &TxRaw) -> Result<()>;
    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>>;
}
