pub mod sqlite;
pub mod mem;
pub mod mysql;

use std::marker::{Send, Sync};

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};

use conn::TxLocation;
use db::DbMetadata;
use tx::TxRaw;
use super::Result;

/// Abstracts over various backends; all that's required for a Logos
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

    fn get_metadata(&self) -> Result<DbMetadata> {
        let serialized = self.get("db_metadata")?;
        let mut de = Deserializer::new(&serialized[..]);
        let metadata: DbMetadata = Deserialize::deserialize(&mut de)?;
        Ok(metadata.clone())
    }

    fn set_metadata(&self, metadata: &DbMetadata) -> Result<()> {
        let mut buf = Vec::new();
        metadata.serialize(&mut Serializer::new(&mut buf))?;

        self.set("db_metadata", &buf)
    }

    fn get_tx_location(&self) -> Result<TxLocation> {
        let serialized = self.get("transactor")?;
        let mut de = Deserializer::new(&serialized[..]);
        let transactor: TxLocation = Deserialize::deserialize(&mut de)?;

        Ok(transactor.clone())
    }

    fn set_tx_location(&self, transactor: &TxLocation) -> Result<()> {
        let mut buf = Vec::new();
        transactor.serialize(&mut Serializer::new(&mut buf))?;

        self.set("transactor", &buf)
    }

    fn add_tx(&self, raw_tx: &TxRaw) -> Result<()>;
    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>>;
}
