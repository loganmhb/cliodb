pub mod sqlite;
pub mod mem;
pub mod cassandra;

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};

use db::DbContents;
use super::Result;

/// Abstracts over various backends; all that's required for a Logos
/// backend is the ability to add a key, retrieve a key, and
/// atomically set/get the DbContents.
pub trait KVStore {
    /// Set a value in the store. This method implies only eventual consistency;
    /// use `compare_and_set` when consistency is required.
    fn set(&self, key: &str, value: &[u8]) -> Result<()>;

    // TODO: implement: fn compare_and_set

    /// Get a value out of the store.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    fn get_contents(&self) -> Result<DbContents> {
        let serialized = self.get("db_contents")?;
        let mut de = Deserializer::new(&serialized[..]);
        let contents: DbContents = Deserialize::deserialize(&mut de)?;
        Ok(contents.clone())
    }

    fn set_contents(&self, contents: &DbContents) -> Result<()> {
        let mut buf = Vec::new();
        contents.serialize(&mut Serializer::new(&mut buf))?;

        self.set("db_contents", &buf)
    }
}
