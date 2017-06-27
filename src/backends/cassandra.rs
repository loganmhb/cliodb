use {KVStore, Result, Record};
use tx::TxRaw;

use serde::{Serialize, Deserialize};
use rmp_serde::{Serializer, Deserializer};

use cdrs::connection_manager::ConnectionManager;
use cdrs::query::QueryBuilder;
use cdrs::compression::Compression;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::types::ByName;
use cdrs::types::value::{Value, Bytes};
use r2d2;

#[derive(Clone)]
pub struct CassandraStore {
    pool: r2d2::Pool<ConnectionManager<NoneAuthenticator, TransportTcp>>,
}

impl CassandraStore {
    pub fn new(addr: &str) -> Result<CassandraStore> {

        let tcp = TransportTcp::new(addr)?;
        let config = r2d2::Config::builder().pool_size(15).build();
        let authenticator = NoneAuthenticator;
        let manager = ConnectionManager::new(tcp, authenticator, Compression::Snappy);
        let pool = r2d2::Pool::new(config, manager)?;

        let store = CassandraStore { pool: pool.clone() };

        let mut session = pool.get()?;
        // TODO: detect new Cass cluster + set up logos keyspace & logos_kvs table
        // real TODO: do that in a different `create-db` function
        // FIXME: This seems to fail when the tables don't already exist.
        let create_kvs = QueryBuilder::new("CREATE TABLE IF NOT EXISTS logos.logos_kvs (
            key text PRIMARY KEY,
            val blob
        )")
                .finalize();

        session.query(create_kvs, false, false)?;

        let create_txs = QueryBuilder::new("CREATE TABLE IF NOT EXISTS logos.logos_txs (
            id bigint PRIMARY KEY,
            val blob
        )")
                .finalize();

        session.query(create_txs, false, false)?;


        Ok(store)
    }
}

impl KVStore for CassandraStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let select_query = QueryBuilder::new("SELECT val FROM logos.logos_kvs WHERE key = ?")
            .values(vec![Value::new_normal(key)])
            .finalize();
        let mut session = self.pool.get()?;
        match session
                  .query(select_query, false, false)
                  .and_then(|r| r.get_body())
                  .map(|b| b.into_rows()) {
            Ok(Some(rows)) => {
                let v: Vec<u8> = rows.get(0)
                    .ok_or("no rows found")?
                    .r_by_name("val")
                    .unwrap();
                Ok(v)
            }
            Ok(None) => Err("node not found".into()),
            Err(e) => Err(e.into()),
        }
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let insert_query = QueryBuilder::new("INSERT INTO logos.logos_kvs (key, val) VALUES (?, ?)",)
            .values(vec![
                Value::new_normal(key.clone()),
                Value::from(Bytes::new(value.to_vec())),
            ])
            .finalize();

        let mut session = self.pool.get()?;

        match session.query(insert_query, false, false) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>> {
        let select_query = QueryBuilder::new("SELECT id, val FROM logos.logos_txs WHERE id >= ?")
            .values(vec![Value::new_normal(from)])
            .finalize();
        let mut session = self.pool.get()?;
        match session
                  .query(select_query, false, false)
                  .and_then(|r| r.get_body())
                  .map(|b| b.into_rows()) {
            Ok(Some(rows)) => {
                let results = rows.iter()
                    .map(|row| {
                             let v: Vec<u8> = row.r_by_name("val").unwrap();
                             let mut de = Deserializer::new(&v[..]);
                             let records: Vec<Record> = Deserialize::deserialize(&mut de)?;

                             let id: i64 = row.r_by_name("id").unwrap();
                             Ok(TxRaw { id: id, records })
                         })
                    .collect::<Vec<Result<TxRaw>>>();

                // Convert Vec<Result<TxRaw>> to Result<Vec<TxRaw>>
                let mut unwrapped_results = vec![];
                for result in results {
                    unwrapped_results.push(result?);
                }

                Ok(unwrapped_results)
            }
            Ok(None) => Ok(vec![]),
            Err(e) => Err(e.into()),
        }
    }

    fn add_tx(&self, tx: &TxRaw) -> Result<()> {
        let mut serialized: Vec<u8> = vec![];
        tx.records.serialize(&mut Serializer::new(&mut serialized))?;

        let insert_query = QueryBuilder::new("INSERT INTO logos.logos_txs (id, val) VALUES (?, ?)")
            .values(vec![
                Value::new_normal(tx.id),
                Value::from(Bytes::new(serialized)),
            ])
            .finalize();

        let mut session = self.pool.get()?;

        match session.query(insert_query, false, false) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use durable_tree::Node;
    use rmp_serde::{Serializer, Deserializer};
    use serde::{Serialize, Deserialize};

    #[test]
    #[ignore]
    fn can_create() {
        let _: CassandraStore = CassandraStore::new("127.0.0.1:9042").unwrap();
    }

    #[test]
    #[ignore]
    fn test_get_and_set() {
        let node = Node::Leaf { items: vec!["hi there".to_string()] };

        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let store: CassandraStore = CassandraStore::new("127.0.0.1:9042").unwrap();

        store.set("my_thing", &buf).unwrap();
        let roundtrip_node_bytes = store.get("my_thing").expect("Could not deserialize node");
        let mut de = Deserializer::new(&roundtrip_node_bytes[..]);
        let deserialized = Deserialize::deserialize(&mut de).unwrap();
        assert_eq!(node, deserialized);
    }
}
