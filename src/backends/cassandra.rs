use std::marker::PhantomData;

use btree::IndexNode;
use super::{KVStore, DbContents};
use ident::IdentMap;

use uuid::Uuid;
use serde::ser::Serialize;
use serde::de::Deserialize;
use rmp_serde::{Deserializer, Serializer};

use cdrs::connection_manager::ConnectionManager;
use cdrs::query::{QueryBuilder};
use cdrs::compression::Compression;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::types::ByName;
use cdrs::types::value::{Value, Bytes};
use r2d2;

#[derive(Clone)]
pub struct CassandraStore<V> {
    phantom: PhantomData<V>,
    pool: r2d2::Pool<ConnectionManager<NoneAuthenticator, TransportTcp>>
}

#[derive(Debug)]
pub struct Error(String);

impl<E: ToString> From<E> for Error {
    fn from(other: E) -> Error {
        Error(other.to_string())
    }
}

impl<'de, V> CassandraStore<V>
    where V: Serialize + Deserialize<'de> + Clone
{
    pub fn new(addr: &str) -> Result<CassandraStore<V>, Error> {

        let tcp = TransportTcp::new(addr)?;
        let config = r2d2::Config::builder().pool_size(15).build();
        let authenticator = NoneAuthenticator;
        let manager = ConnectionManager::new(tcp, authenticator, Compression::Snappy);
        let pool = r2d2::Pool::new(config, manager)?;

        let store = CassandraStore {
            phantom: PhantomData,
            pool: pool.clone()
        };

        let mut session = pool.get()?;
        // TODO: detect new Cass cluster + set up logos keyspace & logos_kvs table
        // real TODO: do that in a different `create-db` function
        let create = QueryBuilder::new("CREATE TABLE IF NOT EXISTS logos.logos_kvs (
            key text PRIMARY KEY,
            val blob
        )").finalize();

        session.query(create, false, false)?;

        let contents = QueryBuilder::new("SELECT val FROM logos_kvs WHERE key = 'db_contents'")
            .finalize();

        match session.query(contents, false, false)
            .and_then(|r| r.get_body())
            .map(|b| b.into_rows())
        {
            Ok(Some(rows)) => {
                let _: Vec<u8> = rows[0].r_by_name("val")?;
            },
            _ => {
                // Db contents doesn't exist yet; initialize.
                // FIXME: DRY w/r/t sqlite, which is identical.
                let empty_root: IndexNode<V> = IndexNode::Leaf {
                    items: vec![],
                };
                let eav_root = store.add(empty_root.clone())?;
                let aev_root = store.add(empty_root.clone())?;
                let ave_root = store.add(empty_root.clone())?;

                store.set_contents(&DbContents {
                    next_id: 0,
                    idents: IdentMap::default(),
                    eav: eav_root,
                    ave: ave_root,
                    aev: aev_root,
                })?;

            }
        };

        Ok(store)
    }
}

impl<'de, V> KVStore for CassandraStore<V>
    where V: Serialize + Deserialize<'de> + Clone
{
    type Item = V;

    fn get(&self, key: &str) -> Result<IndexNode<Self::Item>, String> {
        let select_query = QueryBuilder::new("SELECT val FROM logos.logos_kvs WHERE key = ?")
            .values(vec![Value::new_normal(key)])
            .finalize();
        let mut session = self.pool.get().map_err(|e| e.to_string())?;
        match session.query(select_query, false, false)
            .and_then(|r| r.get_body())
            .map(|b| b.into_rows())
        {
            Ok(Some(rows)) => {
                let v: Vec<u8> = rows[0].r_by_name("val").unwrap();
                let mut de = Deserializer::new(&v[..]);
                match Deserialize::deserialize(&mut de) {
                    Ok(node) => Ok(node),
                    Err(err) => Err(err.to_string())
                }
            },
            Ok(None) => Err("node not found".to_string()),
            Err(e) => Err(e.to_string())
        }
    }

    fn add(&self, value: IndexNode<Self::Item>) -> Result<String, String> {
        let key = Uuid::new_v4().to_string();
        let mut buf = Vec::new();
        value.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let insert_query = QueryBuilder::new("INSERT INTO logos.logos_kvs (key, val) VALUES (?, ?)")
            .values(vec![Value::new_normal(key.clone()), Value::from(Bytes::new(buf))])
            .finalize();

        let mut session = self.pool.get().map_err(|e| e.to_string())?;

        match session.query(insert_query, false, false) {
            Ok(_) => Ok(key),
            Err(e) => Err(e.to_string())
        }
    }

    fn set_contents(&self, contents: &DbContents) -> Result<(), String> {
        let mut buf = Vec::new();
        contents.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let query = QueryBuilder::new("INSERT INTO logos.logos_kvs (key, val) VALUES ('db_contents', ?)")
            .values(vec![Value::from(Bytes::new(buf))])
            .finalize();

        let mut session = self.pool.get()
            .map_err(|e| e.to_string())?;
        session.query(query, false, false)
            .map_err(|e| e.to_string())?;

        Ok(())

    }

    fn get_contents(&self) -> Result<DbContents, String> {
        let mut session = self.pool.get()
            .map_err(|e| e.to_string())?;
        let query = QueryBuilder::new("SELECT val FROM logos.logos_kvs WHERE key = 'db_contents'")
            .finalize();
        match session.query(query, false, false)
            .and_then(|frame| frame.get_body())
            .map(|body| body.into_rows()) {
                Ok(Some(rows)) => {
                    let v: Vec<u8> = rows[0].r_by_name("val").unwrap();
                    let mut de = Deserializer::new(&v[..]);
                    match Deserialize::deserialize(&mut de) {
                        Ok(contents) => Ok(contents),
                        Err(err) => Err(err.to_string())
                    }
                },
                _ => Err("could not retrieve contents".to_string())
            }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn can_create() {
        let _: CassandraStore<String> = CassandraStore::new("127.0.0.1:9042").unwrap();
    }

    #[test]
    fn test_get_and_set() {
        let node = IndexNode::Leaf {
            items: vec!["hi there".to_string()]
        };

        let store: CassandraStore<String> = CassandraStore::new("127.0.0.1:9042").unwrap();

        let key = store.add(node.clone()).expect("Could not add node");
        let roundtrip_node = store.get(&key).expect("Could not deserialize node");
        assert_eq!(node, roundtrip_node);
    }
}
