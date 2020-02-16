use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use {Result, Tx, TxReport, Entity, Record, EAVT, AEVT, AVET, VAET};
use backends::KVStore;
use backends::sqlite::SqliteStore;
use backends::mem::HeapStore;
use backends::mysql::MysqlStore;
use db::{Db, DbMetadata};
use index::Index;
use tx;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxClient {
    Network(SocketAddr),
    Local,
}

// We need a way to ensure, for local stores, that only one thread is
// transacting at a time.
// FIXME: Super kludgy. There must be a better way to do this.
lazy_static! {
    static ref TX_LOCK: Mutex<()> = Mutex::new(());
}

pub struct Conn {
    transactor: TxClient,
    store: Arc<dyn KVStore>,
    latest_db: Option<Db>,
    last_known_tx: Option<i64>,
    last_seen_metadata: Option<DbMetadata>,
}

impl Conn {
    pub fn new(store: Arc<dyn KVStore>) -> Result<Conn> {
        let transactor = store.get_transactor()?;
        Ok(Conn { transactor, store, latest_db: None, last_known_tx: None, last_seen_metadata: None })
    }

    pub fn db(&mut self) -> Result<Db> {
        let metadata: DbMetadata = self.store.get_metadata()?;

        if Some(&metadata) != self.last_seen_metadata.as_ref() {
            // The underlying index has changed, so we need a new database. Invalidate the cache.
            self.last_known_tx = None;
            self.latest_db = None;
            self.last_seen_metadata = Some(metadata.clone());
        }

        // In order to avoid replaying transactions over and over on subsequent calls to db(),
        // we need to keep track of our place in the transaction log.
        let mut last_known_tx: i64 = self.last_known_tx.unwrap_or_else(|| metadata.last_indexed_tx);

        let mut db = self.latest_db.clone().unwrap_or_else(|| Db {
            store: self.store.clone(),
            schema: metadata.schema.clone(),
            eav: Index::new(metadata.eav.clone(), self.store.clone(), EAVT),
            ave: Index::new(metadata.ave.clone(), self.store.clone(), AVET),
            aev: Index::new(metadata.aev.clone(), self.store.clone(), AEVT),
            vae: Index::new(metadata.vae, self.store.clone(), VAET),
        });

        // Read in latest transactions from the log.
        for tx in self.store.get_txs(last_known_tx)? {
            for record in tx.records {
                let Entity(tx_id) = record.tx;
                db = db.add_record(record)?;
                last_known_tx = tx_id;
            }
        }

        self.last_known_tx = Some(last_known_tx).clone();
        self.latest_db = Some(db.clone());

        Ok(db)
    }

    pub fn transact(&self, tx: Tx) -> Result<TxReport> {
        match self.transactor {
            TxClient::Network(_addr) => {
                unimplemented!()
                // let mut core = Core::new().unwrap();
                // let handle = core.handle();
                // let client = TcpClient::new(LineProto).connect(&addr, &handle);

                // core.run(client.and_then(|client| client.call(tx)))
                //     .unwrap_or_else(|e| Err(Error(e.to_string())))
            }
            TxClient::Local => {
                let store = self.store.clone();
                #[allow(unused_variables)]
                let l = TX_LOCK.lock()?;
                let mut transactor = tx::Transactor::new(store)?;
                let result = transactor.process_tx(tx);
                Ok(match result {
                    Ok(new_entities) => TxReport::Success { new_entities },
                    Err(e) => TxReport::Failure(format!("{:?}", e)),
                })
            }
        }
    }
}

pub fn store_from_uri(uri: &str) -> Result<Arc<dyn KVStore>> {
    match &uri.split("//").collect::<Vec<_>>()[..] {
        &["logos:mem:", _] => Ok(Arc::new(HeapStore::new::<Record>()) as Arc<dyn KVStore>),
        &["logos:sqlite:", path] => {
            let sqlite_store = SqliteStore::new(path)?;
            Ok(Arc::new(sqlite_store) as Arc<dyn KVStore>)
        }
        &["logos:mysql:", url] => {
            let mysql_store = MysqlStore::new(&format!("mysql://{}", url))?;
            Ok(Arc::new(mysql_store) as Arc<dyn KVStore>)
        }
        _ => Err("Invalid uri".into()),
    }
}
