use std::sync::{Arc, Mutex};

use rmp_serde;

use {Result, Tx, TxReport, Entity, EAVT, AEVT, AVET, VAET};
use backends::KVStore;
use backends::sqlite::SqliteStore;
use backends::mysql::MysqlStore;
use db::{Db, DbMetadata};
use index::Index;


pub struct Conn {
    socket: Arc<Mutex<zmq::Socket>>, // FIXME: is this actually necessary?
    store: Arc<dyn KVStore>,
    latest_db: Option<Db>,
    last_known_tx: Option<i64>,
    last_seen_metadata: Option<DbMetadata>,
}

// TODO: conn should have a way of subscribing to transactions
// so that it can play them against the db eagerly instead of only
// when a db is requested
impl Conn {
    pub fn new(
        store: Arc<dyn KVStore>,
        transactor_address: &str,
        context: &zmq::Context
    ) -> Result<Conn> {
        let socket = context.socket(zmq::REQ)?;
        socket.connect(transactor_address)?;
        Ok(Conn {
            socket: Arc::new(Mutex::new(socket)),
            store,
            latest_db: None,
            last_known_tx: None,
            last_seen_metadata: None
        })
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
        let mut last_known_tx: i64 = self.last_known_tx.unwrap_or(metadata.last_indexed_tx);

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
        let sock = self.socket.lock()?;
        sock.send(&rmp_serde::to_vec(&tx)?, 0)?;
        let reply = sock.recv_bytes(0)?;
        Ok(rmp_serde::from_read_ref(&reply)?)
    }
}

pub fn store_from_uri(uri: &str) -> Result<Arc<dyn KVStore>> {
    match &uri.split("//").collect::<Vec<_>>()[..] {
        &["cliodb:sqlite:", path] => {
            let sqlite_store = SqliteStore::new(path)?;
            Ok(Arc::new(sqlite_store) as Arc<dyn KVStore>)
        }
        &["cliodb:mysql:", url] => {
            let mysql_store = MysqlStore::new(&format!("mysql://{}", url))?;
            Ok(Arc::new(mysql_store) as Arc<dyn KVStore>)
        }
        _ => Err("Invalid uri".into()),
    }
}
