use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::future::Future;
use tokio_proto::TcpClient;
use tokio_core::reactor::Core;
use tokio_service::Service;

use {Result, Error, Tx, TxReport, Record, EAVT, AEVT, AVET, VAET};
use backends::KVStore;
use backends::sqlite::SqliteStore;
use backends::mem::HeapStore;
use backends::mysql::MysqlStore;
use backends::cassandra::CassandraStore;
use db::{Db, DbContents};
use index::Index;
use network::LineProto;
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
    store: Arc<KVStore>,
}

impl Conn {
    pub fn new(store: Arc<KVStore>) -> Result<Conn> {
        let transactor = store.get_transactor()?;
        Ok(Conn { transactor, store })
    }

    pub fn db(&self) -> Result<Db> {
        let contents: DbContents = self.store.get_contents()?;

        let mut db = Db {
            store: self.store.clone(),
            idents: contents.idents,
            schema: contents.schema,
            eav: Index::new(contents.eav, self.store.clone(), EAVT),
            ave: Index::new(contents.ave, self.store.clone(), AVET),
            aev: Index::new(contents.aev, self.store.clone(), AEVT),
            vae: Index::new(contents.vae, self.store.clone(), VAET),
        };

        // Read in latest transactions from the log.
        // FIXME: This will re-read transactions again and again each
        // time you call db(), but it should be possible to keep track
        // of the latest tx that this connection knows about and only
        // read ones more recent than that, instead of using
        // `contents.last_indexed_tx`.  (This might require some
        // rethinking of retrieving the db contents each time db() is
        // called.)
        for tx in self.store.get_txs(contents.last_indexed_tx)? {
            for record in tx.records {
                db = tx::add(&db, record)?;
            }
        }

        Ok(db)
    }

    pub fn transact(&self, tx: Tx) -> Result<TxReport> {
        match self.transactor {
            TxClient::Network(addr) => {
                let mut core = Core::new().unwrap();
                let handle = core.handle();
                let client = TcpClient::new(LineProto).connect(&addr, &handle);

                core.run(client.and_then(|client| client.call(tx)))
                    .unwrap_or_else(|e| Err(Error(e.to_string())))
            }
            TxClient::Local => {
                let store = self.store.clone();
                #[allow(unused_variables)]
                let l = TX_LOCK.lock()?;
                let mut transactor = tx::Transactor::new(store)?;
                let result = transactor.process_tx(tx);
                result
            }
        }
    }
}

pub fn store_from_uri(uri: &str) -> Result<Arc<KVStore>> {
    match &uri.split("//").collect::<Vec<_>>()[..] {
        &["logos:mem:", _] => Ok(Arc::new(HeapStore::new::<Record>()) as Arc<KVStore>),
        &["logos:sqlite:", path] => {
            let sqlite_store = SqliteStore::new(path)?;
            Ok(Arc::new(sqlite_store) as Arc<KVStore>)
        }
        &["logos:cass:", url] => {
            let cass_store = CassandraStore::new(url)?;
            Ok(Arc::new(cass_store) as Arc<KVStore>)
        }
        &["logos:mysql:", url] => {
            let mysql_store = MysqlStore::new(&format!("mysql://{}", url))?;
            Ok(Arc::new(mysql_store) as Arc<KVStore>)
        }
        _ => Err("Invalid uri".into()),
    }
}
