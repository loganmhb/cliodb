use super::*;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;

use futures::future::Future;
use tokio_proto::TcpClient;
use tokio_core::reactor::Core;
use tokio_service::Service;

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueType {
    String,
    Ident,
    Entity,
    Timestamp,
}

/// An *immutable* view of the database at a point in time.
/// Only used for querying; for transactions, you need a Conn.
pub struct Db {
    pub idents: IdentMap,
    pub schema: HashMap<Entity, ValueType>,
    pub store: Arc<KVStore + 'static>,
    pub eav: Index<Record, EAVT>,
    pub ave: Index<Record, AVET>,
    pub aev: Index<Record, AEVT>,
    pub vae: Index<Record, VAET>,
}

impl Db {
    pub fn new(contents: DbContents, store: Arc<KVStore>) -> Db {
        let db = Db {
            store: store.clone(),
            idents: contents.idents,
            schema: contents.schema,
            eav: Index::new(contents.eav, store.clone(), EAVT),
            ave: Index::new(contents.ave, store.clone(), AVET),
            aev: Index::new(contents.aev, store.clone(), AEVT),
            vae: Index::new(contents.vae, store, VAET),
        };

        db
    }

    pub fn mem_index_size(&self) -> usize {
        self.eav.mem_index_size()
    }

    fn records_matching(&self, clause: &Clause, binding: &Binding) -> Result<Vec<Record>> {
        let expanded = clause.substitute(binding)?;
        match expanded {
            // ?e a v => use the ave index
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {
                match self.idents.get_entity(&a) {
                    Some(attr) => {
                        let range_start = Record::addition(Entity(0), attr, v.clone(), Entity(0));
                        Ok(
                            self.ave
                                .range_from(range_start)
                                .take_while(|rec| rec.attribute == attr && rec.value == v)
                                .collect(),
                        )
                    }
                    _ => return Err("invalid attribute".into()),
                }
            }
            // // e a ?v => use the eav index
            Clause {
                entity: Term::Bound(e),
                attribute: Term::Bound(a),
                value: Term::Unbound(_),
            } => {
                match self.idents.get_entity(&a) {
                    Some(attr) => {
                        // Value::String("") is the lowest-sorted value
                        let range_start =
                            Record::addition(e, attr, Value::String("".into()), Entity(0));
                        Ok(
                            self.eav
                                .range_from(range_start)
                                .take_while(|rec| rec.entity == e && rec.attribute == attr)
                                .collect(),
                        )
                    }
                    _ => return Err("invalid attribute".into()),
                }
            }
            // FIXME: Implement other optimized index use cases? (multiple unknowns? refs?)
            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                Ok(
                    self.eav
                        .iter()
                        .filter(|f| unify(&binding, &self.idents, &clause, &f).is_some())
                        .collect(),
                )
            }
        }
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        // TODO: automatically bind ?tx in queries
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for record in self.records_matching(clause, &binding)? {
                    match unify(&binding, &self.idents, clause, &record) {
                        Some(new_info) => {
                            if record.retracted {
                                // The binding matches the retraction
                                // so we discard any existing bindings
                                // that are the same.  Note that this
                                // relies on the fact that additions
                                // and retractions are sorted by
                                // transaction, so an older retraction
                                // won't delete the binding for a
                                // newer addition.
                                new_bindings.retain(|b| *b != new_info);
                            } else {
                                new_bindings.push(new_info)
                            }
                        }
                        _ => continue,
                    }
                }
            }

            bindings = new_bindings;
        }

        for binding in bindings.iter_mut() {
            *binding = binding
                .iter()
                .filter(|&(k, _)| query.find.contains(k))
                .map(|(var, value)| (var.clone(), value.clone()))
                .collect();
        }

        Ok(QueryResult(query.find.clone(), bindings))
    }
}

/// Attempts to unify a new record and a clause with existing
/// bindings.  If bound fields in the clause match the record, then
/// any fields in the record which match an unbound clause will be
/// bound in the returned binding.  If bound fields in the clause
/// conflict with fields in the record, unification fails.
fn unify(env: &Binding, idents: &IdentMap, clause: &Clause, record: &Record) -> Option<Binding> {
    let mut new_env: Binding = env.clone();

    match clause.entity {
        Term::Bound(ref e) => {
            if *e != record.entity {
                return None;
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::Entity(record.entity) {
                        return None;
                    }
                }
                _ => {
                    new_env.insert(var.clone(), Value::Entity(record.entity));
                }
            }
        }
    }

    match clause.attribute {
        Term::Bound(ref a) => {
            // The query will use an ident to refer to the attribute, but we need the
            // actual attribute entity.
            match idents.get_entity(a) {
                Some(e) => {
                    if e != record.attribute {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::Entity(record.attribute) {
                        return None;
                    }
                }
                _ => {
                    new_env.insert(var.clone(), Value::Entity(record.attribute));
                }
            }
        }
    }

    match clause.value {
        Term::Bound(ref v) => {
            if *v != record.value {
                return None;
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != record.value {
                        return None;
                    }
                }
                _ => {
                    new_env.insert(var.clone(), record.value.clone());
                }
            }
        }
    }

    Some(new_env)
}

/// A structure designed to be stored in the backing store that enables
/// a process to locate the indexes, tx log, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbContents {
    pub next_id: i64,
    pub last_indexed_tx: i64,
    pub idents: IdentMap,
    pub schema: HashMap<Entity, ValueType>,
    pub eav: String,
    pub ave: String,
    pub aev: String,
    pub vae: String,
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
        _ => Err("Invalid uri".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;
    use self::test::{Bencher, black_box};

    use std::iter;
    use std::sync::Arc;

    use backends::mem::HeapStore;
    use db::Db;

    fn expect_query_result(query: &Query, expected: QueryResult) {
        let db = test_db();
        let result = db.query(query).unwrap();
        assert_eq!(expected, result);
    }

    fn test_conn() -> Conn {
        let store = HeapStore::new::<Record>();
        let conn = Conn::new(Arc::new(store)).unwrap();
        let records = vec![
            Fact::new(Entity(10), "name", Value::String("Bob".into())),
            Fact::new(Entity(11), "name", Value::String("John".into())),
            Fact::new(Entity(12), "Hello", Value::String("World".into())),
            Fact::new(Entity(11), "parent", Entity(10)),
        ];

        parse_tx(
            "{db:ident name db:valueType db:type:string}
                  {db:ident parent db:valueType db:type:entity}
                  {db:ident Hello db:valueType db:type:string}",
        ).map_err(|e| e.into())
            .and_then(|tx| conn.transact(tx))
            .map(|tx_result| {
                use TxReport;
                match tx_result {
                    TxReport::Success { .. } => (),
                    TxReport::Failure(msg) => panic!(format!("failed in schema with '{}'", msg)),
                };
            })
            .unwrap();

        conn.transact(Tx {
            items: records
                .iter()
                .map(|x| TxItem::Addition(x.clone()))
                .collect(),
        }).map(|tx_result| {
                use TxReport;
                match tx_result {
                    TxReport::Success { .. } => (),
                    TxReport::Failure(msg) => panic!(format!("failed in insert with '{}'", msg)),
                };
            })
            .unwrap();

        conn
    }

    fn test_db() -> Db {
        test_conn().db().unwrap()
    }

    #[test]
    fn test_query_unknown_entity() {
        // find ?a where (?a name "Bob")
        expect_query_result(
            &parse_query("find ?a where (?a name \"Bob\")").unwrap(),
            QueryResult(
                vec![Var::new("a")],
                vec![
                    iter::once((Var::new("a"), Value::Entity(Entity(10)))).collect(),
                ],
            ),
        );
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        expect_query_result(
            &parse_query("find ?a where (10 name ?a)").unwrap(),
            QueryResult(
                vec![Var::new("a")],
                vec![
                    iter::once((Var::new("a"), Value::String("Bob".into()))).collect(),
                ],
            ),
        );

    }

    // // It's inconvenient to test this because we don't have a ref to the db in
    // // the current setup, and we don't know the entity id of `name` offhand.
    // #[test]
    // fn test_query_unknown_attribute() {
    //     // find ?a where (1 ?a "John")
    //     expect_query_result(&parse_query("find ?a where (1 ?a \"John\")").unwrap(),
    //                         QueryResult(vec![Var::new("a")],
    //                                     vec![
    //         iter::once((Var::new("a"),
    //                     Value::String("name".into())))
    //                 .collect(),
    //     ]));
    // }

    #[test]
    fn test_query_multiple_results() {
        // find ?a ?b where (?a name ?b)
        expect_query_result(
            &parse_query("find ?a ?b where (?a name ?b)").unwrap(),
            QueryResult(
                vec![Var::new("a"), Var::new("b")],
                vec![
                    vec![
                        (Var::new("a"), Value::Entity(Entity(10))),
                        (Var::new("b"), Value::String("Bob".into())),
                    ].into_iter()
                        .collect(),
                    vec![
                        (Var::new("a"), Value::Entity(Entity(11))),
                        (Var::new("b"), Value::String("John".into())),
                    ].into_iter()
                        .collect(),
                ],
            ),
        );
    }

    #[test]
    fn test_query_explicit_join() {
        expect_query_result(
            &parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)").unwrap(),
            QueryResult(
                vec![Var::new("b")],
                vec![
                    iter::once((Var::new("b"), Value::Entity(Entity(11)))).collect(),
                ],
            ),
        );
    }

    #[test]
    fn test_query_implicit_join() {
        expect_query_result(
            &parse_query(
                "find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)",
            ).unwrap(),
            QueryResult(
                vec![Var::new("c")],
                vec![
                    iter::once((Var::new("c"), Value::String("John".into()))).collect(),
                ],
            ),
        );
    }

    #[test]
    fn test_type_mismatch() {
        let db = test_db();
        let q = &parse_query("find ?e ?n where (?e name ?n) (?n name \"hi\")").unwrap();
        assert_equal(db.query(&q), Err("type mismatch".to_string()))
    }

    #[test]
    fn test_retractions() {
        let conn = test_conn();
        conn.transact(parse_tx("retract (11 parent 10)").unwrap())
            .unwrap();
        let result = conn.db()
            .unwrap()
            .query(&parse_query("find ?a ?b where (?a parent ?b)").unwrap())
            .unwrap();

        assert_eq!(
            result,
            QueryResult(vec![Var::new("a"), Var::new("b")], vec![])
        );
    }
    #[bench]
    // Parse + run a query on a small db
    fn parse_bench(b: &mut Bencher) {
        // the implicit join query
        let input = black_box(
            r#"find ?c where (?a name "Bob") (?b name ?c) (?b parent ?a)"#,
        );

        b.iter(|| parse_query(input).unwrap());
    }

    #[bench]
    // Parse + run a query on a small db
    fn run_bench(b: &mut Bencher) {
        // the implicit join query
        let input = black_box(
            r#"find ?c where (?a name "Bob") (?b name ?c) (?b parent ?a)"#,
        );
        let query = parse_query(input).unwrap();
        let db = test_db();

        b.iter(|| db.query(&query));
    }

    #[bench]
    fn bench_add(b: &mut Bencher) {
        let store = HeapStore::new::<Record>();
        let conn = Conn::new(Arc::new(store)).unwrap();
        parse_tx("{db:ident blah}")
            .map(|tx| conn.transact(tx))
            .unwrap()
            .unwrap();

        let mut e = 0;

        b.iter(|| {
            let entity = Entity(e);
            e += 1;

            conn.transact(Tx {
                items: vec![
                    TxItem::Addition(Fact::new(entity, "blah", Value::Entity(entity))),
                ],
            }).unwrap();
        });
    }

    fn test_db_large() -> Db {
        let store = HeapStore::new::<Record>();
        let conn = Conn::new(Arc::new(store)).unwrap();
        let n = 10_000;

        parse_tx("{db:ident name} {db:ident Hello}")
            .map_err(|e| e.into())
            .and_then(|tx| conn.transact(tx))
            .unwrap();

        for i in (0..n).into_iter() {
            let a = if i % 23 <= 10 {
                "name".to_string()
            } else {
                "Hello".to_string()
            };

            let v = if i % 1123 == 0 { "Bob" } else { "Rob" };

            conn.transact(Tx {
                items: vec![TxItem::Addition(Fact::new(Entity(i), a, v))],
            }).unwrap();
        }

        conn.db().unwrap()
    }


    #[test]
    fn test_records_matching() {
        let matching = test_db()
            .records_matching(
                &Clause::new(
                    Term::Unbound("e".into()),
                    Term::Bound("name".into()),
                    Term::Bound(Value::String("Bob".into())),
                ),
                &Binding::default(),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        let rec = &matching[0];
        assert_eq!(rec.entity, Entity(10));
        assert_eq!(rec.value, Value::String("Bob".into()));
    }

    #[bench]
    fn bench_large_db_simple(b: &mut Bencher) {
        // Don't run on 'cargo test', only 'cargo bench'
        if cfg!(not(debug_assertions)) {
            let query = black_box(parse_query(r#"find ?a where (?a name "Bob")"#).unwrap());
            let db = test_db_large();

            b.iter(|| db.query(&query).unwrap());
        }
    }
}
