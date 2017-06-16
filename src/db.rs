use super::*;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;

use btree::IndexNode;
use tx::Transactor;

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

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
}

impl Conn {
    pub fn db(&self) -> Result<Db> {
        let contents: DbContents = self.store.get_contents()?;

        let node_store = btree::NodeStore::new(self.store.clone());
        Ok(Db {
            store: self.store.clone(),
            idents: contents.idents,
            eav: Index::new(contents.eav, node_store.clone(), EAVT),
            ave: Index::new(contents.ave, node_store.clone(), AVET),
            aev: Index::new(contents.aev, node_store, AEVT),
        })
    }

    pub fn transact(&self, tx: Tx) -> Result<TxReport> {
        match self.transactor {
            // TODO: Don't ignore the addr here.
            TxClient::Network(_) => {
                let mut msg_buf: Vec<u8> = Vec::new();
                tx.serialize(&mut Serializer::new(&mut msg_buf))?;
                let ctx = zmq::Context::new();
                let socket = ctx.socket(zmq::REQ)?;
                socket.connect("tcp://localhost:10405")?;
                socket.send_msg(zmq::Message::from_slice(&msg_buf)?, 0)?;

                let result = socket.recv_msg(0)?;
                let mut de = Deserializer::new(&result[..]);
                let report: TxReport = Deserialize::deserialize(&mut de)?;
                Ok(report)
            }
            TxClient::Local => {
                let store = self.store.clone();
                let _ = TX_LOCK.lock()?;
                let mut transactor = Transactor::new(store)?;
                transactor.process_tx(tx)
            }
        }
    }
}

/// An *immutable* view of the database at a point in time.
/// Only used for querying; for transactions, you need a Conn.
pub struct Db {
    pub idents: IdentMap,
    pub store: Arc<KVStore + 'static>,
    pub eav: Index<Record, EAVT>,
    pub ave: Index<Record, AVET>,
    pub aev: Index<Record, AEVT>,
}

impl Db {
    pub fn new(contents: DbContents, store: Arc<KVStore>) -> Db {
        let node_store = btree::NodeStore::new(store.clone());
        let db = Db {
            store: store,
            idents: contents.idents,
            eav: Index::new(contents.eav, node_store.clone(), EAVT),
            ave: Index::new(contents.ave, node_store.clone(), AVET),
            aev: Index::new(contents.aev, node_store, AEVT),
        };

        db
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
                match self.idents.get_entity(a) {
                    Some(attr) => {
                        let range_start = Record::addition(Entity(0), attr, v.clone(), Entity(0));
                        Ok(self.ave
                               .iter_range_from(range_start..)?
                               .map(|res| res.unwrap())
                               .take_while(|rec| rec.attribute == attr && rec.value == v)
                               .collect())
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
                match self.idents.get_entity(a) {
                    Some(attr) => {
                        // Value::String("") is the lowest-sorted value
                        let range_start =
                            Record::addition(e, attr, Value::String("".into()), Entity(0));
                        Ok(self.eav
                               .iter_range_from(range_start..)?
                               .map(|res| res.unwrap())
                               .take_while(|rec| rec.entity == e && rec.attribute == attr)
                               .collect())
                    }
                    _ => return Err("invalid attribute".into()),
                }
            }
            // FIXME: Implement other optimized index use cases? (multiple unknowns? refs?)
            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                Ok(self.eav
                    .iter()
                    .map(|f| f.unwrap()) // FIXME this is not safe :D
                    .filter(|f| unify(&binding, &self.idents, &clause, &f).is_some())
                    .collect())
            }
        }
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        // TODO: automatically bind ?tx in queries
        println!("Starting query: {}", UTC::now());
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

        println!("Finished query: {}", UTC::now());
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
            match idents.get_entity(a.to_owned()) {
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
    pub next_id: u64,
    pub idents: IdentMap,
    pub eav: String,
    pub ave: String,
    pub aev: String,
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

pub fn add_node<T>(store: &KVStore, node: IndexNode<T>) -> Result<String>
    where T: Serialize
{
    let mut buf = Vec::new();
    node.serialize(&mut Serializer::new(&mut buf))?;

    let key: String = Uuid::new_v4().to_string();
    store.set(&key, &buf)?;
    Ok(key)
}

/// Fetches and deserializes the node with the given key.
pub fn get_node<'de, T>(store: &KVStore, key: &str) -> Result<IndexNode<T>>
    where T: Deserialize<'de> + Clone
{
    let serialized = store.get(key)?;
    let mut de = Deserializer::new(&serialized[..]);
    let node: IndexNode<T> = Deserialize::deserialize(&mut de)?;
    Ok(node.clone())
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
            Fact::new(Entity(0), "name", "Bob"),
            Fact::new(Entity(1), "name", "John"),
            Fact::new(Entity(2), "Hello", "World"),
            Fact::new(Entity(1), "parent", Entity(0)),
        ];

        parse_tx("{db:ident name} {db:ident parent} {db:ident Hello}")
            .map_err(|e| e.into())
            .and_then(|tx| conn.transact(tx))
            .unwrap();

        conn.transact(Tx {
                          items: records
                              .iter()
                              .map(|x| TxItem::Addition(x.clone()))
                              .collect(),
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
        expect_query_result(&parse_query("find ?a where (?a name \"Bob\")").unwrap(),
                            QueryResult(vec![Var::new("a")],
                                        vec![
            iter::once((Var::new("a"), Value::Entity(Entity(0))))
                .collect(),
        ]));
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        expect_query_result(&parse_query("find ?a where (0 name ?a)").unwrap(),
                            QueryResult(vec![Var::new("a")],
                                        vec![
            iter::once((Var::new("a"),
                        Value::String("Bob".into())))
                    .collect(),
        ]));

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
        expect_query_result(&parse_query("find ?a ?b where (?a name ?b)").unwrap(),
                            QueryResult(vec![Var::new("a"), Var::new("b")],
                                        vec![
            vec![
                (Var::new("a"), Value::Entity(Entity(0))),
                (Var::new("b"), Value::String("Bob".into())),
            ]
                    .into_iter()
                    .collect(),
            vec![
                (Var::new("a"), Value::Entity(Entity(1))),
                (Var::new("b"), Value::String("John".into())),
            ]
                    .into_iter()
                    .collect(),
        ]));
    }

    #[test]
    fn test_query_explicit_join() {
        // find ?b where (?a name Bob) (?b parent ?a)
        expect_query_result(&parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)")
                                 .unwrap(),
                            QueryResult(vec![Var::new("b")],
                                        vec![
            iter::once((Var::new("b"), Value::Entity(Entity(1))))
                .collect(),
        ]));
    }

    #[test]
    fn test_query_implicit_join() {
        // find ?c where (?a name Bob) (?b name ?c) (?b parent ?a)
        expect_query_result(&parse_query("find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)")
                    .unwrap(),
               QueryResult(vec![Var::new("c")],
                           vec![
            iter::once((Var::new("c"), Value::String("John".into())))
                .collect(),
        ]));
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
        conn.transact(parse_tx("retract (1 parent 0)").unwrap())
            .unwrap();
        let result = conn.db()
            .unwrap()
            .query(&parse_query("find ?a ?b where (?a parent ?b)").unwrap())
            .unwrap();

        assert_eq!(result,
                   QueryResult(vec![Var::new("a"), Var::new("b")], vec![]));
    }
    #[bench]
    // Parse + run a query on a small db
    fn parse_bench(b: &mut Bencher) {
        // the implicit join query
        let input = black_box(r#"find ?c where (?a name "Bob") (?b name ?c) (?b parent ?a)"#);

        b.iter(|| parse_query(input).unwrap());
    }

    #[bench]
    // Parse + run a query on a small db
    fn run_bench(b: &mut Bencher) {
        // the implicit join query
        let input = black_box(r#"find ?c where (?a name "Bob") (?b name ?c) (?b parent ?a)"#);
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
                TxItem::Addition(Fact::new(entity,
                                           "blah",
                                           Value::Entity(entity))),
            ],
                          }).unwrap();
        });
    }

    #[cfg(not(debug_assertions))]
    fn test_db_large() -> Db {
        let store = HeapStore::new();
        let mut db = Db::new(Arc::new(store)).unwrap();
        let n = 10_000_000;

        for i in 0..n {
            let a = if i % 23 < 10 {
                "name".to_string()
            } else {
                "Hello".to_string()
            };

            let v = if i % 1123 == 0 { "Bob" } else { "Rob" };

            let attr = db.idents.get_entity(a).unwrap();
            db.add(Record::addition(Entity(i), attr, v, Entity(0)));
        }

        db
    }


    #[test]
    fn test_records_matching() {
        let matching = test_db()
            .records_matching(&Clause::new(Term::Unbound("e".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound(Value::String("Bob".into()))),
                              &Binding::default())
            .unwrap();
        assert_eq!(matching.len(), 1);
        let rec = &matching[0];
        assert_eq!(rec.entity, Entity(0));
        assert_eq!(rec.value, Value::String("Bob".into()));
    }
    // Don't run on 'cargo test', only 'cargo bench'
    #[cfg(not(debug_assertions))]
    #[bench]
    fn large_db_simple(b: &mut Bencher) {
        let query = black_box(parse_query(r#"find ?a where (?a name "Bob")"#).unwrap());
        let db = test_db_large();

        b.iter(|| db.query(&query));
    }
}
