#![cfg_attr(test, feature(test))]
#![feature(slice_patterns)]

extern crate itertools;

#[macro_use]
extern crate combine;

extern crate prettytable as pt;
extern crate chrono;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate rmp_serde;

extern crate cdrs;
extern crate rusqlite;
extern crate r2d2;

extern crate lru_cache;
extern crate uuid;

extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

#[macro_use]
extern crate lazy_static;

#[cfg(test)]
#[macro_use]
extern crate proptest;

use itertools::*;

use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use std::iter;
use std::ops::RangeBounds;
use std::result;

pub mod db;
pub mod parser;
pub mod index;
pub mod backends;
pub mod tx;
pub mod network;
pub mod conn;
mod query;
mod queries;
mod rbtree;
mod durable_tree;
mod ident;

pub use parser::{parse_input, parse_tx, parse_query, Input};
use query::{Query, Clause, Term, Var};
use index::Comparator;
use backends::KVStore;
pub use ident::IdentMap;

use std::collections::Bound;
use chrono::prelude::{DateTime, Utc};

// The Record struct represents a single (entity, attribute, value,
// transaction) tuple in the database. Note that indices do NOT use
// the derived ordering; instead they use custom sort functions.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct Record {
    pub entity: Entity,
    pub attribute: Entity,
    pub value: Value,
    /// The entity of the transaction in which the record was created.
    pub tx: Entity,
    /// Marks whether the fact is an addition or a retraction.
    /// (It's "retracted" and not "added" to ensure that retractions are sorted
    /// as greater than additions.)
    pub retracted: bool,
}

// We need a struct to represent facts that may not be in the database
// and may not have valid attributes, for use by the parser and
// unifier.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct Fact {
    pub entity: Entity,
    pub attribute: String,
    pub value: Value,
}

impl Fact {
    pub fn new<A: Into<String>, V: Into<Value>>(e: Entity, a: A, v: V) -> Fact {
        Fact {
            entity: e,
            attribute: a.into(),
            value: v.into(),
        }
    }
}

impl Record {
    pub fn addition<V: Into<Value>>(e: Entity, a: Entity, v: V, tx: Entity) -> Record {
        Record {
            entity: e,
            attribute: a,
            value: v.into(),
            tx: tx,
            retracted: false,
        }
    }

    pub fn retraction<V: Into<Value>>(e: Entity, a: Entity, v: V, tx: Entity) -> Record {
        Record {
            entity: e,
            attribute: a,
            value: v.into(),
            tx: tx,
            retracted: true,
        }
    }
}

impl RangeBounds<Record> for Record {
    fn start_bound(&self) -> Bound<&Record> {
        Bound::Included(&self)
    }

    fn end_bound(&self) -> Bound<&Record> {
        Bound::Unbounded
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum Value {
    String(String),
    Ident(String),
    Entity(Entity),
    // FIXME: clock drift is an issue here
    Timestamp(DateTime<Utc>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Value::Entity(e) => format!("{}", e.0),
                Value::String(ref s) => format!("\"{}\"", s),
                Value::Ident(ref s) => format!("{}", s),
                Value::Timestamp(t) => format!("{}", t),
            }
        )
    }
}

impl<T> From<T> for Value
where
    T: Into<String>,
{
    fn from(x: T) -> Self {
        Value::String(x.into())
    }
}

impl From<Entity> for Value {
    fn from(x: Entity) -> Self {
        Value::Entity(x.into())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub struct Entity(pub i64);


#[derive(Debug, Serialize, Deserialize)]
pub struct Error(String);

impl<S: ToString> From<S> for Error {
    fn from(other: S) -> Error {
        Error(other.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;

/// A struct representing the answer to a query. The first term is the find clause of the query,
/// used to order the result bindings into tuples for display; the second term is a vector of bindings
/// that satisfy the query.
// FIXME: this should just be a Vec<Vec<Value>>, probably.
#[derive(Debug, PartialEq)]
pub struct QueryResult(pub Vec<Var>, pub Vec<HashMap<Var, Value>>);

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let num_columns = self.0.len();
        let align = pt::format::Alignment::CENTER;
        let mut titles: pt::row::Row = self.0.iter().map(|var| var.name.clone()).collect();
        titles.iter_mut().foreach(|c| c.align(align));

        let rows = self.1
            .iter()
            .map(|row_ht| {
                self.0.iter().map(|var| format!("{}", row_ht[var])).into()
            })
            .collect_vec();

        let mut table = pt::Table::new();
        table.set_titles(titles);

        table.set_format(*pt::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

        if rows.is_empty() {
            table.add_row(iter::repeat("").take(num_columns).collect());
        }

        for row in rows {
            table.add_row(row);
        }

        for row in table.row_iter_mut() {
            for cell in row.iter_mut() {
                cell.align(align);
            }
        }

        writeln!(f, "{}", table)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Tx {
    pub items: Vec<TxItem>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum TxItem {
    Addition(Fact),
    Retraction(Fact),
    NewEntity(HashMap<String, Value>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum TxReport {
    Success { new_entities: Vec<Entity> },
    Failure(String),
}

type Binding = HashMap<Var, Value>;

impl Clause {
    fn substitute(&self, env: &Binding, idents: &IdentMap) -> Result<Clause> {
        let entity = match &self.entity {
            &Term::Bound(_) => self.entity.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match *val {
                        Value::Entity(e) => Term::Bound(e),
                        _ => return Err("type mismatch".into()),
                    }
                } else {
                    self.entity.clone()
                }
            }
        };

        let attribute = match &self.attribute {
            &Term::Bound(_) => self.attribute.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match val {
                        &Value::String(ref s) => Term::Bound(s.clone()),
                        &Value::Entity(e) => idents.get_ident(e)
                            .map(Term::Bound)
                            .expect("non-attribute bound in attribute position"),
                        _ => return Err("type mismatch".into()),
                    }
                } else {
                    self.attribute.clone()
                }
            }
        };

        let value = match &self.value {
            &Term::Bound(_) => self.value.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    Term::Bound(val.clone())
                } else {
                    self.value.clone()
                }
            }
        };

        Ok(Clause::new(entity, attribute, value))
    }
}


macro_rules! comparator {
    ($name:ident, $first:ident, $second:ident, $third:ident, $fourth:ident) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;

        impl Comparator for $name {
            type Item = Record;

            fn compare(a: &Record, b: &Record) -> std::cmp::Ordering {
                a.$first.cmp(&b.$first)
                    .then(a.$second.cmp(&b.$second))
                    .then(a.$third.cmp(&b.$third))
                    .then(a.$fourth.cmp(&b.$fourth))
                    // retracted is always last
                    .then(a.retracted.cmp(&b.retracted))
            }
        }
    }
}

comparator!(EAVT, entity, attribute, value, tx);
comparator!(AEVT, attribute, entity, value, tx);
comparator!(AVET, attribute, value, entity, tx);
comparator!(VAET, value, attribute, entity, tx);



#[cfg(test)]
pub mod tests {
    use super::*;
    extern crate test;
    use self::test::{Bencher, black_box};

    use std::iter;
    use std::sync::Arc;

    use backends::mem::HeapStore;
    use conn::Conn;
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

    pub fn test_db() -> Db {
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
    fn test_constraint() {
        // find ?a ?b where (?a name ?b) (< ?b "Charlie")
        expect_query_result(
            &parse_query("find ?a ?b where (?a name ?b) (< ?b \"Charlie\")").unwrap(),
            QueryResult(
                vec![Var::new("a"), Var::new("b")],
                vec![
                    vec![
                        (Var::new("a"), Value::Entity(Entity(10))),
                        (Var::new("b"), Value::String("Bob".into())),
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
