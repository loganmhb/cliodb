#![cfg_attr(test, feature(test))]

extern crate itertools;

#[macro_use]
extern crate combine;

extern crate prettytable as pt;
extern crate chrono;

extern crate serde;
extern crate rmp_serde;

extern crate im;
extern crate rusqlite;
extern crate mysql;

extern crate log;
extern crate lru_cache;
extern crate snap;
extern crate uuid;

extern crate zmq;

#[cfg(test)]
#[macro_use]
extern crate proptest;

#[cfg(test)]
extern crate test;

use itertools::*;

use std::fmt::{self, Display, Formatter};
use im::HashMap;
use std::iter;
use std::ops::RangeBounds;
use std::result;

use serde::{Serialize, Deserialize};

pub mod db;
pub mod parser;
pub mod index;
pub mod backends;
pub mod tx;
pub mod conn;
pub mod server;
mod schema;
mod queries;
mod rbtree;
mod durable_tree;

pub use parser::{parse_input, parse_tx, parse_query, Input};
use queries::query::{Clause, Term, Var};
pub use queries::execution::query;
use index::{Comparator, Equivalent};
use backends::KVStore;

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

impl Equivalent for Record {
    fn equivalent(&self, other: &Record) -> bool {
        self.attribute == other.attribute &&
            self.entity == other.entity &&
            self.value == other.value &&
            self.retracted == other.retracted
    }
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub enum Value {
    String(String),
    Ident(String),
    Ref(Entity),
    // FIXME: clock drift is an issue here
    Timestamp(DateTime<Utc>),
    Boolean(bool),
    Long(i64),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Value::Ref(e) => format!("{}", e.0),
                Value::String(ref s) => format!("\"{}\"", s),
                Value::Ident(ref s) => format!("{}", s),
                Value::Timestamp(t) => format!("{}", t),
                Value::Boolean(b) => format!("{}", b),
                Value::Long(l) => format!("{}", l),
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
        Value::Ref(x.into())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub struct Entity(pub i64);

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Ident {
    Name(String),
    Entity(Entity)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Error(String);

impl<S: ToString> From<S> for Error {
    fn from(other: S) -> Error {
        Error(other.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation(pub Vec<Var>, pub Vec<Vec<Value>>);

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let num_columns = self.0.len();
        let align = pt::format::Alignment::CENTER;
        let mut titles: pt::row::Row = self.0.iter().map(|var| var.name.clone()).collect();
        titles.iter_mut().foreach(|c| c.align(align));

        let rows = self.1
            .iter()
            .map(|row| {
                row.iter().map(|val| format!("{}", val)).into()
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

    use uuid::Uuid;

    extern crate test;
    use self::test::{Bencher, black_box};

    use conn::{Conn, store_from_uri};
    use queries::query::Query;
    use queries::execution::query;
    use server::TransactorService;

    // FIXME: conn should just have a way to run a local transactor
    macro_rules! with_test_conn {
        ( $conn:ident $body:block ) => { {
            let mut context = zmq::Context::new();
            let db_name = Uuid::new_v4();
            let store_uri = format!("cliodb:sqlite://file:{}?mode=memory&cache=shared", db_name);
            let server = TransactorService::new(&store_uri, &context).unwrap();
            let join_handle = server.listen("inproc://transactor").unwrap();
            {
                // Need a new scope to make sure the conn is dropped
                // before we try to close the ZMQ context.
                let mut $conn = test_conn(&context, &store_uri);
                $body;
            }
            server.close();
            context.destroy().unwrap();
            join_handle.join().unwrap();
        } }
    }

    fn expect_query_result(q: Query, expected: Relation) {
        with_test_conn!(conn {
            let db = conn.db().unwrap();
            let result = query(q, &db).unwrap();
            assert_eq!(expected, result);
        })
    }

    fn test_conn(context: &zmq::Context, store_uri: &str) -> Conn {
        let store = store_from_uri(store_uri).unwrap();
        let tx_address = "inproc://transactor";
        let conn = Conn::new(store, tx_address, context).unwrap();
        let records = vec![
            Fact::new(Entity(11), "name", Value::String("Bob".into())),
            Fact::new(Entity(12), "name", Value::String("John".into())),
            Fact::new(Entity(13), "Hello", Value::String("World".into())),
            Fact::new(Entity(12), "parent", Entity(11)),
        ];

        parse_tx(
            "{db:ident name db:valueType db:type:string}
                  {db:ident parent db:valueType db:type:ref}
                  {db:ident Hello db:valueType db:type:string}",
        ).map_err(|e| e.into())
            .and_then(|tx| conn.transact(tx))
            .map(|tx_result| {
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
                match tx_result {
                    TxReport::Success { .. } => (),
                    TxReport::Failure(msg) => panic!(format!("failed in insert with '{}'", msg)),
                };
            })
            .unwrap();

        conn
    }

    #[test]
    fn test_query_unknown_entity() {
        // find ?a where (?a name "Bob")
        expect_query_result(
            parse_query("find ?a where (?a name \"Bob\")").unwrap(),
            Relation(
                vec![Var::new("a")],
                vec![
                    vec![Value::Ref(Entity(11))],
                ],
            ),
        );
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        expect_query_result(
            parse_query("find ?a where (11 name ?a)").unwrap(),
            Relation(
                vec![Var::new("a")],
                vec![vec![Value::String("Bob".into())]],
            ),
        );

    }

    // // It's inconvenient to test this because we don't have a ref to the db in
    // // the current setup, and we don't know the entity id of `name` offhand.
    // #[test]
    // fn test_query_unknown_attribute() {
    //     // find ?a where (1 ?a "John")
    //     expect_query_result(&parse_query("find ?a where (1 ?a \"John\")").unwrap(),
    //                         Relation(vec![Var::new("a")],
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
            parse_query("find ?a ?b where (?a name ?b)").unwrap(),
            Relation(
                vec![Var::new("a"), Var::new("b")],
                vec![
                    vec![Value::Ref(Entity(11)), Value::String("Bob".into())],
                    vec![Value::Ref(Entity(12)), Value::String("John".into())]
                ],
            ),
        );
    }

    #[test]
    fn test_constraint() {
        // find ?a ?b where (?a name ?b) (< ?b "Charlie")
        expect_query_result(
            parse_query("find ?a ?b where (?a name ?b) (< ?b \"Charlie\")").unwrap(),
            Relation(
                vec![Var::new("a"), Var::new("b")],
                vec![
                    vec![Value::Ref(Entity(11)), Value::String("Bob".into())],
                ],
            ),
        );
    }

    #[test]
    fn test_query_explicit_join() {
        expect_query_result(
            parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)").unwrap(),
            Relation(
                vec![Var::new("b")],
                vec![
                    vec![Value::Ref(Entity(12))]
                ],
            ),
        );
    }

    #[test]
    fn test_query_implicit_join() {
        expect_query_result(
            parse_query(
                "find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)",
            ).unwrap(),
            Relation(
                vec![Var::new("c")],
                vec![
                    vec![Value::String("John".into())],
                ],
            ),
        );
    }

    #[test]
    fn test_type_mismatch() {
        with_test_conn!(conn {
            let db = conn.db().unwrap();
            let q = parse_query("find ?e ?n where (?e name ?n) (?n name \"hi\")").unwrap();
            assert_equal(query(q, &db), Err("type mismatch".to_string()))
        })
    }

    #[test]
    fn test_retractions() {
        with_test_conn!(conn {
            conn.transact(parse_tx("retract (12 parent 11)").unwrap())
                .unwrap();
            let db = conn.db().unwrap();
            let q = parse_query("find ?a ?b where (?a parent ?b)").unwrap();
            let result = query(q, &db).unwrap();

            assert_eq!(
                result,
                Relation(vec![Var::new("a"), Var::new("b")], vec![])
            );
        })
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
    fn bench_add(b: &mut Bencher) {
        with_test_conn!(conn {
            parse_tx("{db:ident blah}")
                .map(|tx| conn.transact(tx))
                .unwrap()
                .unwrap();

            let mut e = 100;

            b.iter(|| {
                let entity = Entity(e);
                e += 1;

                conn.transact(Tx {
                    items: vec![
                        TxItem::Addition(Fact::new(entity, "blah", Value::Ref(entity))),
                    ],
                }).unwrap();
            });
        })
    }

    #[test]
    fn test_record_equivalence() {
        // We use the Equivalent trait to deduplicate records in the database.
        // This test ensures that doing that deduplication will not erase retractions.

        let records = vec![
            Record::addition(Entity(1), Entity(1), Value::String("someval".into()), Entity(1)),
            Record::addition(Entity(1), Entity(1), Value::String("someval".into()), Entity(2)),
            Record::retraction(Entity(1), Entity(1), Value::String("someval".into()), Entity(3)),
            Record::addition(Entity(1), Entity(1), Value::String("someval".into()), Entity(4)),
        ];

        assert_equal(
            records.into_iter().coalesce(|x, y| if x.equivalent(&y) { Ok(x) } else { Err((x, y)) }),
            vec![
                Record::addition(Entity(1), Entity(1), Value::String("someval".into()), Entity(1)),
                Record::retraction(Entity(1), Entity(1), Value::String("someval".into()), Entity(3)),
                Record::addition(Entity(1), Entity(1), Value::String("someval".into()), Entity(4)),
            ]
        )
    }

    #[bench]
    fn bench_large_db_simple(b: &mut Bencher) {
        // Don't run on 'cargo test', only 'cargo bench'
        if cfg!(not(debug_assertions)) {
            let q = black_box(parse_query(r#"find ?a where (?a name "Bob")"#).unwrap());
            with_test_conn!(conn {
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

                let db = conn.db().unwrap();

                b.iter(|| query(q.clone(), &db).unwrap());
            });
        }
    }


    // tests moved from db.rs, tbd if they can be moved back
    #[test]
    fn test_records_matching() {
        with_test_conn!(conn {
            let db = conn.db().unwrap();
            let matching = db.records_matching(
                    &Clause::new(
                        Term::Unbound("e".into()),
                        Term::Bound(Ident::Name("name".into())),
                        Term::Bound(Value::String("Bob".into())),
                    ),
                    &Binding::default(),
                )
                .unwrap();
            assert_eq!(matching.len(), 1);
            let rec = &matching[0];
            assert_eq!(rec.entity, Entity(11));
            assert_eq!(rec.value, Value::String("Bob".into()));
        })
    }

    #[test]
    fn test_fetch() {
        use queries::query;
        with_test_conn!(conn {
            let db = conn.db().unwrap();
            let name_entity = *db.schema.idents.get("name").unwrap();
            let clause = query::Clause::new(
                Term::Unbound("e".into()),
                Term::Bound(Ident::Entity(name_entity)),
                Term::Unbound("n".into()),
            );

            let relation = db.fetch(&clause).unwrap();
            assert_eq!(relation.0, vec!["e".into(), "n".into()]);
            assert_eq!(relation.1, vec![
                vec![Value::Ref(Entity(11)), Value::String("Bob".into())],
                vec![Value::Ref(Entity(12)), Value::String("John".into())]
            ]);
        })
    }

    #[test]
    fn test_aev_usage() {
        // Regression.
        with_test_conn!(conn {
            let db = conn.db().unwrap();
            let q = parse_query("find ?e where (?e db:ident db:type:string)").unwrap();
            let result = query(q, &db).unwrap();
            assert_eq!(result.1, vec![vec![Value::Ref(Entity(6))]]);
        });
    }
}
