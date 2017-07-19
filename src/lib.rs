#![feature(collections_range)]
#![feature(conservative_impl_trait)]
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

use itertools::*;

use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use std::iter;
use std::result;

pub mod db;
pub mod parser;
pub mod string_ref;
pub mod index;
pub mod backends;
pub mod tx;
pub mod network;
mod query;
mod rbtree;
mod durable_tree;
mod ident;

pub use parser::*;
use query::{Query, Clause, Term, Var};
use index::{Index, Comparator};
use backends::KVStore;
pub use ident::IdentMap;
use backends::cassandra::CassandraStore;
use backends::sqlite::SqliteStore;
use backends::mem::HeapStore;

use std::collections::Bound;
use std::collections::range::RangeArgument;
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

impl RangeArgument<Record> for Record {
    fn start(&self) -> Bound<&Record> {
        Bound::Included(&self)
    }

    fn end(&self) -> Bound<&Record> {
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
    fn substitute(&self, env: &Binding) -> Result<Clause> {
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
