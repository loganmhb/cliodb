use std::collections::Bound;
use std::collections::range::RangeArgument;
use std::fmt;
use std::fmt::{Formatter, Display};
use chrono::prelude::{DateTime, UTC};

// The Record struct represents a single e,a,v,t tuple in the
// database. Note that indices do NOT use the derived ordering;
// instead they use custom sort functions.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct Record {
    pub entity: Entity,
    pub attribute: String,
    pub value: Value,
    pub tx: Entity,
}

// We need a struct to represent facts that may not be in the database,
// i.e. may not have an associated tx, for use by the parser and unifier.
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
    pub fn new<A: Into<String>, V: Into<Value>>(e: Entity, a: A, v: V, tx: Entity) -> Record {
        Record {
            entity: e,
            attribute: a.into(),
            value: v.into(),
            tx: tx,
        }
    }

    pub fn from_fact(h: Fact, tx: Entity) -> Record {
        Record {
            tx: tx,
            entity: h.entity,
            attribute: h.attribute,
            value: h.value,
        }
    }
}

impl PartialEq<Record> for Fact {
    fn eq(&self, other: &Record) -> bool {
        self.entity == other.entity && self.attribute == other.attribute &&
        self.value == other.value
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
    Entity(Entity),
    // FIXME: clock drift is an issue here
    Timestamp(DateTime<UTC>)
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            Value::Entity(e) => format!("{}", e.0),
            Value::String(ref s) => format!("{}", s),
            Value::Timestamp(t) => format!("{}", t)
        })
    }
}

impl<T> From<T> for Value
    where T: Into<String>
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct Entity(pub u64);
