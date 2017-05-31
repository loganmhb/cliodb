#![feature(collections_range)]
#![feature(conservative_impl_trait)]
#![cfg_attr(test, feature(test))]

#[macro_use]
extern crate itertools;

#[macro_use]
extern crate combine;

#[macro_use]
extern crate lazy_static;

use itertools::*;

use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::iter;

pub mod parser;
pub mod string_ref;

pub use parser::*;
pub use string_ref::StringRef;

mod index;
mod print_table;

// A database is just a log of facts. Facts are (entity, attribute, value) triples.
// Attributes and values are both just strings. There are no transactions or histories.

#[derive(Debug, PartialEq)]
pub struct QueryResult(Vec<Var>, Vec<HashMap<Var, Value>>);

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let col_names = self.0.iter().map(|v| &*v.name);

        let aligns = iter::repeat(print_table::Alignment::Center);
        let rows = self.1
            .iter()
            .map(|row_ht| self.0.iter().map(|var| format!("{}", row_ht[var])).collect_vec());

        writeln!(f,
                 "{}",

                 print_table::debug_table("Result", col_names, aligns, rows))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Copy)]
pub enum Value {
    String(StringRef),
    Entity(Entity),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "{}",
               match *self {
                   Value::Entity(e) => format!("{}", e.0),
                   Value::String(ref s) => format!("{:?}", s),
               })
    }
}

impl<T> From<T> for Value
    where T: Into<StringRef>
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Term<T> {
    Bound(T),
    Unbound(Var),
}

// A free [logic] variable
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct Var {
    name: StringRef,
}

impl Var {
    fn new<T: Into<StringRef>>(name: T) -> Var {
        Var::from(name)
    }
}

impl<T: Into<StringRef>> From<T> for Var {
    fn from(x: T) -> Self {
        Var { name: x.into() }
    }
}

// A query looks like `find ?var where (?var <attribute> <value>)`
#[derive(Debug, PartialEq)]
pub struct Query {
    find: Vec<Var>,
    clauses: Vec<Clause>,
}

impl Query {
    fn new(find: Vec<Var>, clauses: Vec<Clause>) -> Query {
        Query {
            find: find,
            clauses: clauses,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Tx {
    items: Vec<TxItem>,
}

#[derive(Debug, PartialEq, Eq)]
enum TxItem {
    Addition(Fact),
    Retraction(Fact),
    NewEntity(HashMap<StringRef, Value>),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct Entity(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Clause {
    entity: Term<Entity>,
    attribute: Term<StringRef>,
    value: Term<Value>,
}

impl Clause {
    fn new(e: Term<Entity>, a: Term<StringRef>, v: Term<Value>) -> Clause {
        Clause {
            entity: e,
            attribute: a,
            value: v,
        }
    }
}

pub trait Database {
    fn add(&mut self, fact: Fact);
    fn facts_matching(&self, clause: &Clause, binding: &Binding) -> Vec<&Fact>;

    fn transact(&mut self, tx: Tx) {
        for item in tx.items {
            match item {
                TxItem::Addition(f) => self.add(f),
                // TODO Implement retractions + new entities
                _ => unimplemented!(),
            }
        }
    }

    fn query(&self, query: &Query) -> QueryResult {
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for fact in self.facts_matching(clause, &binding) {
                    match unify(&binding, clause, &fact) {
                        Ok(new_env) => new_bindings.push(new_env),
                        _ => continue,
                    }
                }
            }

            bindings = new_bindings;
        }

        let result = bindings.into_iter()
            .map(|solution| {
                solution.into_iter()
                    .filter(|&(ref k, _)| query.find.contains(&k))
                    .collect()
            })
            .collect();

        QueryResult(query.find.clone(), result)
    }
}

// The Fact struct represents a fact in the database.
// The derived ordering is used by the EAV index; other
// indices use orderings provided by wrapper structs.
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
pub struct Fact {
    entity: Entity,
    attribute: StringRef,
    value: Value,
}

impl Fact {
    fn new<A: Into<StringRef>, V: Into<Value>>(e: Entity, a: A, v: V) -> Fact {
        Fact {
            entity: e,
            attribute: a.into(),
            value: v.into(),
        }
    }
}

macro_rules! impl_range_arg {
    ($name:ident) => {
        impl RangeArgument<$name> for $name {
            fn start(&self) -> Bound<&$name> {
                Bound::Included(&self)
            }

            fn end(&self) -> Bound<&$name> {
                Bound::Unbounded
            }
        }
    };
}

macro_rules! index_wrapper {
    ($name:ident; $i1:ident, $i2:ident, $i3:ident) => {
        #[derive(PartialEq, Eq, Debug)]
        struct $name(Fact);

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &$name) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &$name) -> std::cmp::Ordering {
                let (this, other) = (&self.0, &other.0);
                this.$i1
                    .cmp(&other.$i1)
                    .then(this.$i2.cmp(&other.$i2))
                    .then(this.$i3.cmp(&other.$i3))
            }
        }

        impl_range_arg!($name);
    };
}

index_wrapper!(AVE; attribute, value, entity);
index_wrapper!(AEV; attribute, entity, value);
impl_range_arg!(Fact);

type Binding = HashMap<Var, Value>;

impl Clause {
    fn substitute(&self, env: &Binding) -> Clause {
        let entity = match &self.entity {
            &Term::Bound(_) => self.entity.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match *val {
                        Value::Entity(e) => Term::Bound(e),
                        _ => unimplemented!(),
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
                        &Value::String(s) => Term::Bound(s),
                        _ => unimplemented!(),
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

        Clause::new(entity, attribute, value)
    }
}

#[derive(Debug, Default)]
pub struct InMemoryLog {
    eav: BTreeSet<Fact>,
    ave: BTreeSet<AVE>,
    aev: BTreeSet<AEV>,
}

use std::collections::range::RangeArgument;
use std::collections::Bound;

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog::default()
    }
}

impl IntoIterator for InMemoryLog {
    type Item = Fact;
    type IntoIter = <std::collections::BTreeSet<Fact> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.eav.into_iter()
    }
}


impl Database for InMemoryLog {
    fn add(&mut self, fact: Fact) {
        self.eav.insert(fact);
        self.ave.insert(AVE(fact));
        self.aev.insert(AEV(fact));
    }

    fn facts_matching(&self, clause: &Clause, binding: &Binding) -> Vec<&Fact> {
        let expanded = clause.substitute(binding);
        match expanded {
            // ?e a v => use the ave index
            Clause { entity: Term::Unbound(_),
                     attribute: Term::Bound(a),
                     value: Term::Bound(v) } => {
                let range_start = Fact::new(Entity(0), a.clone(), v.clone());
                self.ave
                    .range(AVE(range_start))
                    .map(|ave| &ave.0)
                    .take_while(|f| f.attribute == a && f.value == v)
                    .collect()
            }
            // e a ?v => use the eav index
            Clause { entity: Term::Bound(e),
                     attribute: Term::Bound(a),
                     value: Term::Unbound(_) } => {
                // Value::String("") is the lowest-sorted value
                let range_start = Fact::new(e.clone(), a.clone(), Value::String("".into()));
                self.eav
                    .range(range_start)
                    .take_while(|f| f.entity == e && f.attribute == a)
                    .collect()
            }
            // FIXME: Implement other optimized index use cases? (multiple unknowns? refs?)
            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                self.eav
                    .iter()
                    .filter(|f| unify(&binding, &clause, &f).is_ok())
                    .collect()
            }
        }
    }
}

fn unify(env: &Binding, clause: &Clause, fact: &Fact) -> Result<Binding, ()> {
    let mut new_info = HashMap::new();

    match clause.entity {
        Term::Bound(ref e) => {
            if *e != fact.entity {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::Entity(fact.entity) {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert((*var).clone(), Value::Entity(fact.entity));
                }
            }
        }
    }

    match clause.attribute {
        Term::Bound(ref a) => {
            if *a != fact.attribute {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::String(fact.attribute.clone()) {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert((*var).clone(), Value::String(fact.attribute.clone()));
                }
            }
        }
    }

    match clause.value {
        Term::Bound(ref v) => {
            if *v != fact.value {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != fact.value {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert((*var).clone(), fact.value.clone());
                }
            }
        }
    }

    let mut env = env.clone();
    env.extend(new_info);

    Ok(env)
}


#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::{Bencher, black_box};

    use std::iter;

    use super::*;

    #[test]
    fn test_parse_query() {
        assert_eq!(parse_query("find ?a where (?a name \"Bob\")").unwrap(),
                   Query {
                       find: vec![Var::new("a")],
                       clauses: vec![Clause::new(Term::Unbound("a".into()),
                                                 Term::Bound("name".into()),
                                                 Term::Bound(Value::String("Bob".into())))],
                   })
    }

    #[test]
    fn test_parse_tx() {
        assert_eq!(parse_tx("add (0 name \"Bob\")").unwrap(),
                   Tx {
                       items: vec![TxItem::Addition(Fact::new(Entity(0),
                                                              "name",
                                                              Value::String("Bob".into())))],
                   });
        parse_tx("{name \"Bob\" batch \"S1'17\"}").unwrap();
    }

    lazy_static! {
        static ref DB: InMemoryLog = {
            let mut db = InMemoryLog::new();
            let facts = vec![Fact::new(Entity(0), "name", "Bob"),
                             Fact::new(Entity(1), "name", "John"),
                             Fact::new(Entity(2), "Hello", "World"),
                             Fact::new(Entity(1), "parent", Entity(0))];

            for fact in facts {
                db.add(fact);
            }

            db
        };
    }

    #[test]
    fn test_insertion() {
        let fact = Fact::new(Entity(0), "name", "Bob");
        let mut db = InMemoryLog::new();
        db.add(fact);
        let inserted = db.into_iter().take(1).nth(0).unwrap();
        assert!(inserted.entity == Entity(0));
        assert!(&*inserted.attribute == "name");
        assert!(inserted.value == "Bob".into());
    }

    #[test]
    fn test_facts_matching() {
        assert_eq!(DB.facts_matching(&Clause::new(Term::Unbound("e".into()),
                                                  Term::Bound("name".into()),
                                                  Term::Bound(Value::String("Bob".into()))),
                                     &Binding::default()),
                   vec![&Fact::new(Entity(0), "name", Value::String("Bob".into()))])
    }

    #[test]
    fn test_query_unknown_entity() {
        // find ?a where (?a name "Bob")
        helper(&*DB,
               &parse_query("find ?a where (?a name \"Bob\")").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![iter::once((Var::new("a"), Value::Entity(Entity(0)))).collect()]));
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        helper(&*DB,
               &parse_query("find ?a where (0 name ?a)").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![iter::once((Var::new("a"), Value::String("Bob".into())))
                                    .collect()]));

    }
    #[test]
    fn test_query_unknown_attribute() {
        // find ?a where (1 ?a "John")
        helper(&*DB,
               &parse_query("find ?a where (1 ?a \"John\")").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![iter::once((Var::new("a"), Value::String("name".into())))
                                    .collect()]));
    }

    #[test]
    fn test_query_multiple_results() {
        // find ?a ?b where (?a name ?b)
        helper(&*DB,
               &parse_query("find ?a ?b where (?a name ?b)").unwrap(),
               QueryResult(vec![Var::new("a"), Var::new("b")],
                           vec![vec![(Var::new("a"), Value::Entity(Entity(0))),
                                     (Var::new("b"), Value::String("Bob".into()))]
                                    .into_iter()
                                    .collect(),
                                vec![(Var::new("a"), Value::Entity(Entity(1))),
                                     (Var::new("b"), Value::String("John".into()))]
                                    .into_iter()
                                    .collect()]));
    }

    #[test]
    fn test_query_explicit_join() {
        // find ?b where (?a name Bob) (?b parent ?a)
        helper(&*DB,
               &parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)").unwrap(),
               QueryResult(vec![Var::new("b")],
                           vec![iter::once((Var::new("b"), Value::Entity(Entity(1)))).collect()]));
    }

    #[test]
    fn test_query_implicit_join() {
        // find ?c where (?a name Bob) (?b name ?c) (?b parent ?a)
        helper(&*DB,
               &parse_query("find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)")
                   .unwrap(),
               QueryResult(vec![Var::new("c")],
                           vec![iter::once((Var::new("c"), Value::String("John".into())))
                                    .collect()]));
    }

    fn helper<D: Database>(db: &D, query: &Query, expected: QueryResult) {
        let result = db.query(query);
        assert_eq!(expected, result);
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

        helper(&*DB,
               &query,
               QueryResult(vec![Var::new("c")],
                           vec![iter::once((Var::new("c"), Value::String("John".into())))
                                    .collect()]));

        b.iter(|| DB.query(&query));
    }

    // Don't run on 'cargo test', only 'cargo bench'
    #[cfg(not(debug_assertions))]
    #[bench]
    fn large_db_simple(b: &mut Bencher) {
        use std::io::{stdout, Write};

        let quiet = ::std::env::var_os("QUIET").is_some();
        if !quiet {
            println!();
        }

        let query = black_box(parse_query(r#"find ?a where (?a name "Bob")"#).unwrap());
        let mut db = InMemoryLog::new();
        let n = 10_000_000;

        for i in 0..n {
            if !quiet && i % (n / 100) == 0 {
                print!("\rBuilding: {}%", ((i as f32) / (n as f32) * 100.0) as i32);
                stdout().flush().unwrap();
            }

            let a = if i % 23 < 10 {
                "name"
            } else {
                "random_attribute"
            };
            let v = if i % 1123 == 0 { "Bob" } else { "Rob" };

            db.add(Fact::new(Entity(i), a, v));
        }

        if !quiet {
            println!("\nQuerying...");
        }

        b.iter(|| db.query(&query));
    }
}
