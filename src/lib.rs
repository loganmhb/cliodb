#![feature(collections_range)]
#![feature(conservative_impl_trait)]
#![cfg_attr(test, feature(test))]

extern crate itertools;

#[macro_use]
extern crate combine;

extern crate prettytable as pt;

#[macro_use]
extern crate lazy_static;

use itertools::*;

use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use std::iter;
use std::mem;

pub mod parser;
pub mod string_ref;

pub use parser::*;
pub use string_ref::StringRef;

mod index;
use index::Index;

// A database is just a log of facts. Facts are (entity, attribute, value) triples.
// Attributes and values are both just strings. There are no transactions or histories.

#[derive(Debug, PartialEq)]
pub struct QueryResult(Vec<Var>, Vec<HashMap<Var, Value>>);

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let num_columns = self.0.len();
        let align = pt::format::Alignment::CENTER;
        let mut titles: pt::row::Row = self.0.iter().map(|var| var.name).collect();
        titles.iter_mut().foreach(|c| c.align(align));

        let rows = self.1
            .iter()
            .map(|row_ht| self.0.iter().map(|var| format!("{}", row_ht[var])).into())
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

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Copy)]
pub enum Value {
    String(StringRef),
    Entity(Entity),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            Value::Entity(e) => format!("{}", e.0),
            Value::String(ref s) => format!("{}", s),
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
    Addition(Hypothetical),
    Retraction(Hypothetical),
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
    fn next_id(&self) -> u64;

    fn transact(&mut self, tx: Tx) {
        let tx_entity = Entity(self.next_id());
        self.add(Fact::new(tx_entity, "txInstant", "now! (FIXME)", tx_entity));
        for item in tx.items {
            match item {
                TxItem::Addition(f) => self.add(Fact::from_hypothetical(f, tx_entity)),
                // TODO Implement retractions + new entities
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.next_id());
                    for (k, v) in ht {
                        self.add(Fact::new(entity, k, v, tx_entity))
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    fn query(&self, query: &Query) -> QueryResult {
        // TODO: automatically bind ?tx in queries
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for fact in self.facts_matching(clause, &binding) {
                    match unify(&binding, clause, &fact) {
                        Ok(new_info) => {
                            let mut new_env = binding.clone();
                            new_env.extend(new_info);
                            new_bindings.push(new_env)
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
                .map(|(&var, &value)| (var, value))
                .collect();
        }

        QueryResult(query.find.clone(), bindings)
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
    tx: Entity,
}

// We need a struct to represent facts that may not be in the database,
// i.e. may not have an associated tx, for use by the parser and unifier.
// FIXME: I don't like this name. Some better way to distinguish between
// facts that have tx ids vs those that don't would be better.
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
struct Hypothetical {
    entity: Entity,
    attribute: StringRef,
    value: Value,
}

impl Hypothetical {
    fn new<A: Into<StringRef>, V: Into<Value>>(e: Entity, a: A, v: V) -> Hypothetical {
        Hypothetical {
            entity: e,
            attribute: a.into(),
            value: v.into(),
        }
    }
}

impl Fact {
    fn new<A: Into<StringRef>, V: Into<Value>>(e: Entity, a: A, v: V, tx: Entity) -> Fact {
        Fact {
            entity: e,
            attribute: a.into(),
            value: v.into(),
            tx: tx,
        }
    }

    fn from_hypothetical(h: Hypothetical, tx: Entity) -> Fact {
        Fact {
            tx: tx,
            entity: h.entity,
            attribute: h.attribute,
            value: h.value,
        }
    }
}

impl PartialEq<Fact> for Hypothetical {
    fn eq(&self, other: &Fact) -> bool {
        self.entity == other.entity && self.attribute == other.attribute &&
        self.value == other.value
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
        #[derive(PartialEq, Eq, Debug, Clone, Copy)]
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
            &Term::Bound(_) => self.entity,
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match *val {
                        Value::Entity(e) => Term::Bound(e),
                        _ => unimplemented!(),
                    }
                } else {
                    self.entity
                }
            }
        };

        let attribute = match &self.attribute {
            &Term::Bound(_) => self.attribute,
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match val {
                        &Value::String(s) => Term::Bound(s),
                        _ => unimplemented!(),
                    }
                } else {
                    self.attribute
                }
            }
        };

        let value = match &self.value {
            &Term::Bound(_) => self.value,
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    Term::Bound(*val)
                } else {
                    self.value
                }
            }
        };

        Clause::new(entity, attribute, value)
    }
}

#[derive(Debug)]
pub struct InMemoryLog {
    next_id: u64,
    eav: Index<Fact>,
    ave: Index<AVE>,
    aev: Index<AEV>,
}

use std::collections::range::RangeArgument;
use std::collections::Bound;

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog {
            next_id: 0,
            eav: Index::new(),
            ave: Index::new(),
            aev: Index::new(),
        }
    }
}

// impl IntoIterator for InMemoryLog {
//     type Item = Fact;
//     type IntoIter = <std::collections::BTreeSet<Fact> as IntoIterator>::IntoIter;

//     fn into_iter(self) -> Self::IntoIter {
//         self.eav.into_iter()
//     }
// }


impl Database for InMemoryLog {
    fn next_id(&self) -> u64 {
        self.next_id
    }

    fn add(&mut self, fact: Fact) {
        if fact.entity.0 >= self.next_id {
            self.next_id = fact.entity.0 + 1;
        }

        self.eav = self.eav.insert(fact);
        self.ave = self.ave.insert(AVE(fact));
        self.aev = self.aev.insert(AEV(fact));
    }

    fn facts_matching(&self, clause: &Clause, binding: &Binding) -> Vec<&Fact> {
        let expanded = clause.substitute(binding);
        match expanded {
            // ?e a v => use the ave index
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {

                let range_start = Fact::new(Entity(0), a, v, Entity(0));
                self.ave
                    .iter_range_from(AVE(range_start)..)
                    .map(|ave| &ave.0)
                    .take_while(|f| f.attribute == a && f.value == v)
                    .collect()
            }
            // e a ?v => use the eav index
            Clause {
                entity: Term::Bound(e),
                attribute: Term::Bound(a),
                value: Term::Unbound(_),
            } => {
                // Value::String("") is the lowest-sorted value
                let range_start = Fact::new(e, a, Value::String("".into()), Entity(0));
                self.eav
                    .iter_range_from(range_start..)
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

#[derive(Copy, Clone, Default)]
struct SmallBinding([Option<(Var, Value)>; 3]);
struct IntoIter {
    data: [(Var, Value); 3],
    len: u8,
    idx: u8,
}

impl Iterator for IntoIter {
    type Item = (Var, Value);
    fn next(&mut self) -> Option<Self::Item> {
        use std::ptr;
        if self.idx < self.len {
            let ret = unsafe {
                let ptr = &self.data as *const _;
                ptr::read(ptr.offset(self.idx as isize))
            };
            self.idx += 1;
            Some(ret)
        } else {
            None
        }
    }
}

impl IntoIterator for SmallBinding {
    type Item = <IntoIter as Iterator>::Item;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        // Safe because data will never be read past len, where it is initialized
        let mut data: [(Var, Value); 3] = unsafe { mem::uninitialized() };
        let mut len = 0;

        for item in self.0.into_iter() {
            if let Some(value) = *item {
                data[len] = value;
                len += 1;
            }
        }

        IntoIter {
            data: data,
            len: len as u8,
            idx: 0,
        }
    }
}

unsafe fn _assert_small_binding_size() {
    // assert that the Options use no additional size
    let _: [(Var, Value); 3] = mem::transmute(SmallBinding::default());
    let _: (SmallBinding, u8, u8) = mem::transmute(SmallBinding::default().into_iter());
}

fn unify(env: &Binding, clause: &Clause, fact: &Fact) -> Result<SmallBinding, ()> {
    let mut new_info: SmallBinding = Default::default();

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
                    new_info.0[0] = Some((*var, Value::Entity(fact.entity)));
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
                    new_info.0[1] = Some((*var, Value::String(fact.attribute.clone())));
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
                    new_info.0[2] = Some((*var, fact.value.clone()));
                }
            }
        }
    }

    Ok(new_info)
}


#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::{Bencher, black_box};

    use std::iter;

    use super::*;

    fn helper(query: &Query, expected: QueryResult) {
        let db = test_db();
        let result = db.query(query);
        assert_eq!(expected, result);
    }

    fn test_db() -> InMemoryLog {
        let mut db = InMemoryLog::new();
        let facts = vec![
            Hypothetical::new(Entity(0), "name", "Bob"),
            Hypothetical::new(Entity(1), "name", "John"),
            Hypothetical::new(Entity(2), "Hello", "World"),
            Hypothetical::new(Entity(1), "parent", Entity(0)),
        ];

        db.transact(Tx { items: facts.iter().map(|x| TxItem::Addition(*x)).collect() });

        db
    }

    #[allow(dead_code)]
    fn test_db_large() -> InMemoryLog {
        let mut db = InMemoryLog::new();
        let n = 10_000_000;

        for i in 0..n {
            let a = if i % 23 < 10 {
                "name"
            } else {
                "random_attribute"
            };

            let v = if i % 1123 == 0 { "Bob" } else { "Rob" };

            db.add(Fact::new(Entity(i), a, v, Entity(0)));
        }

        db
    }

    #[test]
    fn test_parse_query() {
        assert_eq!(parse_query("find ?a where (?a name \"Bob\")").unwrap(),
                   Query {
                       find: vec![Var::new("a")],
                       clauses: vec![
            Clause::new(Term::Unbound("a".into()),
                        Term::Bound("name".into()),
                        Term::Bound(Value::String("Bob".into()))),
        ],
                   })
    }

    #[test]
    fn test_parse_tx() {
        assert_eq!(parse_tx("add (0 name \"Bob\")").unwrap(),
                   Tx {
                       items: vec![TxItem::Addition(Hypothetical::new(Entity(0),
                                                              "name",
                                                              Value::String("Bob".into())))],
                   });
        parse_tx("{name \"Bob\" batch \"S1'17\"}").unwrap();
    }

    // #[test]
    // fn test_insertion() {
    //     let fact = Fact::new(Entity(0), "name", "Bob");
    //     let mut db = InMemoryLog::new();
    //     db.add(fact);
    //     let inserted = db.into_iter().take(1).nth(0).unwrap();
    //     assert!(inserted.entity == Entity(0));
    //     assert!(&*inserted.attribute == "name");
    //     assert!(inserted.value == "Bob".into());
    // }

    #[test]
    fn test_facts_matching() {
        assert_eq!(vec![&Hypothetical::new(Entity(0), "name", Value::String("Bob".into()))],
                   test_db().facts_matching(&Clause::new(Term::Unbound("e".into()),
                                                         Term::Bound("name".into()),
                                                         Term::Bound(Value::String("Bob".into()))),
                                            &Binding::default()))
    }

    #[test]
    fn test_query_unknown_entity() {
        // find ?a where (?a name "Bob")
        helper(&parse_query("find ?a where (?a name \"Bob\")").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![
            iter::once((Var::new("a"), Value::Entity(Entity(0)))).collect(),
        ]));
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        helper(&parse_query("find ?a where (0 name ?a)").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![
            iter::once((Var::new("a"), Value::String("Bob".into()))).collect(),
        ]));

    }

    #[test]
    fn test_query_unknown_attribute() {
        // find ?a where (1 ?a "John")
        helper(&parse_query("find ?a where (1 ?a \"John\")").unwrap(),
               QueryResult(vec![Var::new("a")],
                           vec![
            iter::once((Var::new("a"), Value::String("name".into())))
                .collect(),
        ]));
    }

    #[test]
    fn test_query_multiple_results() {
        // find ?a ?b where (?a name ?b)
        helper(&parse_query("find ?a ?b where (?a name ?b)").unwrap(),
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
        helper(&parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)").unwrap(),
               QueryResult(vec![Var::new("b")],
                           vec![
            iter::once((Var::new("b"), Value::Entity(Entity(1)))).collect(),
        ]));
    }

    #[test]
    fn test_query_implicit_join() {
        // find ?c where (?a name Bob) (?b name ?c) (?b parent ?a)
        helper(&parse_query("find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)")
                    .unwrap(),
               QueryResult(vec![Var::new("c")],
                           vec![
            iter::once((Var::new("c"), Value::String("John".into())))
                .collect(),
        ]));
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
        let mut db = InMemoryLog::new();

        let a = StringRef::from("blah");

        let mut e = 0;

        b.iter(|| {
                   let entity = Entity(e);
                   e += 1;

                   db.add(Fact::new(entity, a, Value::Entity(entity), Entity(0)));
               });
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
