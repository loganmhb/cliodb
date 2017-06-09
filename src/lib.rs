#![feature(collections_range)]
#![feature(conservative_impl_trait)]
#![cfg_attr(test, feature(test))]

extern crate itertools;

#[macro_use]
extern crate combine;

extern crate prettytable as pt;
extern crate chrono;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate rmp_serde;

extern crate rusqlite;

extern crate uuid;

#[macro_use]
extern crate lazy_static;

use itertools::*;

use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use std::iter;

use chrono::prelude::{UTC};

pub mod parser;
pub mod string_ref;
pub mod btree;
pub mod durable;
mod model;

pub use parser::*;

use model::{Fact, Record, Value, Entity};
pub use string_ref::StringRef;


use btree::{Index, KVStore, Comparator, DbContents};

// A database is just a log of records. Records are (entity, attribute, value) triples.
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




#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Term<T> {
    Bound(T),
    Unbound(Var),
}

// A free [logic] variable
// FIXME: Is it actually safe to intern these statically?
// For a long running process, there could be a theoretically
// unbounded number of logic vars used in queries.
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Tx {
    items: Vec<TxItem>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
enum TxItem {
    Addition(Fact),
    Retraction(Fact),
    NewEntity(HashMap<String, Value>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Clause {
    entity: Term<Entity>,
    attribute: Term<String>,
    value: Term<Value>,
}

impl Clause {
    fn new(e: Term<Entity>, a: Term<String>, v: Term<Value>) -> Clause {
        Clause {
            entity: e,
            attribute: a,
            value: v,
        }
    }
}

pub trait Database {
    fn add(&mut self, record: Record);
    fn records_matching(&self, clause: &Clause, binding: &Binding) -> Vec<Record>;
    fn next_id(&self) -> u64;
    fn save_contents(&self) -> Result<(), String>;

    fn transact(&mut self, tx: Tx) {
        let tx_entity = Entity(self.next_id());
        self.add(Record::new(tx_entity, "txInstant", Value::Timestamp(UTC::now()), tx_entity));
        for item in tx.items {
            match item {
                TxItem::Addition(f) => self.add(Record::from_fact(f, tx_entity)),
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.next_id());
                    for (k, v) in ht {
                        self.add(Record::new(entity, k, v, tx_entity))
                    }
                }
                // TODO Implement retractions
                _ => unimplemented!(),
            }
        }
        self.save_contents().unwrap() // FIXME: propagate the error
    }

    fn query(&self, query: &Query) -> QueryResult {
        // TODO: automatically bind ?tx in queries
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for record in self.records_matching(clause, &binding) {
                    match unify(&binding, clause, &record) {
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
                .map(|(&var, value)| (var, value.clone()))
                .collect();
        }

        QueryResult(query.find.clone(), bindings)
    }
}


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
            &Term::Bound(_) => self.attribute.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match val {
                        &Value::String(ref s) => Term::Bound(s.clone()),
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


macro_rules! comparator {
    ($name:ident, $first:ident, $second:ident, $third:ident, $fourth:ident) => {
        #[derive(Debug, Clone)]
        struct $name;

        impl Comparator for $name {
            type Item = Record;

            fn compare(a: &Record, b: &Record) -> std::cmp::Ordering {
                a.$first.cmp(&b.$first)
                    .then(a.$second.cmp(&b.$second))
                    .then(a.$third.cmp(&b.$third))
                    .then(a.$fourth.cmp(&b.$fourth))
            }
        }
    }
}

comparator!(EAVT, entity, attribute, value, tx);
comparator!(AEVT, attribute, entity, value, tx);
comparator!(AVET, attribute, value, entity, tx);

pub struct Db<S: KVStore<Item=Record>> {
    next_id: u64,
    store: S,
    eav: Index<Record, S, EAVT>,
    ave: Index<Record, S, AVET>,
    aev: Index<Record, S, AEVT>,
}

impl<S> Db<S>
    where S: btree::KVStore<Item=Record>
{
    pub fn new(store: S) -> Result<Db<S>, String> {
        // The store is responsible for making sure that its
        // 'db_contents' key is usable when it is created, and making
        // new root nodes if the store is brand new.
        let contents = store.get_contents()?;

        Ok(Db {
            next_id: contents.next_id,
            store: store.clone(),
            eav: Index::new(contents.eav, store.clone(), EAVT)?,
            ave: Index::new(contents.ave, store.clone(), AVET)?,
            aev: Index::new(contents.aev, store, AEVT)?,
        })
    }
}

impl<S> Database for Db<S>
    where S: btree::KVStore<Item=Record>
{
    fn next_id(&self) -> u64 {
        self.next_id
    }

    fn add(&mut self, record: Record) {
        if record.entity.0 >= self.next_id {
            self.next_id = record.entity.0 + 1;
        }

        self.eav = self.eav.insert(record.clone()).unwrap();
        self.ave = self.ave.insert(record.clone()).unwrap();
        self.aev = self.aev.insert(record).unwrap();
    }

    /// Saves the db metadata (index root nodes, entity ID state) to
    /// storage, when implemented by the storage backend (i.e. when
    /// not using in-memory storage).
    fn save_contents(&self) -> Result<(), String> {
        let contents = DbContents {
            next_id: self.next_id,
            eav: self.eav.root_ref.clone(),
            aev: self.aev.root_ref.clone(),
            ave: self.ave.root_ref.clone()
        };

        self.store.set_contents(&contents)
    }

    fn records_matching(&self, clause: &Clause, binding: &Binding) -> Vec<Record> {
        let expanded = clause.substitute(binding);
        match expanded {
            // ?e a v => use the ave index
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {

                let range_start = Record::new(Entity(0), a.clone(), v.clone(), Entity(0));
                self.ave
                    .iter_range_from(range_start..)
                    .unwrap()
                    .map(|res| res.unwrap())
                    .take_while(|f| f.attribute == a && f.value == v)
                    .collect()
            }
            // // e a ?v => use the eav index
            Clause {
                entity: Term::Bound(e),
                attribute: Term::Bound(a),
                value: Term::Unbound(_),
            } => {
                // Value::String("") is the lowest-sorted value
                let range_start = Record::new(e, a.clone(), Value::String("".into()), Entity(0));
                self.eav
                    .iter_range_from(range_start..)
                    .unwrap()
                    .map(|f| f.unwrap())
                    .take_while(|f| f.entity == e && f.attribute == a)
                    .collect()
            }
            // FIXME: Implement other optimized index use cases? (multiple unknowns? refs?)
            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                self.eav
                    .iter()
                    .map(|f| f.unwrap()) // FIXME this is not safe :D
                    .filter(|f| unify(&binding, &clause, &f).is_ok())
                    .collect()
            }
        }
    }
}

fn unify(env: &Binding, clause: &Clause, record: &Record) -> Result<Binding, ()> {
    let mut new_info: Binding = Default::default();

    match clause.entity {
        Term::Bound(ref e) => {
            if *e != record.entity {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::Entity(record.entity) {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert(*var, Value::Entity(record.entity));
                }
            }
        }
    }

    match clause.attribute {
        Term::Bound(ref a) => {
            if *a != record.attribute {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != Value::String(record.attribute.clone()) {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert(*var, Value::String(record.attribute.clone()));
                }
            }
        }
    }

    match clause.value {
        Term::Bound(ref v) => {
            if *v != record.value {
                return Err(());
            }
        }
        Term::Unbound(ref var) => {
            match env.get(var) {
                Some(e) => {
                    if *e != record.value {
                        return Err(());
                    }
                }
                _ => {
                    new_info.insert(*var, record.value.clone());
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
    use btree::HeapStore;

    fn helper(query: &Query, expected: QueryResult) {
        let db = test_db();
        let result = db.query(query);
        assert_eq!(expected, result);
    }

    fn test_db() -> Db<HeapStore<Record>> {
        let store = HeapStore::new();
        let mut db = Db::new(store).unwrap();
        let records = vec![
            Fact::new(Entity(0), "name", "Bob"),
            Fact::new(Entity(1), "name", "John"),
            Fact::new(Entity(2), "Hello", "World"),
            Fact::new(Entity(1), "parent", Entity(0)),
        ];

        db.transact(Tx { items: records.iter().map(|x| TxItem::Addition(x.clone())).collect() });

        db
    }

    #[allow(dead_code)]
    fn test_db_large() -> Db<HeapStore<Record>> {
        let store = HeapStore::new();
        let mut db = Db::new(store).unwrap();
        let n = 10_000_000;

        for i in 0..n {
            let a = if i % 23 < 10 {
                "name"
            } else {
                "random_attribute"
            };

            let v = if i % 1123 == 0 { "Bob" } else { "Rob" };

            db.add(Record::new(Entity(i), a, v, Entity(0)));
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
                       items: vec![TxItem::Addition(Fact::new(Entity(0),
                                                              "name",
                                                              Value::String("Bob".into())))],
                   });
        parse_tx("{name \"Bob\" batch \"S1'17\"}").unwrap();
    }

    // #[test]
    // fn test_insertion() {
    //     let record = Record::new(Entity(0), "name", "Bob");
    //     let mut db = InMemoryLog::new();
    //     db.add(record);
    //     let inserted = db.into_iter().take(1).nth(0).unwrap();
    //     assert!(inserted.entity == Entity(0));
    //     assert!(&*inserted.attribute == "name");
    //     assert!(inserted.value == "Bob".into());
    // }

    #[test]
    fn test_records_matching() {
        assert_eq!(vec![Fact::new(Entity(0), "name", Value::String("Bob".into()))],
                   test_db().records_matching(&Clause::new(Term::Unbound("e".into()),
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
        let store = HeapStore::new();
        let mut db = Db::new(store).unwrap();

        let a = String::from("blah");

        let mut e = 0;

        b.iter(|| {
                   let entity = Entity(e);
                   e += 1;

                   db.add(Record::new(entity, a.clone(), Value::Entity(entity), Entity(0)));
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
