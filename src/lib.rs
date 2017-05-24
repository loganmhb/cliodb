
#![allow(dead_code)]
#![allow(unused_variables)]

#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;

// # Initial pass
// A database is just a log of facts. Facts are (entity, attribute, value) triples.
// Attributes and values are both just strings. There are no transactions or histories.

#[derive(Debug, PartialEq)]
struct QueryResult(Vec<HashMap<Var, Value>>);

#[derive(Debug, PartialEq, Clone)]
enum Value {
    String(String),
    Integer(i64),
    Entity(Entity),
}

impl<T: Into<String>> From<T> for Value {
    fn from(x: T) -> Self {
        Value::String(x.into())
    }
}


impl From<Entity> for Value {
    fn from(x: Entity) -> Self {
        Value::Entity(x.into())
    }
}


#[derive(Debug)]
enum Term<T> {
    Bound(T),
    Unbound(Var),
}

// A free [logic] variable
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct Var {
    name: String,
}

impl Var {
    fn new<T: Into<String>>(name: T) -> Var {
        Var { name: name.into() }
    }
}

impl<T: Into<String>> From<T> for Var {
    fn from(x: T) -> Self {
        Var { name: x.into() }
    }
}

// A query looks like `find ?var where (?var <attribute> <value>)`
#[derive(Debug)]
struct Query {
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

#[derive(Debug, PartialEq, Clone, Copy)]
struct Entity(u64);

#[derive(Debug)]
struct Clause {
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

trait Database {
    fn add(&mut self, fact: Fact);
    fn query(&self, query: Query) -> QueryResult;
}

#[derive(Debug, PartialEq)]
struct Fact {
    entity: Entity,
    attribute: String,
    value: Value,
}

impl Fact {
    fn new<A: Into<String>, V: Into<Value>>(e: Entity, a: A, v: V) -> Fact {
        Fact {
            entity: e,
            attribute: a.into(),
            value: v.into(),
        }
    }
}

#[derive(Debug)]
struct InMemoryLog {
    facts: Vec<Fact>,
}

impl InMemoryLog {
    fn new() -> InMemoryLog {
        InMemoryLog { facts: Vec::new() }
    }
}

impl IntoIterator for InMemoryLog {
    type Item = Fact;
    type IntoIter = ::std::vec::IntoIter<Fact>;

    fn into_iter(self) -> Self::IntoIter {
        self.facts.into_iter()
    }
}

impl Database for InMemoryLog {
    fn add(&mut self, fact: Fact) {
        self.facts.push(fact);
    }

    // NOTE: find not actually used/doing anything!
    fn query(&self, Query { find: find, clauses }: Query) -> QueryResult {

        let mut result = vec![];


        for fact in &self.facts {
            // NOTE: while it looks like multiple clauses are supported, they aren't really.
            let mut env: HashMap<Var, Value> = HashMap::new();
            let mut sat = true;

            // find ?b where (?a name Bob) (?b parent ?a)
            for &Clause { entity: ref e, attribute: ref a, value: ref v } in &clauses {
                match *e {
                    Term::Bound(entity) => {
                        if fact.entity != entity {
                            sat = false;
                            break;
                        }
                    }
                    Term::Unbound(ref var) => {

                        env.insert((*var).clone(), Value::Entity(fact.entity));
                    }
                }

                match *a {
                    Term::Bound(ref attr) => {
                        if fact.attribute != *attr {
                            sat = false;
                            break;
                        }
                    }
                    Term::Unbound(ref var) => {
                        env.insert((*var).clone(), Value::String(fact.attribute.clone()));
                    }
                }

                match *v {
                    Term::Bound(ref val) => {
                        if fact.value != *val {
                            sat = false;
                            break;
                        }
                    }
                    Term::Unbound(ref var) => {
                        env.insert((*var).clone(), fact.value.clone());
                    }
                }
            }

            if sat {
                result.push(env);
            }
        }

        QueryResult(result)
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use super::*;


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
        assert!(inserted.attribute == "name");
        assert!(inserted.value == "Bob".into());
    }



    #[test]
    fn test_query() {
        // find ?a where (?a name "Bob")
        helper(&*DB,
               Query::new(vec![Var::new("a")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound("Bob".into()))]),
               QueryResult(vec![iter::once((Var::new("a"), Value::Entity(Entity(0)))).collect()]));
    }

    #[test]
    fn test_query2() {
        // find ?a where (0 name ?a)
        helper(&*DB,
               Query::new(vec![Var::new("a")],
                          vec![Clause::new(Term::Bound(Entity(0)),
                                           Term::Bound("name".into()),
                                           Term::Unbound("a".into()))]),
               QueryResult(vec![iter::once((Var::new("a"), Value::String("Bob".into())))
                                    .collect()]));

    }
    #[test]
    fn test_query3() {
        // find ?a where (1 ?a "John")
        helper(&*DB,
               Query::new(vec![Var::new("a")],
                          vec![Clause::new(Term::Bound(Entity(1)),
                                           Term::Unbound("a".into()),
                                           Term::Bound("John".into()))]),
               QueryResult(vec![iter::once((Var::new("a"), Value::String("name".into())))
                                    .collect()]));
    }

    #[test]
    fn test_query4() {
        // find ?a ?b where (?a name ?b)
        helper(&*DB,
               Query::new(vec![Var::new("a"), Var::new("b")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Unbound("b".into()))]),
               QueryResult(vec![vec![(Var::new("a"), Value::Entity(Entity(0))),
                                     (Var::new("b"), Value::String("Bob".into()))]
                                    .into_iter()
                                    .collect(),
                                vec![(Var::new("a"), Value::Entity(Entity(1))),
                                     (Var::new("b"), Value::String("John".into()))]
                                    .into_iter()
                                    .collect()]));
    }

    #[test]
    fn test_query5() {
        // find ?b where (?a name Bob) (?b parent ?a)
        helper(&*DB,
               Query::new(vec![Var::new("a")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound("Bob".into())),
                               Clause::new(Term::Unbound("b".into()),
                                           Term::Bound("parent".into()),
                                           Term::Unbound("a".into()))]),
               QueryResult(vec![iter::once((Var::new("b"), Value::Entity(Entity(1)))).collect()]));
    }

    fn helper<D: Database>(db: &D, query: Query, expected: QueryResult) {
        let result = db.query(query);
        assert_eq!(expected, result);
    }
}
