
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

type Env = HashMap<Var, Value>;

impl Database for InMemoryLog {
    fn add(&mut self, fact: Fact) {
        self.facts.push(fact);
    }

    // NOTE: find not actually used/doing anything!
    fn query(&self, query: Query) -> QueryResult {
        assert!(!query.clauses.is_empty());

        #[derive(Default)]
        struct State {
            env: Env,
            fact_idx: usize,
            clause_idx: usize,
        }

        let initial_state = State::default();
        let mut result = vec![];

        let mut stack = vec![initial_state];

        while let Some(mut state) = stack.pop() {
            let clause = match query.clauses.get(state.clause_idx) {
                Some(clause) => clause,
                _ => {
                    result.push(state.env.clone());
                    continue;
                }
            };

            let fact = match self.facts.get(state.fact_idx) {
                Some(fact) => fact,
                _ => {
                    continue;
                }
            };

            match unify(&state.env, &clause, &fact) {
                Ok(new_env) => {
                    let new_state = State {
                        env: new_env,
                        fact_idx: 0,
                        clause_idx: state.clause_idx + 1,
                    };
                    state.fact_idx += 1;
                    stack.push(state);
                    stack.push(new_state);
                }
                _ => {
                    state.fact_idx += 1;
                    stack.push(state);
                }
            }

        }

        let result = result.into_iter()
            .map(|solution| {
                solution.into_iter().filter(|&(ref k, _)| query.find.contains(&k)).collect()
            })
            .collect();

        QueryResult(result)
    }
}

fn unify(env: &Env, clause: &Clause, fact: &Fact) -> Result<Env, ()> {
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
    fn test_query_unknown_entity() {
        // find ?a where (?a name "Bob")
        helper(&*DB,
               Query::new(vec![Var::new("a")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound("Bob".into()))]),
               QueryResult(vec![iter::once((Var::new("a"), Value::Entity(Entity(0)))).collect()]));
    }

    #[test]
    fn test_query_unknown_value() {
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
    fn test_query_unknown_attribute() {
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
    fn test_query_multiple_results() {
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
    fn test_query_explicit_join() {
        // find ?b where (?a name Bob) (?b parent ?a)
        helper(&*DB,
               Query::new(vec![Var::new("b")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound("Bob".into())),
                               Clause::new(Term::Unbound("b".into()),
                                           Term::Bound("parent".into()),
                                           Term::Unbound("a".into()))]),
               QueryResult(vec![iter::once((Var::new("b"), Value::Entity(Entity(1)))).collect()]));
    }

    #[test]
    fn test_query_implicit_join() {
        // find ?c where (?a name Bob) (?b name ?c) (?b parent ?a)
        helper(&*DB,
               Query::new(vec![Var::new("c")],
                          vec![Clause::new(Term::Unbound("a".into()),
                                           Term::Bound("name".into()),
                                           Term::Bound("Bob".into())),
                               Clause::new(Term::Unbound("b".into()),
                                           Term::Bound("name".into()),
                                           Term::Unbound("c".into())),
                               Clause::new(Term::Unbound("b".into()),
                                           Term::Bound("parent".into()),
                                           Term::Unbound("a".into()))]),
               QueryResult(vec![iter::once((Var::new("c"), Value::String("John".into()))).collect()]));
    }

    fn helper<D: Database>(db: &D, query: Query, expected: QueryResult) {
        let result = db.query(query);
        assert_eq!(expected, result);
    }
}
