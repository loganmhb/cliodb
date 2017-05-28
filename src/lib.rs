#![allow(dead_code)]
#![allow(unused_variables)]
#![feature(collections_range)]

extern crate combine;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::collections::BTreeSet;

// # Initial pass
// A database is just a log of facts. Facts are (entity, attribute, value) triples.
// Attributes and values are both just strings. There are no transactions or histories.

#[derive(Debug, PartialEq)]
struct QueryResult(Vec<HashMap<Var, Value>>);

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
enum Value {
    String(String),
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

#[derive(Debug, PartialEq, Eq, Clone)]
enum Term<T> {
    Bound(T),
    Unbound(Var),
}

impl<'a, T: PartialEq> Term<T> {
    fn satisfied_by(&self, val: &'a T) -> bool
        where &'a T: PartialEq
    {
        match self {
            &Term::Bound(ref binding) => *val == *binding,
            &Term::Unbound(_) => true,
        }
    }
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
#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
struct Entity(u64);

#[derive(Debug, PartialEq, Eq)]
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

// The Fact struct represents a fact in the database.
// The derived ordering is used by the EAV index; other
// indices use orderings provided by wrapper structs.
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
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

// Fact wrappers provide ordering for indexes.
#[derive(PartialEq, Eq, Debug)]
struct AVE(Fact);

impl PartialOrd for AVE {
    fn partial_cmp(&self, other: &AVE) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AVE {
    fn cmp(&self, other: &AVE) -> std::cmp::Ordering {
        self.0
            .attribute
            .cmp(&other.0.attribute)
            .then(self.0.value.cmp(&other.0.value))
            .then(self.0.entity.cmp(&other.0.entity))
    }
}

#[derive(PartialEq, Eq, Debug)]
struct AEV(Fact);

impl PartialOrd for AEV {
    fn partial_cmp(&self, other: &AEV) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AEV {
    fn cmp(&self, other: &AEV) -> std::cmp::Ordering {
        self.0
            .attribute
            .cmp(&other.0.attribute)
            .then(self.0.entity.cmp(&other.0.entity))
            .then(self.0.value.cmp(&other.0.value))
    }
}

//// Parser
use combine::char::{spaces, string, char, letter, digit};
use combine::primitives::Stream;
use combine::{Parser, ParseError, many1, between, none_of, eof};

fn parse_query<I>(input: I) -> Result<Query, ParseError<I>>
    where I: Stream<Item = char>
{
    // Lexers for ignoring spaces following tokens
    let lex_char = |c| char(c).skip(spaces());
    let lex_string = |s| string(s).skip(spaces());

    // Variables and literals
    let free_var = || {
        char('?')
            .and(many1(letter()))
            .skip(spaces())
            .map(|x| x.1)
            .map(|name: String| Var::new(name))
    }; // don't care about the ?

    let string_lit =
        between(char('"'), char('"'), many1(none_of("\"".chars()))).map(|s| Value::String(s));
    // FIXME: Number literals should be able to be entities or just integers; this
    // probably requires a change to the types/maybe change to the unification system.
    let number_lit = || many1(digit()).map(|n: String| Entity(n.parse().unwrap()));

    let entity = || number_lit();
    let attribute = many1(letter()).map(|x| x);
    let value = string_lit.or(number_lit().map(|e| Value::Entity(e)));

    // There is probably a way to DRY these out but I couldn't satisfy the type checker.
    let entity_term = free_var()
        .map(|x| Term::Unbound(x))
        .or(entity().map(|x| Term::Bound(x)))
        .skip(spaces());
    let attribute_term = free_var()
        .map(|x| Term::Unbound(x))
        .or(attribute.map(|x| Term::Bound(x)))
        .skip(spaces());
    let value_term = free_var()
        .map(|x| Term::Unbound(x))
        .or(value.map(|x| Term::Bound(x)))
        .skip(spaces());

    // Clause structure
    let clause_contents = (entity_term, attribute_term, value_term);
    let clause = between(lex_char('('), lex_char(')'), clause_contents).map(|(e, a, v)| {
                                                                                Clause::new(e, a, v)
                                                                            });
    let find_spec = lex_string("find").and(many1(free_var())).map(|x| x.1);
    let where_spec = lex_string("where").and(many1(clause)).map(|x| x.1);

    let mut query = find_spec.and(where_spec)
        // FIXME: add find vars
        .map(|x| Query{find: x.0, clauses: x.1})
        .and(eof())
        .map(|x| x.0);
    let result = query.parse(input);
    match result {
        Ok((q, _)) => Ok(q),
        Err(err) => Err(err),
    }
}

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
                        &Value::String(ref s) => Term::Bound(s.to_owned()),
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
struct InMemoryLog {
    eav: BTreeSet<Fact>,
    ave: BTreeSet<AVE>,
    aev: BTreeSet<AEV>,
}

use std::collections::range::RangeArgument;
use std::collections::Bound;

impl RangeArgument<AEV> for AEV {
    fn start(&self) -> Bound<&AEV> {
        Bound::Included(&self)
    }

    fn end(&self) -> Bound<&AEV> {
        Bound::Unbounded
    }
}

impl RangeArgument<AVE> for AVE {
    fn start(&self) -> Bound<&AVE> {
        Bound::Included(&self)
    }

    fn end(&self) -> Bound<&AVE> {
        Bound::Unbounded
    }
}

impl InMemoryLog {
    fn new() -> InMemoryLog {
        InMemoryLog::default()
    }

    // Efficiently retrieve facts matching a clause
    fn facts_matching(&self, clause: &Clause, binding: &Binding) -> Vec<&Fact> {
        let expanded = clause.substitute(binding);
        match clause {
            // ?e a v => use the ave index
            &Clause {
                 entity: Term::Unbound(_),
                 attribute: Term::Bound(ref a),
                 value: Term::Bound(ref v),
             } => {
                let range_start = Fact::new(Entity(0), a.clone(), v.clone());
                self.ave
                    .range(AVE(range_start))
                    .map(|ave| &ave.0)
                    .take_while(|f| f.attribute == *a && f.value == *v)
                    .collect()
            }
            // FIXME: Implement other optimized index use cases.

            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                self.eav
                    .iter()
                    .filter(|f| {
                                clause.entity.satisfied_by(&f.entity) &&
                                clause.attribute.satisfied_by(&f.attribute) &&
                                clause.value.satisfied_by(&f.value)
                            })
                    .collect()
            }
        }
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
        self.eav.insert(fact.clone());
        self.ave.insert(AVE(fact.clone()));
        self.aev.insert(AEV(fact.clone()));
    }

    fn query(&self, query: Query) -> QueryResult {
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

        let result = bindings
            .into_iter()
            .map(|solution| {
                     solution
                         .into_iter()
                         .filter(|&(ref k, _)| query.find.contains(&k))
                         .collect()
                 })
            .collect();

        QueryResult(result)
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
mod test {
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
               parse_query("find ?a where (?a name \"Bob\")").unwrap(),
               QueryResult(vec![iter::once((Var::new("a"), Value::Entity(Entity(0)))).collect()]));
    }

    #[test]
    fn test_query_unknown_value() {
        // find ?a where (0 name ?a)
        helper(&*DB,
               parse_query("find ?a where (0 name ?a)").unwrap(),
               QueryResult(vec![iter::once((Var::new("a"), Value::String("Bob".into())))
                                    .collect()]));

    }
    #[test]
    fn test_query_unknown_attribute() {
        // find ?a where (1 ?a "John")
        helper(&*DB,
               parse_query("find ?a where (1 ?a \"John\")").unwrap(),
               QueryResult(vec![iter::once((Var::new("a"), Value::String("name".into())))
                                    .collect()]));
    }

    #[test]
    fn test_query_multiple_results() {
        // find ?a ?b where (?a name ?b)
        helper(&*DB,
               parse_query("find ?a ?b where (?a name ?b)").unwrap(),
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
               parse_query("find ?b where (?a name \"Bob\") (?b parent ?a)").unwrap(),
               QueryResult(vec![iter::once((Var::new("b"), Value::Entity(Entity(1)))).collect()]));
    }

    #[test]
    fn test_query_implicit_join() {
        // find ?c where (?a name Bob) (?b name ?c) (?b parent ?a)
        helper(&*DB,
               parse_query("find ?c where (?a name \"Bob\") (?b name ?c) (?b parent ?a)").unwrap(),
               QueryResult(vec![iter::once((Var::new("c"), Value::String("John".into())))
                                    .collect()]));
    }

    fn helper<D: Database>(db: &D, query: Query, expected: QueryResult) {
        let result = db.query(query);
        assert_eq!(expected, result);
    }
}
