use model::{Entity, Value};

// A query looks like `find ?var where (?var <attribute> <value>)`
#[derive(Debug, PartialEq)]
pub struct Query {
    pub find: Vec<Var>,
    pub clauses: Vec<Clause>,
}

impl Query {
    pub fn new(find: Vec<Var>, clauses: Vec<Clause>) -> Query {
        Query {
            find: find,
            clauses: clauses,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Clause {
    pub entity: Term<Entity>,
    pub attribute: Term<String>,
    pub value: Term<Value>,
}

impl Clause {
    pub fn new(e: Term<Entity>, a: Term<String>, v: Term<Value>) -> Clause {
        Clause {
            entity: e,
            attribute: a,
            value: v,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Term<T> {
    Bound(T),
    Unbound(Var),
}

// A free [logic] variable
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Var {
    pub name: String,
}

impl Var {
    pub fn new<T: Into<String>>(name: T) -> Var {
        Var::from(name)
    }
}

impl<T: Into<String>> From<T> for Var {
    fn from(x: T) -> Self {
        Var { name: x.into() }
    }
}
