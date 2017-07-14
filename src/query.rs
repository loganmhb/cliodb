use {Entity, Value};

// A query looks like `find ?var where (?var <attribute> <value>)`
#[derive(Debug, PartialEq)]
pub struct Query {
    pub find: Vec<Var>,
    pub clauses: Vec<Clause>,
    pub constraints: Vec<Constraint>,
}

impl Query {
    pub fn new(find: Vec<Var>, clauses: Vec<Clause>, constraints: Vec<Constraint>) -> Query {
        Query {
            find: find,
            clauses: clauses,
            constraints: constraints,
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

#[derive(Debug, PartialEq, Eq)]
pub enum Comperator {
    GreaterThan,
    LesserThan,
    NotEqualTo,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Constraint {
    pub comperator: Comperator,
    pub first_value: Term<Value>,
    pub second_value: Term<Value>,
}
