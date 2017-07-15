use {Entity, Value, Binding};

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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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

fn bound_val<'v: 'b, 'b>(val: &'v Term<Value>, binding: &'b Binding) -> Option<&'b Value> {
    match val {
        &Term::Bound(ref x) => Some(x),
        &Term::Unbound(ref var) => binding.get(var),
    }
}

impl Constraint {
    pub fn check(&self, binding: &Binding) -> bool {
        match (
            self.comperator,
            bound_val(&self.first_value, binding),
            bound_val(&self.second_value, binding),
        ) {
            (_, _, None) => true,
            (_, None, _) => true,
            (Comperator::GreaterThan, Some(fst_val), Some(snd_val)) => fst_val > snd_val,
            (Comperator::LesserThan, Some(fst_val), Some(snd_val)) => fst_val < snd_val,
            (Comperator::NotEqualTo, Some(fst_val), Some(snd_val)) => fst_val != snd_val,
        }
    }
}
