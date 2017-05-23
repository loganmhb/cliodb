// For now, something in a variable position can be either an unbound variable
// (e.g. ?a) or a string literal.
#[derive(Debug)]
pub enum Var {
    Symbol(String),
    StringLit(String)
}

#[derive(Debug)]
pub struct Clause {
    entity: Var,
    attribute: String,
    value: Var
}

impl Clause {
    pub fn new(e: Var, a: String, v: Var) -> Clause {
        Clause { entity: e, attribute: a, value: v}
    }
}

// A query looks like `find ?var where (?var <attribute> <value>)`
#[derive(Debug)]
pub struct Query {
    find: Var,
    clauses: Vec<Clause>
}

impl Query {
    pub fn new(find: Var, clauses: Vec<Clause>) -> Query {
        Query { find: find, clauses: clauses }
    }
}
