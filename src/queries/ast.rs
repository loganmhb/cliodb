use {Entity, Value};

use queries::query::{Var, Term, Constraint};

pub struct Ast {
    pub find: Vec<Var>,
    pub clauses: Vec<AstClause>,
    pub constraints: Vec<Constraint>,
}

/// Represents a single (entity, attribute, value) query clause. Each term in the clause may be a variable (unbound) or a value (bound).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AstClause {
    pub entity: Term<Entity>,
    pub attribute: Term<String>,
    pub value: Term<Value>,
}

impl AstClause {
    pub fn new(e: Term<Entity>, a: Term<String>, v: Term<Value>) -> AstClause {
        AstClause {
            entity: e,
            attribute: a,
            value: v,
        }
    }
}
