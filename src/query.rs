use {Result, Entity, Value, Binding};
use queries::query::{Term, Var};
use std::collections::HashSet;

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

    pub fn validate(&self) -> Result<()> {
        let mut where_set = HashSet::new();

        // Macro to turn a heterogeneous argument list of Term<T>s into
        // a list of the names of the terms that are unbound.
        macro_rules! names {
            ($($term:expr),*) => {{
                let mut name_vec = Vec::new();
                $(
                    match $term {
                        Term::Unbound(Var { ref name }) => name_vec.push(name.clone()),
                        Term::Bound(_) => (),
                    }
                )*
                name_vec
            }}
        }

        for clause in &self.clauses {
            for name in names![clause.entity, clause.attribute, clause.value] {
                where_set.insert(name);
            }
        }

        for constraint in &self.constraints {
            for name in names![constraint.first_value, constraint.second_value] {
                where_set.insert(name);
            }
        }

        for &Var { ref name } in &self.find {
            if !where_set.contains(name) {
                return Err(
                    "Variables in `find` spec must match those appearing in clauses".into(),
                );
            }
        }

        Ok(())
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Comparator {
    GreaterThan,
    LesserThan,
    NotEqualTo,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Constraint {
    pub comparator: Comparator,
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
        let res = match (
            self.comparator,
            bound_val(&self.first_value, binding),
            bound_val(&self.second_value, binding),
        ) {
            (_, _, None) => true,
            (_, None, _) => true,
            (Comparator::GreaterThan, Some(fst_val), Some(snd_val)) => fst_val > snd_val,
            (Comparator::LesserThan, Some(fst_val), Some(snd_val)) => fst_val < snd_val,
            (Comparator::NotEqualTo, Some(fst_val), Some(snd_val)) => fst_val != snd_val,
        };

        res
    }
}
