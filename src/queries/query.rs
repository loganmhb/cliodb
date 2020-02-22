use im::HashMap;

use {Entity, Value, Result, Ident};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Query {
    pub find: Vec<Var>,
    pub clauses: Vec<Clause>,
    pub constraints: Vec<Constraint>,
}

/// A free logic variable
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Var {
    pub name: String,
}

impl Var {
    pub fn new<T: Into<String>>(name: T) -> Var {
        Var {
            name: name.into(),
        }
    }
}

impl<T: Into<String>> From<T> for Var {
    fn from(x: T) -> Self {
        Var { name: x.into() }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Clause {
    pub entity: Term<Entity>,
    pub attribute: Term<Ident>,
    pub value: Term<Value>,
}

impl Clause {
    pub fn new(e: Term<Entity>, a: Term<Ident>, v: Term<Value>) -> Clause {
        Clause {
            entity: e,
            attribute: a,
            value: v,
        }
    }

    pub fn unbound_vars(&self) -> Vec<Var> {
        let mut unbound: Vec<Var> = vec![];

        if let Term::Unbound(ref e_var) = self.entity {
            unbound.push(e_var.clone());
        }

        if let Term::Unbound(ref a_var) = self.attribute {
            unbound.push(a_var.clone());
        }

        if let Term::Unbound(ref v_var) = self.value {
            unbound.push(v_var.clone());
        }

        return unbound;
    }

    pub fn substitute(&self, env: &HashMap<Var, Value>) -> Result<Clause> {
        let entity = match &self.entity {
            &Term::Bound(_) => self.entity.clone(),
            &Term::Unbound(ref var) => {
                if let Some(val) = env.get(&var) {
                    match *val {
                        Value::Ref(e) => Term::Bound(e),
                        _ => return Err("type mismatch".into()),
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
                        &Value::Ref(e) => Term::Bound(Ident::Entity(e)),
                        _ => return Err("type mismatch".into()),
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

        Ok(Clause::new(entity, attribute, value))
    }
}

/// An item in a query clause. Either bound (associated with a value) or unbound (linked to a variable, which it will bind to a set of possible values).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Term<T> {
    Bound(T),
    Unbound(Var),
}

/// A comparator is <, > or !=.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Comparator {
    GreaterThan,
    LessThan,
    NotEqualTo,
}

/// A constraint differs from a clause in that it cannot add new items
/// to the result set; it only constrains the existing result set to
/// items which match the constraint.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Constraint {
    pub comparator: Comparator,
    pub left_hand_side: Term<Value>,
    pub right_hand_side: Term<Value>,
}

impl Constraint {
    pub fn satisfied_by(&self, binding: &HashMap<&Var, &Value>) -> bool {
        let lhs_value = match self.left_hand_side {
            Term::Bound(ref val) => val,
            Term::Unbound(ref var) => binding[var],
        };
        let rhs_value = match self.right_hand_side {
            Term::Bound(ref val) => val,
            Term::Unbound(ref var) => binding[var],
        };

        match self.comparator {
            Comparator::GreaterThan => lhs_value > rhs_value,
            Comparator::LessThan => lhs_value < rhs_value,
            Comparator::NotEqualTo => lhs_value != rhs_value,
        }
    }
}
