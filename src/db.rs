use super::*;

use std::sync::Arc;

use {Result, EAVT, AEVT, AVET, VAET};
use index::Index;
use queries::query;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueType {
    String,
    Ident,
    Entity,
    Timestamp,
}

/// An *immutable* view of the database at a point in time.
/// Only used for querying; for transactions, you need a Conn.
#[derive(Clone)]
pub struct Db {
    pub idents: IdentMap,
    pub schema: HashMap<Entity, ValueType>,
    pub store: Arc<KVStore + 'static>,
    pub eav: Index<Record, EAVT>,
    pub ave: Index<Record, AVET>,
    pub aev: Index<Record, AEVT>,
    pub vae: Index<Record, VAET>,
}

impl Db {
    pub fn new(contents: DbContents, store: Arc<KVStore>) -> Db {
        let db = Db {
            store: store.clone(),
            idents: contents.idents,
            schema: contents.schema,
            eav: Index::new(contents.eav, store.clone(), EAVT),
            ave: Index::new(contents.ave, store.clone(), AVET),
            aev: Index::new(contents.aev, store.clone(), AEVT),
            vae: Index::new(contents.vae, store, VAET),
        };

        db
    }

    pub fn mem_index_size(&self) -> usize {
        self.eav.mem_index_size()
    }

    fn ident_entity(&self, ident: &Ident) -> Option<Entity> {
        match ident {
            &Ident::Entity(e) => Some(e),
            &Ident::Name(ref name) => self.idents.get_entity(name)
        }
    }

    fn records_matching(&self, clause: &Clause, binding: &Binding) -> Result<Vec<Record>> {
        let expanded = clause.substitute(binding)?;
        match expanded {
            // ?e a v => use the ave index
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {
                match self.ident_entity(&a) {
                    Some(attr) => {
                        let range_start = Record::addition(Entity(0), attr, v.clone(), Entity(0));
                        Ok(
                            self.ave
                                .range_from(range_start)
                                .take_while(|rec| rec.attribute == attr && rec.value == v)
                                .collect(),
                        )
                    }
                    _ => return Err("invalid attribute".into()),
                }
            }
            // e a ?v => use the eav index
            Clause {
                entity: Term::Bound(e),
                attribute: Term::Bound(a),
                value: Term::Unbound(_),
            } => {
                match self.ident_entity(&a) {
                    Some(attr) => {
                        // Value::String("") is the lowest-sorted value
                        let range_start =
                            Record::addition(e, attr, Value::String("".into()), Entity(0));
                        Ok(
                            self.eav
                                .range_from(range_start)
                                .take_while(|rec| rec.entity == e && rec.attribute == attr)
                                .collect(),
                        )
                    }
                    _ => return Err("invalid attribute".into()),
                }
            }
            // FIXME: Implement other optimized index use cases? (multiple unknowns? refs?)
            // Fallthrough case: just scan the EAV index. Correct but slow.
            _ => {
                Ok(
                    self.eav
                        .iter()
                        .filter(|f| self.unify(&binding, &clause, &f).is_some())
                        .collect(),
                )
            }
        }
    }

    /// Given a clause, fetch the relation of matching records.
    pub fn fetch(&self, clause: &query::Clause) -> Result<Relation> {
        let mut vars = vec![];
        let mut selectors: Vec<Box<Fn(&Record) -> Value>> = vec![];

        match clause.entity {
            query::Term::Bound(_) => {},
            query::Term::Unbound(ref var) => {
                vars.push(query::Var::new(var.name.clone()));
                selectors.push(Box::new(|record: &Record| Value::Entity(record.entity)));
            }
        };

        match clause.attribute {
            query::Term::Bound(_) => {},
            query::Term::Unbound(ref var) => {
                vars.push(query::Var::new(var.name.clone()));
                selectors.push(Box::new(|record: &Record| Value::Entity(record.attribute)));
            }
        };
        match clause.value {
            query::Term::Bound(_) => {},
            query::Term::Unbound(ref var) => {
                vars.push(query::Var::new(var.name.clone()));
                selectors.push(Box::new(|record: &Record| record.value.clone()));
            }
        };

        let mut values: Vec<Vec<Value>> = vec![];
        // FIXME: will need to remove retracted records from the relation
        // (and eventually deal with cardinality:one)

        for record in self.records_matching(&clause, &HashMap::new())? {
            let mut tuple: Vec<Value> = vec![];
            if record.retracted {
                // If the matching record is a retraction, the fact it
                // retracts will be the fact matched immediately
                // beforehand.
                values.pop();
            } else {
                for selector in selectors.iter() {
                    tuple.push(selector(&record));
                }
                values.push(tuple);
            }
        }

        Ok(Relation(vars, values))
    }

    /// Attempts to unify a new record and a clause with existing
    /// bindings.  If bound fields in the clause match the record, then
    /// any fields in the record which match an unbound clause will be
    /// bound in the returned binding.  If bound fields in the clause
    /// conflict with fields in the record, unification fails.
    fn unify(&self, env: &Binding, clause: &Clause, record: &Record) -> Option<Binding> {
        let mut new_env: Binding = env.clone();

        match clause.entity {
            Term::Bound(ref e) => {
                if *e != record.entity {
                    return None;
                }
            }
            Term::Unbound(ref var) => {
                match env.get(var) {
                    Some(e) => {
                        if *e != Value::Entity(record.entity) {
                            return None;
                        }
                    }
                    _ => {
                        new_env.insert(var.clone(), Value::Entity(record.entity));
                    }
                }
            }
        }

        match clause.attribute {
            Term::Bound(ref a) => {
                // The query will use an ident to refer to the attribute, but we need the
                // actual attribute entity.
                match self.ident_entity(a) {
                    Some(e) => {
                        if e != record.attribute {
                            return None;
                        }
                    }
                    _ => return None,
                }
            }
            Term::Unbound(ref var) => {
                match env.get(var) {
                    Some(e) => {
                        if *e != Value::Entity(record.attribute) {
                            return None;
                        }
                    }
                    _ => {
                        new_env.insert(var.clone(), Value::Entity(record.attribute));
                    }
                }
            }
        }

        match clause.value {
            Term::Bound(ref v) => {
                if *v != record.value {
                    return None;
                }
            }
            Term::Unbound(ref var) => {
                match env.get(var) {
                    Some(e) => {
                        if *e != record.value {
                            return None;
                        }
                    }
                    _ => {
                        new_env.insert(var.clone(), record.value.clone());
                    }
                }
            }
        }

        Some(new_env)
    }
}

/// A structure designed to be stored in the backing store that enables
/// a process to locate the indexes, tx log, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbContents {
    pub next_id: i64,
    pub last_indexed_tx: i64,
    pub idents: IdentMap,
    pub schema: HashMap<Entity, ValueType>,
    pub eav: String,
    pub ave: String,
    pub aev: String,
    pub vae: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tests::test_db;

    #[test]
    fn test_records_matching() {
        let matching = test_db()
            .records_matching(
                &Clause::new(
                    Term::Unbound("e".into()),
                    Term::Bound(Ident::Name("name".into())),
                    Term::Bound(Value::String("Bob".into())),
                ),
                &Binding::default(),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        let rec = &matching[0];
        assert_eq!(rec.entity, Entity(10));
        assert_eq!(rec.value, Value::String("Bob".into()));
    }

    #[test]
    fn test_fetch() {
        let name_entity = test_db().idents.get_entity("name").unwrap();
        let clause = query::Clause::new(
            Term::Unbound("e".into()),
            Term::Bound(Ident::Entity(name_entity)),
            Term::Unbound("n".into()),
        );
        let relation = test_db().fetch(&clause).unwrap();
        assert_eq!(relation.0, vec!["e".into(), "n".into()]);
        assert_eq!(relation.1, vec![
            vec![Value::Entity(Entity(10)), Value::String("Bob".into())],
            vec![Value::Entity(Entity(11)), Value::String("John".into())]
        ]);
    }
}
