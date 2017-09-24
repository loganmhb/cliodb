use super::*;
use std::sync::Arc;

use {Result, EAVT, AEVT, AVET, VAET};
use index::Index;

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

    fn records_matching(&self, clause: &Clause, binding: &Binding) -> Result<Vec<Record>> {
        let expanded = clause.substitute(binding, &self.idents)?;
        match expanded {
            // ?e a v => use the ave index
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {
                match self.idents.get_entity(&a) {
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
                match self.idents.get_entity(&a) {
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
                        .filter(|f| unify(&binding, &self.idents, &clause, &f).is_some())
                        .collect(),
                )
            }
        }
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        query.validate()?;

        // TODO: automatically bind ?tx in queries
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for record in self.records_matching(clause, &binding)? {
                    if let Some(new_info) = unify(&binding, &self.idents, clause, &record)
                        .into_iter()
                        .filter(|potential_binding| {
                            query.constraints.iter().all(|constraint| {
                                constraint.check(potential_binding)
                            })
                        })
                        .next()
                    {
                        if record.retracted {
                            // The binding matches the retraction
                            // so we discard any existing bindings
                            // that are the same.  Note that this
                            // relies on the fact that additions
                            // and retractions are sorted by
                            // transaction, so an older retraction
                            // won't delete the binding for a
                            // newer addition.
                            new_bindings.retain(|b| *b != new_info);
                        } else {
                            new_bindings.push(new_info)
                        }
                    }
                }
            }

            bindings = new_bindings;
        }

        for binding in bindings.iter_mut() {
            *binding = binding
                .iter()
                .filter(|&(k, _)| query.find.contains(k))
                .map(|(var, value)| (var.clone(), value.clone()))
                .collect();
        }

        Ok(QueryResult(query.find.clone(), bindings))
    }
}

/// Attempts to unify a new record and a clause with existing
/// bindings.  If bound fields in the clause match the record, then
/// any fields in the record which match an unbound clause will be
/// bound in the returned binding.  If bound fields in the clause
/// conflict with fields in the record, unification fails.
fn unify(env: &Binding, idents: &IdentMap, clause: &Clause, record: &Record) -> Option<Binding> {
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
            match idents.get_entity(a) {
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
                    Term::Bound("name".into()),
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
}
