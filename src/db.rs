use super::*;

use std::sync::Arc;
use serde::{Serialize, Deserialize};

use im::HashMap;
use {Result, EAVT, AEVT, AVET, VAET};
use index::Index;
use schema::{Schema, ValueType};
use queries::query;

/// An *immutable* view of the database at a point in time.
/// Only used for querying; for transactions, you need a Conn.
#[derive(Clone)]
pub struct Db {
    pub schema: Schema,
    pub store: Arc<dyn KVStore + 'static>,
    pub eav: Index<Record, EAVT>,
    pub ave: Index<Record, AVET>,
    pub aev: Index<Record, AEVT>,
    pub vae: Index<Record, VAET>,
}

/// A structure designed to be stored in the backing store that enables
/// a process to locate the indexes, tx log, etc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbMetadata {
    pub next_id: i64,
    pub last_indexed_tx: i64,
    pub schema: Schema,
    pub eav: String,
    pub ave: String,
    pub aev: String,
    pub vae: String,
}

impl Db {
    pub fn new(metadata: DbMetadata, store: Arc<dyn KVStore>) -> Db {
        let db = Db {
            store: store.clone(),
            schema: metadata.schema,
            eav: Index::new(metadata.eav, store.clone(), EAVT),
            ave: Index::new(metadata.ave, store.clone(), AVET),
            aev: Index::new(metadata.aev, store.clone(), AEVT),
            vae: Index::new(metadata.vae, store, VAET),
        };

        db
    }

    pub fn mem_index_size(&self) -> usize {
        self.eav.mem_index_size()
    }

    fn ident_entity(&self, ident: &Ident) -> Option<Entity> {
        match ident {
            &Ident::Entity(e) => Some(e),
            &Ident::Name(ref name) => self.schema.idents.get(name).map(|e| *e)
        }
    }

    // FIXME: make private
    // FIXME: should return a fallible iterator instead of a vec
    pub fn records_matching(&self, clause: &Clause, binding: &Binding) -> Result<Vec<Record>> {
        let expanded = clause.substitute(binding)?;
        match expanded {
            // ?e a v => use the VAE index if value type is ref, AVET if indexed, otherwise AEV
            Clause {
                entity: Term::Unbound(_),
                attribute: Term::Bound(a),
                value: Term::Bound(v),
            } => {
                let attr = self.ident_entity(&a).ok_or(format!("invalid attribute: {:?}", a))?;
                let range_start = Record::addition(Entity(0), attr, v.clone(), Entity(0));


                if let Value::Ref(_) = v {
                    // Since the value type is Ref, we can use the VAE index.
                    Ok(
                        self.vae
                            .range_from(range_start)
                            .take_while(|rec| rec.attribute == attr && rec.value == v)
                            .collect()
                    )
                } else if self.schema.is_indexed(attr) {
                    Ok(
                        self.ave
                            .range_from(range_start)
                            .take_while(|rec| rec.attribute == attr && rec.value == v)
                            .collect()
                    )
                } else {
                    Ok(
                        self.aev
                            .range_from(range_start)
                            .take_while(|rec| rec.attribute == attr)
                            .filter(|rec| rec.value == v)
                            .collect()
                    )
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
            // FIXME: Implement other optimized index use cases? (multiple unknowns?)
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
        let mut selectors: Vec<Box<dyn Fn(&Record) -> Value>> = vec![];

        match clause.entity {
            query::Term::Bound(_) => {},
            query::Term::Unbound(ref var) => {
                vars.push(query::Var::new(var.name.clone()));
                selectors.push(Box::new(|record: &Record| Value::Ref(record.entity)));
            }
        };

        match clause.attribute {
            query::Term::Bound(_) => {},
            query::Term::Unbound(ref var) => {
                vars.push(query::Var::new(var.name.clone()));
                selectors.push(Box::new(|record: &Record| Value::Ref(record.attribute)));
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
                        if *e != Value::Ref(record.entity) {
                            return None;
                        }
                    }
                    _ => {
                        new_env.insert(var.clone(), Value::Ref(record.entity));
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
                        if *e != Value::Ref(record.attribute) {
                            return None;
                        }
                    }
                    _ => {
                        new_env.insert(var.clone(), Value::Ref(record.attribute));
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

    /// Add a record to the database. Does not validate that the fact
    /// fits the schema, in order to allow bootstrapping.
    pub fn add_record(&self, record: Record) -> Result<Db> {
        let new_eav = self.eav.insert(record.clone());
        let new_aev = self.aev.insert(record.clone());

        let mut new_vae = self.vae.clone();
        // TODO: only add to AVET if db:indexed is true
        let new_ave = self.ave.insert(record.clone());

        if let Record { value: Value::Ref(_), .. } = record {
            new_vae = new_vae.insert(record.clone());
        }

        // If the record modifies a schema attribute, we need to update the schema.
        let mut new_schema = self.schema.clone();
        if record.attribute == *self.schema.idents.get("db:ident").expect("`db:ident` not in ident map") {
            match record.value {
                Value::Ident(ref s) => new_schema = new_schema.add_ident(record.entity, s.clone()),
                _ => return Err("db:ident value must be an ident".into()),
            };
        };

        if record.attribute == *self.schema.idents.get("db:valueType").expect("db:valueType not in ident map") {
            let value_type = match record.value {
                Value::Ident(ref s) => {
                    match s.as_str() {
                        "db:type:string" => ValueType::String,
                        "db:type:ident" => ValueType::Ident,
                        "db:type:timestamp" => ValueType::Timestamp,
                        "db:type:ref" => ValueType::Ref,
                        "db:type:boolean" => ValueType::Boolean,
                        _ => return Err(format!("{} is not a valid primitive type", s).into()),
                    }
                },
                _ => return Err("db:valueType must be an identifier".into()),
            };

            new_schema = new_schema.add_value_type(record.entity, value_type);
        };

        if record.attribute == *self.schema.idents.get("db:indexed").unwrap() {
            let indexed = match record.value {
                Value::Boolean(b) => b,
                v => return Err(format!("invalid value type {:?} passed with db:indexed", v).into())
            };

            if indexed {
                new_schema = new_schema.add_indexed(record.attribute);
            } else {
                new_schema = new_schema.remove_indexed(&record.attribute);
            }
        }

        Ok(Db {
            eav: new_eav,
            ave: new_ave,
            aev: new_aev,
            vae: new_vae,
            schema: new_schema,
            store: self.store.clone(),
        })
    }

    /// Add a record to the DB, validating that it matches the schema.
    pub fn add(&self, fact: Fact, tx_entity: Entity) -> Result<(Db, Record)> {
        let attr = match self.schema.idents.get(&fact.attribute) {
            Some(a) => a,
            None => return Err(format!("invalid attribute: ident '{:?}' does not exist", &fact.attribute).into())
        };

        let fact_value_type = match fact.value {
            Value::String(_) => ValueType::String,
            Value::Ref(_) => ValueType::Ref,
            Value::Timestamp(_) => ValueType::Timestamp,
            Value::Ident(_) => ValueType::Ident,
            Value::Boolean(_) => ValueType::Boolean,
            Value::Long(_) => ValueType::Long,
        };

        match self.schema.value_types.get(&attr) {
            Some(schema_type) => {
                if *schema_type == fact_value_type {
                    let record = Record::addition(fact.entity, *attr, fact.value, tx_entity);
                    return self.add_record(record.clone()).map(|new_db| (new_db, record));
                } else {
                    return Err(format!(
                        "type error: attribute {:?} does not match expected value type {:?}",
                        fact.attribute,
                        fact_value_type
                    ).into())
                }
            },
            None => return Err(format!("ident {:?} is not a valid attribute", fact.attribute).into())
        }
    }

    pub fn retract(&self, fact: Fact, tx_entity: Entity) -> Result<(Db, Record)> {
        // FIXME: dry
        let attr = match self.schema.idents.get(&fact.attribute) {
            Some(a) => a,
            None => return Err(format!("invalid attribute: ident '{:?}' does not exist", &fact.attribute).into())
        };

        let fact_value_type = match fact.value {
            Value::String(_) => ValueType::String,
            Value::Ref(_) => ValueType::Ref,
            Value::Timestamp(_) => ValueType::Timestamp,
            Value::Ident(_) => ValueType::Ident,
            Value::Boolean(_) => ValueType::Boolean,
            Value::Long(_) => ValueType::Long,
        };

        match self.schema.value_types.get(&attr) {
            Some(schema_type) => {
                if *schema_type == fact_value_type {
                    let record = Record::retraction(fact.entity, *attr, fact.value, tx_entity);
                    return self.add_record(record.clone()).map(|new_db| (new_db, record));
                } else {
                    return Err(format!(
                        "type error: attribute {:?} does not match expected value type {:?}",
                        fact.attribute,
                        fact_value_type
                    ).into())
                }
            },
            None => return Err(format!("ident {:?} is not a valid attribute", fact.attribute).into())
        }
    }
}
