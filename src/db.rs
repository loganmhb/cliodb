use std::collections::HashMap;
use super::*;
use std::sync::Mutex;

pub trait Database where Self: Send {
    fn add(&mut self, fact: Fact);
    fn facts_matching(&self, clause: &Clause, binding: &Binding) -> Vec<Fact>;
    fn next_id(&self) -> u64;

    fn transact(&mut self, tx: Tx) {
        let tx_entity = Entity(self.next_id());
        self.add(Fact::new(tx_entity, "txInstant", Value::Timestamp(UTC::now()), tx_entity));
        for item in tx.items {
            match item {
                TxItem::Addition(f) => self.add(Fact::from_hypothetical(f, tx_entity)),
                // TODO Implement retractions + new entities
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.next_id());
                    for (k, v) in ht {
                        self.add(Fact::new(entity, k, v, tx_entity))
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    fn query(&self, query: &Query) -> QueryResult {
        // TODO: automatically bind ?tx in queries
        let mut bindings = vec![HashMap::new()];

        for clause in &query.clauses {
            let mut new_bindings = vec![];

            for binding in bindings {
                for fact in self.facts_matching(clause, &binding) {
                    match unify(&binding, clause, &fact) {
                        Ok(new_info) => {
                            let mut new_env = binding.clone();
                            new_env.extend(new_info);
                            new_bindings.push(new_env)
                        }
                        _ => continue,
                    }
                }
            }

            bindings = new_bindings;
        }

        for binding in bindings.iter_mut() {
            *binding = binding
                .iter()
                .filter(|&(k, _)| query.find.contains(k))
                .map(|(&var, value)| (var, value.clone()))
                .collect();
        }

        QueryResult(query.find.clone(), bindings)
    }
}

fn db_by_url(url: &str) -> Box<Database> {
    lazy_static! {
        static ref DBS: Mutex<HashMap<String, Box<Database>>> = HashMap::new();
    }
}
