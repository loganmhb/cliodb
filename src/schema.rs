use std::collections::HashMap;
use super::{Entity};
use idents::{IdentMap};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueType {
    String,
    Ident,
    Entity,
    Timestamp,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    idents: IdentMap,
    value_types: HashMap<Entity, ValueType>,
    cardinalities: HashMap<Entity, Cardinality>,
}

// TODO: use persistent data structures
impl Schema {
    pub fn add_ident(&self, entity: Entity, identifier: String) -> Schema {
        let mut new = self.clone();
        new = new.idents.add(identifier, entity);
        new
    }

    fn add_cardinality(entity: Entity, cardinality: Cardinality) -> Schema {
    }

    fn add_value_type(entity: Entity, value_type: ValueType) -> Schema {
    }
}
