use serde::{Serialize, Deserialize};
use im::{HashMap, HashSet};
use super::{Entity};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueType {
    String,
    Ident,
    Ref,
    Timestamp,
    Boolean,
    Long
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub idents: HashMap<String, Entity>,
    pub value_types: HashMap<Entity, ValueType>,
    pub cardinalities: HashMap<Entity, Cardinality>,
    pub indexed: HashSet<Entity>,
}

impl Schema {
    pub fn add_ident(&self, entity: Entity, identifier: String) -> Schema {
        let mut new = self.clone();
        new.idents.insert(identifier, entity);
        new
    }

    pub fn add_cardinality(&self, entity: Entity, cardinality: Cardinality) -> Schema {
        let mut new = self.clone();
        new.cardinalities.insert(entity, cardinality);
        new
    }

    pub fn add_value_type(&self, entity: Entity, value_type: ValueType) -> Schema {
        let mut new = self.clone();
        new.value_types.insert(entity, value_type);
        new
    }

    pub fn index_attribute(&self, entity: Entity) -> Schema {
        let mut new = self.clone();
        new.indexed.insert(entity);
        new
    }

    pub fn is_indexed(&self, entity: Entity) -> bool {
        self.indexed.contains(&entity)
    }

    pub fn add_indexed(&self, entity: Entity) -> Schema {
        let mut new = self.clone();
        new.indexed.insert(entity);
        new
    }

    pub fn remove_indexed(&self, entity: &Entity) -> Schema {
        let mut new = self.clone();
        new.indexed.remove(entity);
        new
    }

    pub fn empty() -> Schema {
        Schema {
            idents: HashMap::new(),
            value_types: HashMap::new(),
            cardinalities: HashMap::new(),
            indexed: HashSet::new(),
        }
    }
}
