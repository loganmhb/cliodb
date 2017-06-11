use model::Entity;

/// Double-mapping of ident->entity and entity->ident.
#[derive(Default, Clone)]
pub struct IdentMap {
    mappings: Vec<(String, Entity)>
}

impl IdentMap {
    pub fn add(&self, ident: String, entity: Entity) -> IdentMap {
        let mut new = self.clone();
        new.mappings.push((ident, entity));
        new
    }

    pub fn retract(&self, ident: &str, entity: Entity) -> IdentMap {
        IdentMap {
            mappings: self.mappings.iter()
                .filter({|&&(ref i, e)| (i.as_str(), e) == (ident, entity)})
                .cloned()
                .collect::<Vec<_>> ()
        }
    }

    pub fn get_entity(&self, ident: String) -> Option<Entity> {
        self.mappings.iter().find(|&&(ref i, _)| *i == ident)
            .map(|&(_, e)| e.clone())
    }

    pub fn get_ident(&self, entity: Entity) -> Option<String> {
        self.mappings.iter().find(|&&(_, e)| e == entity)
            .map(|&(ref i, _)| i.clone())
    }
}


mod tests {

}
