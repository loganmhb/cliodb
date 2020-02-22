use im::{HashSet, HashMap};
use {Result, Value, Error, Relation, Ident};
use db::Db;
use queries::query::{Query, Var, Clause, Term, Constraint};
use queries::planner::{Plan};

pub fn query(q: Query, db: &Db) -> Result<Relation> {
    let plan = Plan::for_query(q);
    execute_plan(&plan, db)
}

fn execute_plan(plan: &Plan, db: &Db) -> Result<Relation> {
    match plan {
        Plan::Join(plan_a, plan_b) => {
            // join the two relations:
            // 1. determine join key (= set of overlapping variables)
            // 2. hash-join the two relations on the join key (inner join)
            Ok(join(execute_plan(&plan_a, db)?, execute_plan(&plan_b, db)?))
        },
        Plan::LookupEach(prior_plan, clause) => {
            let relation = execute_plan(prior_plan, db)?;

            lookup_each(db, relation, &clause)
        },
        Plan::Fetch(clause) => {
            db.fetch(clause)
        },
        Plan::CartesianProduct(ref plans) => {
            let mut relations = vec![];
            for plan in plans.iter() {
                let result = execute_plan(plan, db)?;
                relations.push(result);
            }

            Ok(cartesian_product(relations))
        },
        Plan::Project(ref plan, projection) => {
            execute_plan(plan, db).and_then(|relation| project(relation, projection.clone()))
        }
        Plan::Constrain(ref plan, constraints) => {
            execute_plan(plan, db).map(|relation| constrain(relation, constraints))
        }
    }
}

fn project(relation: Relation, projection: Vec<Var>) -> Result<Relation> {
    let Relation(vars, tuples) = relation;
    let projected_indices = projection.iter().filter_map(|projected_var| {
        vars.iter().position(|v| v == projected_var)
    }).collect::<Vec<usize>>();

    if projected_indices.len() != projection.len() {
        // some projected var wasn't found in the relation
        return Err(Error(format!("not all vars found in relation {:?} for projection {:?}", vars, projection)))
    }

    Ok(Relation(
        projection,
        tuples.iter().map(|tuple| {
            projected_indices.iter().map(|&idx| tuple[idx].clone()).collect()
        }).collect()
    ))
}

fn constrain(relation: Relation, constraints: &Vec<Constraint>) -> Relation {
    //FIXME: assumes constraint is valid i.e. unbound vars in the constraint are present in the relation
    let Relation(vars, tuples) = relation;

    let out_tuples = tuples.into_iter().filter(|tuple| {
        let bindings: HashMap<&Var, &Value> = vars.iter().zip(tuple.iter()).collect();
        constraints.iter().all(|constraint| constraint.satisfied_by(&bindings))
    }).collect();

    Relation(vars, out_tuples)
}

fn lookup_each(db: &Db, relation: Relation, clause: &Clause) -> Result<Relation> {
    // for each binding in the relation, bind the clause and fetch matching records
    // then, use results to build a new output relation including new vars which the clause binds
    let Relation(in_vars, in_tuples) = relation;

    if in_tuples.len() == 0 {
        return Ok(Relation(in_vars, in_tuples));
    }

    let entity_index: Option<usize> = match clause.entity {
        Term::Bound(_) => None,
        Term::Unbound(ref var) => in_vars.iter().position(|v| v == var)
    };

    let attribute_index: Option<usize> = match clause.attribute {
        Term::Bound(_) => None,
        Term::Unbound(ref var) => in_vars.iter().position(|v| v == var)
    };

    let value_index: Option<usize> = match clause.value {
        Term::Bound(_) => None,
        Term::Unbound(ref var) => in_vars.iter().position(|v| v == var)
    };

    fn bind_clause(clause: &Clause, entity: Option<Value>, attribute: Option<Value>, value: Option<Value>) -> Result<Clause> {
        let entity = if let Some(entity_val) = entity {
            match entity_val {
                Value::Ref(e) => Some(e),
                other_value => return Err(Error(format!["Attempted to bind non-entity {:?} in entity position for clause {:?}", other_value, clause]))
            }
        } else { None };

        let attribute = if let Some(attr_val) = attribute {
            match attr_val {
                Value::Ref(e) => Some(Ident::Entity(e)),
                other_value => return Err(Error(format!["Attempted to bind non-entity {:?} in attribute position for clause {:?}", other_value, clause]))
            }
        } else { None };

        Ok(Clause::new(
            entity.map_or(clause.entity.clone(), |e|  Term::Bound(e)),
            attribute.map_or(clause.attribute.clone(), |a| Term::Bound(a)),
            value.map_or(clause.value.clone(), |v| Term::Bound(v))
        ))
    }

    let substitute_clause = |tuple: &Vec<Value>| {
        bind_clause(
            clause,
            entity_index.map(|idx| tuple[idx].clone()),
            attribute_index.map(|idx| tuple[idx].clone()),
            value_index.map(|idx| tuple[idx].clone()),
        )
    };

    // New vars will be set by the first query. Every subsequent query
    // should return the same set of out vars.
    let mut new_vars: Option<Vec<Var>> = None;
    let mut out_tuples: Vec<Vec<Value>> = vec![];
    for tuple in in_tuples {
        let sub_clause = substitute_clause(&tuple)?;
        let Relation(new_var_results, new_tuples) = db.fetch(&sub_clause)?;

        new_vars.get_or_insert_with(|| new_var_results.clone());
        assert_eq!(new_vars.clone().unwrap(), new_var_results);

        for new_tuple in new_tuples {
            let mut out_tuple = tuple.clone();
            out_tuple.extend(new_tuple);
            out_tuples.push(out_tuple);
        }
    }

    let mut out_vars = in_vars.clone();
    out_vars.extend(new_vars.unwrap());
    Ok(Relation(out_vars, out_tuples))
}

/// Implements the cartesian product of relations, none of which
/// should share fields (otherwise they should be joined).
/// Horribly inefficient implementation!
fn cartesian_product(relations: Vec<Relation>) -> Relation {
    relations.iter().fold(Relation(vec![], vec!()), |acc, relation| {
        let Relation(old_vars, old_vals) = acc;
        let Relation(new_vars, new_vals) = relation;

        let mut next_vars = vec![];
        next_vars.extend(old_vars);
        next_vars.extend(new_vars.clone());

        let mut next_vals = vec![];
        for old_val in old_vals {
            for new_val in new_vals {
                let mut tuple = old_val.clone();
                tuple.extend(new_val.clone());
                next_vals.push(tuple);
            }
        }

        Relation(next_vars, next_vals)
    })
}


/// Implements the natural join between relations, outputting one
/// tuple for each combination of tuples in the two relations which
/// match on all overlapping variables.
fn join(rel_a: Relation, rel_b: Relation) -> Relation {
    // The join key is a vector of vars in both a and b, ordered as they are in a.
    let join_key: Vec<Var> = derive_join_key(&rel_a, &rel_b);
    let output_key = derive_output_key(&rel_a, &rel_b);

    let rel_a_vars = rel_a.0.iter().cloned().collect::<HashSet<Var>>();
    let rel_b_out_indices = rel_b.0.iter().enumerate()
        .filter(|(_idx, var)| !rel_a_vars.contains(var))
        .map(|(idx, _var)| idx)
        .collect::<Vec<usize>>();

    let rel_b_map = hash_relation(&join_key, rel_b);

    let project = |mut tuple_a: Vec<Value>, tuple_b: &Vec<Value>| {
        for idx in rel_b_out_indices.iter() {
            let val = tuple_b[*idx].clone();
            tuple_a.push(val);
        }

        tuple_a
    };

    // Join tuples in a and b on matching join key
    let rel_a_key_indices = key_indices(&join_key, &rel_a);
    let joined: Vec<Vec<Value>> = rel_a.1.iter().fold(vec![], |mut output, tuple_a| {
        if let Some(matches) = rel_b_map.get(&key_for_tuple(&rel_a_key_indices, &tuple_a)) {
            for tuple_b in matches {
                output.push(project(tuple_a.clone(), tuple_b));
            }
        }

        output
    });

    Relation(output_key, joined)
}

/// The join key is a vector containing the vars in both relations a
/// and b, ordered as they are in relation a.
fn derive_join_key(a: &Relation, b: &Relation) -> Vec<Var> {
    let b_vars_set: HashSet<Var> = b.0.iter().cloned().collect();

    // The join key is a vector of vars in both a and b, ordered as they are in a.
    a.0.iter()
        .filter(|var| b_vars_set.contains(&var))
        .cloned().collect()
}

/// The output key is a vector containing the union of vars in
/// relations a and b ordered as vars in a as they are ordered in a,
/// followed by vars only in b as they are (relatively) ordered in b.
fn derive_output_key(a: &Relation, b: &Relation) -> Vec<Var> {
    let a_vars_set: HashSet<Var> = a.0.iter().cloned().collect();
    a.0.iter().cloned()
        .chain(b.0.iter().filter(|var| !a_vars_set.contains(&var)).cloned())
        .collect()
}

fn key_indices(join_key: &Vec<Var>, relation: &Relation) -> Vec<usize> {
    join_key.iter().map(|ref key_var| {
        relation.0.iter().position(|ref var| var == key_var)
            .expect("Join key variable not found in relation")
    }).collect()
}

fn key_for_tuple(key_indices: &Vec<usize>, tuple: &Vec<Value>) -> Vec<Value> {
    key_indices.iter()
        .map(|&idx| tuple[idx].clone())
        .collect()
}

fn hash_relation(
    join_key: &Vec<Var>,
    relation: Relation
) -> HashMap<Vec<Value>, Vec<Vec<Value>>> {
    let indices: Vec<usize> = key_indices(join_key, &relation);
    // Build hashmap of join key -> tuple in rel b
    relation.1.into_iter().fold(
        HashMap::new(),
        |mut m: HashMap<Vec<Value>, Vec<Vec<Value>>>, tuple| {
            {
                let entry = m.entry(key_for_tuple(&indices, &tuple)).or_insert(vec![]);
                (*entry).push(tuple);
            }
            m
        }
    )
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tests::test_db;
//     use {Value, Entity};
//     use itertools::assert_equal;

//     #[test]
//     fn test_join_on_single_field() {
//         let rel_a = Relation(vec!["name".into(), "fav_color".into()], vec![
//             vec![Value::String("Bob".into()), Value::String("red".into())],
//             vec![Value::String("Jane".into()), Value::String("blue".into())],
//             vec![Value::String("Alice".into()), Value::String("green".into())],
//         ]);
//         let rel_b = Relation(vec!["name".into(), "fav_flavor".into()], vec![
//             // fav_flavor is cardinality many
//             vec![Value::String("Bob".into()), Value::String("chocolate".into())],
//             vec![Value::String("Bob".into()), Value::String("double chocolate".into())],
//             vec![Value::String("Jane".into()), Value::String("vanilla".into())],
//             vec![Value::String("Cliff".into()), Value::String("peanut butter".into())],
//         ]);

//         let Relation(joined_vars, joined_values) = join(rel_a, rel_b);

//         assert_equal(joined_vars, vec!["name".into(), "fav_color".into(), "fav_flavor".into()]);
//         assert_equal(joined_values, vec![
//             vec![Value::String("Bob".into()), Value::String("red".into()), Value::String("chocolate".into())],
//             vec![Value::String("Bob".into()), Value::String("red".into()), Value::String("double chocolate".into())],
//             vec![Value::String("Jane".into()), Value::String("blue".into()), Value::String("vanilla".into())]
//         ]);
//     }

//     #[test]
//     fn test_lookup_each() {
//         let db = test_db();
//         let name_entity = *db.schema.idents.get("name").unwrap();
//         let parent_entity = *test_db().schema.idents.get("parent").unwrap();
//         let fetch_clause = Clause::new(
//             Term::Unbound("person".into()),
//             Term::Bound(Ident::Entity(name_entity)),
//             Term::Bound(Value::String("Bob".into()))
//         );
//         let prior_relation = db.fetch(&fetch_clause).unwrap();
//         let lookup_clause = Clause::new(
//             Term::Unbound("parent".into()),
//             Term::Bound(Ident::Entity(parent_entity)),
//             Term::Unbound("person".into())
//         );

//         let result = lookup_each(&db, prior_relation, &lookup_clause).unwrap();
//         assert_eq!(
//             result,
//             Relation(
//                 vec!["person".into(), "parent".into()],
//                 vec![
//                     vec![Value::Entity(Entity(10)), Value::Entity(Entity(11))]
//                 ]
//             )
//         )
//     }

//     #[test]
//     fn test_execute() {
//         let db = test_db();
//         let name_entity = *db.schema.idents.get("name").unwrap();
//         let parent_entity = *db.schema.idents.get("parent").unwrap();
//         let q = Query {
//             find: vec!["name".into()],
//             clauses: vec![
//                 Clause::new(Term::Unbound("person".into()), Term::Bound(Ident::Entity(name_entity)), Term::Bound(Value::String("Bob".into()))),
//                 Clause::new(Term::Unbound("child".into()), Term::Bound(Ident::Entity(name_entity)), Term::Unbound("name".into())),
//                 Clause::new(Term::Unbound("child".into()), Term::Bound(Ident::Entity(parent_entity)), Term::Unbound("person".into()))
//             ],
//             constraints: vec![]
//         };

//         assert_eq!(
//             query(q, &db).unwrap(),
//             Relation(
//                 vec!["name".into()],
//                 vec![
//                     vec!["John".into()]
//                 ]
//             )
//         )
//     }
// }
