use std::collections::{HashSet, HashMap};
use {Result, Value};
use db::Db;
use queries::query::{Query, Var};
use queries::planner::{Plan};

// temporary definition -- will go in lib.rs
#[derive(Debug, Clone)]
pub struct Relation(pub Vec<Var>, pub Vec<Vec<Value>>);

pub fn execute(query: Query, db: &Db) -> Result<Relation> {
    let plan = Plan::for_query(query);
    execute_plan(&plan, db)
}

fn execute_plan(plan: &Plan, db: &Db) -> Result<Relation> {
    // TODO: apply constraints?
    match plan {
        Plan::Join(plan_a, plan_b) => {
            // join the two relations:
            // 1. determine join key (= set of overlapping variables)
            // 2. hash-join the two relations on the join key (inner join)
            Ok(join(execute_plan(&plan_a, db)?, execute_plan(&plan_b, db)?))
        },
        Plan::LookupEach(p, clause) => {
            // for each binding in the relation, bind the clause and fetch matching records
            // use results to build a new output relation including new vars which the clause binds
            unimplemented!()
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
    }
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
    println!("output key {:?}", output_key);

    let rel_a_vars = rel_a.0.iter().cloned().collect::<HashSet<Var>>();
    let rel_b_out_indices = rel_b.0.iter().enumerate()
        .filter(|(_idx, var)| !rel_a_vars.contains(var))
        .map(|(idx, _var)| idx)
        .collect::<Vec<usize>>();

    println!("{:?}", rel_b_out_indices);
    let rel_b_map = hash_relation(&join_key, rel_b);

    let project = |mut tuple_a: Vec<Value>, tuple_b: &Vec<Value>| {
        for idx in rel_b_out_indices.iter() {
            let val = tuple_b[*idx].clone();
            tuple_a.push(val);
        }
        println!("projected {:?}", tuple_a);

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

#[cfg(test)]
mod tests {
    use super::*;
    use {Value};
    use itertools::assert_equal;

    #[test]
    fn test_join_on_single_field() {
        let rel_a = Relation(vec!["name".into(), "fav_color".into()], vec![
            vec![Value::String("Bob".into()), Value::String("red".into())],
            vec![Value::String("Jane".into()), Value::String("blue".into())],
            vec![Value::String("Alice".into()), Value::String("green".into())],
        ]);
        let rel_b = Relation(vec!["name".into(), "fav_flavor".into()], vec![
            // fav_flavor is cardinality many
            vec![Value::String("Bob".into()), Value::String("chocolate".into())],
            vec![Value::String("Bob".into()), Value::String("double chocolate".into())],
            vec![Value::String("Jane".into()), Value::String("vanilla".into())],
            vec![Value::String("Cliff".into()), Value::String("peanut butter".into())],
        ]);

        let Relation(joined_vars, joined_values) = join(rel_a, rel_b);

        assert_equal(joined_vars, vec!["name".into(), "fav_color".into(), "fav_flavor".into()]);
        assert_equal(joined_values, vec![
            vec![Value::String("Bob".into()), Value::String("red".into()), Value::String("chocolate".into())],
            vec![Value::String("Bob".into()), Value::String("red".into()), Value::String("double chocolate".into())],
            vec![Value::String("Jane".into()), Value::String("blue".into()), Value::String("vanilla".into())]
        ]);
    }

    #[test]
    fn test_join_on_multiple_fields() {
    }
}
