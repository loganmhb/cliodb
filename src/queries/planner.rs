use queries::query::{Var, Clause, Query, Constraint};
use std::collections::HashSet;
///! The query planner converts a query into an execution plan. In the
///! future it will be possible to improve the performance of queries
///! by using heuristics to decide between possible execution plans,
///! but this version will use a simpler heuristic which the user can
///! influence by reordering query clauses.
///!
///! The planner uses two strategies for fetching the facts matching a
///! particular query clause: fetch facts matching the clause and the
///! join to existing facts in the result set, or bind the clause
///! using each item in the result set and then look up matching
///! facts. A fetch + join is desirable if a large number of lookups
///! would need to be performed, while looking up each bound result is
///! desirable if the number of bindings is expected to be small or
///! the difference in size between the set of facts matching the
///! bound clause and the unbound clause is large.
///!
///! Making a good decision about which strategy to use requires
///! guessing how many bindings will be in the current result set and
///! how many facts will match the unbound clause but not the bound
///! clause. This would be possible to do by tracking statistics about
///! the data in the database, but there is a simpler implementation
///! possible as well: do a fetch + join for vars which are not yet
///! bound in the result set, and do a lookup for vars which are.
///!
///! This allows users to impact the strategy by reordering query
///! clauses.  For example, consider the following query to find all
///! Canadian actors who have starred in Tarantino movies:
///!
///! find ?actor
///! where
///!  (?movie castmember ?actor)
///!  (?movie director ?director)
///!  (?director name "Quentin Tarantino")
///!  (?actor birthplace "Canada")
///!
///! The optimal execution plan here is most likely:
///! 1) Find the director named "Quentin Tarantino"
///! 2) Look up all movies he has directed
///! 3) Look up all actors in those movies
///! 4) Look up their birthplaces and filter out non-Canadians
///!
///! The planner would execute just that plan given clauses in this order:
///!
///! find ?actor
///! where
///!  (?director name "Quentin Tarantino")
///!  (?movie director ?director)
///!  (?movie castmember ?actor)
///!  (?actor birthplace "Canada")
///!
///! However, that might not be the optimal plan, depending on the
///! dataset. Suppose most Canadian actors have starred in a Tarantino
///! film. Rather than do a lookup for every actor in a Tarantino film
///! to determine whether their birthplace is Canada, it would be
///! preferable to do one lookup to retrieve all Canadian actors, and
///! then do an in-memory join between that relation and the relation
///! of actors in Tarantino films. You could get the planner to output
///! such a plan by ordering the clauses like so:
///!
///! find ?actor
///! where
///!  (?director name "Quentin Tarantino") -- introduces new binding ?director, causes fetch
///!  (?actor birthplace "Canada") -- introduces new binding ?actor, causes fetch
///!  (?movie director ?director) -- introduces new _dependent_ binding ?movie, causes lookup for each ?director result (in this case we know one)
///!  (?movie castmember ?actor) -- introduces no new bindings, only constrains result set
///!
///! Assuming there is only one director named Quentin Tarantino, we
///! know this query will do only three index lookups (one of which,
///! Canadian actors, could be large) whereas the previous version
///! would do one lookup for every Tarantino movie and one for every
///! castmember of those movies.
///!
///! It would be better not to require the user to order query clauses
///! like this, but in the absence of a more sophisticated planner it
///! at least offers some control over performance.

/// A representation of an execution plan for answering a query or
/// a part of one.  It consists of either a simple fetch or a way of
/// combining or building on a previous plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    Join(Box<Plan>, Box<Plan>),
    Fetch(Clause),
    LookupEach(Box<Plan>, Clause),
    CartesianProduct(Vec<Box<Plan>>),
    Project(Box<Plan>, Vec<Var>),
    Constrain(Box<Plan>, Vec<Constraint>)
}

impl Plan {
    pub fn outputs(&self) -> HashSet<Var> {
        use self::Plan::*;
        match self {
            &Join(ref plan_a, ref plan_b) => plan_a.outputs()
                .union(&plan_b.outputs())
                .cloned()
                .collect(),
            &Fetch(ref clause) => clause.unbound_vars().clone().into_iter().collect(),
            &LookupEach(ref plan, ref clause) => plan.outputs()
                .union(&clause.unbound_vars().clone().into_iter().collect())
                .cloned()
                .collect(),
            &CartesianProduct(ref plans) => plans
                .iter()
                .flat_map(|p| p.outputs().clone())
                .collect(),
            &Project(ref _plan, ref projection) => projection.iter().cloned().collect(),
            &Constrain(ref plan, _) => plan.outputs()
        }
    }

    pub fn for_query(q: Query) -> Plan {
        let final_relations = q.clauses.iter().fold(vec![], |relations, clause| {
            // Cases to care about:
            //
            // 1. Some unbound vars in clause match at least one relation.
            //    Do an each-lookup, by binding the clause to each element
            //    in turn of the relation matching the most fields.
            //
            // 2. No vars in clause match a relation. In this case, add a
            //    new Plan to fetch the clause and add it to the list of
            //    current relations.
            //
            // 3. All unbound vars in the clause match the same
            //    relation (at least one). The clause essentially acts
            //    as a constraint, but an each-lookup is still
            //    required.
            let (mut overlapping, mut non_overlapping): (Vec<Plan>, Vec<Plan>) = relations
                .iter()
                .cloned()
                .partition(|r| overlaps(&clause, &r));

            if overlapping.len() > 0 {
                // add clause to relation
                let prior_rel = overlapping[0].clone();
                let mut outputs: HashSet<Var> = HashSet::new();

                for output in prior_rel.outputs().iter().chain(clause.unbound_vars().iter()) {
                    outputs.insert(output.clone());
                }

                // Replace the old Plan with a new Plan that contains it as a child
                overlapping[0] = Plan::LookupEach(Box::new(prior_rel), clause.clone());

                // If there are multiple relations that overlap with the
                // clause, they can now be joined.
                non_overlapping.push(join(overlapping));
                non_overlapping
            } else {
                non_overlapping.push(
                    Plan::Fetch(clause.clone())
                );
                non_overlapping
            }
        });

        // TODO: it's fine for correctness to just apply constraints
        // at the end, but it would be better for performance to apply
        // them as soon as the bindings they require are satisfied as
        // well.
        let constrained_relations: Vec<Plan> = if q.constraints.len() > 0 {
            final_relations.into_iter().map(|r| Plan::Constrain(Box::new(r), q.constraints.clone())).collect()
        } else {
            final_relations
        };

        if constrained_relations.len() == 1 {
            Plan::Project(Box::new(constrained_relations[0].clone()), q.find)
        } else {
            Plan::Project(Box::new(Plan::CartesianProduct(constrained_relations.into_iter().map(|r| Box::new(r)).collect())), q.find)
        }
    }
}

fn overlaps(clause: &Clause, relation: &Plan) -> bool {
    let outputs = relation.outputs();
    for var in clause.unbound_vars() {
        if outputs.contains(&var) {
            return true;
        }
    }

    return false;
}


/// Given a vector of joinable relations, returns plan step
/// representing the necessary joins.
fn join(mut relations: Vec<Plan>) -> Plan {
    // Can't join zero relations
    assert!(relations.len() > 0);

    // For now, no optimization of join order -- just join at the first opportunity
    let first_rel = relations.remove(0);
    relations.into_iter().fold(
        first_rel,
        |acc_step, next_step| {
            assert!(!acc_step.outputs().is_disjoint(&next_step.outputs()));
            Plan::Join(Box::new(acc_step), Box::new(next_step))
        }
    )
}


#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::Strategy;

    use {Entity, Value, Ident};
    use queries::query::{Query, Clause, Term};
    use queries::query::Term::{Bound, Unbound};
    use queries::planner::{Plan};

    #[test]
    fn test_plan_single_clause() {
        let clause = Clause::new(Unbound("a".into()), Bound(Ident::Entity(Entity(1))), Unbound("b".into()));
        let find = vec!["a".into(), "b".into()];
        let query = Query {
            find: find.clone(),
            clauses: vec![clause.clone()],
            constraints: vec![],
        };
        let plan = Plan::for_query(query);
        assert_eq!(
            plan,
            Plan::Project(Box::new(Plan::Fetch(clause)), find)
        )
    }

    #[test]
    fn test_plan_fetch_and_lookup() {
        let clause_a = Clause::new(Unbound("a".into()), Bound(Ident::Entity(Entity(1))), Unbound("b".into()));
        let clause_b = Clause::new(Unbound("b".into()), Bound(Ident::Entity(Entity(2))), Unbound("c".into()));
        let find = vec!["a".into(), "b".into(), "c".into()];
        let query = Query {
            find: find.clone(),
            clauses: vec![clause_a.clone(), clause_b.clone()],
            constraints: vec![],
        };
        let fetch_plan = Plan::Fetch(clause_a);
        assert_eq!(
            Plan::for_query(query),
            Plan::Project(Box::new(Plan::LookupEach(Box::new(fetch_plan), clause_b)), find)
        )
    }

    prop_compose! {
        fn arb_entity_term()(entity in any::<i64>(), var in "[a-z]", is_bound in any::<bool>()) -> Term<Entity> {
            if is_bound {
                Term::Bound(Entity(entity))
            } else {
                Term::Unbound(var.into())
            }
        }
    }

    prop_compose! {
        fn arb_attribute_term()(entity in any::<i64>(), name in "[a-z]", is_bound in any::<bool>(), is_name in any::<bool>()) -> Term<Ident> {
            if is_bound {
                if is_name {
                    Term::Bound(Ident::Name(name))
                } else {
                    Term::Bound(Ident::Entity(Entity(entity)))
                }
            } else {
                Term::Unbound(name.into())
            }
        }
    }

    #[allow(dead_code)]
    fn arb_value() -> BoxedStrategy<Value> {
        prop_oneof![
            any::<i64>().prop_map(|i| Value::Ref(Entity(i))),
            any::<String>().prop_map(|s| Value::String(s))
        ].boxed()
    }

    prop_compose! {
        fn arb_value_term()(value in arb_value(), var in any::<String>(), is_bound in any::<bool>()) -> Term<Value> {
            if is_bound {
                Term::Bound(value)
            } else {
                Term::Unbound(var.into())
            }
        }
    }

    prop_compose! {
        fn arb_clause()(entity in arb_entity_term(), attr in arb_attribute_term(), value in arb_value_term()) -> Clause {
            Clause::new(entity, attr, value)
        }
    }

    fn arb_plan_step() -> BoxedStrategy<Plan> {
        let leaf = arb_clause().prop_map(|c| Plan::Fetch(c));
        leaf.prop_recursive(
            4,
            64,
            5,
            // FIXME: make sure generated plans make sense (joins + lookups are not disjoint)
            |inner| prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| Plan::Join(Box::new(a), Box::new(b))),
                (inner.clone(), arb_clause()).prop_map(|(p, c)| Plan::LookupEach(Box::new(p), c)),
                prop::collection::vec(inner.clone(), 0..5)
                    .prop_map(|v| v.into_iter().map(|item| Box::new(item)).collect())
                    .prop_map(Plan::CartesianProduct)
            ].boxed()
        ).boxed()
    }

    // proptest! {
    //      #[test]
    //      #[ignore]
    //     fn test_join_outputs_are_union_of_inputs
    //         (ref relations in prop::collection::vec(arb_plan_step(), 0..10)
    //          .prop_filter(|v| /* Vector needs to consist of suitably overlapping joins */))
    //     {
    //          println!("test run");
    //          let mut sets: Vec<HashSet<String>> = Vec::new();
    //          let test:HashSet<String> = hashset![];
    //          let outputs_of_input_relations = relations.iter().cloned()
    //              .fold(hashset![], |s, r| s.union(&r.outputs()).cloned().collect());
    //          let joined_relation = super::join(relations.to_vec());
    //          assert_eq!(outputs_of_input_relations, joined_relation.outputs())
    //      }
    //  }

    // Plan props: contains all clauses, outputs match find spec?


    #[test]
    fn test_plan_with_join() {
        // fetch, fetch, lookup, join?
        // TODO
        let clause_a = Clause::new(Unbound("a".into()), Bound(Ident::Entity(Entity(1))), Unbound("b".into()));
        let clause_b = Clause::new(Unbound("c".into()), Bound(Ident::Entity(Entity(2))), Unbound("d".into()));
        let clause_c = Clause::new(Unbound("b".into()), Bound(Ident::Entity(Entity(3))), Unbound("c".into()));
        let find = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let query = Query {
            find: find.clone(),
            clauses: vec![clause_a.clone(), clause_b.clone(), clause_c.clone()],
            constraints: vec![],
        };
        let fetch_plan_a = Plan::Fetch(clause_a);
        let fetch_plan_b = Plan::Fetch(clause_b);
        let lookup_plan = Plan::LookupEach(Box::new(fetch_plan_a), clause_c);
        assert_eq!(
            Plan::for_query(query),
            Plan::Project(Box::new(Plan::Join(Box::new(lookup_plan), Box::new(fetch_plan_b))), find)
        );
    }
}
