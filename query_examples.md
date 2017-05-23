possible styles:

- classic datalog:

    query(A, B) :- name(A, bob), ancestor(A, B).
    query(A)?

- modified datomic:

    -- Find all people named "bob" who are an ancestor of someone with the given name
    find ?a ?b
    in $db ?name
    where name(?a, "bob"),
          ancestor(?a, ?b),
          name(?b, ?name);

- full datomic:

    [:find ?a ?b :where [?a :name "bob"] [?a :ancestor ?b] [?b :name "john"]]