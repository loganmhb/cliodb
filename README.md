# logos

This is a project I'm working on while I'm at the Recurse Center. The
idea is to implement a stripped-down version of a Datomic-style
database: immutable facts, queried with Datalog, with separation of
reads and writes.

# Running

You will need a recent nightly version of Rust to compile the project.

    cargo run

This starts a repl where you can add facts and query the database. Adding a fact looks like this:

     add (0 name "Logan")

`(0 name "Logan")` is a fact in entity, attribute, value form. To see
all the facts currently in the database, you can type `dump`.

Queries look like this:

    find ?entity where (?entity name "Logan")

There can be any number of clauses after the `where`, e.g.:

    find ?name where (?person name "Bob) (?person parent ?child) (?child name ?name)

Symbols starting with a question mark are free logic variables, and
the query is executed by trying to unify the variables in all the
clauses. So the above query is asking, "What is the name of the child
of the person named "Bob"?

Currently there is no type checking of attributes, and values can only
be strings or references to other entities, but I hope to extend the
query language soon to support more primitive types and more
sophisticated relationships.

# Contributing

Help is most welcome! Let me know if you're interested.