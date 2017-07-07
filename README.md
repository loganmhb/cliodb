# logos

This is a project I'm working on while I'm at the Recurse Center. The
idea is to implement a stripped-down version of a Datomic-style
database: immutable facts, queried with Datalog, with scalable
concurrent reads while writes are serialized through a transactor.

# Running

You will need a recent nightly version of Rust to compile the project,
as well as ZeroMQ 3.2 or newer. (On recent Debian or Ubuntu releases,
you should be able to install `libzmq3-dev`.) Then:

    cargo build

To start a repl where you can add facts and query a SQLite-backed
database, first run the transactor:

    target/debug/logos-transactor --uri logos:sqlite:///path/to/sqlite/file.db

Then, in a different terminal:

    target/debug/logos-cli logos:sqlite:///path/to/sqlite/file.db

Adding a fact looks like this:

     add (0 name "Logan")

`(0 name "Logan")` is a fact in `entity, attribute, value` form. To see
all the facts currently in the database, you can type `dump`.

Facts are never deleted from the database. Instead, when a fact should
no longer be true, you can issue a retraction:

    retract (0 name "Logan")
    add (0 name "Logan's new name")

In the future, this will enable querying the database *as of* some
earlier point in time, leaving an auditable trail of changes to the DB.

You can add a number of attributes about the same entity
more concisely using this dictionary-style syntax:

    {name "Logan" github:username "loganmhb" project "Logos"}

In order to use an attribute in a fact, you must first register it in
the database. You do this by adding an entity with the `db:ident`
attribute:

    {db:ident name}

In the future, information about the attribute's type and cardinality
will be required as well.

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

Help is most welcome! Let me know if you're interested and I am happy
to provide direction and assistance.

Specific features that I've thought about and have ideas on implementing are:

1. Attribute-level schemas (enforcing that e.g. `person:name` must be
a string and `person:age` must be a number)

2. Idents as shorthand for entities -- in Datomic, when you register
the `:db/ident` attribute for an entity, you can use that value as a
shorthand to refer to the entity. This allows you to define enumerated
types, among other things; for example (shown without any schema):

    # Define attributes
    {db:ident color:red}
    {db:ident color:blue}
    {db:ident person:favcolor}
    {db:ident person:name}

    # Add some facts
    {person:name "Logan" person:favcolor color:red}
    {person:name "John" person:favcolor color:blue}

3. Improvments to the query language (negation, basic comparisons;
eventually there should be a way of extending the query language with
a programming language, just as in Datomic you can use arbitrary
Clojure in your queries)

4. Ability to query the database as of a particular transaction or
point in time

# Known issues

There are many problems, and FIXMEs littered throughout the code
base. It does not work very well!

The biggest issue right now is probably that most of the networking
code doesn't handle failure cases or include timeouts; lots of
`unwrap`s will need to be replaced with an actual error handling
story.