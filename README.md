# logos

This is a project I'm working on while I'm at the Recurse Center. The
idea is to implement a stripped-down version of a Datomic-style
database: immutable facts, queried with Datalog, with scalable
concurrent reads while writes are serialized through a transactor.

# Running

You will need a recent nightly version of Rust to compile the project,
In order to use the SQLite backend you also need SQLite
(`sqlite-devel`, often). Then:

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

You can simultaneously create a new entity and add a number of
attributes about it using this dictionary-style syntax:

    {name "Logan" github:username "loganmhb" project "Logos"}

In order to use an attribute in a fact, you must first register it in
the database. You do this by adding an entity with the `db:ident` and
`db:valueType` attributes (the `db:ident` attribute defines the
identifier, and the `db:valueType` attribute specifies which primitive
type the attribute's value can be):

    {db:ident name db:valueType db:type:string}

In the future, information about the attribute's uniqueness and cardinality
will be required as well.

Queries look like this:

    find ?entity where (?entity name "Logan")

There can be any number of clauses after the `where`, e.g.:

    find ?name where (?person name "Bob) (?person parent ?child) (?child name ?name)

Symbols starting with a question mark are free logic variables, and
the query is executed by trying to unify the variables in all the
clauses. So the above query is asking, "What is the name of the child
of the person named "Bob"?

Currently values can only be strings, timestamps, identifiers or
references to other entities, but I hope to extend the query language
soon to support more primitive types and more sophisticated
relationships.

# Contributing

Help is most welcome! Let me know if you're interested and I am happy
to provide direction and assistance.

Specific features that I've thought about and have ideas on implementing are:

1. Idents as shorthand for entities -- in Datomic, when you register
the `:db/ident` attribute for an entity, you can use that value as a
shorthand to refer to the entity. This allows you to define enumerated
types, among other things; for example (shown without any schema):

```
# Define attributes
{db:ident color:red}
{db:ident color:blue}
{db:ident person:favcolor}
{db:ident person:name}

# Add some facts
{person:name "Logan" person:favcolor color:red}
{person:name "John" person:favcolor color:blue}
```

Something similar is possible now by relying on identifiers rather
than entities as the enumerated types, but without interning the
identifiers as entities this is much less efficient and provides less
security -- adding a reference to a non-existent entity would fail,
whereas a typo in a non-interned ident would just give you a new,
different ident.

2. Improvments to the query language (negation, basic comparisons;
eventually there should be a way of extending the query language with
a programming language, just as in Datomic you can use arbitrary
Clojure in your queries)

3. Ability to query the database as of a particular transaction or
point in time

4. More efficient reindexing: currently, when enough new data
accumulates in-memory in the transactor, it stops everything and
builds a totally new index. Both of these are unnecessary; it should
instead construct an index in the background (might require throttling
transactions if background indexing can't keep up with new
transactions) and the index should re-use any segments that don't need
to change. (If you're importing sorted data, the whole index could get
reused for at least the EAVT index.)

5. Data partitions: Datomic assigns entities to partitions, which are
encoded in the upper bits of the entity id. This causes entities in
the same partition to get sorted together, which improves cache
availablitity and also has some nice benefits for the amount of
copying needed during reindexing, I think.

6. Cardinality/uniqueness options: it should be possible to specify
whether an attribute can have one or many values for a particular
entity, and whether an attribute must be unique.

# Known issues

There are many problems, and FIXMEs littered throughout the code
base. It does not work very well!

The biggest reliability issue right now is probably that most of the
networking code doesn't handle failure cases or include timeouts; lots
of `unwrap`s will need to be replaced with an actual error handling
story. "Background" reindexing also does not occur in the background,
so when it happens one unlucky transaction will take a long time to
complete.