# cliodb

ClioDB is a relational, non-SQL database patterned after Datomic. Key features:

1. Records are stored as immutable, append-only triples of (entity,
attribute, value), stored in covering indexes in a pluggable key-value
store backend

2. A declarative query language similar to Datalog or SPARQL

3. CQRS-style separation of reads and writes; writes go through a
single transactor process for ACID compliance [incomplete], while reads are
executed independently by the client accessing the backing store, so
that reads can be scaled independently of writes and benefit from
pervasive caching

4. Queries are executed on the client via a peer library that accesses
the backing store, and do not go through the transactor process

5. Database-as-value: because the database is immutable and
append-only, queries can be executed against a snapshot of the
database at a point in time -- either when the query began, or any
arbitrary point in the past [incomplete]

6. Transactions are reified as entities in the database, and can be
queried like any other entity [incomplete]

It is pre-alpha quality software, very much not done, and you should
not trust it with your data! But if you'd like to help make it better,
contributions are very welcome.

# Running

You will need a recent nightly version of Rust to compile the project,
In order to use the SQLite backend you also need to have SQLite
installed. Then:

    cargo build

To start a repl where you can add facts and query a SQLite-backed
database, first run the transactor:

    target/debug/clio-transactor --store cliodb:sqlite:///path/to/sqlite/file.db

Then, in a different terminal:

    target/debug/clio-cli cliodb:sqlite:///path/to/sqlite/file.db tcp://localhost:10405

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

    {name "Logan" github:username "loganmhb" project "ClioDB"}

In order to use an attribute in a fact, you must first register it in
the database. You do this by adding an entity with the `db:ident` and
`db:valueType` attributes (the `db:ident` attribute defines the
identifier, and the `db:valueType` attribute specifies which primitive
type the attribute's value can be):

    {db:ident name db:valueType db:type:string}

In the future, information about the attribute's uniqueness and
cardinality will be required as well; currently, the database does not
enforce uniqueness constraints and all attributes have an implicit
cardinality of many.

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

2. Improvments to the query language. The two biggest missing pieces
are more primitive types (e.g. numbers) and user-defined
functions. User-defined functions are not as important as they would
be in some other database systems because it's not substantially more
efficient to execute one big query than several small ones, since
they're all executed clientside and make a similar number of fetches
to the backing store, but this is critical for supporting useful
transactions. The most obvious path is to define a `db:function`
primitive type that stores Lua scripts or something like that which
the transactor can execute. Alternatively, implementing some kind of
primitive 'compare-and-set' functionality would allow you to implement
basic transactional behavior.

3. Ability to query the database as of a particular transaction or
point in time -- this just requires filtering records by transaction
time and should be pretty simple to implement

4. Improved indexing strategies: not everything should be added to the
AVET or VAET indexes, for example. These are only necessary for
supporting indexed attributes, uniqueness constraints, and reference
types. The query engine also does not use these indexes optimally, and
it should for certain queries.

5. Data partitions: Datomic assigns entities to partitions, which are
encoded in the upper bits of the entity id. This causes entities in
the same partition to get sorted together, which improves cache
availablitity and also has some nice benefits for the amount of
copying needed during reindexing, I think.

6. Cardinality/uniqueness options: it should be possible to specify
whether an attribute can have one or many values for a particular
entity, and whether an attribute must be unique.

7. Entity API -- given a database entity id, you should be able to
access its attributes hash-map style

8. Pipelined transactions -- currently transactions are processed and
committed one at a time, but if more than one transaction is in flight
at a time it should be possible to batch their writes to the backing
store together. (The transactions would still commit or rollback
individually, so the semantics would remain unchanged but performance
could improve considerably, especially if most transactions are
simple.) It might even be desirable to reorder transactions received
by the transactor in order to facilitate this batching -- for
instance, to prioritize simple transactions over ones that do
"compare-and-set" operations)

# Known issues

There are many problems, and FIXMEs littered throughout the code
base. It does not work very well!

The biggest reliability issue right now is probably that most of the
networking code doesn't handle failure cases or include timeouts; lots
of `unwrap`s will need to be replaced with an actual error handling
story. There are unsound `.unwrap()` calls in a number of places as
well, and some of the most complex code (the persistent B-tree
implementation in durable_tree.rs) is not tested as well as it should
be.