use super::*;

use queries::query::{Query, Term, Clause, Var, Constraint, Comparator};

//// Parser
use combine::char::{spaces, string, char, letter, digit};
use combine::primitives::Stream;
use combine::{Parser, ParseError, many1, between, none_of, eof};

pub enum Input {
    Query(Query),
    Tx(Tx),
    SampleDb,
    Dump,
}

enum ClauseConstraint {
    Constraint(Constraint),
    Clause(Clause),
}

pub fn parse_input<I>(input: I) -> result::Result<Input, ParseError<I>>
where
    I: combine::Stream<Item = char>,
{
    choice!(
        query_parser().map(Input::Query),
        tx_parser().map(Input::Tx),
        sample_db_parser(),
        dump_parser()
    ).parse(input)
        .map(|(r, _)| r)
}

pub fn parse_query<I>(input: I) -> result::Result<Query, ParseError<I>>
where
    I: Stream<Item = char>,
{
    query_parser().parse(input).map(|(r, _)| r)
}

pub fn parse_tx<I>(input: I) -> result::Result<Tx, ParseError<I>>
where
    I: Stream<Item = char>,
{
    tx_parser().parse(input).map(|(r, _)| r)
}

fn sample_db_parser<I>() -> impl Parser<Input = I, Output = Input>
where
    I: combine::Stream<Item = char>,
{
    lex_string("test").and(eof()).map(|_| Input::SampleDb)
}

fn dump_parser<I>() -> impl Parser<Input = I, Output = Input>
where
    I: combine::Stream<Item = char>,
{
    lex_string("dump").and(eof()).map(|_| Input::Dump)
}

fn free_var<I: combine::Stream<Item = char>>() -> impl Parser<Input = I, Output = Var> {
    char('?')
        .and(many1(letter()))
        .skip(spaces())
        .map(|x| x.1)
        .map(|name: String| Var::new(name))
}

fn comparator<I: combine::Stream<Item = char>>() -> impl Parser<Input = I, Output = Comparator> {
    string(">")
        .or(string("<"))
        .or(string("not"))
        .skip(spaces())
        .map(|s| match s {
            ">" => Comparator::GreaterThan,
            "<" => Comparator::LessThan,
            _ => Comparator::NotEqualTo,
        })
}

fn number_lit<I: combine::Stream<Item = char>>() -> impl Parser<Input = I, Output = Entity> {
    many1(digit()).map(|n: String| Entity(n.parse().unwrap()))
}


fn string_lit<I: combine::Stream<Item = char>>() -> impl Parser<Input = I, Output = Value> {
    between(char('"'), char('"'), many1(none_of(vec!['\"']))).map(|s| Value::String(s))
}

fn ident<I: combine::Stream<Item = char>>() -> impl Parser<Input = I, Output = String> {
    many1(letter().or(char(':'))).skip(spaces())
}

fn query_parser<I>() -> impl Parser<Input = I, Output = Query>
where
    I: combine::Stream<Item = char>,
{
    // FIXME: Number literals should be able to be entities or just
    // integers; this probably requires a change to the types/maybe
    // change to the unification system, or a specific syntax like $0
    // for entity ids that allows the parser to distinguish them.

    let entity = number_lit;
    let value = || {
        string_lit().or(number_lit().map(|e| Value::Ref(e))).or(
            ident().map(|i| Value::Ident(i)),
        )
    };

    // There is probably a way to DRY these out but I couldn't satisfy the type checker.
    let comparator_term = comparator().skip(spaces());
    let entity_term = free_var()
        .map(|x| Term::Unbound(x))
        .or(entity().map(|x| Term::Bound(x)))
        .skip(spaces());
    let ident_term = free_var()
        .map(|x| Term::Unbound(x))
        .or(ident().map(|x| Term::Bound(Ident::Name(x))))
        .skip(spaces());
    let value_term = || {
        free_var()
            .map(|x| Term::Unbound(x))
            .or(value().map(|x| Term::Bound(x)))
            .skip(spaces())
    };

    // Clause structure
    let constraint_metadata = (comparator_term, value_term(), value_term()).map(|(c, fst, snd)| {
        ClauseConstraint::Constraint(Constraint {
            comparator: c,
            left_hand_side: fst,
            right_hand_side: snd,
        })
    });
    let clause_metadata = (entity_term, ident_term, value_term()).map(|(e, a, v)| {
        ClauseConstraint::Clause(Clause::new(e, a, v))
    });
    let constraint_clause = between(
        lex_char('('),
        lex_char(')'),
        constraint_metadata.or(clause_metadata),
    );

    let find_spec = lex_string("find").and(many1(free_var())).map(|x| x.1);
    let where_spec = lex_string("where").and(many1(constraint_clause)).map(
        |(_, clause_constraint_vec): (_, Vec<ClauseConstraint>)| {
            let mut constraints = Vec::new();
            let mut clauses = Vec::new();

            for cc in clause_constraint_vec {
                match cc {
                    ClauseConstraint::Clause(c) => clauses.push(c),
                    ClauseConstraint::Constraint(x) => constraints.push(x),
                }
            }

            (clauses, constraints)
        },
    );

    find_spec.and(where_spec)
        // FIXME: add find vars
        .map(|(find, (clauses, constraints))| Query {
            find: find,
            clauses: clauses,
            constraints: constraints
        })
        .and(eof())
        .map(|x| x.0)
}

fn lex_string<I>(s: &'static str) -> impl Parser<Input = I>
where
    I: Stream<Item = char>,
{
    string(s).skip(spaces())
}

fn lex_char<I>(c: char) -> impl Parser<Input = I>
where
    I: Stream<Item = char>,
{
    char(c).skip(spaces())
}

fn tx_parser<I>() -> impl Parser<Input = I, Output = Tx>
where
    I: combine::Stream<Item = char>,
{
    let entity = || number_lit().skip(spaces());
    let value = || {
        string_lit()
            .or(number_lit().map(|e| Value::Ref(e)))
            .or(ident().map(|i| Value::Ident(i)))
            .skip(spaces())
    };

    let fact = || {
        between(lex_char('('), lex_char(')'), (entity(), ident(), value()))
            .map(|f| Fact::new(f.0, f.1, f.2))
    };

    let attr_pair = || (ident(), value());
    let new_entity = || {
        between(
            lex_char('{'),
            lex_char('}'),
            many1::<HashMap<_, _>, _>(attr_pair()),
        ).map(|x| TxItem::NewEntity(x))
    };

    let addition = || {
        lex_string("add")
            .and(fact().map(|i| TxItem::Addition(i)))
            .map(|x| x.1)
    };
    let retraction = || {
        lex_string("retract")
            .and(fact().map(|i| TxItem::Retraction(i)))
            .map(|x| x.1)
    };

    let tx_item = || choice!(addition(), retraction(), new_entity());

    many1::<Vec<_>, _>(tx_item())
        .map(|tx| Tx { items: tx })
        .and(eof())
        .map(|x| x.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query() {
        assert_eq!(
            parse_query("find ?a where (?a name \"Bob\") (> ?age 50) (?a age ?age)").unwrap(),
            Query {
                find: vec![Var::new("a")],
                clauses: vec![
                    Clause::new(
                        Term::Unbound("a".into()),
                        Term::Bound(Ident::Name("name".into())),
                        Term::Bound(Value::String("Bob".into()))
                    ),
                    Clause::new(
                        Term::Unbound("a".into()),
                        Term::Bound(Ident::Name("age".into())),
                        Term::Unbound("age".into())
                    ),
                ],
                constraints: vec![
                    Constraint {
                        comparator: Comparator::GreaterThan,
                        left_hand_side: Term::Unbound("age".into()),
                        right_hand_side: Term::Bound(Value::Ref(Entity(50))),
                    },
                ],
            }
        )
    }

    #[test]
    fn test_parse_tx() {
        assert_eq!(
            parse_tx("add (0 name \"Bob\")").unwrap(),
            Tx {
                items: vec![
                    TxItem::Addition(
                        Fact::new(Entity(0), "name", Value::String("Bob".into()))
                    ),
                ],
            }
        );
        parse_tx("{name \"Bob\" batch \"S1'17\"}").unwrap();
    }

    #[test]
    fn test_parsing_idents() {
        let q = Query {
            find: vec![Var::new("p")],
            clauses: vec![
                Clause::new(
                    Term::Unbound("p".into()),
                    Term::Bound(Ident::Name("country".into())),
                    Term::Bound(Value::Ident("country:US".into()))
                ),
            ],
            constraints: vec![],
        };

        assert_eq!(
            parse_query("find ?p where (?p country country:US)").unwrap(),
            q
        );
    }
}
