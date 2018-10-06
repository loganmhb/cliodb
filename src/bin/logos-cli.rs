#![feature(duration_as_u128)]
extern crate logos;
extern crate rustyline;

use logos::*;
use logos::conn::{Conn, store_from_uri};
use std::time::{Instant};

use std::error::Error;
use std::env::args;

fn run(uri: &str) {
    println!(
        "
logos
Commands:
  quit - exit the progam;
  test - load sample data (overwrites your current DB!)
  dump - display the contents of the DB as a table.
"
    );
    let store = store_from_uri(uri).expect("Couldn't create store");
    let mut conn = Conn::new(store.clone()).expect("Couldn't connect to DB -- does it exist?");
    let mut rl = rustyline::Editor::<()>::new();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                if line == "quit" {
                    break;
                }
                rl.add_history_entry(&line);

                match parse_input(&*line) {
                    Ok(Input::Query(q)) => {
                        let start = Instant::now();
                        let db = conn.db().unwrap();
                        let db_fetched_at = Instant::now();
                        let db_fetch_time = db_fetched_at.duration_since(start);
                        match query(q, &db) {
                            Ok(res) => {
                                let end = Instant::now();
                                let total_time = end.duration_since(start);
                                let query_time = end.duration_since(db_fetched_at);
                                println!("{}", res);
                                println!("Query executed in {} ms ({} to fetch db, {} to execute query)", total_time.as_millis(), db_fetch_time.as_millis(), query_time.as_millis());
                            },
                            Err(e) => println!("ERROR: {:?}", e),
                        }
                    }
                    Ok(Input::Tx(tx)) => {
                        match conn.transact(tx) {
                            Ok(report) => println!("{:?}", report),
                            Err(e) => println!("ERROR: {:?}", e),
                        }
                    }
                    Ok(Input::SampleDb) => {
                        let sample = [
                            r#"{db:ident name} {db:ident parent}"#,
                            // FIXME: Don't hardcode entities; need a way to get the entity id of a tx
                            // (tempid system?)
                            r#"add (0 name "Bob")"#,
                            r#"add (1 name "John")"#,
                            r#"add (0 parent 1)"#,
                            r#"add (2 name "Hello")"#,
                        ];

                        for tx in sample.into_iter().map(|l| parse_tx(*l).unwrap()) {
                            conn.transact(tx).unwrap();
                        }
                    }
                    Ok(Input::Dump) => {
                        println!(
                            "{}",
                            query(
                                parse_query(
                                    "find ?ent ?attname ?val where (?ent ?att \
                                     ?val) (?att db:ident ?attname)",
                                ).unwrap(),
                                &conn.db().unwrap()
                            ).unwrap()
                        )
                    }
                    Err(e) => println!("Oh no! {}", e),
                };
            }
            Err(e) => {
                if let rustyline::error::ReadlineError::Eof = e {
                    break;
                }
                println!("Error! {:?}", e.description())
            }
        }
    }
}

fn main() {

    let argv: Vec<_> = args().collect();
    if argv.len() != 2 {
        println!("Usage: {} <db-uri>", argv[0]);
        std::process::exit(1);
    }

    run(&argv[1]);
}
