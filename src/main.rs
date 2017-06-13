#![feature(slice_patterns)]
#![feature(conservative_impl_trait)]
extern crate logos;
extern crate rustyline;

use logos::*;
use logos::backends::KVStore;
use logos::backends::cassandra::CassandraStore;
use logos::backends::mem::HeapStore;
use logos::backends::sqlite::SqliteStore;

use std::error::Error;
use std::env::args;

fn run_uri(uri: &str) {
    match &uri.split("//").collect::<Vec<_>>()[..] {
        &["logos:mem:", _] => {
            let store = HeapStore::new();
            run(Db::new(store).unwrap());
        }
        &["logos:sqlite:", path] => {
            let store = SqliteStore::new(path).unwrap();
            run(Db::new(store).unwrap());
        }
        &["logos:cass:", url] => {
            let store = CassandraStore::new(url).unwrap();
            run(Db::new(store).unwrap());
        }
        _ => {
            println!("Invalid uri!");
            std::process::exit(1);
        }
    }
}

fn run<S: KVStore<Item = Record>>(mut db: Db<S>) {
    println!("
logos
Commands:
  quit - exit the progam;
  test - load sample data (overwrites your current DB!)
  dump - display the contents of the DB as a table.
");
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
                        match db.query(&q) {
                            Ok(res) => println!("{}", res),
                            Err(e) => println!("ERROR: {:?}", e),
                        }
                    }
                    Ok(Input::Tx(tx)) => {
                        match db.transact(tx) {
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
                            db.transact(tx).unwrap();
                        }
                    }
                    Ok(Input::Dump) => {
                        println!("{}",
                                 db.query(&parse_query("find ?ent ?att ?val where (?ent ?att \
                                                       ?val)")
                                                   .unwrap())
                                     .unwrap())
                    }
                    Err(e) => println!("Oh no! {}", e),
                };
            }
            Err(e) => println!("Error! {:?}", e.description()),
        }
    }
}

fn main() {

    let argv: Vec<_> = args().collect();
    if argv.len() != 2 {
        println!("Usage: {} <db-uri>", argv[0]);
        std::process::exit(1);
    }

    run_uri(&argv[1]);
}
