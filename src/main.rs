extern crate logos;
extern crate rustyline;

use logos::*;
// use logos::btree::HeapStore;
use logos::durable::SqliteStore;

use std::error::Error;

fn main() {
    // TODO print usage e.g. `quit` command
    println!("
logos
Commands:
  quit - exit the progam;
  test - load sample data (overwrites your current DB!)
  dump - display the contents of the DB as a table.
");
    let mut rl = rustyline::Editor::<()>::new();
    let store = SqliteStore::new("/tmp/logos.db").unwrap();
    let mut db = Db::new(store).unwrap();
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
                            Err(e) => println!("ERROR: {}", e)
                        }
                    },
                    Ok(Input::Tx(tx)) => db.transact(tx).unwrap_or_else(|e| {
                        println!("ERROR: {}", e);
                    }),
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
                                                   .unwrap()).unwrap())
                    }
                    Err(e) => println!("Oh no! {}", e),
                };
            }
            Err(e) => println!("Error! {:?}", e.description()),
        }
    }
}
