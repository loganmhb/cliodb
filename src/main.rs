use std::io;
use std::io::prelude::*;

extern crate logos;
extern crate rustyline;

use logos::*;
use std::error::Error;

fn main() {
    // TODO print usage e.g. `quit` command
    println!("Hello, world!");
    let mut rl = rustyline::Editor::<()>::new();
    let mut db = InMemoryLog::new();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                if line == "quit" {
                    break;
                }

                match parse_input(&*line) {
                    Ok(Input::Query(q)) => println!("{:#?}", db.query(q)),
                    Ok(Input::Tx(tx)) => db.transact(tx),
                    Err(e) => println!("Oh no! {}", e)
                };
            },
            Err(e) => println!("Error! {:?}", e.description())
        }
        let mut input = String::new();
    }
}
