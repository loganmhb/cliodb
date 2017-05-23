use std::io;
use std::io::prelude::*;

extern crate logos;
use logos::parser::parse_Query;


fn main() {
    println!("Hello, world!");
    let stdin = io::stdin();
    loop {
        print!("> ");
        io::stdout().flush();

        let mut input = String::new();
        stdin.lock().read_line(&mut input).unwrap();
        println!("Input: '{}'", input);
        println!("{:?}", parse_Query(&input).unwrap());
    }
}
