extern crate logos;
extern crate clap;

use logos::server::TransactorService;
use clap::{Arg, App};

fn main() {
    let matches = App::new("Logos transactor")
        .version("0.1.0")
        .arg(
            Arg::with_name("uri")
                .short("u")
                .long("uri")
                .value_name("URI")
                .help("Sets the location of the backing key-value store")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("create")
                .short("c")
                .long("create")
                .help("Indicates to create the database if it does not exist")
                .required(false),
        )
        .get_matches();

    let backing_store_uri = matches.value_of("uri").unwrap();
    // FIXME: accept as arg
    let bind_address ="tcp://127.0.0.1:10405";

    let context = zmq::Context::new();
    let server = TransactorService::new(backing_store_uri, &context).unwrap();
    server.listen(bind_address).join().unwrap();
}
