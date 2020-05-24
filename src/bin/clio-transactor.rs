extern crate cliodb;
extern crate clap;
extern crate log;
extern crate env_logger;

use std::process;
use log::error;

use cliodb::server::TransactorService;
use clap::{Arg, App};

fn main() {
    env_logger::init();
    let matches = App::new("ClioDB transactor")
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
    server.listen(bind_address).unwrap_or_else(|e| {
        error!("Failed to start server: {:?}", e);
        process::exit(1);
    }).join();
}
