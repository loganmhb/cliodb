extern crate logos;
extern crate clap;
extern crate serde;
extern crate rmp_serde;
extern crate chrono;
extern crate tokio_proto;

use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use std::str::FromStr;

use logos::conn::{store_from_uri, TxClient};
use logos::tx::Transactor;
use logos::network::{LineProto, TransactorService};

use clap::{Arg, App};
use tokio_proto::TcpServer;

fn main() {
    let matches = App::new("Logos transactor")
        .version("0.1.0")
        .arg(
            Arg::with_name("uri")
                .short("u")
                .long("uri")
                .value_name("URI")
                .help("Sets the location of the database")
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

    let uri = matches.value_of("uri").unwrap();
    let store = store_from_uri(uri).expect("could not use backing store");
    let addr = SocketAddr::from_str("127.0.0.1:10405").unwrap();
    store
        .set_transactor(&TxClient::Network(addr.clone()))
        .unwrap();

    let transactor = Transactor::new(store).expect("could not create transactor");
    let server = TcpServer::new(LineProto, addr);
    let mutex = Arc::new(Mutex::new(transactor));

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    println!("Serving on {}", addr);
    server.serve(move || Ok(TransactorService { mutex: mutex.clone() }));
}
