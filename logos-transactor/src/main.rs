extern crate logos;
extern crate zmq;
extern crate clap;
extern crate serde;
extern crate rmp_serde;
extern crate chrono;

use std::net::SocketAddr;
use std::str::FromStr;

use logos::db::{store_from_uri, TxClient};
use logos::Tx;
use logos::tx::Transactor;

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};
use clap::{Arg, App};

fn main() {
    let matches = App::new("Logos transactor")
        .version("0.1.0")
        .arg(Arg::with_name("uri")
                 .short("u")
                 .long("uri")
                 .value_name("URI")
                 .help("Sets the location of the database")
                 .required(true)
                 .takes_value(true))
        .arg(Arg::with_name("create")
                 .short("c")
                 .long("create")
                 .help("Indicates to create the database if it does not exist")
                 .required(false))
        .get_matches();

    let uri = matches.value_of("uri").unwrap();
    let store = store_from_uri(uri).expect("could not use backing store");
    let addr = SocketAddr::from_str("127.0.0.1:10405").unwrap();
    store.set_transactor(&TxClient::Network(addr)).unwrap();
    let mut transactor = Transactor::new(store.clone()).expect("could not create transactor");

    if matches.is_present("create") {
        // FIXME: Make sure the store is not already initialized.
        logos::tx::create_db(store).expect("Failed to initialize store");
        println!("Created new database at uri {}", uri);
    }

    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP).unwrap();
    socket.bind("tcp://127.0.0.1:10405").unwrap();
    println!("Listening on port 10405...");
    loop {
        let msg = socket.recv_msg(0).unwrap();
        let mut de = Deserializer::new(&msg[..]);
        let tx: Tx = Deserialize::deserialize(&mut de).unwrap();
        println!("Message: {:?}", tx);
        let report = transactor.process_tx(tx).unwrap();

        let mut msg_buf = Vec::new();
        report.serialize(&mut Serializer::new(&mut msg_buf)).unwrap();
        socket.send_msg(zmq::Message::from_slice(&msg_buf[..]).unwrap(), 0).unwrap()
    }
}
