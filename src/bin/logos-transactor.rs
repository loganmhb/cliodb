extern crate logos;
extern crate clap;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;

use logos::conn::{store_from_uri, TxLocation};
use logos::tx::{Transactor, TxHandle};
//use logos::network::{LineProto, TransactorService};

use clap::{Arg, App};

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
        .set_tx_location(&TxLocation::Network(addr.clone()))
        .unwrap();

    let mut transactor = Transactor::new(store).expect("could not create transactor");
    let _tx_handle = Arc::new(Mutex::new(TxHandle::new(&transactor)));
    thread::spawn(move || transactor.run());
    unimplemented!()
    // TODO: implement on new tokio
    // let server = TcpServer::new(LineProto, addr);

    // // We provide a way to *instantiate* the service for each new
    // // connection; here, we just immediately return a new instance.
    // println!("Serving on {}", addr);
    // server.serve(move || {
    //     Ok(TransactorService {
    //         tx_handle: tx_handle.lock().unwrap().clone(),
    //     })
    // });
}
