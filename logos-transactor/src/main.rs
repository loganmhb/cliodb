extern crate logos;
extern crate zmq;
extern crate clap;
extern crate serde;
extern crate rmp_serde;
extern crate chrono;

use logos::db::{add_node, get_node, store_from_uri, Db, DbContents};
use logos::backends::KVStore;
use logos::{Result, Record, IdentMap, Tx, TxItem, TxReport, Entity, Value};
use logos::btree::IndexNode;
use chrono::prelude::{DateTime, UTC};

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};
use clap::{Arg, App};

fn process_tx(tx: Tx, db: &mut Db) -> Result<TxReport> {
    let mut new_entities = vec![];
    let tx_entity = Entity(db.get_id());
    let attr = db.idents.get_entity("db:txInstant".to_string()).unwrap();
    db.add(Record::addition(tx_entity, attr, Value::Timestamp(UTC::now()), tx_entity));
    for item in tx.items {
        match item {
            TxItem::Addition(f) => {
                let attr = db.idents
                    .get_entity(f.attribute)
                    .ok_or("invalid attribute".to_string())?;
                db.add(Record::addition(f.entity, attr, f.value, tx_entity))
            }
            TxItem::NewEntity(ht) => {
                let entity = Entity(db.get_id());
                for (k, v) in ht {
                    let attr = db.idents
                        .get_entity(k)
                        .ok_or("invalid attribute".to_string())?;
                    db.add(Record::addition(entity, attr, v, tx_entity))
                }
                new_entities.push(entity);
            }
            TxItem::Retraction(f) => {
                let attr = db.idents
                    .get_entity(f.attribute)
                    .ok_or("invalid attribute".to_string())?;
                db.add(Record::retraction(f.entity, attr, f.value, tx_entity))
            }
        }
    }
    db.save_contents()?;
    Ok(TxReport { new_entities })
}

fn initialize(store: &KVStore) -> Result<()> {

    let empty_root: IndexNode<Record> = IndexNode::Leaf { items: vec![] };

    let eav_root = add_node(store, empty_root.clone())?;
    let aev_root = add_node(store, empty_root.clone())?;
    let ave_root = add_node(store, empty_root.clone())?;

    let contents = DbContents {
        next_id: 0,
        idents: IdentMap::default(),
        eav: eav_root,
        ave: ave_root,
        aev: aev_root,
    };

    store.set_contents(&contents)?;
    Ok(())
}

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

    if matches.is_present("create") {
        // FIXME: Make sure the store is not already initialized.
        initialize(&*store).expect("Failed to initialize store");
        println!("Created new database at uri {}", uri);
    }

    let mut db = Db::new(store).expect("Could not connect to DB");
    let ctx = zmq::Context::new();
    let mut socket = ctx.socket(zmq::REP).unwrap();
    socket.bind("tcp://127.0.0.1:10405").unwrap();
    println!("Listening on port 10405...");
    loop {
        let msg = socket.recv_msg(0).unwrap();
        let mut de = Deserializer::new(&msg[..]);
        let tx: Tx = Deserialize::deserialize(&mut de).unwrap();
        println!("Message: {:?}", tx);
        let report = process_tx(tx, &mut db).unwrap();

        let mut msg_buf = Vec::new();
        report.serialize(&mut Serializer::new(&mut msg_buf)).unwrap();
        socket.send_msg(zmq::Message::from_slice(&msg_buf[..]).unwrap(), 0).unwrap()
    }
}
