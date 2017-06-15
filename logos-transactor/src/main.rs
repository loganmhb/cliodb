extern crate logos;

extern crate clap;

use logos::db::{add_node, get_node, store_from_uri, Db, DbContents};
use logos::backends::KVStore;
use logos::{Result, Record, IdentMap};
use logos::btree::IndexNode;

use clap::{Arg, App};

fn initialize(store: &KVStore) -> Result<()> {

    let empty_root: IndexNode<Record> = IndexNode::Leaf {
        items: vec![],
    };

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
             .required(false)
        ).get_matches();

    let uri = matches.value_of("uri").unwrap();
    let store = store_from_uri(uri).expect("could not use backing store");

    if matches.is_present("create") {
        // FIXME: Make sure the store is not already initialized.
        initialize(&*store).expect("Failed to initialize store");
        let db = Db::new(store).expect("Could not connect to DB");
        println!("Created new database at uri {}", uri);
    }
}
