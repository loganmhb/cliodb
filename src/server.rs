use std::thread;

use zmq;
use rmp_serde;
use log::{info, error};

use {Result, Tx};
use conn::store_from_uri;
use tx::{TxHandle, Transactor};

/// Run a 0MQ-based server to accept transaction requests and process
/// them. Because it uses 0MQ sockets to abstract over the transport
/// medium, it can be used for both in-process and networked
/// transactors by providing an appropriate 0MQ bind address.

pub struct TransactorService {
    tx_handle: TxHandle,
    context: zmq::Context,
    tx_join_handle: thread::JoinHandle<Result<()>>,
}

impl TransactorService {
    pub fn new(store_uri: &str, context: &zmq::Context) -> Result<TransactorService> {
        let kvstore = store_from_uri(store_uri)?;

        let mut transactor = Transactor::new(kvstore)?;
        let tx_handle = TxHandle::new(&transactor);

        let join_handle = thread::spawn(move || transactor.run());

        Ok(TransactorService { tx_handle, context: context.clone(), tx_join_handle: join_handle })
    }

    pub fn listen(&self, bind_address: &str) -> Result<thread::JoinHandle<()>> {
        let tx_handle = self.tx_handle.clone();
        let context = self.context.clone();
        let addr = bind_address.to_string();
        let socket = context.socket(zmq::REP)?;
        socket.bind(&addr)?;
        info!("Listening on {}", addr);

        Ok(thread::spawn(move || {
            // TODO: support multiple simultaneous transactions using zmq::ROUTER socket
            // or an asynchronous transaction mechanism
            // FIXME: less unwrapping!
            loop {
                let msg = match socket.recv_bytes(0) {
                    Ok(msg) => msg,
                    Err(zmq::Error::ETERM) => {
                        break;
                    },
                    Err(e) => {
                        error!("unexpected error recving bytes: {}", e);
                        break;
                    }
                };
                let tx_request: Tx = rmp_serde::from_read_ref(&msg).unwrap();
                let result = tx_handle.transact(tx_request).unwrap();
                socket.send(rmp_serde::to_vec(&result).unwrap(), 0).unwrap();
            }
        }))
    }

    pub fn close(self) {
        self.tx_handle.close().unwrap();
        self.tx_join_handle.join().unwrap().unwrap();
    }
}
