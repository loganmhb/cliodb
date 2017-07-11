use super::*;

use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::tx::Transactor;

use bytes::{BufMut, BytesMut};

use futures::{future, Future, BoxFuture};

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_proto::pipeline::{ClientProto, ServerProto};
use tokio_service::Service;

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};

///! This module abstracts away the network encoding between
///! the clients and the transactor. For now, we use json-encoding
///! of our transactions and reports and line based frames.

pub struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = TxReport;
    type Error = io::Error;

    // FIXME: we're using newlines to split messages, but the soundness of this
    // depends completely on the encoding used!
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if let Some(i) = buf.iter().position(|&b| b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.split_to(i);

            // Also remove the '\n'
            buf.split_to(1);

            let mut de = Deserializer::new(&line[..]);

            match Deserialize::deserialize(&mut de) {
                Err(..) => Err(io::Error::new(io::ErrorKind::Other,
                                             "failed decoding tx-report")),
                Ok(tx) => Ok(Some(tx)),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder for ClientCodec {
    type Item = Tx;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        let mut writer = BufMut::writer(buf);
        msg.serialize(&mut Serializer::new(&mut writer)).map_err(|_| io::Error::new(io::ErrorKind::Other,
                                             "encode of tx failed"))?;
        writer.write_all(b"\n")
    }
}


pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Tx;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if let Some(i) = buf.iter().position(|&b| b == b'\n') {
            let line = buf.split_to(i);
            buf.split_to(1);

            let mut de = Deserializer::new(&line[..]);

            match Deserialize::deserialize(&mut de) {
                Err(..) => Err(io::Error::new(io::ErrorKind::Other,
                                             "failed decoding tx")),
                Ok(tx) => Ok(Some(tx)),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder for ServerCodec {
    type Item = TxReport;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        let mut writer = BufMut::writer(buf);
        msg.serialize(&mut Serializer::new(&mut writer)).map_err(|_| io::Error::new(io::ErrorKind::Other,
                                             "encode of tx-report failed"))?;
        writer.write_all(b"\n")
    }
}


pub struct LineProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for LineProto {
    /// For this protocol style, `Request` matches the `Item` type of the codec's `Encoder`
    type Request = Tx;

    /// For this protocol style, `Response` matches the `Item` type of the codec's `Decoder`
    type Response = TxReport;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = io::Result<Self::Transport>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec))
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for LineProto {
    type Request = Tx;

    type Response = TxReport;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, ClientCodec>;
    type BindTransport = io::Result<Self::Transport>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ClientCodec))
    }
}


pub struct TransactorService {
    pub mutex: Arc<Mutex<Transactor>>,
}

impl Service for TransactorService {
    // These types must match the corresponding protocol types:
    type Request = Tx;
    type Response = TxReport;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = BoxFuture<Self::Response, Self::Error>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        let mut transactor = self.mutex.lock().unwrap();
        let report = transactor.process_tx(req).map_err(|_| io::Error::new(io::ErrorKind::Other,
                                             "transactor failed"));

        println!("Received tx! Report: {:?}", &report);

        future::done(report).boxed()
    }
}
