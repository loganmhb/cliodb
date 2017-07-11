use super::*;

use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use super::tx::Transactor;

use bytes::{Buf, BufMut, BytesMut, BigEndian};

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

fn serialize<S: Serialize>(msg: S, buf: &mut BytesMut) -> io::Result<()> {
    let mut debug_buf = Vec::new();
    msg.serialize(&mut Serializer::new(&mut debug_buf)).unwrap();
    println!("Serialized:\n{:?}", debug_buf);

    // TODO: we're doing serialization twice. we can just serialize onto
    // the buffer directly (offset by 4 bytes) and then prefix it by its
    // length
    let serialized_len = debug_buf.len() as u32;
    buf.put_u32::<BigEndian>(serialized_len);

    let mut writer = BufMut::writer(buf);
    Ok(msg.serialize(&mut Serializer::new(&mut writer)).map_err(
        |_| {
            io::Error::new(io::ErrorKind::Other, "encode of tx failed")
        },
    )?)
}

fn deserialize<D: Deserialize<'static>>(buf: &mut BytesMut) -> io::Result<Option<D>> {
    let (msg_len, result) = {
        let view = buf.as_ref();
        if view.len() < 4 {
            return Ok(None);
        }
        let msg_len = Cursor::new(view).get_u32::<BigEndian>();
        if (view.len() as u32) < 4 + msg_len {
            return Ok(None);
        }

        let mut de = Deserializer::new(&view[4..(4 + msg_len as usize)]);

        (
            msg_len,
            match Deserialize::deserialize(&mut de) {
                Err(..) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed decoding tx-report",
                )),
                Ok(tx) => Ok(Some(tx)),
            },
        )
    };

    buf.split_to(4 + msg_len as usize);

    result
}


pub struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Result<TxReport>;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        deserialize(buf)
    }
}

impl Encoder for ClientCodec {
    type Item = Tx;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        serialize(msg, buf)
    }
}


pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Tx;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        deserialize(buf)
    }
}

impl Encoder for ServerCodec {
    type Item = Result<TxReport>;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        serialize(msg, buf)
    }
}


pub struct LineProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for LineProto {
    /// For this protocol style, `Request` matches the `Item` type of the codec's `Encoder`
    type Request = Tx;

    /// For this protocol style, `Response` matches the `Item` type of the codec's `Decoder`
    type Response = Result<TxReport>;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = io::Result<Self::Transport>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec))
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for LineProto {
    type Request = Tx;

    type Response = Result<TxReport>;

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
    type Response = Result<TxReport>;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = BoxFuture<Self::Response, Self::Error>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        let mut transactor = self.mutex.lock().unwrap();
        let report = transactor.process_tx(req);

        println!("Transacted tx! Report: {:?}", &report);

        future::ok(report).boxed()
    }
}
