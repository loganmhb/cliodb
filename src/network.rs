use super::*;

use std::io::{self, Cursor};

use bytes::{Buf, BufMut, BytesMut, BigEndian};

use futures::{future, Future, BoxFuture};

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_proto::pipeline::{ClientProto, ServerProto};
use tokio_service::Service;

use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};

///! This module takes care of the network implementation details for
///! communication between the clients and the transactor.

fn serialize<S: Serialize>(msg: S, buf: &mut BytesMut) -> io::Result<()> {
    let mut debug_buf = Vec::new();
    msg.serialize(&mut Serializer::new(&mut debug_buf))
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "encode failed"))?;

    let serialized_len = debug_buf.len() as u32;
    buf.put_u32::<BigEndian>(serialized_len);

    // We have to copy the serialized bytes into the buffer instead of
    // writing to the buffer immediately because the serialized stream
    // is prefixed by its length, and we don't know that until after
    // serialization. BytesMut is append only, so we cannot change the
    // prefix after we've written the serialized object.
    buf.put_slice(&debug_buf[..]);

    Ok(())
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
        let serialization_result = match Deserialize::deserialize(&mut de) {
            Err(..) => Err(io::Error::new(io::ErrorKind::Other, "decode failed")),
            Ok(tx) => Ok(Some(tx)),
        };

        (msg_len, serialization_result)
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
    pub tx_handle: tx::TxHandle,
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
        let report = self.tx_handle.transact(req);
        future::ok(report).boxed()
    }
}
