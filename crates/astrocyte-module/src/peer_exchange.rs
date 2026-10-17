//! Framing shared by the read-only peer clients and the management driver.
//! Callers own their deadlines, authentication, routes and retry policies.

use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) async fn connect_endpoint(host: &str, port: u16) -> std::io::Result<TcpStream> {
    TcpStream::connect(format!("{host}:{port}")).await
}

/// Select reply matching and diagnostics: background fusiform/QTA clients
/// use `Peer`, and the spend management driver uses `Management`.
#[derive(Clone, Copy)]
pub enum ExchangeMode {
    /// Background peer calls accept a correlated reply on any route header.
    Peer,
    /// Management writes match the route too and warn about unknown outcomes.
    Management,
}

/// Send one request and service control Pings until its reply arrives.
pub async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: Vec<u8>,
    mode: ExchangeMode,
) -> Result<Frame, String> {
    let management = matches!(mode, ExchangeMode::Management);
    let request = Frame::build(
        FrameType::Request,
        Flags::new(
            false,
            if management {
                Priority::Interactive
            } else {
                Priority::Passive
            },
            false,
        ),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| {
        if management {
            error.to_string()
        } else {
            format!("frame build: {error:?}")
        }
    })?;
    write_frame(stream, &request)
        .await
        .map_err(|error| format!("write: {error:?}"))?;
    stream.flush().await.map_err(|error| {
        if management {
            error.to_string()
        } else {
            format!("flush: {error}")
        }
    })?;
    loop {
        let frame = read_frame(stream)
            .await
            .map_err(|error| format!("read: {error:?}"))?
            .ok_or_else(|| {
                if management {
                    "connection closed; write outcome may be unknown"
                } else {
                    "connection closed"
                }
                .to_string()
            })?;
        match frame.header.ty {
            FrameType::Ping => {
                let pong = Frame::build_with_version(
                    frame.header.ver,
                    FrameType::Pong,
                    frame.header.flags,
                    0,
                    0,
                    frame.header.corr,
                    Vec::new(),
                )
                .map_err(|error| {
                    if management {
                        error.to_string()
                    } else {
                        format!("pong build: {error:?}")
                    }
                })?;
                write_frame(stream, &pong).await.map_err(|error| {
                    if management {
                        format!("pong: {error:?}")
                    } else {
                        format!("pong write: {error:?}")
                    }
                })?;
                stream.flush().await.map_err(|error| {
                    if management {
                        error.to_string()
                    } else {
                        format!("pong flush: {error}")
                    }
                })?;
            }
            FrameType::Response | FrameType::Error
                if frame.header.corr == corr
                    && (!management
                        || (frame.header.channel == channel && frame.header.epoch == epoch)) =>
            {
                return Ok(frame)
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::HANG_GUARD;
    use tokio::io::AsyncReadExt;

    async fn assert_ipv6_connects(host: &str) {
        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::time::timeout(HANG_GUARD, async {
            let mut client = connect_endpoint(host, port).await.unwrap();
            client.write_all(&[7]).await.unwrap();
            let (mut server, _) = listener.accept().await.unwrap();
            assert_eq!(server.read_u8().await.unwrap(), 7);
        })
        .await
        .expect("IPv6 connection did not complete");
    }

    #[tokio::test]
    async fn raw_ipv6_endpoint_delivers_bytes() {
        assert_ipv6_connects("::1").await;
    }

    #[tokio::test]
    async fn bracketed_ipv6_endpoint_still_delivers_bytes() {
        assert_ipv6_connects("[::1]").await;
    }
}
