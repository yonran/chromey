use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::ready;

use futures::stream::Stream;
use futures::task::{Context, Poll};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{tungstenite::protocol::WebSocketConfig, WebSocketStream};

use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{CallId, EventMessage, Message, MethodCall, MethodId};

use crate::error::CdpError;
use crate::error::Result;

type ConnectStream = MaybeTlsStream<tokio::net::TcpStream>;

/// Exchanges the messages with the websocket
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
pub struct Connection<T: EventMessage> {
    /// Queue of commands to send.
    pending_commands: VecDeque<MethodCall>,
    /// The websocket of the chromium instance
    ws: WebSocketStream<ConnectStream>,
    /// The identifier for a specific command
    next_id: usize,
    /// A flush is required.
    needs_flush: bool,
    /// The message that is currently being proceessed
    pending_flush: Option<MethodCall>,
    /// The phantom marker.
    _marker: PhantomData<T>,
}

lazy_static::lazy_static! {
    /// Nagle's algorithm disabled?
    static ref DISABLE_NAGLE: bool = match std::env::var("DISABLE_NAGLE") {
        Ok(disable_nagle) => disable_nagle == "true",
        _ => true
    };
    /// Websocket config defaults
    static ref WEBSOCKET_DEFAULTS: bool = match std::env::var("WEBSOCKET_DEFAULTS") {
        Ok(d) => d == "true",
        _ => false
    };
}

impl<T: EventMessage + Unpin> Connection<T> {
    pub async fn connect(debug_ws_url: impl AsRef<str>) -> Result<Self> {
        let mut config = WebSocketConfig::default();

        if *WEBSOCKET_DEFAULTS == false {
            config.max_message_size = None;
            config.max_frame_size = None;
        }

        let (ws, _) = tokio_tungstenite::connect_async_with_config(
            debug_ws_url.as_ref(),
            Some(config),
            *DISABLE_NAGLE,
        )
        .await?;

        Ok(Self {
            pending_commands: Default::default(),
            ws,
            next_id: 0,
            needs_flush: false,
            pending_flush: None,
            _marker: Default::default(),
        })
    }
}

impl<T: EventMessage> Connection<T> {
    fn next_call_id(&mut self) -> CallId {
        let id = CallId::new(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Queue in the command to send over the socket and return the id for this
    /// command
    pub fn submit_command(
        &mut self,
        method: MethodId,
        session_id: Option<SessionId>,
        params: serde_json::Value,
    ) -> serde_json::Result<CallId> {
        let id = self.next_call_id();
        let call = MethodCall {
            id,
            method,
            session_id: session_id.map(Into::into),
            params,
        };
        self.pending_commands.push_back(call);
        Ok(id)
    }

    /// flush any processed message and start sending the next over the conn
    /// sink
    fn start_send_next(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if self.needs_flush {
            if let Poll::Ready(Ok(())) = self.ws.poll_flush_unpin(cx) {
                self.needs_flush = false;
            }
        }
        if self.pending_flush.is_none() && !self.needs_flush {
            if let Some(cmd) = self.pending_commands.pop_front() {
                tracing::trace!("Sending {:?}", cmd);
                let msg = serde_json::to_string(&cmd)?;
                self.ws.start_send_unpin(msg.into())?;
                self.pending_flush = Some(cmd);
            }
        }
        Ok(())
    }
}

impl<T: EventMessage + Unpin> Stream for Connection<T> {
    type Item = Result<Box<Message<T>>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        // flush pending outgoing messages
        loop {
            if let Err(err) = pin.start_send_next(cx) {
                return Poll::Ready(Some(Err(err)));
            }

            if let Some(call) = pin.pending_flush.take() {
                if pin.ws.poll_ready_unpin(cx).is_ready() {
                    pin.needs_flush = true;
                    // try another flush in this same poll
                    continue;
                } else {
                    pin.pending_flush = Some(call);
                }
            }

            break;
        }

        // read from the websocket
        match ready!(pin.ws.poll_next_unpin(cx)) {
            Some(Ok(WsMessage::Text(text))) => {
                match decode_message::<T>(text.as_bytes(), Some(&text)) {
                    Ok(msg) => Poll::Ready(Some(Ok(msg))),
                    Err(err) => {
                        tracing::debug!(
                            target: "chromiumoxide::conn::raw_ws::parse_errors",
                            "Dropping malformed text WS frame: {err}",
                        );
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Some(Ok(WsMessage::Binary(buf))) => match decode_message::<T>(&buf, None) {
                Ok(msg) => Poll::Ready(Some(Ok(msg))),
                Err(err) => {
                    tracing::debug!(
                        target: "chromiumoxide::conn::raw_ws::parse_errors",
                        "Dropping malformed binary WS frame: {err}",
                    );
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
            Some(Ok(WsMessage::Close(_))) => Poll::Ready(None),
            // ignore ping and pong
            Some(Ok(WsMessage::Ping(_))) | Some(Ok(WsMessage::Pong(_))) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Ok(msg)) => {
                // Unexpected WS message type, but not fatal.
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    "Unexpected WS message type: {:?}",
                    msg
                );
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(CdpError::Ws(err)))),
            None => {
                // ws connection closed
                Poll::Ready(None)
            }
        }
    }
}

/// Shared decode path for both text and binary WS frames.
/// `raw_text_for_logging` is only provided for textual frames so we can log the original
/// payload on parse failure if desired.
#[cfg(not(feature = "serde_stacker"))]
fn decode_message<T: EventMessage>(
    bytes: &[u8],
    raw_text_for_logging: Option<&str>,
) -> Result<Box<Message<T>>> {
    let _ = raw_text_for_logging;
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    let msg = if value.get("method").is_some() {
        Message::Event(serde_json::from_value::<T>(value)?)
    } else {
        Message::Response(serde_json::from_value::<chromiumoxide_types::Response>(
            value,
        )?)
    };
    tracing::trace!("Received {:?}", msg);
    Ok(Box::new(msg))
}

#[cfg(test)]
mod tests {
    use super::decode_message;
    use chromiumoxide_cdp::cdp::{CdpEvent, CdpEventMessage};
    use chromiumoxide_types::Message;

    #[test]
    fn decode_message_parses_network_event_frames_by_shape() {
        let raw = r#"{
            "method":"Network.requestWillBeSentExtraInfo",
            "params":{
                "requestId":"84477.1",
                "associatedCookies":[],
                "headers":{
                    "Accept":"*/*",
                    "Content-Length":"17",
                    "content-type":"text/plain"
                },
                "connectTiming":{
                    "requestTime":4366801.0
                },
                "siteHasCookieInOtherPartition":false
            },
            "sessionId":"1005A6C7B1F1C90A7D2CEB10EA82E560"
        }"#;

        let message = decode_message::<CdpEventMessage>(raw.as_bytes(), Some(raw))
            .expect("network event should decode");
        match *message {
            Message::Event(CdpEventMessage {
                params: CdpEvent::NetworkRequestWillBeSentExtraInfo(event),
                ..
            }) => {
                assert_eq!(event.request_id.as_ref(), "84477.1");
                assert_eq!(
                    event.headers.inner()["content-type"].as_str(),
                    Some("text/plain")
                );
                assert!(
                    event.client_security_state.is_none(),
                    "missing clientSecurityState should stay optional"
                );
            }
            other => panic!("expected Network.requestWillBeSentExtraInfo event, got {other:?}"),
        }
    }

    #[test]
    fn decode_message_parses_command_responses_by_shape() {
        let raw = r#"{"id":35,"result":{"postData":"{\"hello\":\"world\"}","base64Encoded":false},"sessionId":"session-1"}"#;

        let message = decode_message::<CdpEventMessage>(raw.as_bytes(), Some(raw))
            .expect("response frame should decode");
        match *message {
            Message::Response(response) => {
                assert_eq!(response.id, chromiumoxide_types::CallId::new(35));
                assert!(response.result.is_some());
            }
            other => panic!("expected response message, got {other:?}"),
        }
    }
}

/// Shared decode path for both text and binary WS frames.
/// `raw_text_for_logging` is only provided for textual frames so we can log the original
/// payload on parse failure if desired.
#[cfg(feature = "serde_stacker")]
fn decode_message<T: EventMessage>(
    bytes: &[u8],
    raw_text_for_logging: Option<&str>,
) -> Result<Box<Message<T>>> {
    use serde::Deserialize;
    let _ = raw_text_for_logging;
    let mut de = serde_json::Deserializer::from_slice(bytes);
    de.disable_recursion_limit();
    let value = serde_stacker::deserialize::<serde_json::Value>(&mut de)?;
    let msg = if value.get("method").is_some() {
        Message::Event(T::deserialize(value)?)
    } else {
        Message::Response(chromiumoxide_types::Response::deserialize(value)?)
    };
    tracing::trace!("Received {:?}", msg);
    Ok(Box::new(msg))
}
