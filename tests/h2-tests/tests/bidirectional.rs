use futures::{FutureExt, StreamExt};
use h2_support::prelude::*;
use std::task::Poll;

#[tokio::test]
async fn receive_data_while_pong_is_blocked() {
    receive_data_while_writing(Control::Pong).await;
}

#[tokio::test]
async fn receive_data_while_user_ping_is_blocked() {
    receive_data_while_writing(Control::UserPing).await;
}

#[tokio::test]
async fn receive_data_while_local_settings_are_blocked() {
    receive_data_while_writing(Control::LocalSettings).await;
}

#[tokio::test]
async fn receive_data_while_settings_ack_is_blocked() {
    receive_data_while_writing(Control::SettingsAck).await;
}

#[tokio::test]
async fn receive_data_preserves_pending_control_order() {
    receive_data_while_writing(Control::Multiple).await;
}

#[derive(Clone, Copy)]
enum Control {
    Pong,
    UserPing,
    LocalSettings,
    SettingsAck,
    Multiple,
}

async fn receive_data_while_writing(control: Control) {
    h2_support::trace_init!();
    // The preface, SETTINGS exchange, and request HEADERS fit; DATA does not.
    let (io, mut peer) = mock::new_with_write_capacity(73);
    let (mut client, mut conn) = client::handshake(io).await.expect("handshake");
    join(
        peer.assert_client_handshake(),
        conn.drive(util::yield_once()),
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://example.com/")
        .body(())
        .unwrap();
    let (response, mut send) = client.send_request(request, false).unwrap();
    let request_body = Bytes::from((0..32_768).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    send.send_data(request_body.clone(), true).unwrap();
    poll_connection(&mut conn).await;
    peer.recv_frame(frames::headers(1).request("POST", "https://example.com/"))
        .now_or_never()
        .expect("request headers fit before writes are blocked");

    let first = Bytes::from(vec![17; 16_384]);
    let second = Bytes::from(vec![29; 16_384]);
    peer.send_frame(frames::headers(1).response(200)).await;
    peer.send_frame(frames::data(1, first.clone())).await;
    poll_connection(&mut conn).await;
    let mut body = response
        .now_or_never()
        .expect("response headers are readable while request DATA is blocked")
        .unwrap()
        .into_body();
    assert_eq!(body.data().now_or_never().unwrap().unwrap().unwrap(), first);

    let mut ping_pong = None;
    match control {
        Control::Pong => peer.send_frame(frames::ping([1; 8])).await,
        Control::UserPing => {
            let mut ping = client::Connection::ping_pong(&mut conn).unwrap();
            ping.send_ping(Ping::opaque()).unwrap();
            ping_pong = Some(ping);
        }
        Control::LocalSettings => conn.set_initial_window_size(65_536).unwrap(),
        Control::SettingsAck => {
            peer.send_frame(frames::settings().initial_window_size(65_536))
                .await;
        }
        Control::Multiple => {
            peer.send_frame(frames::ping([1; 8])).await;
            peer.send_frame(frames::settings().initial_window_size(65_536))
                .await;
            peer.send_frame(frames::ping([2; 8])).await;
            peer.send_frame(frames::settings().initial_window_size(65_537))
                .await;
        }
    }
    peer.send_frame(frames::data(1, second.clone()).eos()).await;
    poll_connection(&mut conn).await;
    assert_eq!(
        body.data()
            .now_or_never()
            .expect("response DATA must progress while a control frame cannot be written")
            .unwrap()
            .unwrap(),
        second
    );
    assert!(body.data().now_or_never().unwrap().is_none());

    peer.unbounded_bytes().await;
    conn.drive(async {
        peer.recv_frame(frames::data(1, request_body.slice(..16_384)))
            .await;
        match control {
            Control::Pong => peer.recv_frame(frames::ping([1; 8]).pong()).await,
            Control::UserPing => {
                peer.recv_frame(frames::ping(frame::Ping::USER)).await;
                peer.send_frame(frames::ping(frame::Ping::USER).pong())
                    .await;
            }
            Control::LocalSettings => {
                peer.recv_frame(frames::settings().initial_window_size(65_536))
                    .await;
                peer.send_frame(frames::settings_ack()).await;
            }
            Control::SettingsAck => peer.recv_frame(frames::settings_ack()).await,
            Control::Multiple => {
                peer.recv_frame(frames::ping([1; 8]).pong()).await;
                peer.recv_frame(frames::settings_ack()).await;
                peer.recv_frame(frames::ping([2; 8]).pong()).await;
                peer.recv_frame(frames::settings_ack()).await;
            }
        }
        peer.recv_frame(frames::data(1, request_body.slice(16_384..)).eos())
            .await;
    })
    .await;
    if let Some(ping) = &mut ping_pong {
        conn.drive(poll_fn(|cx| ping.poll_pong(cx))).await.unwrap();
    }
    poll_connection(&mut conn).await;

    drop(ping_pong);
    drop(body);
    drop(send);
    drop(client);
    join(
        async {
            peer.recv_frame(frames::go_away(0)).await;
            peer.recv_eof().await;
        },
        async { conn.await.unwrap() },
    )
    .await;
}

async fn poll_connection(conn: &mut client::Connection<mock::Mock, Bytes>) {
    poll_fn(|cx| {
        assert!(conn.poll_unpin(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn settings_ack_follows_blocked_continuations() {
    h2_support::trace_init!();
    let (io, mut peer) = mock::new_with_write_capacity(73);
    let (mut client, mut conn) = client::handshake(io).await.unwrap();
    join(
        peer.assert_client_handshake(),
        conn.drive(util::yield_once()),
    )
    .await;

    let mut ping = client::Connection::ping_pong(&mut conn).unwrap();
    ping.send_ping(Ping::opaque()).unwrap();
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("https://example.com/");
    for (name, value) in build_large_headers() {
        request = request.header(name, value);
    }
    request = request.header("x-repeated", "must survive a header table size change");
    let request = request.body(()).unwrap();
    let fields = request.headers().clone();
    let (response, send) = client.send_request(request, true).unwrap();
    poll_connection(&mut conn).await;
    peer.recv_frame(frames::ping(frame::Ping::USER))
        .now_or_never()
        .expect("PING is written before the blocked header block");

    peer.send_frame(
        frames::settings()
            .header_table_size(0)
            .max_frame_size(32_768),
    )
    .await;
    peer.codec_mut().set_max_recv_frame_size(32_768);
    peer.send_frame(frames::ping(frame::Ping::USER).pong())
        .await;
    poll_connection(&mut conn).await;
    poll_fn(|cx| ping.poll_pong(cx))
        .now_or_never()
        .expect("PONG must be read while the SETTINGS ACK waits for CONTINUATION")
        .unwrap();

    peer.unbounded_bytes().await;
    conn.drive(async {
        // The decoder rejects a SETTINGS ACK interleaved into the header block.
        peer.recv_frame(
            frames::headers(1)
                .request("POST", "https://example.com/")
                .fields(fields)
                .eos(),
        )
        .await;
        peer.recv_frame(frames::settings_ack()).await;
        peer.codec_mut().set_recv_header_table_size(0);
        peer.send_frame(frames::headers(1).response(204).eos())
            .await;
    })
    .await;
    assert_eq!(conn.drive(response).await.unwrap().status(), 204);
    drop(send);

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://example.com/")
        .header("x-repeated", "must survive a header table size change")
        .body(())
        .unwrap();
    let (response, mut send) = client.send_request(request, false).unwrap();
    let data = Bytes::from(vec![41; 32_768]);
    send.send_data(data.clone(), true).unwrap();
    conn.drive(async {
        peer.recv_frame(
            frames::headers(3)
                .request("POST", "https://example.com/")
                .field("x-repeated", "must survive a header table size change"),
        )
        .await;
        peer.recv_frame(frames::data(3, data).eos()).await;
        peer.send_frame(frames::headers(3).response(204).eos())
            .await;
    })
    .await;
    assert_eq!(conn.drive(response).await.unwrap().status(), 204);
    drop(send);
    drop(ping);
    drop(client);
    join(
        async {
            peer.recv_frame(frames::go_away(0)).await;
            peer.recv_eof().await;
        },
        async { conn.await.unwrap() },
    )
    .await;
}

#[tokio::test]
async fn graceful_shutdown_reads_data_while_writes_are_blocked() {
    h2_support::trace_init!();
    let (io, mut peer) = mock::new_with_write_capacity(1024);
    peer.write_preface().await;
    peer.send_frame(frame::Settings::default()).await;
    let mut conn = server::handshake(io).await.unwrap();
    poll_server_connection(&mut conn).await;
    peer.recv_frame(frame::Settings::default()).await;
    peer.recv_frame(frames::settings_ack()).await;
    peer.send_frame(frames::settings_ack()).await;
    peer.send_frame(frames::headers(1).request("POST", "https://example.com/"))
        .await;
    let (request, mut respond) = conn.accept().await.unwrap().unwrap();
    let mut body = request.into_body();
    let mut send = respond
        .send_response(Response::builder().status(200).body(()).unwrap(), false)
        .unwrap();
    let response_body = Bytes::from(vec![53; 32_768]);
    send.send_data(response_body.clone(), true).unwrap();
    poll_server_connection(&mut conn).await;
    peer.recv_frame(frames::headers(1).response(200))
        .now_or_never()
        .expect("response headers fit before writes are blocked");

    let first = Bytes::from(vec![67; 16_384]);
    let second = Bytes::from(vec![79; 16_384]);
    peer.send_frame(frames::data(1, first.clone())).await;
    poll_server_connection(&mut conn).await;
    assert_eq!(body.data().now_or_never().unwrap().unwrap().unwrap(), first);

    conn.graceful_shutdown();
    peer.send_frame(frames::data(1, second.clone()).eos()).await;
    poll_server_connection(&mut conn).await;
    assert_eq!(
        body.data()
            .now_or_never()
            .expect("request DATA must progress while graceful GOAWAY is blocked")
            .unwrap()
            .unwrap(),
        second
    );
    assert!(body.data().now_or_never().unwrap().is_none());
    drop(body);
    drop(send);
    drop(respond);

    peer.unbounded_bytes().await;
    join(
        async move {
            assert!(conn.accept().await.is_none());
        },
        async {
            peer.recv_frame(frames::data(1, response_body.slice(..16_384)))
                .await;
            peer.recv_frame(frames::go_away(2_147_483_647)).await;
            peer.recv_frame(frames::ping(frame::Ping::SHUTDOWN)).await;
            peer.send_frame(frames::ping(frame::Ping::SHUTDOWN).pong())
                .await;
            peer.recv_frame(frames::window_update(0, 32_768)).await;
            peer.recv_frame(frames::data(1, response_body.slice(16_384..)).eos())
                .await;
            peer.recv_frame(frames::go_away(1)).await;
            peer.recv_eof().await;
        },
    )
    .await;
}

async fn poll_server_connection(conn: &mut server::Connection<mock::Mock, Bytes>) {
    poll_fn(|cx| {
        assert!(conn.poll_accept(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn saturated_control_buffer_resumes_without_losing_frames() {
    h2_support::trace_init!();
    let (io, mut peer) = mock::new_with_write_capacity(73);
    let (mut client, mut conn) = client::handshake(io).await.unwrap();
    join(
        peer.assert_client_handshake(),
        conn.drive(util::yield_once()),
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://example.com/")
        .body(())
        .unwrap();
    let (response, mut send) = client.send_request(request, false).unwrap();
    let request_body = Bytes::from((0..32_768).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    send.send_data(request_body.clone(), true).unwrap();
    poll_connection(&mut conn).await;
    peer.recv_frame(frames::headers(1).request("POST", "https://example.com/"))
        .now_or_never()
        .expect("request headers fit before writes are blocked");

    let first = Bytes::from(vec![83; 16_384]);
    let second = Bytes::from(vec![97; 16_384]);
    peer.send_frame(frames::headers(1).response(200)).await;
    peer.send_frame(frames::data(1, first.clone())).await;
    poll_connection(&mut conn).await;
    let mut body = response
        .now_or_never()
        .expect("response headers are readable before the control buffer fills")
        .unwrap()
        .into_body();
    assert_eq!(body.data().now_or_never().unwrap().unwrap().unwrap(), first);

    // More PONGs than the control buffer can hold must apply backpressure.
    const PINGS: u64 = 1_200;
    for payload in 0..PINGS {
        peer.send_frame(frames::ping(payload.to_be_bytes())).await;
    }
    peer.send_frame(frames::data(1, second.clone()).eos()).await;
    poll_connection(&mut conn).await;
    assert!(body.data().now_or_never().is_none());

    peer.unbounded_bytes().await;
    conn.drive(async {
        let mut next_pong = 0u64;
        let mut sent_data = 0;
        while next_pong < PINGS || sent_data < request_body.len() {
            match peer.next().await.expect("connection remains open").unwrap() {
                frame::Frame::Ping(pong) => {
                    assert!(pong.is_ack());
                    assert!(next_pong < PINGS, "unexpected duplicate PONG");
                    assert_eq!(pong.payload(), &next_pong.to_be_bytes());
                    next_pong += 1;
                }
                frame::Frame::Data(data) => {
                    assert_eq!(data.stream_id(), 1);
                    let end = sent_data + data.payload().len();
                    assert!(end <= request_body.len());
                    assert_eq!(data.payload(), &request_body.slice(sent_data..end));
                    assert_eq!(data.is_end_stream(), end == request_body.len());
                    sent_data = end;
                }
                other => panic!(
                    "unexpected frame while recovering from backpressure: {:?}",
                    other
                ),
            }
        }
    })
    .await;
    assert_eq!(conn.drive(body.data()).await.unwrap().unwrap(), second);
    assert!(body.data().now_or_never().unwrap().is_none());
    poll_connection(&mut conn).await;

    drop(body);
    drop(send);
    drop(client);
    join(
        async {
            peer.recv_frame(frames::go_away(0)).await;
            peer.recv_eof().await;
        },
        async { conn.await.unwrap() },
    )
    .await;
}
