// Copyright 2026 Deno Land Inc. All rights reserved.

//! Seam: production rpc/fetch and their shared send_inner, against a real HTTP/1
//! upgrade peer. The peer's in-memory mutation ledger is transport-component
//! evidence, not application dispatch, storage, authentication, or caller retry
//! qualification. No sender/error is fabricated and the private pool is untouched.
//! Run under the separately admitted test-process deadline; no sleeps or timing
//! thresholds establish the fault or the absence of a retry.
//!
//! Unstarted-request recovery uses a manually driven old duplex connection and
//! a dispatch-wake witness, followed by a fresh real TCP peer. This controls the
//! transport boundary, not TCP idle-close timing: a synchronous TCP close/ack
//! also pauses the client's reactor and would leave EOF readiness racy.

// This real-I/O test peer is outside the application's execution boundary.
#![allow(clippy::disallowed_methods)]

use super::*;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Default)]
struct Ledger {
    connections: usize,
    requests: Vec<(usize, String)>,
    mutations: usize,
}

struct TestPeer {
    addr: String,
    ledger: Arc<Mutex<Ledger>>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TestPeer {
    async fn start(kind: &'static str, lose_reply: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let observations = ledger.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                celld::asyncrt::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let id = {
                            let mut ledger = observations.lock().unwrap();
                            ledger.connections += 1;
                            ledger.connections
                        };
                        connections.spawn(serve_peer(
                            socket, id, kind, lose_reply, observations.clone(),
                        ));
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled(), "peer task panicked: {error}");
                }
            }
        });
        Self {
            addr,
            ledger,
            stop: Some(stop),
            task,
        }
    }

    async fn stop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        (&mut self.task).await.unwrap();
    }
}

impl Drop for TestPeer {
    fn drop(&mut self) {
        // Also cancels all owned connection tasks if a test assertion fires.
        self.task.abort();
    }
}

async fn serve_peer(
    socket: TcpStream,
    id: usize,
    kind: &'static str,
    lose_reply: bool,
    ledger: Arc<Mutex<Ledger>>,
) {
    let upgrade = Arc::new(Mutex::new(None));
    let captured_upgrade = upgrade.clone();
    let outer = service_fn(move |mut request: Request<Incoming>| {
        assert_eq!(request.method(), hyper::Method::GET);
        assert_eq!(request.uri().path(), "/peer/tunnel");
        assert_eq!(request.headers()[KIND_HEADER], kind);
        assert_eq!(request.headers()[VERSION_HEADER], TUNNEL_VERSION);
        *captured_upgrade.lock().unwrap() = Some(hyper::upgrade::on(&mut request));
        async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(hyper::header::CONNECTION, "upgrade")
                    .header(hyper::header::UPGRADE, TUNNEL_PROTOCOL)
                    .header(VERSION_HEADER, TUNNEL_VERSION)
                    .header(
                        peer_auth::RESPONSE_VERSION_HEADER,
                        peer_auth::PROTOCOL_VERSION_TEXT,
                    )
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
        }
    });
    hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(socket), outer)
        .with_upgrades()
        .await
        .unwrap();
    let upgrade = upgrade.lock().unwrap().take().unwrap();
    let socket = upgrade.await.unwrap();
    let inner = service_fn(move |request: Request<Incoming>| {
        let ledger = ledger.clone();
        async move {
            assert_eq!(request.method(), hyper::Method::POST);
            assert_eq!(request.headers()[SCOPE_HEADER], "retry-test");
            assert_eq!(request.headers()[NAME_HEADER], "counter");
            let operation = if kind == "rpc" {
                assert_eq!(
                    request.headers()[hyper::header::CONTENT_TYPE],
                    "application/json"
                );
                request.headers()[METHOD_HEADER]
                    .to_str()
                    .unwrap()
                    .to_owned()
            } else {
                assert_eq!(request.headers()["x-test-marker"], "preserved");
                request.uri().path().trim_start_matches('/').to_owned()
            };
            assert!(matches!(operation.as_str(), "warm" | "append"));
            let payload = request.into_body().collect().await.unwrap().to_bytes();
            let expected: &[u8] = if operation == "append" {
                b"[\"once\"]"
            } else {
                b"[]"
            };
            assert_eq!(payload.as_ref(), expected);
            {
                let mut ledger = ledger.lock().unwrap();
                ledger.requests.push((id, operation.clone()));
                if operation == "append" {
                    ledger.mutations += 1;
                }
            }
            if lose_reply && operation == "append" {
                // The complete request has been consumed and its mutation recorded.
                // A service error closes h1 without producing a response head/body.
                return Err(std::io::Error::other(
                    "discard the completed mutation reply",
                ));
            }
            Ok(Response::new(http_body_util::Full::new(
                Bytes::from_static(b"ok"),
            )))
        }
    });
    // A service error is deliberate for append in loss cases; client outcome and
    // the ledger below distinguish it from an early handshake/framing failure.
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(socket, inner)
        .await;
}

async fn call(
    peer: &Peer<'_>,
    kind: &str,
    operation: &str,
) -> anyhow::Result<Response<PooledResponseBody>> {
    let payload = if operation == "append" {
        "[\"once\"]"
    } else {
        "[]"
    };
    if kind == "rpc" {
        rpc(
            peer,
            "retry-test",
            Some("counter"),
            operation,
            &celld::js::RpcData::Json(payload.to_owned()),
        )
        .await
    } else {
        fetch(
            peer,
            &TunnelControl {
                scope: "retry-test".to_owned(),
                name: Some("counter".to_owned()),
                request_id: None,
                capacity_handoff: false,
            },
            format!("http://cell.test/{operation}"),
            "POST".to_owned(),
            vec![("x-test-marker".to_owned(), "preserved".to_owned())],
            celld::js::RequestBody::Bytes(Bytes::from_static(payload.as_bytes())),
        )
        .await
    }
}

async fn consume(response: Response<PooledResponseBody>) {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ok"
    );
}

async fn check_case(kind: &'static str, warm: bool, lose_reply: bool) {
    let mut server = TestPeer::start(kind, lose_reply).await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let auth = PeerAuth::new([0x42; 32], "retry-client").unwrap();
    let peer = Peer {
        http: &http,
        auth: &auth,
        addr: &server.addr,
        node: "retry-peer",
    };
    if warm {
        // Consume through clean EOF: production PooledResponseBody parks the sender.
        consume(call(&peer, kind, "warm").await.unwrap()).await;
    }
    let result = call(&peer, kind, "append").await;
    if lose_reply {
        match result {
            Ok(_) => panic!("{kind}: a discarded response must not report success"),
            Err(error) => assert!(
                error.downcast_ref::<hyper::Error>().is_some(),
                "{kind}: expected the original Hyper transport error"
            ),
        }
    } else {
        consume(result.unwrap()).await;
    }
    server.stop().await;
    let ledger = server.ledger.lock().unwrap();
    assert_eq!(ledger.connections, 1, "{kind}: no fresh transport retry");
    assert_eq!(ledger.mutations, 1, "{kind}: append must execute once");
    let expected = if warm {
        vec![(1, "warm".to_owned()), (1, "append".to_owned())]
    } else {
        vec![(1, "append".to_owned())]
    };
    assert_eq!(
        ledger.requests, expected,
        "{kind}: exact request count and tunnel reuse"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_buffered_rpc_and_fetch_reuse_the_warmed_tunnel() {
    for kind in ["rpc", "do"] {
        check_case(kind, true, false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_buffered_rpc_and_fetch_do_not_retry_after_mutation() {
    for kind in ["rpc", "do"] {
        check_case(kind, false, true).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_buffered_rpc_and_fetch_do_not_replay_after_mutation() {
    for kind in ["rpc", "do"] {
        check_case(kind, true, true).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_buffered_fetch_recovers_the_original_queued_request() {
    use std::future::{poll_fn, Future};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Weak;
    use std::task::{Context, Poll, Wake, Waker};

    // Seam: controlled in-memory old HTTP/1 transport, then production send_inner
    // fallback to fresh real TCP. This does not cover old tunnel establishment,
    // TCP idle-close timing, or main-caller orchestration.
    struct AbortOnDrop(JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut server = TestPeer::start("do", false).await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let auth = PeerAuth::new([0x42; 32], "retry-client").unwrap();
    let peer = Peer {
        http: &http,
        auth: &auth,
        addr: &server.addr,
        node: "retry-peer",
    };
    let key = format!("{}#do", server.addr);
    // Size the duplex buffer from the warm request fixture, not a resource limit.
    let (client_io, server_io) = tokio::io::duplex(
        b"POST /warm HTTP/1.1\r\nhost: cell.test\r\ncontent-length: 2\r\n\r\n[]".len(),
    );
    let warm_requests = Arc::new(AtomicUsize::new(0));
    let observed = warm_requests.clone();
    let mut warm_server = AbortOnDrop(tokio::spawn(async move {
        let service = service_fn(move |request: Request<Incoming>| {
            let observed = observed.clone();
            async move {
                assert_eq!(request.method(), hyper::Method::POST);
                assert_eq!(request.uri().path(), "/warm");
                assert_eq!(
                    request.into_body().collect().await.unwrap().to_bytes(),
                    "[]"
                );
                observed.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>(Response::new(http_body_util::Full::new(
                    Bytes::from_static(b"ok"),
                )))
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server_io), service)
            .await
            .unwrap();
    }));
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    // Keep the driver local: nothing can poll it between shutdown and checkout.
    let mut connection = Box::pin(connection);
    {
        let request = Request::builder()
            .method("POST")
            .uri("/warm")
            .header(hyper::header::HOST, "cell.test")
            .body(full_body(Bytes::from_static(b"[]")))
            .unwrap();
        let mut warm = Box::pin(async {
            consume(send_on_tunnel(&key, sender, request).await.unwrap()).await;
        });
        poll_fn(|cx| {
            let result = warm.as_mut().poll(cx);
            assert!(connection.as_mut().poll(cx).is_pending());
            result
        })
        .await;
    }

    // At idle, Hyper registers this waker on its empty dispatch queue as well
    // as the transport. Finish shutdown before clearing its transport wake.
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let driver_waker = Waker::from(wakes.clone());
    assert!(connection
        .as_mut()
        .poll(&mut Context::from_waker(&driver_waker))
        .is_pending());
    warm_server.0.abort();
    assert!((&mut warm_server.0).await.unwrap_err().is_cancelled());
    wakes.0.store(0, Ordering::SeqCst);

    let builds = AtomicUsize::new(0);
    let original = Mutex::new(Weak::<()>::new());
    let mut send = Box::pin(send_inner(
        &peer,
        "do",
        |body| {
            builds.fetch_add(1, Ordering::SeqCst);
            let mut request = Request::builder()
                .method("POST")
                .uri("http://cell.test/append")
                .header(hyper::header::HOST, "cell.test")
                .header(SCOPE_HEADER, "retry-test")
                .header(NAME_HEADER, "counter")
                .header("x-test-marker", "preserved")
                .body(body)?;
            let sentinel = Arc::new(());
            *original.lock().unwrap() = Arc::downgrade(&sentinel);
            request.extensions_mut().insert(sentinel);
            Ok(request)
        },
        celld::js::RequestBody::Bytes(Bytes::from_static(b"[\"once\"]")),
    ));
    poll_fn(|cx| {
        assert!(!driver_waker.will_wake(cx.waker()));
        assert!(send.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "send_inner must check out the warmed sender and wake its dispatch queue"
    );

    // EOF is now observable before Hyper dequeues append. Drop the completed
    // driver too: destruction of the queued envelope returns the original.
    let _ = connection.as_mut().await;
    drop(connection);
    assert!(
        original.lock().unwrap().upgrade().is_some(),
        "the returned original request must still own its extension"
    );
    consume(send.await.unwrap()).await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(warm_requests.load(Ordering::SeqCst), 1);
    server.stop().await;
    let ledger = server.ledger.lock().unwrap();
    assert_eq!(ledger.connections, 1);
    assert_eq!(ledger.requests, vec![(1, "append".to_owned())]);
    assert_eq!(ledger.mutations, 1);
}
