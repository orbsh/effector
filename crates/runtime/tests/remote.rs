//! Phase 3 acceptance: a fake control plane over a real WS endpoint.
//! Register -> Registered -> push one ToolCall -> assert the ToolResult.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use effector_config::{CapabilitySurface, EffectorConfig, NetworkPolicy};
use effector_protocol::{CodeRef, Frame, ToolCall, ToolResult};
use effector_runtime::remote;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// The token this test node dials with. It rides the URL, and the fake plane asserts it is
/// there — a token has to arrive by the same path the real control plane reads.
const TOKEN: &str = "test-token";

async fn fake_control_plane(listener: TcpListener, code: CodeRef) {
    let (stream, _) = listener.accept().await.unwrap();
    // The token arrives on the DIAL, so the check is here — before the socket is served, which
    // is where the real plane does it too (ADR-0003: the connection is the authority).
    // The `Err` arm is the handshake API's own type (a full HTTP response), so its size is not
    // ours to choose.
    #[allow(clippy::result_large_err)]
    let ws = tokio_tungstenite::accept_hdr_async(
        stream,
        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
         resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let query = req.uri().query().unwrap_or_default();
            assert!(
                query.split('&').any(|kv| kv == format!("token={TOKEN}")),
                "the dial carries the node's token, got query `{query}`"
            );
            Ok(resp)
        },
    )
    .await
    .unwrap();
    let (mut sink, mut stream) = ws.split();

    // Then one declaration: what this node is, never what it knows.
    let Message::Text(text) = stream.next().await.unwrap().unwrap() else {
        panic!("expected text frame");
    };
    match serde_json::from_str::<Frame>(&text).unwrap() {
        Frame::Register {
            node_alias,
            carriers,
        } => {
            assert_eq!(node_alias, "test-node");
            assert!(carriers.contains(&"steel".to_string()));
        }
        other => panic!("expected Register, got {other:?}"),
    }
    sink.send(Message::Text(
        serde_json::to_string(&Frame::Registered).unwrap(),
    ))
    .await
    .unwrap();

    // Push one call: a steel counter handler. Executor identity and entry
    // name are separate: residency is keyed by `executor` alone.
    let call = ToolCall {
        call_id: "c-1".into(),
        kind: effector_protocol::CallKind::IterateStart,
        stream: Some("s-1".into()),
        executor: "counter/k1".into(),
        entry: "counter".into(),
        language: "steel".into(),
        encoding: induction_protocol::Encoding::Json,
        args: serde_json::json!({"n": 3}),
        code: code.clone(),
    };
    sink.send(Message::Text(
        serde_json::to_string(&Frame::Call(call)).unwrap(),
    ))
    .await
    .unwrap();

    // Expect the result.
    let Message::Text(text) = stream.next().await.unwrap().unwrap() else {
        panic!("expected text frame");
    };
    match serde_json::from_str::<Frame>(&text).unwrap() {
        Frame::Result(ToolResult { call_id, outcome }) => {
            assert_eq!(call_id, "c-1");
            assert_eq!(
                outcome.unwrap(),
                serde_json::json!({"done": true, "value": {"doubled": 6}})
            );
        }
        other => panic!("expected Result, got {other:?}"),
    }
}

#[tokio::test]
async fn outbound_registration_and_task_downlink() {
    // Code is content-addressed (ADR-0027): the fake plane sends a
    // reference; the bytes live behind a local HTTP source.
    let code_bytes = br#"
(define (counter args)
  (hash "doubled" (* 2 (hash-ref args "n"))))
"#
    .to_vec();
    let (code_port, _sha, _hits) = serve_code(code_bytes.clone());
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&code_bytes);
    let sha = hex::encode(hasher.finalize());
    let code = CodeRef {
        url: format!("http://127.0.0.1:{code_port}/code"),
        sha256: sha,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(fake_control_plane(listener, code));

    let config = EffectorConfig {
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        sandbox: true,
        token: TOKEN.into(),
        capabilities: CapabilitySurface {
            carriers: vec!["steel".into()],
            node_alias: "test-node".into(),
            network: NetworkPolicy::None,
            ..Default::default()
        },
        lease: Default::default(),
    };

    // The effector serves one connection; the fake plane drops it after
    // asserting, then run() reconnects forever — bound the effector with a
    // timeout (the server task holds the real assertions).
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), remote::run(config)).await;
    server.await.unwrap();
}

// --------------------------------- ADR-0027: content-addressed payloads ----

/// Serve code bytes over plain HTTP (any number of requests — the effector's
/// per-hash cache must keep request count at ONE across repeated calls of
/// the same code); returns (port, sha256 hex, hits).
fn serve_code(bytes: Vec<u8>) -> (u16, String, Arc<std::sync::atomic::AtomicUsize>) {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let sha = hex::encode(hasher.finalize());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits2 = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            hits2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body = bytes.clone();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    (port, sha, hits)
}

use std::io::{Read as _, Write as _};

#[tokio::test]
async fn code_ref_fetch_verify_cache_and_mismatch_rejection() {
    let code = br#"
(define (triple args)
  (hash "tripled" (* 3 (hash-ref args "n"))))
"#
    .to_vec();
    let (http_port, sha, hits) = serve_code(code);
    let hits_check = hits.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut sink, mut stream) = ws.split();

        let msg = stream.next().await.unwrap().unwrap();
        let f: Frame = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert!(matches!(f, Frame::Register { .. }));
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Registered).unwrap(),
        ))
        .await
        .unwrap();

        // Link call with the CORRECT hash: fetch, verify, execute.
        let call = ToolCall {
            call_id: "c-link".into(),
            kind: effector_protocol::CallKind::IterateStart,
            stream: Some("s-1".into()),
            executor: "triple/k1".into(),
            entry: "triple".into(),
            language: "steel".into(),
            encoding: induction_protocol::Encoding::Json,
            args: serde_json::json!({"n": 5}),
            code: CodeRef {
                url: format!("http://127.0.0.1:{http_port}/code"),
                sha256: sha.clone(),
            },
        };
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Call(call)).unwrap(),
        ))
        .await
        .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        match serde_json::from_str::<Frame>(msg.to_text().unwrap()).unwrap() {
            Frame::Result(r) => {
                assert_eq!(r.call_id, "c-link");
                assert_eq!(
                    r.outcome.unwrap(),
                    serde_json::json!({"done": true, "value": {"tripled": 15}})
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }

        // A SECOND call with the SAME code (new executor so the resident
        // load actually re-resolves): the per-hash cache must serve it —
        // the HTTP source stays at exactly one hit across the exchange.
        let call = ToolCall {
            call_id: "c-cached".into(),
            kind: effector_protocol::CallKind::IterateStart,
            stream: Some("s-1".into()),
            executor: "triple/k2".into(),
            entry: "triple".into(),
            language: "steel".into(),
            encoding: induction_protocol::Encoding::Json,
            args: serde_json::json!({"n": 2}),
            code: CodeRef {
                url: format!("http://127.0.0.1:{http_port}/code"),
                sha256: sha.clone(),
            },
        };
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Call(call)).unwrap(),
        ))
        .await
        .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        match serde_json::from_str::<Frame>(msg.to_text().unwrap()).unwrap() {
            Frame::Result(r) => {
                assert_eq!(r.call_id, "c-cached");
                assert_eq!(
                    r.outcome.unwrap(),
                    serde_json::json!({"done": true, "value": {"tripled": 6}})
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
        assert_eq!(
            hits_check.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "repeat of the same code resolves from the hash cache, not the source"
        );

        // Link call with a WRONG hash: error value, never a silent accept.
        let call = ToolCall {
            call_id: "c-bad".into(),
            kind: effector_protocol::CallKind::IterateStart,
            stream: Some("s-1".into()),
            executor: "triple/k1".into(),
            entry: "triple".into(),
            language: "steel".into(),
            encoding: induction_protocol::Encoding::Json,
            args: serde_json::json!({"n": 5}),
            code: CodeRef {
                url: format!("http://127.0.0.1:{http_port}/code"),
                sha256: "deadbeef".into(),
            },
        };
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Call(call)).unwrap(),
        ))
        .await
        .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        match serde_json::from_str::<Frame>(msg.to_text().unwrap()).unwrap() {
            Frame::Result(r) => {
                assert_eq!(r.call_id, "c-bad");
                assert!(r.outcome.is_err(), "hash mismatch must be an error value");
            }
            other => panic!("expected Result, got {other:?}"),
        }
    });

    let config = EffectorConfig {
        control_plane_url: format!("ws://127.0.0.1:{ws_port}"),
        sandbox: true,
        token: TOKEN.into(),
        capabilities: CapabilitySurface {
            carriers: vec!["steel".into()],
            node_alias: "link-node".into(),
            // `none` is the only egress declaration this node can HONOR
            // (nothing wires an allowlist; the jail denies egress). A
            // non-`none` value is a startup error now — effector ADR-0001 —
            // so this test, which is about content-addressed code and not
            // about the network, declares the enforceable thing.
            network: NetworkPolicy::None,
            ..Default::default()
        },
        lease: Default::default(),
    };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), remote::run(config)).await;
    server.await.unwrap();
}

/// ADR-0001: a declaration this node cannot enforce is refused BEFORE it
/// dials — the control plane never receives a capability that would not hold.
/// The config crate pins the message; this pins that `run` consults it.
#[tokio::test]
async fn an_unenforceable_declaration_fails_before_dialing() {
    let config = EffectorConfig {
        // Nothing listens on port 1: a real dial would fail with a DIFFERENT
        // error (connection refused), so the named config error is the proof
        // that the node never dialed at all.
        control_plane_url: "ws://127.0.0.1:1".into(),
        // No jail, so the allowlist has no enforcement point — the declaration is refused at
        // load, which is the startup-fact rule (ADR-0001) and why nothing dials.
        sandbox: false,
        token: TOKEN.into(),
        capabilities: CapabilitySurface {
            carriers: vec!["steel".into()],
            node_alias: "refusing-node".into(),
            network: NetworkPolicy::Allow(vec!["github.com:443".into()]),
            ..Default::default()
        },
        lease: Default::default(),
    };
    let err = remote::run(config).await.unwrap_err().to_string();
    assert!(
        err.contains("network") && err.contains("jail"),
        "the refusal names the missing enforcement point: {err}"
    );
}

/// Slice 2's claim, end to end: a node that declares a lease leaves on its own when nothing
/// calls it, `run` returns Ok (so `docker run --rm` ends the container with the process), and
/// it does NOT dial again — a reconnect would keep a container alive forever, which is the one
/// thing the lease exists to prevent.
#[tokio::test]
async fn a_leased_node_leaves_when_the_work_stops() {
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let config = EffectorConfig {
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        sandbox: false,
        token: TOKEN.into(),
        capabilities: CapabilitySurface {
            node_alias: "leased-node".into(),
            carriers: vec!["steel".into()],
            ..Default::default()
        },
        lease: effector_config::LeaseConfig {
            idle_secs: 1,
            max_idle_secs: 1,
            exit_on_disconnect: false,
        },
    };

    // The node goes first: its dial is what `accept` is waiting for.
    let started = Instant::now();
    let node = tokio::spawn(remote::run(config));
    let (stream, _) = listener.accept().await.unwrap();

    // The plane says hello and then goes silent: idleness is the input, nothing else.
    let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
    let plane = tokio::spawn(async move {
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let _declaration = stream.next().await.unwrap().unwrap();
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Registered).unwrap(),
        ))
        .await
        .unwrap();
        // Hold the socket open until the test says the node has left.
        let _ = close_rx.await;
        drop(sink);
    });

    let outcome = tokio::time::timeout(Duration::from_secs(10), node)
        .await
        .expect("the lease must end the node, not hang it")
        .expect("the node task must not panic");
    let elapsed = started.elapsed();
    assert!(
        outcome.is_ok(),
        "a lease is a clean end, not an error: {outcome:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "the node left before its grace ran out ({elapsed:?})"
    );

    let _ = close_tx.send(());
    let _ = plane.await;
    // Nothing dialed a second time.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.accept())
            .await
            .is_err(),
        "a leased node must not reconnect — that is the container that never ends"
    );
}

/// A plane that accepts the socket and then says nothing must not leave a leased node alive:
/// the first grace bounds the wait for `registered` too. Without it, the container form hangs
/// with no work and no way to be given any — the exact shape the lease exists to end.
#[tokio::test]
async fn a_plane_that_accepts_and_goes_silent_ends_a_leased_node() {
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let config = EffectorConfig {
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        sandbox: false,
        token: TOKEN.into(),
        capabilities: CapabilitySurface {
            node_alias: "silent-plane-node".into(),
            carriers: vec!["steel".into()],
            ..Default::default()
        },
        lease: effector_config::LeaseConfig {
            idle_secs: 1,
            max_idle_secs: 1,
            exit_on_disconnect: true,
        },
    };

    let started = Instant::now();
    let node = tokio::spawn(remote::run(config));
    let (stream, _) = listener.accept().await.unwrap();
    let plane = tokio::spawn(async move {
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut _sink, mut stream) = ws.split();
        // Read the declaration, answer nothing at all, hold the socket.
        let _declaration = stream.next().await.unwrap().unwrap();
        while stream.next().await.is_some() {}
    });

    let outcome = tokio::time::timeout(Duration::from_secs(10), node)
        .await
        .expect("the first grace must bound the handshake, not hang it")
        .expect("the node task must not panic");
    let elapsed = started.elapsed();
    let err = outcome.expect_err("an unanswered handshake is not a success");
    assert!(
        err.to_string().contains("registered"),
        "the error names what never arrived: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "it waited far past its first grace ({elapsed:?})"
    );
    drop(plane);
}
