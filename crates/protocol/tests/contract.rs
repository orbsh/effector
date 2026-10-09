use effector_protocol::{ChannelEncoding, CodeRef, ToolCall};

#[test]
fn roundtrip_call_carries_a_code_reference() {
    let call = ToolCall {
        call_id: "c1".into(),
        kind: effector_protocol::CallKind::IterateStart,
        stream: None,
        session: "notes/7".into(),
        entry: "read_file".into(),
        language: "bgi".into(),
        encoding: ChannelEncoding::Json,
        args: serde_json::json!({ "path": "~/notes.md" }),
        code: CodeRef {
            url: "http://code.local/ab12".into(),
            sha256: "ab12".into(),
        },
    };
    let s = serde_json::to_string(&call).unwrap();
    let back: ToolCall = serde_json::from_str(&s).unwrap();
    assert_eq!(back.call_id, "c1");
    assert_eq!(back.session, "notes/7", "residency identity survives the wire");
    assert_eq!(back.entry, "read_file", "entry name survives the wire");
    assert_eq!(back.language, "bgi");
    assert_eq!(back.code.sha256, "ab12", "the frame asserts the hash");
    assert_eq!(back.code.url, "http://code.local/ab12");
}

#[test]
fn code_ref_has_no_version_field() {
    // ADR-0027: the content hash IS the version identity — the retired
    // Link arm's `version` string must not resurface in the wire shape.
    let p = CodeRef {
        url: "https://cdn.example/abc".into(),
        sha256: "abc".into(),
    };
    let s = serde_json::to_string(&p).unwrap();
    assert!(!s.contains("version"), "no version token: {s}");
    assert!(s.contains("\"url\"") && s.contains("\"sha256\""));
}

#[test]
fn store_emit_carries_the_instruction_as_data() {
    // Phase 4.14 gate 1 (ADR-0026 §3 over the wire): the okm Collection
    // instruction rides inside `op: "store_emit"` with its payload under
    // `instruction` — the field must NOT be `op` (the discriminator
    // collides). The effector never parses the instruction; this test is
    // the shape lock the type system deliberately does not carry.
    let op = effector_protocol::HostOp::StoreEmit {
        instruction: serde_json::json!({
            "collection": "counters", "op": "get_document", "key": {"id": 1}
        }),
    };
    let s = serde_json::to_string(&op).unwrap();
    assert!(s.starts_with(r#"{"op":"store_emit","instruction":"#), "wire shape: {s}");
    let back: effector_protocol::HostOp = serde_json::from_str(&s).unwrap();
    match back {
        effector_protocol::HostOp::StoreEmit { instruction } => {
            assert_eq!(instruction["collection"], "counters");
            assert_eq!(instruction["op"], "get_document", "the instruction's own op token survives");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn capability_surface_defaults_deny() {
    let c = effector_config::CapabilitySurface::default();
    assert!(c.fs_scope.is_empty());
    assert!(!c.command_exec);
    assert!(matches!(c.network, effector_config::NetworkPolicy::None));
}
