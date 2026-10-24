#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unwrap_in_result, clippy::panic_in_result_fn)]
use effector_runtime::carrier::python::introspect;
use serde_json::json;

// ADR-0026 §4: the python script declares its collections with class
// definitions that mirror the Rust derive — type annotations drive the
// layout (width/offset/hot-cold split), the decorators carry the
// declaration metadata. The introspected schema must contain the
// `storage.collections` block in the exact serde form of
// okm_core::schema::CollectionSchema (StorePlan::from_schema consumes it
// verbatim).

#[test]
fn key_and_document_classes_derive_the_schema() {
    // Mirror of okm's dynamic_cross_test User/UserKey shape (u32 + u64 key,
    // hot u32/u16 payload + cold String) — the same declarations the Rust
    // derive consumes, so the two modes must agree byte-for-byte.
    let src = r##"
@KeyEncode
class UserKey:
    org_id: u32
    user_id: u64

@DocumentEncode
@ok_ref(UserKey)
@ok_ns(41)
@ok_layout(version=2)
@ok_index("by_level", fields=("level",))
class User:
    level: u32
    score: u16
    name: str

@on("add_to_cart", key="user_id")
def add(args):
    return {}
"##;
    let schema = introspect(src).unwrap();
    let collections = schema["storage"]["collections"]["User"]["schema"].clone();

    assert_eq!(collections["key_len"], 12);
    assert_eq!(collections["layout_version"], 2);
    assert_eq!(collections["hot_width"], 6);
    assert_eq!(collections["payload_header_len"], 3);
    // Key fields: declaration order, contiguous offsets, no tags.
    assert_eq!(
        collections["key_fields"],
        json!([
            { "name": "org_id", "ty": "U32", "width": 4, "offset": 0, "tag": null, "default": null, "expect_len": null },
            { "name": "user_id", "ty": "U64", "width": 8, "offset": 4, "tag": null, "default": null, "expect_len": null },
        ])
    );
    // Hot payload fields: contiguous offsets from 0 after the header.
    assert_eq!(
        collections["hot_fields"],
        json!([
            { "name": "level", "ty": "U32", "width": 4, "offset": 0, "tag": null, "default": null, "expect_len": null },
            { "name": "score", "ty": "U16", "width": 2, "offset": 4, "tag": null, "default": null, "expect_len": null },
        ])
    );
    // Variable-width kinds are cold TLV: width 0, tag = declaration index.
    assert_eq!(
        collections["cold_fields"],
        json!([
            { "name": "name", "ty": "Str", "width": 0, "offset": 0, "tag": 2, "default": null, "expect_len": null },
        ])
    );
    // Fixed slot map (ADR-0016).
    assert_eq!(
        collections["slots"],
        json!({ "primary": 0, "dynamic": 1, "field_vocab_id": 2, "field_vocab_name": 3,
                "declared_index_base": 4097, "declared_reduce_base": 8193, "junction_base": 12288 })
    );
}

#[test]
fn index_declaration_carries_slot_and_field_names() {
    let src = r##"
@KeyEncode
class UserKey:
    org_id: u32
    user_id: u64

@DocumentEncode
@ok_ref(UserKey)
@ok_ns(41)
@ok_index("by_org", fields=("org_id", "created_at"), includes=("bio_len",))
@ok_index("by_level", fields=("level",))
class User:
    org_id: u32
    created_at: u64
    bio_len: u16
    level: u32
"##;
    let schema = introspect(src).unwrap();
    let collection = &schema["storage"]["collections"]["User"];
    // Index slots: INDEX segment counter starting at 1, declaration order.
    assert_eq!(
        collection["indexes"],
        json!([
            { "name": "by_org", "slot": 4097, "fields": ["org_id", "created_at"], "includes": ["bio_len"], "kind": "plain" },
            { "name": "by_level", "slot": 4098, "fields": ["level"], "includes": [], "kind": "plain" },
        ])
    );
}

#[test]
fn ns_is_omitted_for_booth_side_auto_allocation() {
    // aura injects the ns at registration (the type registry allocates it);
    // a class without @ok_ns still produces a valid schema — the ns rides
    // the plan, not the declaration.
    let src = r##"
@KeyEncode
class UserKey:
    id: u64

@DocumentEncode
@ok_ref(UserKey)
class Counter:
    count: u64
"##;
    let schema = introspect(src).unwrap();
    let collections = &schema["storage"]["collections"]["Counter"]["schema"];
    assert_eq!(collections["key_len"], 8);
    assert_eq!(collections["hot_fields"][0]["name"], "count");
}

#[test]
fn explicit_interface_schema_still_merges() {
    // The explicit half keeps contributing what the decorators cannot
    // express (lifecycle); storage stays decorator-derived.
    let src = r##"
@KeyEncode
class UserKey:
    id: u64

@DocumentEncode
@ok_ref(UserKey)
class Counter:
    count: u64

def interface_schema(args=None):
    return {"lifecycle": {"idle_ttl": "5m"}}
"##;
    let schema = introspect(src).unwrap();
    assert_eq!(schema["lifecycle"]["idle_ttl"], "5m");
    assert!(schema["storage"]["collections"]["Counter"].is_object());
}

#[test]
fn bad_declaration_is_an_error_not_a_silent_drop() {
    // Variable-width fields cannot be located in an index segment after a
    // following field (no static width) — the same rule the Rust derive
    // enforces at compile time, enforced at introspection here.
    let src = r##"
@KeyEncode
class UserKey:
    id: u64

@DocumentEncode
@ok_ref(UserKey)
@ok_index("by_name", fields=("name", "level"))
class User:
    name: str
    level: u32
"##;
    assert!(introspect(src).is_err());
}
