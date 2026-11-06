#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
use effector_runtime::carrier::python::introspect;

#[test]
fn py_introspect_derives_receives() {
    let src = r##"
@on("add_to_cart", key="user_id")
def add(args):
    return {"added": args["item"]}

@on("remove_from_cart")
def remove(args):
    return {"removed": True}
"##;
    let schema = introspect(src).unwrap();
    println!("SCHEMA: {schema}");
    assert_eq!(schema["receives"]["add_to_cart"]["key"], "user_id");
    assert!(schema["receives"]["remove_from_cart"].get("key").is_none());
}

#[test]
fn py_introspect_user_schema_wins() {
    let src = r##"
def interface_schema(args=None):
    return {"lifecycle": {"idle_ttl": "5m"}}

@on("add_to_cart", key="user_id")
def add(args):
    return {}
"##;
    let schema = introspect(src).unwrap();
    println!("SCHEMA2: {schema}");
    assert_eq!(schema["lifecycle"]["idle_ttl"], "5m");
}

#[test]
fn py_auth_rides_the_decorator() {
    // `auth=True` is a knob of the `@on` that names the event — the same place `key` lives.
    let src = r##"
@on("ui:turn.submit", key="device", auth=True)
def turn(args):
    return {"ran": True}

@on("ui:connect", key="device")
def shell(args):
    return {}
"##;
    let schema = introspect(src).unwrap();
    println!("PY AUTH: {schema}");
    assert_eq!(schema["receives"]["ui:turn.submit"]["auth"], true);
    assert_eq!(schema["receives"]["ui:turn.submit"]["key"], "device");
    assert!(schema["receives"]["ui:connect"].get("auth").is_none(), "{schema}");
    assert!(schema.get("auth").is_none(), "no second table: {schema}");
}
