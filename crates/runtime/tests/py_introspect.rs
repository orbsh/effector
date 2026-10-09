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
