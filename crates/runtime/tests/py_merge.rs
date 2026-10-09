use effector_runtime::carrier::python::introspect;

#[test]
fn py_schema_merges_decorators_and_explicit() {
    // The decisive case: decorators contribute receives, the explicit
    // declaration contributes lifecycle — the merged schema has BOTH.
    let src = r##"
@on("add_to_cart", key="user_id")
def add(args):
    return {}

def interface_schema(args=None):
    return {"lifecycle": {"idle_ttl": "5m"}}
"##;
    let schema = introspect(src).unwrap();
    println!("MERGED: {schema}");
    assert_eq!(schema["receives"]["add_to_cart"]["key"], "user_id");
    assert_eq!(schema["lifecycle"]["idle_ttl"], "5m");
}
