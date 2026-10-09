use effector_runtime::carrier::steel::introspect;

#[test]
fn steel_introspect_derives_and_merges() {
    let src = r##"
(on "add_to_cart" "user_id" (lambda (args) (hash "ok" #t)))
(on "order.*" "" (lambda (args) #t))

(define (interface_schema args)
  (hash "lifecycle" (hash "idle_ttl" "5m")))
"##;
    let schema = introspect(src).unwrap();
    println!("STEEL: {schema}");
    assert_eq!(schema["receives"]["add_to_cart"]["key"], "user_id");
    assert_eq!(schema["wildcard_receives"][0], "order.*");
    assert_eq!(schema["lifecycle"]["idle_ttl"], "5m");
}

#[test]
fn steel_introspect_derived_only() {
    let src = r##"
(on "remove_from_cart" "" (lambda (args) #t))
"##;
    let schema = introspect(src).unwrap();
    assert!(schema["receives"]["remove_from_cart"].is_object());
    assert!(schema["wildcard_receives"].as_array().unwrap().is_empty());
}

#[test]
fn steel_explicit_receives_survive_merge() {
    // The shadowing bug this locks: the collector always contributes a
    // (possibly empty) `receives` map; the old top-level `or_insert` let
    // that empty map win, silently dropping a HAND-WRITTEN receives block
    // — exactly the shape steel schema-literal booths declare (no `on`
    // calls at all, PLAN 4.9 steel/nushell form).
    let src = r##"
(define (interface_schema args)
  (hash "receives" (hash "evt.a" (hash "key" "k"))
        "wildcard_receives" (list "evt.w.*")
        "lifecycle" (hash "idle_ttl" "3m")))
(define (a args) #t)
"##;
    let schema = introspect(src).unwrap();
    assert_eq!(schema["receives"]["evt.a"]["key"], "k", "explicit receives survive merge: {schema}");
    assert_eq!(schema["wildcard_receives"][0], "evt.w.*");
    assert_eq!(schema["lifecycle"]["idle_ttl"], "3m");
}

#[test]
fn steel_collector_and_explicit_receives_deep_merge() {
    // Both sources contribute their own keys; a collision keeps the
    // collector's (decorator-wins, the python merge contract).
    let src = r##"
(on "from.collector" "uid" (lambda (args) #t))
(define (interface_schema args)
  (hash "receives" (hash "from.explicit" (hash "key" "k")
                         "from.collector" (hash "key" "shadowed"))))
"##;
    let schema = introspect(src).unwrap();
    assert!(schema["receives"]["from.collector"].is_object());
    assert_eq!(schema["receives"]["from.collector"]["key"], "uid", "collector wins the collision");
    assert_eq!(schema["receives"]["from.explicit"]["key"], "k", "explicit-only key contributes");
}
