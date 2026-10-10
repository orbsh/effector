# bgi nushell fixture (ADR-0035 §8, two-fifo adapter): the author script
# — `def main [req rep]` IS the loop, the parent spawns `nu <this> <req>
# <rep>` and speaks the SAME line protocol as the pipes shape (frames on
# req, ctx replies on rep, results on stdout). The dispatch is a match on
# event names — nu has no eval and `source` rejects dynamic paths, so the
# entry function is the hand-written shape every language without a
# runtime name lookup uses (the dispatch-table ruling 2026-09-29).
# Host ctx calls ride literal-name defs (the author's own helpers; the
# frame is TYPED — the discriminator `type`, ADR-0037 §2; JSON encoding
# only: nu's stdlib has no CBOR codec, the entrance criterion).
#
# Pitfalls measured on this shape (do not "simplify" them away):
# - the reply fifo is a SECOND channel: two readers on one fifo race the
#   outer loop against the inline ctx read (measured deadlock).
# - `else` must sit on the same line as the branch's `}`.
# - `$env` writes do NOT escape an `each` closure — the batch loop is a
#   `for` (block scope, persists), the PTY carrier's residency rule.

def ctx.invoke [args] {
    print ({host: {type: "invoke", args: $args}} | to json --raw)
    let rep = (open --raw $env.BGI_REP | lines | first | from json)
    $rep.host_reply.ok? | default {__error: ($rep.host_reply.error? | default "")}
}

def ctx.store [instruction] {
    print ({host: {type: "store", op: $instruction}} | to json --raw)
    let rep = (open --raw $env.BGI_REP | lines | first | from json)
    $rep.host_reply.ok? | default {__error: ($rep.host_reply.error? | default "")}
}

# ctx.emit (ADR-0043 §5): fire-and-forget by contract — the parent
# resolves routes, sends, and answers NOTHING; a publish failure lands
# on the parent's stderr. Reading a reply here would block the loop.
def ctx.emit [event data] {
    print ({host: {type: "emit", args: {event: $event, data: $data}}} | to json --raw)
}

def --env interface_schema [] {
    { receives: { echo: {}, sum: {}, ctx_round_trip: {}, store_round_trip: {}, count: {} },
      wildcard_receives: [],
      lifecycle: { idle_ttl: "5m" },
      storage: { collections: { counters: { schema: {
        key_len: 8,
        key_fields: [{name: id, ty: U64, width: 8, offset: 0, tag: 0}],
        layout_version: 1, hot_width: 8, payload_header_len: 3,
        hot_fields: [{name: count, ty: U64, width: 8, offset: 0, tag: 0}],
        cold_fields: [],
        slots: {primary: 0, dynamic: 1, dict_id: 2, dict_name: 3,
                declared_index_base: 4096, declared_reduce_base: 8192,
                junction_base: 12288}
      }}}}}
}

def --env dispatch [m] {
    let args = ($m.args? | default {})
    let kind = ($m.kind? | default "call")
    let event = ($m.event? | default "")
    # Unified seam (ADR-0036): a plain handler arrives as either kind —
    # `call` (the carrier-internal primitive: introspection, effector-side
    # tests) or `iterate_start` (the dispatch seam: invoke is the stream
    # whose first round is terminal — the bare reply below is wrapped to
    # {done:true,value} by the carrier).
    if ($kind == "call" or $kind == "iterate_start") and $event != "stream" {
        match $event {
            "interface_schema" => { interface_schema }
            "echo" => { {echoed: $args} }
            # nu pipelines survive the bgi shape untouched (the retired
            # PTY carrier's structured-data demos).
            "sum" => { {sum: ($args.items | math sum)} }
            "ctx_round_trip" => { {invoked: (ctx.invoke $args)} }
            "store_round_trip" => {
                let put = ($args | get -o put | default {})
                ctx.store $put | ignore
                let read_back = (ctx.store ($args | get -o get | default {}))
                {read_back: $read_back}
            }
            "count" => { $env.C = (($env.C? | default 0) + 1); {count: $env.C} }
            _ => { {error: "unknown handler"} }
        }
    } else if $kind == "iterate_start" and $event == "stream" {
        $env.SG = {pulled: 1, total: ($args.total? | default 1)}
        if ($args.total? | default 1) == 0 { {done: true} } else { {item: "i0", done: false} }
    } else if $kind == "iterate_next" {
        let g = ($env.SG? | default {pulled: 0, total: 0})
        if $g.pulled >= $g.total { $env.SG = null; {done: true} } else { $env.SG = ($g | update pulled ($g.pulled + 1)); {item: $"i($g.pulled)", done: false} }
    } else {
        $env.SG = null
        {}
    }
}

def main [req: string, rep: string] {
    $env.BGI_REP = $rep
    loop {
        for line in (open --raw $req | lines) {
            let m = ($line | from json)
            print ({id: $m.id, result: (dispatch $m)} | to json --raw)
        }
    }
}
