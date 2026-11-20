# 0006 — the node states the box it is, and nothing about what it is for

Status: Accepted (2026-11-20)

## Context

`Register` carried an alias, carriers and a class. An alias is a name a CONNECTION answers to: it can be reclaimed by anyone who dials with it (ADR-0015 §7 replacement discipline), it is chosen by the deployment, and it disappears with the process. So "the same node" meant "whatever most recently dialled as `x`", and the wire could not answer the two questions that fact leaves open:

- A box that restarts under a different alias looks like a different machine. Anything the plane had learned about it — placement, history, a grant — belongs to a name that no longer exists.
- Two boxes claiming one alias show up as a takeover, never as "which hardware".

And labels could not fill the gap: ADR-0005 §2b settled that a node cannot name its labels without being able to name someone else's. So the registration frame had nothing about the hardware in it at all.

The question: what may a node say about itself, if it may not say what it is FOR?

## Decision

1. **`Frame::Register` gains `machine`: an opaque string naming the box.** Not a credential, not authorization, and not a claim on anyone's privilege — a statement about the hardware the process is running on. Empty means "unidentified", which is a fact and not an error.

2. **The node fills it from the first source that answers**, in this order:
   1. `capabilities.machine` — the deployment's word. It wins because an operator running twelve nodes knows which box each is on, and a container's `/sys` is the host's or nobody's.
   2. The hardware's own serial: SMBIOS UUID, then product serial, then board serial, then the device tree's `serial-number`. A container normally sees the host's DMI, which is exactly what "which box is this" needs.
   3. An id MINTED ONCE AND KEPT in the node's state directory (`$EFFECTOR_STATE_DIR`, else `$XDG_STATE_HOME/effector`, else `~/.local/state/effector`). This is the case for a VM, a jail with `/sys` hidden, an SBC with no DMI: there is no serial to read, and a node that reported nothing would restart its own history every time it reconnected. `create_new` makes the write race-safe: two nodes starting together on one box, one wins, the other reads the winner's id.
   4. Nothing available → empty (unidentified).

3. **What makes self-reporting safe is uniqueness, not harmlessness.** The distinction is not "privilege vs no privilege"; it is whether a wrong claim can be TOLD APART from a right one.

   A machine identifier is unique by construction: it belongs to one box, nobody can guess it, and two nodes reporting it is a contradiction — a copied serial or a lied-about box, and the plane can say so (aura reports it as `same_machine`). That report IS the check that lets the value be self-declared.

   A label is the opposite on every axis. `site:home` is a small, guessable, publicly meaningful string; MANY machines are legitimately `site:home`, so a wrong claim collides harmlessly with a right one and nothing contradicts it. An identifier cannot be guessed and cannot be shared without something being wrong; a label is guessable and shared by design.

4. **Nothing about labels changes.** They remain the admitting party's word (ADR-0005 §2b), minted from a grant at admission and handed to whoever holds the connection.

## Rejected

- **Derive the identity from `/etc/machine-id`.** It is the OS INSTALLATION's identity: it resets on reinstall, so a reinstalled box would silently become a new machine while its serial stayed the same. Borrowing it puts the node's identity under somebody else's lifecycle.
- **Hash the alias and call that the machine.** It is derived from the thing that is unstable by definition; a rename would fork the machine.
- **Mint an id in memory when no state directory exists.** An id that changes on every launch is worse than an empty one: empty says "unidentified" and the plane can decide, while a fresh id every run looks like an endless supply of new boxes.
- **Make the machine part of the alias.** It would fuse the name a connection answers to with the box it runs on, so a box could never be renamed and a name could never be moved — and the takeover rule (ADR-0015 §7) exists precisely because those are different things.

## Consequences

- **A box is identifiable across reconnects and renames**, which is what lets a grant, a placement or an operator's history attach to hardware instead of to a name a process chose.
- **The plane keeps both facts separately**: the alias it routes by, the machine it correlates by. Two connections reporting one machine are reported — the same box under another name, or a serial that two nodes both claim — and nothing decides which reading is right.
- **An unidentified node is not every other unidentified node.** Empty never matches empty: two boxes that could not say who they are must not be conflated by a filter.
- **Nothing authenticates on the machine id.** If a future decision wants grants bound to a serial, verification has to come from admission (the registry), not from this field.

## Tests

- `crates/runtime/src/remote.rs` `machine_tests` — an id is minted once, written to the state directory and read back unchanged; a file already on disk is never overwritten; a stated machine wins and an empty one falls through the chain without panicking.
- `crates/runtime/tests/remote.rs` — the node's registration carries the deployment's `machine`, asserted as received by the plane.
- aura `crates/realm/src/pool.rs` — a machine seen under two aliases is reported, not merged; `by_machine` answers for a real serial and refuses `""`.

## Related records

ADR-0003 (one token at the dial; the connection is the authority — this record adds a fact, not a proof), ADR-0005 §2b (labels are the control plane's word), ADR-0015 §7 (alias replacement is reported, never silent), aura ADR-0053 (the pool that records both).