//! Effect replication: resolve a non-deterministic mutating command to a
//! deterministic effect at the *originating* node (PR-RD-2).
//!
//! Some Redis writes have a result that depends on server-side randomness or
//! arithmetic the protocol can't reproduce identically on each replica
//! (`SPOP` removes a *random* member; `INCRBYFLOAT`/`HINCRBYFLOAT` depend on
//! float representation). If broadcast verbatim, each replica would resolve the
//! non-determinism independently and **diverge**.
//!
//! The fix — exactly what Redis's own replication and RedisRaft do — is to
//! resolve the non-determinism *once, at the origin*, against the committed
//! local state, and broadcast the resulting **deterministic effect**:
//!
//! | command        | effect broadcast      | client reply (the resolved value) |
//! |----------------|-----------------------|-----------------------------------|
//! | `SPOP k`       | `SREM k <member>`     | the popped member (bulk)          |
//! | `SPOP k n`     | `SREM k <m1>…<mn>`    | the popped members (array)        |
//! | `INCRBYFLOAT`  | `SET k <resolved>`    | the resolved value (bulk)         |
//! | `HINCRBYFLOAT` | `HSET k f <resolved>` | the resolved value (bulk)         |
//!
//! The effect is applied in total order on every replica → convergence. The
//! origin returns the resolved value to its client (which differs from the
//! effect's own apply reply — e.g. `SREM` returns a count, but the client
//! issued `SPOP` and expects the member). That resolved reply rides the
//! `WriteOp` (`client_reply`) so it is released when the effect is delivered.
//!
//! **Resolution reads committed state.** A command must be resolved *after* its
//! inputs are committed; under concurrent multi-writer there is a
//! resolve-vs-apply window (the effect may no-op if an interleaved write
//! changed the key first). That window is the standard "resolve at origin"
//! limitation, acceptable for AO's mostly-single-writer-per-key coordination
//! plane and tightened by PR-RD-3 (dedup) / single-writer assumptions.

use crate::command::Command;
use crate::resp::Reply;
use crate::store::RedisStore;

/// The outcome of resolving a non-deterministic command.
#[derive(Debug)]
pub enum Resolution {
    /// Answer the client now; nothing to broadcast (empty pop, type error, …).
    Immediate(Reply),
    /// Broadcast this deterministic effect command; when it is delivered back,
    /// return `client_reply` to the originating client.
    Broadcast {
        argv: Vec<Vec<u8>>,
        client_reply: Reply,
    },
}

/// Resolve a non-deterministic mutating command against committed local state.
pub fn resolve<S: RedisStore>(cmd: &Command, store: &S) -> Resolution {
    match cmd.name.as_str() {
        "SPOP" => resolve_spop(cmd, store),
        "INCRBYFLOAT" => resolve_incrbyfloat(cmd, store),
        "HINCRBYFLOAT" => resolve_hincrbyfloat(cmd, store),
        other => Resolution::Immediate(Reply::error(format!(
            "ERR '{other}' is not a resolvable non-deterministic command"
        ))),
    }
}

fn resolve_spop<S: RedisStore>(cmd: &Command, store: &S) -> Resolution {
    let Some(key) = cmd.key() else {
        return Resolution::Immediate(wrongargs("spop"));
    };
    // Read committed members in deterministic order (SMEMBERS = BTreeSet order).
    let members = match store.query(&q(&[b"SMEMBERS", key])) {
        Reply::Array(items) => items
            .into_iter()
            .filter_map(|r| match r {
                Reply::Bulk(b) => Some(b),
                _ => None,
            })
            .collect::<Vec<_>>(),
        Reply::Error(e) => return Resolution::Immediate(Reply::Error(e)),
        _ => Vec::new(),
    };

    match cmd.arg(2) {
        // SPOP key — pop a single member. "Random" is resolved to a concrete
        // deterministic choice by the origin (first in iteration order); the
        // effect SREM is what every replica applies, so they converge.
        None => match members.into_iter().next() {
            Some(m) => Resolution::Broadcast {
                argv: vec![b"SREM".to_vec(), key.to_vec(), m.clone()],
                client_reply: Reply::Bulk(m),
            },
            None => Resolution::Immediate(Reply::Nil),
        },
        // SPOP key count — pop up to `count` members.
        Some(count_b) => {
            let count = std::str::from_utf8(count_b)
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok());
            let Some(count) = count else {
                return Resolution::Immediate(Reply::error(
                    "ERR value is not an integer or out of range",
                ));
            };
            let take = count.max(0) as usize;
            let popped: Vec<Vec<u8>> = members.into_iter().take(take).collect();
            if popped.is_empty() {
                return Resolution::Immediate(Reply::Array(vec![]));
            }
            let mut argv = vec![b"SREM".to_vec(), key.to_vec()];
            argv.extend(popped.iter().cloned());
            Resolution::Broadcast {
                argv,
                client_reply: Reply::Array(popped.into_iter().map(Reply::Bulk).collect()),
            }
        }
    }
}

fn resolve_incrbyfloat<S: RedisStore>(cmd: &Command, store: &S) -> Resolution {
    let (Some(key), Some(incr_b)) = (cmd.arg(1), cmd.arg(2)) else {
        return Resolution::Immediate(wrongargs("incrbyfloat"));
    };
    let Some(incr) = parse_f64(incr_b) else {
        return Resolution::Immediate(Reply::error("ERR value is not a valid float"));
    };
    let cur = match store.query(&q(&[b"GET", key])) {
        Reply::Bulk(b) => match parse_f64(&b) {
            Some(v) => v,
            None => return Resolution::Immediate(Reply::error("ERR value is not a valid float")),
        },
        Reply::Nil => 0.0,
        Reply::Error(e) => return Resolution::Immediate(Reply::Error(e)),
        _ => 0.0,
    };
    let next = cur + incr;
    if !next.is_finite() {
        return Resolution::Immediate(Reply::error("ERR increment would produce NaN or Infinity"));
    }
    let formatted = format_float(next).into_bytes();
    Resolution::Broadcast {
        argv: vec![b"SET".to_vec(), key.to_vec(), formatted.clone()],
        client_reply: Reply::Bulk(formatted),
    }
}

fn resolve_hincrbyfloat<S: RedisStore>(cmd: &Command, store: &S) -> Resolution {
    let (Some(key), Some(field), Some(incr_b)) = (cmd.arg(1), cmd.arg(2), cmd.arg(3)) else {
        return Resolution::Immediate(wrongargs("hincrbyfloat"));
    };
    let Some(incr) = parse_f64(incr_b) else {
        return Resolution::Immediate(Reply::error("ERR value is not a valid float"));
    };
    let cur = match store.query(&q(&[b"HGET", key, field])) {
        Reply::Bulk(b) => match parse_f64(&b) {
            Some(v) => v,
            None => return Resolution::Immediate(Reply::error("ERR hash value is not a float")),
        },
        Reply::Nil => 0.0,
        Reply::Error(e) => return Resolution::Immediate(Reply::Error(e)),
        _ => 0.0,
    };
    let next = cur + incr;
    if !next.is_finite() {
        return Resolution::Immediate(Reply::error("ERR increment would produce NaN or Infinity"));
    }
    let formatted = format_float(next).into_bytes();
    Resolution::Broadcast {
        argv: vec![b"HSET".to_vec(), key.to_vec(), field.to_vec(), formatted.clone()],
        client_reply: Reply::Bulk(formatted),
    }
}

/// Build a read [`Command`] from raw parts (always non-empty → `unwrap` safe).
fn q(parts: &[&[u8]]) -> Command {
    Command::parse(parts.iter().map(|p| p.to_vec()).collect()).expect("non-empty argv")
}

fn parse_f64(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    let v: f64 = s.parse().ok()?;
    v.is_finite().then_some(v)
}

/// Format a float the way the resolved value should be stored/returned: Rust's
/// shortest round-trip representation (`12.0` → "12", `11.5` → "11.5"), which
/// matches Redis's trailing-zero trimming closely enough for the model.
fn format_float(v: f64) -> String {
    format!("{v}")
}

fn wrongargs(name: &str) -> Reply {
    Reply::error(format!("ERR wrong number of arguments for '{name}' command"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn cmd(parts: &[&str]) -> Command {
        Command::parse(parts.iter().map(|s| s.as_bytes().to_vec()).collect()).unwrap()
    }

    #[test]
    fn spop_resolves_to_srem_of_a_concrete_member() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["SADD", "s", "a", "b", "c"]));
        match resolve(&cmd(&["SPOP", "s"]), &store) {
            Resolution::Broadcast { argv, client_reply } => {
                assert_eq!(argv[0], b"SREM");
                assert_eq!(argv[1], b"s");
                let member = argv[2].clone();
                // The popped member is a real member, returned to the client.
                assert_eq!(client_reply, Reply::Bulk(member.clone()));
                assert!([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].contains(&member));
            }
            other => panic!("expected Broadcast, got {other:?}"),
        }
    }

    #[test]
    fn spop_is_deterministic_across_calls_on_same_state() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["SADD", "s", "a", "b", "c"]));
        let r1 = resolve(&cmd(&["SPOP", "s"]), &store);
        let r2 = resolve(&cmd(&["SPOP", "s"]), &store);
        // Same committed state ⇒ same resolution (this is what guarantees that
        // an origin's effect is reproducible / auditable).
        let m = |r: Resolution| match r {
            Resolution::Broadcast { argv, .. } => argv[2].clone(),
            _ => panic!("expected broadcast"),
        };
        assert_eq!(m(r1), m(r2));
    }

    #[test]
    fn spop_count_resolves_to_multi_srem() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["SADD", "s", "a", "b", "c", "d"]));
        match resolve(&cmd(&["SPOP", "s", "2"]), &store) {
            Resolution::Broadcast { argv, client_reply } => {
                assert_eq!(argv[0], b"SREM");
                assert_eq!(argv.len(), 4); // SREM key m1 m2
                match client_reply {
                    Reply::Array(items) => assert_eq!(items.len(), 2),
                    other => panic!("expected array reply, got {other:?}"),
                }
            }
            other => panic!("expected Broadcast, got {other:?}"),
        }
    }

    #[test]
    fn spop_empty_is_immediate_nil() {
        let store = MemStore::new();
        assert!(matches!(
            resolve(&cmd(&["SPOP", "missing"]), &store),
            Resolution::Immediate(Reply::Nil)
        ));
    }

    #[test]
    fn incrbyfloat_resolves_to_set() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["SET", "price", "10.5"]));
        match resolve(&cmd(&["INCRBYFLOAT", "price", "0.1"]), &store) {
            Resolution::Broadcast { argv, client_reply } => {
                assert_eq!(argv[0], b"SET");
                assert_eq!(argv[1], b"price");
                assert_eq!(argv[2], b"10.6");
                assert_eq!(client_reply, Reply::Bulk(b"10.6".to_vec()));
            }
            other => panic!("expected Broadcast, got {other:?}"),
        }
    }

    #[test]
    fn incrbyfloat_on_missing_starts_from_zero() {
        let store = MemStore::new();
        match resolve(&cmd(&["INCRBYFLOAT", "x", "5"]), &store) {
            Resolution::Broadcast { argv, .. } => assert_eq!(argv[2], b"5"),
            other => panic!("expected Broadcast, got {other:?}"),
        }
    }

    #[test]
    fn incrbyfloat_on_nonfloat_is_immediate_error() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["SET", "x", "abc"]));
        assert!(matches!(
            resolve(&cmd(&["INCRBYFLOAT", "x", "1"]), &store),
            Resolution::Immediate(Reply::Error(_))
        ));
    }

    #[test]
    fn hincrbyfloat_resolves_to_hset() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["HSET", "h", "f", "1.0"]));
        match resolve(&cmd(&["HINCRBYFLOAT", "h", "f", "2.5"]), &store) {
            Resolution::Broadcast { argv, client_reply } => {
                assert_eq!(argv[0], b"HSET");
                assert_eq!(argv[3], b"3.5");
                assert_eq!(client_reply, Reply::Bulk(b"3.5".to_vec()));
            }
            other => panic!("expected Broadcast, got {other:?}"),
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // R-01 (T-tr-10) — regression: non-finite floats must NEVER reach the
    // broadcast path. The protection lives in two complementary places:
    //   - `parse_f64` filters NaN / ±Inf at parse time (the increment),
    //   - `next.is_finite()` in the resolver catches overflow (cur+incr).
    // Both paths return Resolution::Immediate(Reply::Error) without
    // broadcasting. These tests lock that contract in.
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn incrbyfloat_with_nan_increment_is_rejected_at_parse() {
        let store = MemStore::new();
        match resolve(&cmd(&["INCRBYFLOAT", "x", "nan"]), &store) {
            Resolution::Immediate(Reply::Error(msg)) => {
                assert!(msg.contains("float"),
                        "expected 'float' in error, got {msg:?}");
            }
            other => panic!("expected Immediate Error, got {other:?}"),
        }
    }

    #[test]
    fn incrbyfloat_with_inf_increment_is_rejected_at_parse() {
        let store = MemStore::new();
        for s in ["inf", "+inf", "-inf", "infinity", "-infinity"] {
            assert!(
                matches!(resolve(&cmd(&["INCRBYFLOAT", "x", s]), &store),
                         Resolution::Immediate(Reply::Error(_))),
                "expected Immediate Error for increment={s:?}"
            );
        }
    }

    #[test]
    fn incrbyfloat_overflow_to_infinity_is_rejected_at_resolve() {
        // cur = 1e308 ; incr = 1e308 → next = +inf
        let mut store = MemStore::new();
        store.apply(&cmd(&["SET", "x", "1e308"]));
        match resolve(&cmd(&["INCRBYFLOAT", "x", "1e308"]), &store) {
            Resolution::Immediate(Reply::Error(msg)) => {
                assert!(msg.contains("NaN") || msg.contains("Infinity"),
                        "expected NaN/Infinity in error, got {msg:?}");
            }
            other => panic!("expected Immediate Error, got {other:?}"),
        }
    }

    #[test]
    fn hincrbyfloat_with_nan_increment_is_rejected_at_parse() {
        let store = MemStore::new();
        assert!(matches!(
            resolve(&cmd(&["HINCRBYFLOAT", "h", "f", "nan"]), &store),
            Resolution::Immediate(Reply::Error(_))
        ));
    }

    #[test]
    fn hincrbyfloat_overflow_to_infinity_is_rejected_at_resolve() {
        let mut store = MemStore::new();
        store.apply(&cmd(&["HSET", "h", "f", "1e308"]));
        match resolve(&cmd(&["HINCRBYFLOAT", "h", "f", "1e308"]), &store) {
            Resolution::Immediate(Reply::Error(msg)) => {
                assert!(msg.contains("NaN") || msg.contains("Infinity"),
                        "expected NaN/Infinity in error, got {msg:?}");
            }
            other => panic!("expected Immediate Error, got {other:?}"),
        }
    }

    #[test]
    fn parse_f64_filters_non_finite() {
        // White-box test on the parse helper itself — guards both
        // INCRBYFLOAT and HINCRBYFLOAT against non-finite increments.
        assert_eq!(parse_f64(b"1.5"), Some(1.5));
        assert_eq!(parse_f64(b"-0.0"), Some(-0.0));
        assert_eq!(parse_f64(b"nan"), None);
        assert_eq!(parse_f64(b"NaN"), None);
        assert_eq!(parse_f64(b"inf"), None);
        assert_eq!(parse_f64(b"-inf"), None);
        assert_eq!(parse_f64(b"infinity"), None);
        assert_eq!(parse_f64(b"abc"), None);
        assert_eq!(parse_f64(b""), None);
    }
}
