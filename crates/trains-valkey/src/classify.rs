//! Static read/write classification of Redis commands.
//!
//! State-machine replication needs to route **reads** to the local replica and
//! **mutating writes** through total-order broadcast. The table below reflects
//! Redis command *semantics* (does it mutate?), independent of whether the
//! RD-1 [`crate::store::MemStore`] happens to implement the command — when the
//! apply target is a real `redis-server` (RD-4), every classified write is
//! applicable.
//!
//! A third bucket, [`Class::NonDeterministic`], names the mutating commands
//! whose result depends on server-side randomness/time/iteration state. Those
//! must be resolved to a deterministic *effect* at the origin before broadcast
//! (PR-RD-2). Until then the proxy rejects them rather than silently diverging
//! replicas.

/// How a command must be handled by the replication layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Read-only — answer from the local replica, no broadcast.
    Read,
    /// Deterministic mutation — broadcast verbatim; every replica applies it.
    Write,
    /// Mutation whose effect is non-deterministic at the originating node and
    /// must be resolved before broadcast (PR-RD-2). Rejected in RD-1.
    NonDeterministic,
    /// Not in the RD-1 command table (or a connection/admin command we don't
    /// replicate). Rejected.
    Unsupported,
}

/// Classify a command by its upper-cased name.
pub fn classify(name: &str) -> Class {
    match name {
        // ── Connection / no-op (answered locally, never broadcast) ──
        "PING" | "ECHO" | "COMMAND" | "HELLO" => Class::Read,

        // ── Reads ──
        "GET" | "MGET" | "STRLEN" | "GETRANGE" | "SUBSTR" | "EXISTS" | "TYPE"
        | "TTL" | "PTTL" | "KEYS" | "DBSIZE" | "HGET" | "HMGET" | "HGETALL"
        | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS" | "HSTRLEN" | "LLEN" | "LRANGE"
        | "LINDEX" | "SCARD" | "SMEMBERS" | "SISMEMBER" | "ZCARD" | "ZSCORE"
        | "ZRANGE" | "GETBIT" | "BITCOUNT"
        // Non-deterministic *reads*: their result varies across replicas but
        // they mutate nothing, so a deterministic local answer cannot diverge
        // state (PR-RD-2). They are answered locally, never broadcast.
        | "SRANDMEMBER" | "RANDOMKEY" | "TIME" | "SCAN" | "HSCAN" | "SSCAN"
        | "ZSCAN" => Class::Read,

        // ── Deterministic writes ──
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "MSET"
        | "MSETNX" | "APPEND" | "SETRANGE" | "SETBIT" | "DEL" | "UNLINK"
        | "INCR" | "INCRBY" | "DECR" | "DECRBY" | "EXPIRE" | "PEXPIRE"
        | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "RENAME" | "RENAMENX"
        | "COPY" | "HSET" | "HSETNX" | "HMSET" | "HDEL" | "HINCRBY" | "SADD"
        | "SREM" | "SMOVE" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LSET"
        | "LREM" | "ZADD" | "ZREM" | "ZINCRBY" | "FLUSHDB" | "FLUSHALL" => {
            Class::Write
        }

        // ── Non-deterministic *mutations* — the origin resolves the effect and
        //    broadcasts the deterministic rewrite (PR-RD-2):
        //    SPOP→SREM, INCRBYFLOAT→SET, HINCRBYFLOAT→HSET. ──
        "SPOP" | "INCRBYFLOAT" | "HINCRBYFLOAT" => Class::NonDeterministic,

        _ => Class::Unsupported,
    }
}

/// Convenience predicate: does this command mutate state (deterministically or
/// not)? Used by the proxy to decide read-vs-write routing.
pub fn is_write(name: &str) -> bool {
    matches!(classify(name), Class::Write | Class::NonDeterministic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_are_reads() {
        for c in ["GET", "HGETALL", "EXISTS", "TTL", "LLEN", "PING"] {
            assert_eq!(classify(c), Class::Read, "{c}");
        }
    }

    #[test]
    fn nondeterministic_reads_are_local_reads() {
        // PR-RD-2: these vary per replica but mutate nothing → answered locally.
        for c in ["SRANDMEMBER", "RANDOMKEY", "TIME", "SCAN", "HSCAN", "SSCAN"] {
            assert_eq!(classify(c), Class::Read, "{c}");
            assert!(!is_write(c), "{c} must not be broadcast");
        }
    }

    #[test]
    fn deterministic_writes() {
        for c in ["SET", "DEL", "HSET", "INCR", "EXPIRE", "LPUSH", "SADD"] {
            assert_eq!(classify(c), Class::Write, "{c}");
            assert!(is_write(c), "{c}");
        }
    }

    #[test]
    fn nondeterministic_mutations_flagged_not_plain_write() {
        // Only the *mutating* non-deterministic commands need effect resolution.
        for c in ["SPOP", "INCRBYFLOAT", "HINCRBYFLOAT"] {
            assert_eq!(classify(c), Class::NonDeterministic, "{c}");
            assert!(is_write(c), "{c} mutates state");
        }
    }

    #[test]
    fn unknown_is_unsupported() {
        assert_eq!(classify("WUTANG"), Class::Unsupported);
        assert!(!is_write("WUTANG"));
    }
}
