//! Parsed command + the wire envelope that rides the TRAINS ring.
//!
//! A [`Command`] is a decoded `argv` with its name normalised to upper-case for
//! classification/dispatch (Redis command names are case-insensitive). A
//! [`WriteOp`] is what the proxy `oBroadcast`s for a mutating command: the
//! command itself plus the `(origin, request_id)` identity that PR-RD-3 will use
//! for at-least-once dedup. We carry it now so the wire format is stable from
//! the first PR.

use serde::{Deserialize, Serialize};
use trains_core::ProcId;

use crate::resp::Reply;

/// A decoded client command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Upper-cased command name (e.g. `"SET"`), for classification + dispatch.
    pub name: String,
    /// The raw argument vector exactly as received; `argv[0]` is the original
    /// (possibly mixed-case) command name, `argv[1..]` the operands.
    pub argv: Vec<Vec<u8>>,
}

impl Command {
    /// Parse an `argv` into a command. Returns `None` for an empty `argv`.
    pub fn parse(argv: Vec<Vec<u8>>) -> Option<Command> {
        let first = argv.first()?;
        let name = String::from_utf8_lossy(first).to_ascii_uppercase();
        Some(Command { name, argv })
    }

    /// Operand `i` (1-based: `arg(1)` is the first operand after the name).
    pub fn arg(&self, i: usize) -> Option<&[u8]> {
        self.argv.get(i).map(|v| v.as_slice())
    }

    /// The key operand (`arg(1)`), present for the vast majority of commands.
    pub fn key(&self) -> Option<&[u8]> {
        self.arg(1)
    }

    /// Operand `i` decoded as a UTF-8 string (lossy).
    pub fn arg_str(&self, i: usize) -> Option<String> {
        self.arg(i).map(|b| String::from_utf8_lossy(b).into_owned())
    }

    /// Number of operands (excludes the command name).
    pub fn operand_count(&self) -> usize {
        self.argv.len().saturating_sub(1)
    }
}

/// The unit broadcast over the ring for a mutating command.
///
/// `origin` + `request_id` uniquely identify the write so (a) the originating
/// node can match the delivered op back to the waiting client, and (b) PR-RD-3
/// can dedup at-least-once re-broadcasts. The protocol treats the encoded bytes
/// as an opaque [`trains_core::Payload::data`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOp {
    /// Node that received this write from a client.
    pub origin: ProcId,
    /// Per-origin monotonically increasing request id.
    pub request_id: u64,
    /// The full command `argv` (name + operands). For a deterministic write
    /// this is the client's command verbatim; for a resolved non-deterministic
    /// command (PR-RD-2) this is the *effect* (e.g. `SPOP`→`SREM member`).
    pub argv: Vec<Vec<u8>>,
    /// The reply the *originating* client should receive, when it differs from
    /// the effect's own apply result. `Some` only for resolved non-deterministic
    /// commands (e.g. `SPOP` returns the popped member, but its effect `SREM`
    /// returns a count). `None` for deterministic writes — the origin returns
    /// the apply result. Non-origin replicas ignore this field.
    #[serde(default)]
    pub client_reply: Option<Reply>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteOpError {
    #[error("bincode encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("bincode decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

impl WriteOp {
    /// A deterministic write op (the origin returns the apply result).
    pub fn new(origin: ProcId, request_id: u64, argv: Vec<Vec<u8>>) -> Self {
        WriteOp { origin, request_id, argv, client_reply: None }
    }

    /// Attach an origin-resolved client reply (PR-RD-2 effect replication).
    pub fn with_client_reply(mut self, reply: Reply) -> Self {
        self.client_reply = Some(reply);
        self
    }

    /// Encode for transport as a `Payload`'s opaque bytes. Matches the bincode
    /// conventions used by `trains-net::codec` (`bincode::serde`, standard cfg).
    pub fn encode(&self) -> Result<Vec<u8>, WriteOpError> {
        Ok(bincode::serde::encode_to_vec(self, bincode::config::standard())?)
    }

    /// Decode bytes produced by [`WriteOp::encode`].
    pub fn decode(bytes: &[u8]) -> Result<WriteOp, WriteOpError> {
        let (op, _) =
            bincode::serde::decode_from_slice::<WriteOp, _>(bytes, bincode::config::standard())?;
        Ok(op)
    }

    /// The command carried by this op.
    pub fn command(&self) -> Option<Command> {
        Command::parse(self.argv.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_parse_uppercases_name_keeps_raw() {
        let c = Command::parse(vec![b"set".to_vec(), b"K".to_vec(), b"v".to_vec()]).unwrap();
        assert_eq!(c.name, "SET");
        assert_eq!(c.argv[0], b"set"); // raw preserved
        assert_eq!(c.key(), Some(&b"K"[..]));
        assert_eq!(c.arg(2), Some(&b"v"[..]));
        assert_eq!(c.operand_count(), 2);
    }

    #[test]
    fn command_parse_empty_is_none() {
        assert!(Command::parse(vec![]).is_none());
    }

    #[test]
    fn writeop_roundtrips() {
        let op = WriteOp::new(2, 7, vec![b"INCR".to_vec(), b"counter".to_vec()]);
        let bytes = op.encode().unwrap();
        let back = WriteOp::decode(&bytes).unwrap();
        assert_eq!(op, back);
        assert_eq!(back.command().unwrap().name, "INCR");
    }

    #[test]
    fn writeop_binary_safe() {
        let op = WriteOp::new(0, 0, vec![b"SET".to_vec(), b"k".to_vec(), vec![0u8, 1, 2, 255]]);
        let back = WriteOp::decode(&op.encode().unwrap()).unwrap();
        assert_eq!(back.argv[2], vec![0u8, 1, 2, 255]);
    }
}
