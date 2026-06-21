//! RESP2 protocol: an incremental request decoder and a reply encoder.
//!
//! Scope is exactly what the write-interception proxy needs (PR-RD-1):
//!   * decode client **requests** — RESP arrays of bulk strings (what every
//!     real client sends), plus the inline form (`SET a 1\r\n`) so a node can
//!     be poked with `nc`/`redis-cli --pipe`-free tooling in tests.
//!   * encode **replies** — the RESP2 reply value tree.
//!
//! We do not parse replies here: in RD-1 the apply target is the in-process
//! [`crate::store::MemStore`], which returns a [`Reply`] directly. A reply
//! parser only becomes necessary when the apply target is a real `redis-server`
//! over TCP (PR-RD-4 / on-EC2), where the proxy forwards raw bytes and reads a
//! reply back — that lands with the real backend, not in this skeleton.

/// A RESP2 reply value.
///
/// `Serialize`/`Deserialize` so an origin-resolved reply for a non-deterministic
/// command can ride the `WriteOp` to be released to the originating client when
/// the effect is delivered (PR-RD-2).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Reply {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:1000\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`
    Bulk(Vec<u8>),
    /// `$-1\r\n` — null bulk string (e.g. `GET` of a missing key).
    Nil,
    /// `*-1\r\n` — null array.
    NilArray,
    /// `*N\r\n …`
    Array(Vec<Reply>),
}

impl Reply {
    /// Convenience: a simple-string `OK`.
    pub fn ok() -> Reply {
        Reply::Simple("OK".to_string())
    }

    /// Convenience: an error reply with the conventional `ERR ` prefix already
    /// applied iff the message doesn't already start with an upper-case code.
    pub fn error(msg: impl Into<String>) -> Reply {
        Reply::Error(msg.into())
    }

    /// Append the wire encoding of this reply to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Reply::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Integer(i) => {
                out.push(b':');
                out.extend_from_slice(i.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Bulk(b) => {
                out.push(b'$');
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Reply::Nil => out.extend_from_slice(b"$-1\r\n"),
            Reply::NilArray => out.extend_from_slice(b"*-1\r\n"),
            Reply::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for it in items {
                    it.encode(out);
                }
            }
        }
    }

    /// The wire encoding as a fresh `Vec`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RespError {
    #[error("protocol error: {0}")]
    Protocol(&'static str),
}

// ── Client side: encode a request, parse a reply ─────────────────────────────
//
// Used by the real-engine backend (PR-RD-4), which forwards commands to a
// co-located `redis-server`/Valkey over a blocking connection and reads one
// reply back. (RD-1..3 only needed the server side: decode requests, encode
// replies.)

/// Encode a command `argv` as a RESP2 client request (array of bulk strings).
pub fn encode_request(argv: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", argv.len()).into_bytes();
    for a in argv {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn parse_reply_int(b: &[u8]) -> std::io::Result<i64> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad RESP integer"))
}

/// Read one RESP2 reply from a blocking buffered reader. Binary-safe for bulk
/// strings (so `DUMP` blobs round-trip). Recurses for arrays.
pub fn read_reply<R: std::io::BufRead>(r: &mut R) -> std::io::Result<Reply> {
    use std::io::{Error, ErrorKind};
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line)? == 0 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "eof reading reply"));
    }
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    if line.is_empty() {
        return Err(Error::new(ErrorKind::InvalidData, "empty reply line"));
    }
    let body = &line[1..];
    Ok(match line[0] {
        b'+' => Reply::Simple(String::from_utf8_lossy(body).into_owned()),
        b'-' => Reply::Error(String::from_utf8_lossy(body).into_owned()),
        b':' => Reply::Integer(parse_reply_int(body)?),
        b'$' => {
            let len = parse_reply_int(body)?;
            if len < 0 {
                Reply::Nil
            } else {
                let mut buf = vec![0u8; len as usize + 2]; // payload + CRLF
                r.read_exact(&mut buf)?;
                buf.truncate(len as usize);
                Reply::Bulk(buf)
            }
        }
        b'*' => {
            let len = parse_reply_int(body)?;
            if len < 0 {
                Reply::NilArray
            } else {
                let mut items = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    items.push(read_reply(r)?);
                }
                Reply::Array(items)
            }
        }
        other => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown RESP reply marker {other:?}"),
            ))
        }
    })
}

/// An incremental RESP request decoder.
///
/// Feed it bytes as they arrive off the socket with [`RespDecoder::feed`], then
/// pull complete commands with [`RespDecoder::next_command`]. A command is the
/// raw argument vector (`argv`): `argv[0]` is the command name, the rest are
/// operands. Returns `Ok(None)` when more bytes are needed; the buffered
/// partial frame is retained.
#[derive(Default)]
pub struct RespDecoder {
    buf: Vec<u8>,
}

impl RespDecoder {
    pub fn new() -> Self {
        RespDecoder { buf: Vec::new() }
    }

    /// Append freshly-read socket bytes.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Try to decode one complete command, consuming its bytes from the buffer.
    pub fn next_command(&mut self) -> Result<Option<Vec<Vec<u8>>>, RespError> {
        match parse_command(&self.buf)? {
            Some((argv, used)) => {
                self.buf.drain(..used);
                Ok(Some(argv))
            }
            None => Ok(None),
        }
    }
}

/// A decoded command (`argv`) plus the number of input bytes it consumed.
type ParsedFrame = (Vec<Vec<u8>>, usize);

/// Find the first CRLF; return (line-without-crlf, bytes-consumed-incl-crlf).
fn parse_crlf_line(b: &[u8]) -> Option<(&[u8], usize)> {
    let idx = b.windows(2).position(|w| w == b"\r\n")?;
    Some((&b[..idx], idx + 2))
}

/// Parse one command from the front of `b`. Returns the argv and the number of
/// bytes consumed, or `None` if `b` does not yet hold a complete command.
fn parse_command(b: &[u8]) -> Result<Option<ParsedFrame>, RespError> {
    if b.is_empty() {
        return Ok(None);
    }
    if b[0] != b'*' {
        return parse_inline(b);
    }

    let mut cur = 0usize;
    let (header, used) = match parse_crlf_line(&b[cur..]) {
        Some(x) => x,
        None => return Ok(None),
    };
    let n: i64 = std::str::from_utf8(&header[1..])
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or(RespError::Protocol("bad multibulk header"))?;
    cur += used;
    if n <= 0 {
        // Empty inline of the form "*0\r\n" — treat as a no-op command.
        return Ok(Some((Vec::new(), cur)));
    }

    let mut argv: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let (bulk_hdr, used) = match parse_crlf_line(&b[cur..]) {
            Some(x) => x,
            None => return Ok(None),
        };
        if bulk_hdr.first() != Some(&b'$') {
            return Err(RespError::Protocol("expected bulk-string element"));
        }
        let len: i64 = std::str::from_utf8(&bulk_hdr[1..])
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or(RespError::Protocol("bad bulk length"))?;
        cur += used;
        if len < 0 {
            argv.push(Vec::new());
            continue;
        }
        let len = len as usize;
        if b[cur..].len() < len + 2 {
            return Ok(None); // need the payload + its trailing CRLF
        }
        if &b[cur + len..cur + len + 2] != b"\r\n" {
            return Err(RespError::Protocol("missing CRLF after bulk payload"));
        }
        argv.push(b[cur..cur + len].to_vec());
        cur += len + 2;
    }
    Ok(Some((argv, cur)))
}

/// Inline command form: a single `\n`-terminated (optionally `\r\n`) line split
/// on ASCII whitespace. Tolerant by design — used for hand-driven testing.
fn parse_inline(b: &[u8]) -> Result<Option<ParsedFrame>, RespError> {
    let nl = match b.iter().position(|&c| c == b'\n') {
        Some(i) => i,
        None => return Ok(None),
    };
    let mut line = &b[..nl];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let argv: Vec<Vec<u8>> = line
        .split(|&c| c == b' ' || c == b'\t')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect();
    Ok(Some((argv, nl + 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_and_error_and_int() {
        assert_eq!(Reply::ok().to_bytes(), b"+OK\r\n");
        assert_eq!(Reply::error("ERR nope").to_bytes(), b"-ERR nope\r\n");
        assert_eq!(Reply::Integer(42).to_bytes(), b":42\r\n");
        assert_eq!(Reply::Integer(-3).to_bytes(), b":-3\r\n");
    }

    #[test]
    fn encode_bulk_and_nils() {
        assert_eq!(Reply::Bulk(b"hello".to_vec()).to_bytes(), b"$5\r\nhello\r\n");
        assert_eq!(Reply::Nil.to_bytes(), b"$-1\r\n");
        assert_eq!(Reply::NilArray.to_bytes(), b"*-1\r\n");
    }

    #[test]
    fn encode_nested_array() {
        let r = Reply::Array(vec![Reply::Bulk(b"a".to_vec()), Reply::Integer(1)]);
        assert_eq!(r.to_bytes(), b"*2\r\n$1\r\na\r\n:1\r\n");
    }

    #[test]
    fn decode_multibulk_set() {
        let mut d = RespDecoder::new();
        d.feed(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
        let argv = d.next_command().unwrap().unwrap();
        assert_eq!(argv, vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]);
        assert!(d.next_command().unwrap().is_none(), "buffer drained");
    }

    #[test]
    fn decode_inline() {
        let mut d = RespDecoder::new();
        d.feed(b"SET a 1\r\n");
        let argv = d.next_command().unwrap().unwrap();
        assert_eq!(argv, vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn decode_partial_then_complete() {
        let mut d = RespDecoder::new();
        d.feed(b"*2\r\n$3\r\nGET\r\n$1\r\n"); // value bytes missing
        assert!(d.next_command().unwrap().is_none());
        d.feed(b"a\r\n");
        let argv = d.next_command().unwrap().unwrap();
        assert_eq!(argv, vec![b"GET".to_vec(), b"a".to_vec()]);
    }

    #[test]
    fn decode_two_pipelined_commands() {
        let mut d = RespDecoder::new();
        d.feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nx\r\n");
        assert_eq!(d.next_command().unwrap().unwrap(), vec![b"PING".to_vec()]);
        assert_eq!(
            d.next_command().unwrap().unwrap(),
            vec![b"GET".to_vec(), b"x".to_vec()]
        );
        assert!(d.next_command().unwrap().is_none());
    }

    #[test]
    fn decode_binary_safe_payload() {
        // Bulk strings carry arbitrary bytes, including embedded CRLF.
        let mut d = RespDecoder::new();
        d.feed(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n");
        let argv = d.next_command().unwrap().unwrap();
        assert_eq!(argv[2], b"a\r\nb".to_vec());
    }

    #[test]
    fn encode_request_roundtrips_through_the_decoder() {
        let req = encode_request(&[b"SET", b"k", b"v"]);
        let mut d = RespDecoder::new();
        d.feed(&req);
        assert_eq!(
            d.next_command().unwrap().unwrap(),
            vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]
        );
    }

    #[test]
    fn read_reply_handles_each_type() {
        use std::io::Cursor;
        let mut c = Cursor::new(b"+OK\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::ok());
        let mut c = Cursor::new(b"-ERR bad\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::Error("ERR bad".into()));
        let mut c = Cursor::new(b":42\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::Integer(42));
        let mut c = Cursor::new(b"$5\r\nhello\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::Bulk(b"hello".to_vec()));
        let mut c = Cursor::new(b"$-1\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::Nil);
        let mut c = Cursor::new(b"*2\r\n$1\r\na\r\n:7\r\n".to_vec());
        assert_eq!(
            read_reply(&mut c).unwrap(),
            Reply::Array(vec![Reply::Bulk(b"a".to_vec()), Reply::Integer(7)])
        );
    }

    #[test]
    fn read_reply_bulk_is_binary_safe() {
        use std::io::Cursor;
        // A DUMP blob can contain CRLF and NULs; the length prefix governs.
        let mut c = Cursor::new(b"$4\r\n\x00\r\n\xff\r\n".to_vec());
        assert_eq!(read_reply(&mut c).unwrap(), Reply::Bulk(vec![0, b'\r', b'\n', 0xff]));
    }
}
