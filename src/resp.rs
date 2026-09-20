//! A minimal RESP2 (REdis Serialization Protocol) encoder/decoder.
//!
//! Redis's wire protocol is deliberately simple — five types, each starting
//! with a one-byte sigil, everything terminated by `\r\n`. This implements
//! exactly the subset `INFO`, `SLOWLOG GET`, `SLOWLOG LEN`, `AUTH`, and
//! `PING` actually use. No pipelining, no RESP3 (`HELLO 3`) push types —
//! Redis still answers plain RESP2 to a client that never asks for RESP3,
//! which is all this tool needs.

use anyhow::{anyhow, bail, Context, Result};
use std::io::{BufRead, Write};

/// A decoded RESP value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    /// `None` is a RESP nil bulk string (`$-1\r\n`).
    BulkString(Option<Vec<u8>>),
    /// `None` is a RESP nil array (`*-1\r\n`).
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Convenience: bulk string bytes as UTF-8, lossily if needed.
    pub fn as_str(&self) -> Option<String> {
        match self {
            RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            RespValue::SimpleString(s) => Some(s.clone()),
            RespValue::Integer(i) => Some(i.to_string()),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            RespValue::Array(Some(v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            _ => None,
        }
    }
}

/// Encode a command as a RESP "array of bulk strings" — the format every
/// real Redis client sends requests in, regardless of the command.
pub fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        let bytes = a.as_bytes();
        out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub fn write_command<W: Write>(w: &mut W, args: &[&str]) -> Result<()> {
    w.write_all(&encode_command(args))
        .context("writing command to redis")?;
    w.flush().context("flushing command to redis")?;
    Ok(())
}

/// Read one line ending in `\r\n`, returning it without the terminator.
fn read_line<R: BufRead>(r: &mut R) -> Result<String> {
    let mut line = String::new();
    let n = r.read_line(&mut line).context("reading line from redis")?;
    if n == 0 {
        bail!("connection closed by redis while reading a line");
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(line)
}

/// Read exactly `n` bytes followed by the mandatory `\r\n` terminator.
fn read_exact_crlf<R: BufRead>(r: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)
        .context("reading bulk string payload from redis")?;
    let mut crlf = [0u8; 2];
    r.read_exact(&mut crlf)
        .context("reading bulk string terminator from redis")?;
    if &crlf != b"\r\n" {
        bail!("malformed RESP bulk string: missing CRLF terminator");
    }
    Ok(buf)
}

/// Read and decode one RESP value from the stream.
pub fn read_value<R: BufRead>(r: &mut R) -> Result<RespValue> {
    let line = read_line(r)?;
    let (sigil, rest) = line
        .split_at_checked(1)
        .ok_or_else(|| anyhow!("empty line where a RESP type sigil was expected"))?;
    match sigil {
        "+" => Ok(RespValue::SimpleString(rest.to_string())),
        "-" => Ok(RespValue::Error(rest.to_string())),
        ":" => {
            let i: i64 = rest
                .parse()
                .with_context(|| format!("malformed RESP integer: {rest:?}"))?;
            Ok(RespValue::Integer(i))
        }
        "$" => {
            let n: i64 = rest
                .parse()
                .with_context(|| format!("malformed RESP bulk string length: {rest:?}"))?;
            if n < 0 {
                Ok(RespValue::BulkString(None))
            } else {
                let bytes = read_exact_crlf(r, n as usize)?;
                Ok(RespValue::BulkString(Some(bytes)))
            }
        }
        "*" => {
            let n: i64 = rest
                .parse()
                .with_context(|| format!("malformed RESP array length: {rest:?}"))?;
            if n < 0 {
                Ok(RespValue::Array(None))
            } else {
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(read_value(r)?);
                }
                Ok(RespValue::Array(Some(items)))
            }
        }
        other => bail!("unrecognized RESP type sigil {other:?} (line: {line:?})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encodes_command_as_bulk_string_array() {
        let out = encode_command(&["SLOWLOG", "GET"]);
        assert_eq!(out, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n");
    }

    #[test]
    fn encodes_zero_arg_command() {
        // Not realistic for this tool, but the encoder shouldn't special-case it.
        let out = encode_command(&[]);
        assert_eq!(out, b"*0\r\n");
    }

    #[test]
    fn decodes_simple_string() {
        let mut c = Cursor::new(b"+OK\r\n".to_vec());
        assert_eq!(
            read_value(&mut c).unwrap(),
            RespValue::SimpleString("OK".into())
        );
    }

    #[test]
    fn decodes_error() {
        let mut c = Cursor::new(b"-ERR unknown command\r\n".to_vec());
        assert_eq!(
            read_value(&mut c).unwrap(),
            RespValue::Error("ERR unknown command".into())
        );
    }

    #[test]
    fn decodes_integer() {
        let mut c = Cursor::new(b":1000\r\n".to_vec());
        assert_eq!(read_value(&mut c).unwrap(), RespValue::Integer(1000));
    }

    #[test]
    fn decodes_negative_integer() {
        let mut c = Cursor::new(b":-1\r\n".to_vec());
        assert_eq!(read_value(&mut c).unwrap(), RespValue::Integer(-1));
    }

    #[test]
    fn decodes_bulk_string() {
        let mut c = Cursor::new(b"$5\r\nhello\r\n".to_vec());
        assert_eq!(
            read_value(&mut c).unwrap(),
            RespValue::BulkString(Some(b"hello".to_vec()))
        );
    }

    #[test]
    fn decodes_empty_bulk_string() {
        let mut c = Cursor::new(b"$0\r\n\r\n".to_vec());
        assert_eq!(
            read_value(&mut c).unwrap(),
            RespValue::BulkString(Some(Vec::new()))
        );
    }

    #[test]
    fn decodes_nil_bulk_string() {
        let mut c = Cursor::new(b"$-1\r\n".to_vec());
        assert_eq!(read_value(&mut c).unwrap(), RespValue::BulkString(None));
    }

    #[test]
    fn decodes_nil_array() {
        let mut c = Cursor::new(b"*-1\r\n".to_vec());
        assert_eq!(read_value(&mut c).unwrap(), RespValue::Array(None));
    }

    #[test]
    fn decodes_nested_array_like_slowlog_get() {
        // A single slowlog entry: [id, timestamp, duration_usec, [args...], client_addr, client_name]
        let raw = b"*1\r\n*6\r\n:0\r\n:1789888171\r\n:200068\r\n*3\r\n$5\r\nDEBUG\r\n$5\r\nSLEEP\r\n$3\r\n0.2\r\n$16\r\n172.17.0.1:34778\r\n$0\r\n\r\n";
        let mut c = Cursor::new(raw.to_vec());
        let v = read_value(&mut c).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        let entry = arr[0].as_array().unwrap();
        assert_eq!(entry.len(), 6);
        assert_eq!(entry[0].as_integer(), Some(0));
        assert_eq!(entry[2].as_integer(), Some(200068));
        let args = entry[3].as_array().unwrap();
        assert_eq!(args[0].as_str().unwrap(), "DEBUG");
    }

    #[test]
    fn malformed_length_is_error_not_panic() {
        let mut c = Cursor::new(b"$notanumber\r\nhello\r\n".to_vec());
        assert!(read_value(&mut c).is_err());
    }

    #[test]
    fn truncated_stream_is_error_not_panic() {
        let mut c = Cursor::new(b"$5\r\nhel".to_vec());
        assert!(read_value(&mut c).is_err());
    }

    #[test]
    fn empty_stream_is_error_not_panic() {
        let mut c = Cursor::new(Vec::<u8>::new());
        assert!(read_value(&mut c).is_err());
    }
}
