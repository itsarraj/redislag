//! Parsing for `SLOWLOG GET`'s reply.
//!
//! Each entry is itself a RESP array. Redis has shipped two shapes over
//! time:
//!
//! - 4 fields (old): `[id, timestamp, duration_usec, args]`
//! - 6 fields (Redis 4+, what every currently-supported version sends):
//!   `[id, timestamp, duration_usec, args, client_addr, client_name]`
//!
//! Both are handled; the 4-field shape just leaves `client_addr`/
//! `client_name` as `None`.

use crate::resp::RespValue;
use anyhow::{bail, Context, Result};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SlowlogEntry {
    pub id: i64,
    pub timestamp: i64,
    pub duration_usec: i64,
    pub args: Vec<String>,
    pub client_addr: Option<String>,
    pub client_name: Option<String>,
}

pub fn parse_slowlog(value: &RespValue) -> Result<Vec<SlowlogEntry>> {
    let entries = match value {
        RespValue::Array(Some(entries)) => entries,
        RespValue::Array(None) => return Ok(Vec::new()),
        other => bail!("expected SLOWLOG GET to return an array, got {other:?}"),
    };

    entries.iter().map(parse_entry).collect()
}

fn parse_entry(entry: &RespValue) -> Result<SlowlogEntry> {
    let fields = entry
        .as_array()
        .context("slowlog entry was not itself an array")?;
    if fields.len() < 4 {
        bail!(
            "slowlog entry had {} field(s), expected at least 4",
            fields.len()
        );
    }
    let id = fields[0]
        .as_integer()
        .context("slowlog entry field 0 (id) was not an integer")?;
    let timestamp = fields[1]
        .as_integer()
        .context("slowlog entry field 1 (timestamp) was not an integer")?;
    let duration_usec = fields[2]
        .as_integer()
        .context("slowlog entry field 2 (duration_usec) was not an integer")?;
    let args = fields[3]
        .as_array()
        .context("slowlog entry field 3 (args) was not an array")?
        .iter()
        .map(|v| v.as_str().unwrap_or_default())
        .collect();
    let client_addr = fields.get(4).and_then(|v| v.as_str());
    let client_name = fields.get(5).and_then(|v| v.as_str());

    Ok(SlowlogEntry {
        id,
        timestamp,
        duration_usec,
        args,
        client_addr,
        client_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[test]
    fn parses_six_field_entry_matching_real_redis_7_output() {
        // Captured verbatim (same field order/values) from a real
        // `redis:7-alpine` container's SLOWLOG GET after DEBUG SLEEP 0.2.
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::Integer(0),
            RespValue::Integer(1789888171),
            RespValue::Integer(200068),
            RespValue::Array(Some(vec![bulk("DEBUG"), bulk("SLEEP"), bulk("0.2")])),
            bulk("172.17.0.1:34778"),
            bulk(""),
        ]))]));
        let entries = parse_slowlog(&value).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.id, 0);
        assert_eq!(e.timestamp, 1789888171);
        assert_eq!(e.duration_usec, 200068);
        assert_eq!(e.args, vec!["DEBUG", "SLEEP", "0.2"]);
        assert_eq!(e.client_addr.as_deref(), Some("172.17.0.1:34778"));
        assert_eq!(e.client_name.as_deref(), Some(""));
    }

    #[test]
    fn parses_four_field_legacy_entry() {
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::Integer(1),
            RespValue::Integer(1000000),
            RespValue::Integer(500),
            RespValue::Array(Some(vec![bulk("GET"), bulk("key")])),
        ]))]));
        let entries = parse_slowlog(&value).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].client_addr, None);
        assert_eq!(entries[0].client_name, None);
    }

    #[test]
    fn empty_slowlog_is_empty_vec_not_error() {
        let value = RespValue::Array(Some(Vec::new()));
        let entries = parse_slowlog(&value).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn nil_array_is_empty_vec_not_error() {
        let value = RespValue::Array(None);
        let entries = parse_slowlog(&value).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn non_array_top_level_is_an_error_not_a_panic() {
        let value = RespValue::Integer(0);
        assert!(parse_slowlog(&value).is_err());
    }

    #[test]
    fn entry_with_too_few_fields_is_an_error_not_a_panic() {
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::Integer(0),
            RespValue::Integer(0),
        ]))]));
        assert!(parse_slowlog(&value).is_err());
    }

    #[test]
    fn multiple_entries_parsed_in_order() {
        let make = |id: i64| {
            RespValue::Array(Some(vec![
                RespValue::Integer(id),
                RespValue::Integer(1),
                RespValue::Integer(1),
                RespValue::Array(Some(vec![bulk("PING")])),
            ]))
        };
        let value = RespValue::Array(Some(vec![make(2), make(1), make(0)]));
        let entries = parse_slowlog(&value).unwrap();
        assert_eq!(
            entries.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![2, 1, 0]
        );
    }
}
