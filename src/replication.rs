//! Parsing for `INFO replication`'s `# Replication` section, and turning
//! that into a lag report.
//!
//! The section is plain `key:value` lines, one directive Redis has used
//! unchanged since the early `INFO` command. The two shapes that matter
//! here:
//!
//! ```text
//! role:master
//! connected_slaves:1
//! slave0:ip=172.17.0.3,port=6380,state=online,offset=14210,lag=0
//! master_repl_offset:14210
//! ```
//!
//! or, on a replica:
//!
//! ```text
//! role:slave
//! master_host:172.17.0.2
//! master_port:6379
//! master_link_status:up
//! master_last_io_seconds_ago:0
//! master_repl_offset:14210
//! ```

use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SlaveInfo {
    pub index: u32,
    pub ip: String,
    pub port: u16,
    pub state: String,
    pub offset: u64,
    /// Present on Redis versions that report per-replica ack lag directly
    /// (seconds since the master last heard from this replica).
    pub lag_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct ReplicationInfo {
    pub role: String,
    pub connected_slaves: u32,
    pub master_repl_offset: u64,
    pub slaves: Vec<SlaveInfo>,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub master_link_status: Option<String>,
    pub master_last_io_seconds_ago: Option<i64>,
}

/// Parse one `slaveN:ip=...,port=...,state=...,offset=...,lag=...` value
/// (the part after the first `:`) into its comma-separated `key=value`
/// fields.
fn parse_kv_csv(value: &str) -> HashMap<String, String> {
    value
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub fn parse_info_replication(text: &str) -> ReplicationInfo {
    let mut info = ReplicationInfo::default();
    let mut slaves = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if let Some(idx_str) = key.strip_prefix("slave") {
            if let Ok(index) = idx_str.parse::<u32>() {
                let fields = parse_kv_csv(value);
                let ip = fields.get("ip").cloned().unwrap_or_default();
                let port = fields.get("port").and_then(|p| p.parse().ok()).unwrap_or(0);
                let state = fields.get("state").cloned().unwrap_or_default();
                let offset = fields
                    .get("offset")
                    .and_then(|o| o.parse().ok())
                    .unwrap_or(0);
                let lag_seconds = fields.get("lag").and_then(|l| l.parse().ok());
                slaves.push(SlaveInfo {
                    index,
                    ip,
                    port,
                    state,
                    offset,
                    lag_seconds,
                });
                continue;
            }
        }
        match key {
            "role" => info.role = value.to_string(),
            "connected_slaves" => info.connected_slaves = value.parse().unwrap_or(0),
            "master_repl_offset" => info.master_repl_offset = value.parse().unwrap_or(0),
            "master_host" => info.master_host = Some(value.to_string()),
            "master_port" => info.master_port = value.parse().ok(),
            "master_link_status" => info.master_link_status = Some(value.to_string()),
            "master_last_io_seconds_ago" => info.master_last_io_seconds_ago = value.parse().ok(),
            _ => {}
        }
    }

    slaves.sort_by_key(|s| s.index);
    info.slaves = slaves;
    info
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplicaLag {
    pub index: u32,
    pub addr: String,
    pub state: String,
    /// Bytes the master's replication stream is ahead of what this
    /// replica has acknowledged. Derived locally (`master_repl_offset -
    /// slave_offset`) so it's meaningful even on Redis versions that
    /// don't report `lag=` themselves.
    pub bytes_behind: u64,
    /// Redis's own `lag=` field when present (seconds since last ACK).
    pub lag_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LagReport {
    pub role: String,
    pub master_repl_offset: u64,
    pub replicas: Vec<ReplicaLag>,
    /// Set when this instance is itself a replica: bytes it hasn't yet
    /// received relative to its *own* last-known offset can't be computed
    /// without asking the master, so this only ever reports link health.
    pub replica_of: Option<ReplicaOfStatus>,
    /// Human-readable summary of the no-replica / role case, e.g.
    /// "no replicas connected — lag N/A".
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplicaOfStatus {
    pub master_host: String,
    pub master_port: u16,
    pub link_status: String,
    pub last_io_seconds_ago: Option<i64>,
}

pub fn build_lag_report(info: &ReplicationInfo) -> LagReport {
    if info.role == "slave" {
        let replica_of = match (&info.master_host, info.master_port) {
            (Some(host), Some(port)) => Some(ReplicaOfStatus {
                master_host: host.clone(),
                master_port: port,
                link_status: info
                    .master_link_status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                last_io_seconds_ago: info.master_last_io_seconds_ago,
            }),
            _ => None,
        };
        return LagReport {
            role: info.role.clone(),
            master_repl_offset: info.master_repl_offset,
            replicas: Vec::new(),
            replica_of,
            note: Some(
                "this instance is a replica; byte-lag behind its own master isn't \
                 observable from here without also querying the master"
                    .to_string(),
            ),
        };
    }

    if info.connected_slaves == 0 || info.slaves.is_empty() {
        return LagReport {
            role: info.role.clone(),
            master_repl_offset: info.master_repl_offset,
            replicas: Vec::new(),
            replica_of: None,
            note: Some("no replicas connected — lag N/A".to_string()),
        };
    }

    let replicas = info
        .slaves
        .iter()
        .map(|s| ReplicaLag {
            index: s.index,
            addr: format!("{}:{}", s.ip, s.port),
            state: s.state.clone(),
            bytes_behind: info.master_repl_offset.saturating_sub(s.offset),
            lag_seconds: s.lag_seconds,
        })
        .collect();

    LagReport {
        role: info.role.clone(),
        master_repl_offset: info.master_repl_offset,
        replicas,
        replica_of: None,
        note: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_master_with_no_replicas() {
        let text = "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\nmaster_failover_state:no-failover\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.role, "master");
        assert_eq!(info.connected_slaves, 0);
        assert_eq!(info.master_repl_offset, 0);
        assert!(info.slaves.is_empty());
    }

    #[test]
    fn parses_master_with_one_replica_and_lag_field() {
        let text = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=172.17.0.3,port=6380,state=online,offset=14210,lag=0\r\nmaster_repl_offset:14210\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.connected_slaves, 1);
        assert_eq!(info.slaves.len(), 1);
        let s = &info.slaves[0];
        assert_eq!(s.ip, "172.17.0.3");
        assert_eq!(s.port, 6380);
        assert_eq!(s.state, "online");
        assert_eq!(s.offset, 14210);
        assert_eq!(s.lag_seconds, Some(0));
    }

    #[test]
    fn parses_multiple_replicas_out_of_order_and_sorts_by_index() {
        let text = "role:master\r\nconnected_slaves:2\r\nslave1:ip=10.0.0.2,port=6379,state=online,offset=100\r\nslave0:ip=10.0.0.1,port=6379,state=online,offset=200\r\nmaster_repl_offset:200\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.slaves.len(), 2);
        assert_eq!(info.slaves[0].index, 0);
        assert_eq!(info.slaves[1].index, 1);
    }

    #[test]
    fn parses_replica_role_fields() {
        let text = "role:slave\r\nmaster_host:10.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:2\r\nmaster_repl_offset:9999\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.role, "slave");
        assert_eq!(info.master_host.as_deref(), Some("10.0.0.1"));
        assert_eq!(info.master_port, Some(6379));
        assert_eq!(info.master_link_status.as_deref(), Some("up"));
        assert_eq!(info.master_last_io_seconds_ago, Some(2));
    }

    #[test]
    fn ignores_comment_and_blank_lines() {
        let text = "# Replication\r\n\r\nrole:master\r\n\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.role, "master");
    }

    #[test]
    fn tolerates_missing_fields_in_slave_line() {
        // A hypothetical truncated line shouldn't panic; missing numeric
        // fields fall back to 0 rather than erroring the whole parse.
        let text =
            "role:master\r\nconnected_slaves:1\r\nslave0:ip=10.0.0.1\r\nmaster_repl_offset:50\r\n";
        let info = parse_info_replication(text);
        assert_eq!(info.slaves.len(), 1);
        assert_eq!(info.slaves[0].port, 0);
        assert_eq!(info.slaves[0].offset, 0);
    }

    #[test]
    fn report_for_master_with_no_replicas_is_lag_na() {
        let info = ReplicationInfo {
            role: "master".into(),
            connected_slaves: 0,
            master_repl_offset: 0,
            ..Default::default()
        };
        let report = build_lag_report(&info);
        assert!(report.replicas.is_empty());
        assert!(report.note.unwrap().contains("N/A"));
    }

    #[test]
    fn report_computes_bytes_behind_from_offsets() {
        let info = ReplicationInfo {
            role: "master".into(),
            connected_slaves: 1,
            master_repl_offset: 1000,
            slaves: vec![SlaveInfo {
                index: 0,
                ip: "10.0.0.5".into(),
                port: 6379,
                state: "online".into(),
                offset: 850,
                lag_seconds: None,
            }],
            ..Default::default()
        };
        let report = build_lag_report(&info);
        assert_eq!(report.replicas.len(), 1);
        assert_eq!(report.replicas[0].bytes_behind, 150);
        assert_eq!(report.replicas[0].addr, "10.0.0.5:6379");
    }

    #[test]
    fn report_bytes_behind_never_underflows_if_slave_offset_races_ahead() {
        // Shouldn't happen in practice, but a stale read could momentarily
        // see this; saturating_sub must not panic/wrap.
        let info = ReplicationInfo {
            role: "master".into(),
            connected_slaves: 1,
            master_repl_offset: 100,
            slaves: vec![SlaveInfo {
                index: 0,
                ip: "10.0.0.5".into(),
                port: 6379,
                state: "online".into(),
                offset: 500,
                lag_seconds: None,
            }],
            ..Default::default()
        };
        let report = build_lag_report(&info);
        assert_eq!(report.replicas[0].bytes_behind, 0);
    }

    #[test]
    fn report_for_replica_role_reports_link_status_not_bytes() {
        let info = ReplicationInfo {
            role: "slave".into(),
            master_host: Some("10.0.0.1".into()),
            master_port: Some(6379),
            master_link_status: Some("up".into()),
            master_last_io_seconds_ago: Some(1),
            master_repl_offset: 500,
            ..Default::default()
        };
        let report = build_lag_report(&info);
        assert!(report.replicas.is_empty());
        let ro = report.replica_of.unwrap();
        assert_eq!(ro.master_host, "10.0.0.1");
        assert_eq!(ro.link_status, "up");
    }
}
