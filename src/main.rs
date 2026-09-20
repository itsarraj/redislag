use anyhow::{Context, Result};
use clap::Parser;
use redislag::{build_lag_report, parse_info_replication, parse_slowlog, RedisClient};
use serde::Serialize;
use std::time::Duration;

/// Reports Redis replication lag and slowlog contents.
#[derive(Parser, Debug)]
#[command(name = "redislag", version, about)]
struct Args {
    /// Redis host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Redis port.
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Password, if the server requires AUTH.
    #[arg(long)]
    password: Option<String>,

    /// Connect/read/write timeout in seconds.
    #[arg(long, default_value_t = 5)]
    timeout: u64,

    /// How many SLOWLOG entries to fetch (0 disables the slowlog section).
    #[arg(long, default_value_t = 10)]
    slowlog_count: i64,

    /// Emit machine-readable JSON instead of the human-readable report.
    #[arg(long)]
    json: bool,

    /// Exit with status 1 if any replica's byte-lag exceeds this many
    /// bytes. CI/cron-friendly gate; omitted, no threshold is enforced.
    #[arg(long)]
    max_lag_bytes: Option<u64>,
}

#[derive(Serialize)]
struct Report {
    host: String,
    port: u16,
    replication: redislag::LagReport,
    slowlog: Vec<redislag::SlowlogEntry>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timeout = Duration::from_secs(args.timeout);

    let mut client = RedisClient::connect(&args.host, args.port, timeout)
        .with_context(|| format!("connecting to redis at {}:{}", args.host, args.port))?;

    if let Some(password) = &args.password {
        client.auth(password)?;
    }

    client
        .ping()
        .context("PING failed after connecting — is this actually a Redis server?")?;

    let info_text = client.info_replication()?;
    let info = parse_info_replication(&info_text);
    let lag_report = build_lag_report(&info);

    let slowlog = if args.slowlog_count > 0 {
        let raw = client.slowlog_get(args.slowlog_count)?;
        parse_slowlog(&raw)?
    } else {
        Vec::new()
    };

    let exceeded = args
        .max_lag_bytes
        .is_some_and(|max| lag_report.replicas.iter().any(|r| r.bytes_behind > max));

    if args.json {
        let report = Report {
            host: args.host.clone(),
            port: args.port,
            replication: lag_report,
            slowlog,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&args.host, args.port, &lag_report, &slowlog);
    }

    if exceeded {
        std::process::exit(1);
    }
    Ok(())
}

fn print_human(
    host: &str,
    port: u16,
    report: &redislag::LagReport,
    slowlog: &[redislag::SlowlogEntry],
) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    // BrokenPipe-tolerant: piping into `head`/`less` shouldn't panic.
    macro_rules! w {
        ($($arg:tt)*) => {
            if writeln!(out, $($arg)*).is_err() {
                return;
            }
        };
    }

    w!("redis://{host}:{port}  role={}", report.role);
    w!("master_repl_offset: {}", report.master_repl_offset);

    if let Some(note) = &report.note {
        w!("{note}");
    }
    if let Some(ro) = &report.replica_of {
        w!(
            "  master: {}:{}  link={}  last_io_seconds_ago={}",
            ro.master_host,
            ro.master_port,
            ro.link_status,
            ro.last_io_seconds_ago
                .map(|s| s.to_string())
                .unwrap_or_else(|| "?".to_string())
        );
    }
    for r in &report.replicas {
        let lag = r
            .lag_seconds
            .map(|s| format!("{s}s"))
            .unwrap_or_else(|| "?".to_string());
        w!(
            "  replica[{}] {} state={} bytes_behind={} lag={}",
            r.index,
            r.addr,
            r.state,
            r.bytes_behind,
            lag
        );
    }

    w!();
    if slowlog.is_empty() {
        w!("slowlog: empty");
    } else {
        w!(
            "slowlog ({} entr{}):",
            slowlog.len(),
            if slowlog.len() == 1 { "y" } else { "ies" }
        );
        for e in slowlog {
            let addr = e.client_addr.as_deref().unwrap_or("?");
            w!(
                "  #{} {}ms  {}  [{}]",
                e.id,
                e.duration_usec as f64 / 1000.0,
                e.args.join(" "),
                addr
            );
        }
    }
}
