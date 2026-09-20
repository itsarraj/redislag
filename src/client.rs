//! A tiny synchronous Redis client: connect, optionally `AUTH`, send a
//! command, read back one RESP value. Deliberately not a general-purpose
//! client — no connection pooling, no pipelining, no pub/sub — just enough
//! to run the handful of commands `redislag` needs.

use crate::resp::{read_value, write_command, RespValue};
use anyhow::{bail, Context, Result};
use std::io::BufReader;
use std::net::TcpStream;
use std::time::Duration;

pub struct RedisClient {
    write_half: TcpStream,
    read_half: BufReader<TcpStream>,
}

impl RedisClient {
    /// Connect to `host:port` with the given timeout applied to the
    /// connect, every read, and every write.
    pub fn connect(host: &str, port: u16, timeout: Duration) -> Result<Self> {
        let addr = format!("{host}:{port}");
        let mut last_err = None;
        let mut stream = None;
        for sockaddr in std::net::ToSocketAddrs::to_socket_addrs(&addr)
            .with_context(|| format!("resolving {addr}"))?
        {
            match TcpStream::connect_timeout(&sockaddr, timeout) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }
        let stream = stream.ok_or_else(|| {
            anyhow::anyhow!(
                "could not connect to {addr}: {}",
                last_err.map(|e| e.to_string()).unwrap_or_default()
            )
        })?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        stream.set_nodelay(true).ok();
        let read_half = stream.try_clone().context("cloning socket for reading")?;
        Ok(RedisClient {
            write_half: stream,
            read_half: BufReader::new(read_half),
        })
    }

    /// Send a command (as separate string args, exactly like `redis-cli`)
    /// and return the single decoded reply.
    pub fn command(&mut self, args: &[&str]) -> Result<RespValue> {
        write_command(&mut self.write_half, args)?;
        read_value(&mut self.read_half)
    }

    /// Run `AUTH <password>` and bail with the server's own error text if
    /// it's rejected (wrong password, or auth not required/configured).
    pub fn auth(&mut self, password: &str) -> Result<()> {
        match self.command(&["AUTH", password])? {
            RespValue::SimpleString(s) if s == "OK" => Ok(()),
            RespValue::Error(e) => bail!("AUTH failed: {e}"),
            other => bail!("unexpected reply to AUTH: {other:?}"),
        }
    }

    /// `INFO replication` — returns the raw bulk-string body, unparsed.
    pub fn info_replication(&mut self) -> Result<String> {
        match self.command(&["INFO", "replication"])? {
            RespValue::BulkString(Some(bytes)) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
            RespValue::Error(e) => bail!("INFO replication failed: {e}"),
            other => bail!("unexpected reply to INFO replication: {other:?}"),
        }
    }

    /// `SLOWLOG GET <count>` — returns the raw decoded RESP array.
    pub fn slowlog_get(&mut self, count: i64) -> Result<RespValue> {
        let count_str = count.to_string();
        match self.command(&["SLOWLOG", "GET", &count_str])? {
            arr @ RespValue::Array(_) => Ok(arr),
            RespValue::Error(e) => bail!("SLOWLOG GET failed: {e}"),
            other => bail!("unexpected reply to SLOWLOG GET: {other:?}"),
        }
    }

    /// `PING` — used as a cheap "is this actually a Redis server" check.
    pub fn ping(&mut self) -> Result<()> {
        match self.command(&["PING"])? {
            RespValue::SimpleString(s) if s.eq_ignore_ascii_case("PONG") => Ok(()),
            RespValue::Error(e) => bail!("PING failed: {e}"),
            other => bail!("unexpected reply to PING: {other:?}"),
        }
    }
}
