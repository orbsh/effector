//! The egress allowlist's other half (ADR-0001's remaining promise): the node
//! runs the proxies, and the jail reaches them through a socket it can see.
//!
//! The two sides live in different network namespaces, so they cannot meet on
//! loopback. They meet on a UNIX socket instead: the node listens on it and
//! bridges every connection to the proxy's own `127.0.0.1` port, while inside
//! the jail a `socat` (spawned by the sandbox runtime's generated command)
//! listens on an ordinary loopback port and connects out to that socket. The
//! script then speaks proxy protocol to `localhost:<port>` like anything else.
//!
//! Two facts decide the layout and both are load-bearing:
//!
//! - the socket paths must be OUTSIDE `/tmp` and `/run` — the jail mounts empty
//!   tmpfs over both, so a socket there is invisible from inside;
//! - the in-jail ports are FIXED numbers, not discovered ones: the jail's bridges
//!   bind them before the script runs, so nothing has to be read back to the
//!   script.
//!
//! What this does NOT do is make the allowlist spacious: the filter decides by
//! hostname, so a declared `host:port` contributes its host and the port is the
//! client's business.

use crate::sandbox::ProxyFace;
use sandbox_runtime::config::NetworkConfig;
use sandbox_runtime::proxy::{DomainFilter, HttpProxy, Socks5Proxy};
use std::path::{Path, PathBuf};

/// The loopback ports the jail's bridges listen on. Constants rather than
/// discovered values: the generated jail command binds them before any script
/// runs, and a script that had to learn its proxy port would need a channel
/// back out of the namespace it is confined to.
pub const JAIL_HTTP_PORT: u16 = 18080;
pub const JAIL_SOCKS_PORT: u16 = 18081;

/// The live egress face: both proxies, the two bridges into the namespace, and
/// the socket directory they share. Dropping it closes all four and removes the
/// directory — the lifetime is the node's, so a reconnect never leaks a listener.
pub struct Egress {
    http: HttpProxy,
    socks: Socks5Proxy,
    face: ProxyFace,
    dir: PathBuf,
    bridges: Vec<tokio::task::JoinHandle<()>>,
}

impl Egress {
    /// Start the proxies for `domains` (declared as `host` or `host:port`).
    ///
    /// An empty list is refused by `EffectorConfig::validate` before this runs:
    /// an EMPTY allowlist means "allow everything" to the filter, which is the
    /// opposite of what a declaration of two domains asks for.
    pub async fn start(domains: &[String]) -> anyhow::Result<Self> {
        let dir = socket_dir()?;
        std::fs::create_dir_all(&dir)
            .map_err(|e| anyhow::anyhow!("egress socket dir {}: {e}", dir.display()))?;

        // The filter matches HOSTNAMES; a declared `host:port` is one target the
        // operator wrote in the form a client uses, so the port is dropped here
        // rather than silently failing to match.
        let patterns: Vec<String> = domains
            .iter()
            .map(|entry| match entry.rsplit_once(':') {
                Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host.to_string(),
                _ => entry.clone(),
            })
            .collect();
        let filter = || {
            DomainFilter::from_config(&NetworkConfig {
                allowed_domains: patterns.clone(),
                ..Default::default()
            })
        };

        let mut http = HttpProxy::new(filter(), None)
            .await
            .map_err(|e| anyhow::anyhow!("egress: http proxy: {e}"))?;
        let mut socks = Socks5Proxy::new(filter())
            .await
            .map_err(|e| anyhow::anyhow!("egress: socks proxy: {e}"))?;
        let http_port = http.port();
        let socks_port = socks.port();
        http.start()
            .map_err(|e| anyhow::anyhow!("egress: http proxy start: {e}"))?;
        socks
            .start()
            .map_err(|e| anyhow::anyhow!("egress: socks proxy start: {e}"))?;

        let http_socket = dir.join("http.sock");
        let socks_socket = dir.join("socks.sock");
        let bridges = vec![
            bridge(&http_socket, http_port)
                .await
                .map_err(|e| anyhow::anyhow!("egress: http bridge: {e}"))?,
            bridge(&socks_socket, socks_port)
                .await
                .map_err(|e| anyhow::anyhow!("egress: socks bridge: {e}"))?,
        ];

        Ok(Self {
            http,
            socks,
            face: ProxyFace {
                http_socket: http_socket.display().to_string(),
                socks_socket: socks_socket.display().to_string(),
                http_port: JAIL_HTTP_PORT,
                socks_port: JAIL_SOCKS_PORT,
            },
            dir,
            bridges,
        })
    }

    /// What the jail needs to know: two socket paths and two loopback ports.
    pub fn face(&self) -> &ProxyFace {
        &self.face
    }
}

impl Drop for Egress {
    fn drop(&mut self) {
        self.http.stop();
        self.socks.stop();
        for bridge in &self.bridges {
            bridge.abort();
        }
        // The sockets are files; a listener that is gone leaves a stale path
        // behind, and the next node on this machine would find it occupied.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Listen on a UNIX socket and forward each connection to the proxy's own
/// loopback port. Deliberately in-process rather than `socat`: the bridge is
/// part of the node's lifetime (a child process would need its own supervisor).
async fn bridge(socket: &Path, port: u16) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket)?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let Ok(mut tcp) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut conn, &mut tcp).await;
            });
        }
    }))
}

/// The command the jail runs before the script: the two bridges that put the
/// proxies on the jail's own loopback, then the environment that points every
/// ordinary client at them.
///
/// Written here rather than left to the sandbox runtime's own generator: that
/// generator joins its parts with `;` after each bridge's trailing `&`, which no
/// shell accepts (`socat … & ; socat …`), so its proxy path cannot start. The
/// ports and sockets are the same values it would have used — this is the same
/// bridge, spelled so a shell can parse it.
pub fn jail_prelude(face: &ProxyFace, quoted_command: &str) -> String {
    let http = face.http_port;
    let socks = face.socks_port;
    // Two details are load-bearing, and both were found by running it:
    //
    // - `set -m` first: a POSIX shell sends a background job's standard input to
    //   `/dev/null`, and inside this jail `/dev/null` is the read-only host root's
    //   device node the namespace may not open — so without job control the
    //   bridges die with "cannot redirect standard input from /dev/null".
    // - the bridges write to STDERR, never to `/dev/null`: same reason, and the
    //   child's stdout is the protocol channel.
    // The bridges are killed on the way out and the shell does NOT `exec` the
    // script: an exec would drop the cleanup, leaving two `socat` listeners (and
    // the descriptors they inherited) alive after every call.
    format!(
        "set -m; \
         socat TCP-LISTEN:{http},fork,reuseaddr UNIX-CONNECT:{http_sock} 1>&2 & http=$!; \
         socat TCP-LISTEN:{socks},fork,reuseaddr UNIX-CONNECT:{socks_sock} 1>&2 & socks=$!; \
         sleep 0.2; \
         export http_proxy='http://127.0.0.1:{http}' https_proxy='http://127.0.0.1:{http}' \
                HTTP_PROXY='http://127.0.0.1:{http}' HTTPS_PROXY='http://127.0.0.1:{http}' \
                ALL_PROXY='socks5://127.0.0.1:{socks}' all_proxy='socks5://127.0.0.1:{socks}'; \
         {{ {quoted_command}; status=$?; }}; \
         kill -TERM -$http -$socks 2>/dev/null || kill $http $socks 2>/dev/null; \
         exit $status",
        http_sock = face.http_socket,
        socks_sock = face.socks_socket,
    )
}

/// Where the sockets live. `$HOME/.cache/effector/egress-<pid>` — outside the two
/// paths the jail replaces with empty tmpfs (`/tmp`, `/run`), and outside the
/// ordinary deny list (`~/.ssh`, `~/.config`).
fn socket_dir() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "egress allowlists need $HOME: the proxies' sockets must live outside /tmp \
                 and /run (the jail replaces both with empty tmpfs), so there is nowhere \
                 else to put them"
            )
        })?;
    Ok(home.join(".cache").join("effector").join(format!(
        "egress-{}-{}",
        std::process::id(),
        UNIQUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )))
}

/// Two `Egress` values in one process (a test, or a node that restarts its face)
/// must not fight over a socket path: the directory is per instance, not per
/// process.
static UNIQUE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);