//! The egress allowlist, through a REAL jail: the node's proxies, the UNIX
//! socket the two namespaces meet on, and the in-jail bridges the generated
//! bwrap command starts.
//!
//! What this pins that no unit test can: the socket path is visible inside the
//! jail (it must live outside `/tmp` and `/run`, which the jail replaces with
//! empty tmpfs), the in-jail `socat` bridge reaches it, and the filter is what
//! decides — a host on the list answers, a host off the list is refused.
//!
//! Requires `bwrap`, `socat` and `python3` on PATH, and network isolation is the
//! jail's own (`--unshare-net`), so the test needs no external network.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{Read as _, Write as _};

/// The generator's mandatory-deny discovery reads `dirs::home_dir()` from THIS
/// process, so a test that wants a jail without the host's shell rc files has to
/// move `HOST_HOME` for the duration and refuse to run beside another one.
static HOME_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Point this process's `HOME` at `at`, and put it back on drop.
struct HomeAt {
    previous: Option<std::ffi::OsString>,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl HomeAt {
    fn new(at: &std::path::Path) -> Self {
        let guard = HOME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var_os("HOME");
        std::env::set_var("HOME", at);
        Self {
            previous,
            _guard: guard,
        }
    }
}

impl Drop for HomeAt {
    fn drop(&mut self) {
        match &self.previous {
            Some(home) => std::env::set_var("HOME", home),
            None => std::env::remove_var("HOME"),
        }
    }
}

/// A one-shot HTTP server on loopback: answers any GET with `body`.
fn server(body: &'static str) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });
    port
}

/// The jail command the carrier builds: same generator, same policy shape.
///
/// The cwd is a fresh empty directory, and the child runs with `HOME` pointed at
/// it, because the generator discovers "dangerous files" under the home (shell
/// rc files) and binds them read-only — on a nix home those are SYMLINKS, and
/// bwrap refuses `--ro-bind` onto a symlink destination. That failure predates
/// this test and is about the deny list, not about egress: the test isolates it
/// so the bridge is what is being measured.
fn jail_command(face: &effector_runtime::sandbox::ProxyFace, cwd: &std::path::Path, script: &str) -> String {
    let config = sandbox_runtime::config::SandboxRuntimeConfig {
        filesystem: sandbox_runtime::config::FilesystemConfig {
            allow_write: Vec::new(),
            deny_read: Vec::new(),
            ..Default::default()
        },
        ..Default::default()
    };
    // The prelude takes shell TEXT (the carrier hands it its already-quoted argv);
    // passing a quoted string here would make the braces run it as a command NAME.
    let quoted = effector_runtime::egress::jail_prelude(face, script);
    let (wrapped, _) = sandbox_runtime::sandbox::linux::generate_bwrap_command(
        &quoted,
        &config,
        cwd,
        None,
        None,
        0,
        0,
        // Same resolution the carrier does: NixOS has no `/bin/bash`.
        Some(if std::path::Path::new("/bin/bash").exists() {
            "/bin/bash"
        } else {
            "/bin/sh"
        }),
    )
    .expect("the bwrap command builds");
    // The same correction the carrier applies: the generator mounts its own /dev
    // and /proc BEFORE binding the host root over them, which leaves the jail with
    // the host's unreadable `/dev/null`.
    effector_runtime::carrier::exec::own_dev_and_proc(&wrapped)
}

/// Run the jail command and give up after `SECONDS`.
///
/// Output goes to FILES, not pipes, and the wait is bounded: the jail's bridges
/// are background processes that outlive the script for a moment, and any child
/// still holding a pipe makes `output()` wait for an EOF that never comes. A
/// file has no such handshake.
fn run_jailed(command: &str, home: &std::path::Path) -> String {
    const SECONDS: u64 = 30;
    let dir = tempfile::tempdir().expect("a log dir");
    let out_path = dir.path().join("out");
    let err_path = dir.path().join("err");
    let mut child = std::process::Command::new("bash")
        .arg("-c")
        .arg(command)
        .env("HOME", home)
        // `NO_PROXY=*` (set in this build environment) makes clients bypass the
        // proxy and try the target directly — which inside the namespace is the
        // refusal this test is supposed to prove is NOT the path.
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .stdout(std::fs::File::create(&out_path).expect("log"))
        .stderr(std::fs::File::create(&err_path).expect("log"))
        .spawn()
        .expect("bash runs");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(SECONDS);
    loop {
        if matches!(child.try_wait(), Ok(Some(_))) {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
    format!("{stdout}{stderr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_allowed_host_is_reachable_through_the_jail_and_another_is_not() {
    let port = server("ok-through-the-proxy");
    // The script asks the proxy (via the jail's bridges the prelude starts) for the
    // loopback server. It never reaches the target's own stack: the PROXY makes
    // that connection, on the host side of the boundary.
    let script = format!(
        "python3 -c \"import urllib.request;\
         print(urllib.request.urlopen('http://127.0.0.1:{port}/').read().decode())\"",
        port = port
    );

    // One HOME for both phases: `HomeAt` holds the process-wide lock, so a second
    // one in the same thread would wait on itself.
    let home = tempfile::tempdir().unwrap();
    let _home = HomeAt::new(home.path());

    // Allowed: the loopback address is on the list.
    let allowed = effector_runtime::egress::Egress::start(&["127.0.0.1".to_string()])
        .await
        .expect("the proxies start");
    let command = jail_command(allowed.face(), home.path(), &script);
    let out = run_jailed(&command, home.path());
    assert!(
        out.contains("ok-through-the-proxy"),
        "an allowed host must answer through the jail: {out}\n\ncommand:\n{command}"
    );

    // Denied: the SAME script, a list that does not name the host. The proxy is
    // the only path out, so a refusal there is a refusal everywhere.
    let denied = effector_runtime::egress::Egress::start(&["example.invalid".to_string()])
        .await
        .expect("the proxies start");
    let denied_command = jail_command(denied.face(), home.path(), &script);
    let out = run_jailed(&denied_command, home.path());
    assert!(
        !out.contains("ok-through-the-proxy"),
        "a host off the allowlist must not answer: {out}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_generated_jail_command_carries_the_bridges() {
    // The pass-through the carrier does, pinned: a face in the policy is a pair of
    // sockets and ports in the command, and the in-jail bridges are what make the
    // proxy env variables mean anything.
    let egress = effector_runtime::egress::Egress::start(&["127.0.0.1".to_string()])
        .await
        .expect("the proxies start");
    let face = egress.face();
    let home = tempfile::tempdir().unwrap();
    let _home = HomeAt::new(home.path());
    let command = jail_command(face, home.path(), "true");
    assert!(
        command.contains(&face.http_socket),
        "the http socket rides the command: {command}"
    );
    assert!(
        command.contains(&face.socks_socket),
        "the socks socket rides the command: {command}"
    );
    assert!(
        command.contains(&format!("TCP-LISTEN:{}", face.http_port)),
        "the in-jail bridge listens on the declared port: {command}"
    );
    assert!(
        command.contains("--unshare-net"),
        "the namespace is still unshared — the proxies are the only way out"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sockets_live_outside_the_tmpfs_paths_the_jail_replaces() {
    // `/tmp` and `/run` are empty tmpfs inside the jail, so a socket there would be
    // invisible; the directory is chosen for that reason and this pins it.
    let egress = effector_runtime::egress::Egress::start(&["127.0.0.1".to_string()])
        .await
        .expect("the proxies start");
    let face = egress.face();
    for path in [&face.http_socket, &face.socks_socket] {
        assert!(
            !path.starts_with("/tmp/") && !path.starts_with("/run/"),
            "{path} must not sit under a path the jail replaces with tmpfs"
        );
        assert!(std::path::Path::new(path).exists(), "{path} listens");
    }
}