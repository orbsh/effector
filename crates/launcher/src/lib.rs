//! The launcher's two verbs, kept out of `main` so they can be tested without a container.
//!
//! The launcher is the privileged half of ADR-0003 §10: it holds the Docker socket, mints the
//! node's token in the control plane's registry, and hands the resulting process one thing —
//! where to dial. Everything else about the node (its alias, its carriers, its lease) lives in
//! the config file the image ships, because that is the shape the image was built with.
//!
//! Two rules are structural here rather than documented:
//!
//! - the token never appears in argv or the environment (`/proc/<pid>/cmdline` is world-readable
//!   on the host, and the node's environment is inherited by the guest it spawns), so it goes to
//!   the container's **stdin**;
//! - the launcher mints, it never verifies: a node's authority is the connection it opens, and
//!   the control plane is the only place that decides (ADR-0003 §11).

use std::io::{Read, Write};
use std::net::TcpStream;

/// What the control plane answered when it minted a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Minted {
    pub token: String,
}

/// Split `http://host:port[/prefix]` into (host:port, prefix). Kept deliberately small: the
/// admin surface is plain HTTP on a loopback or private address (the control plane's own
/// deployment decides that), and a launcher that tried to speak TLS would be a second place
/// holding certificates.
pub fn host_and_prefix(control_plane: &str) -> Result<(String, String), String> {
    let rest = control_plane
        .strip_prefix("http://")
        .ok_or_else(|| format!("control plane `{control_plane}`: expected http://host:port"))?;
    let (authority, prefix) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if authority.is_empty() {
        return Err(format!("control plane `{control_plane}`: no host"));
    }
    if authority.contains('@') {
        return Err(format!(
            "control plane `{control_plane}`: credentials do not belong in the URL"
        ));
    }
    Ok((
        authority.to_string(),
        prefix.trim_end_matches('/').to_string(),
    ))
}

/// The address the node dials: the same host and port, the mount path, over `ws://`.
pub fn mount_url(control_plane: &str, alias: &str) -> Result<String, String> {
    let (authority, prefix) = host_and_prefix(control_plane)?;
    Ok(format!("ws://{authority}{prefix}/effector/{alias}"))
}

/// Mint one token for `alias` through the operator's face (`POST /admin/nodes`).
///
/// The response is the only time a token is readable anywhere (ADR-0003 §11): the registry keeps
/// its digest, so this call is not a lookup but a rotation — a launcher that runs twice leaves
/// the first container unable to re-dial, which is exactly the revocation model.
pub fn mint_token(control_plane: &str, alias: &str) -> Result<Minted, String> {
    let (authority, prefix) = host_and_prefix(control_plane)?;
    let body = format!("{{\"alias\":{}}}", json_string(alias));
    let request = format!(
        "POST {prefix}/admin/nodes HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let mut stream =
        TcpStream::connect(&authority).map_err(|e| format!("control plane {authority}: {e}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("control plane {authority}: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("control plane {authority}: {e}"))?;

    let (head, payload) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("control plane {authority}: not an HTTP response"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("control plane {authority}: no status line"))?;
    if status != "201" && status != "200" {
        return Err(format!(
            "control plane {authority}: minting `{alias}` failed with {status}: {}",
            payload.trim()
        ));
    }
    let token = token_from_json(payload)
        .ok_or_else(|| format!("control plane {authority}: the mint response carried no token"))?;
    Ok(Minted { token })
}

/// The argv that turns an image into a node: `docker run --rm --init -i <image> effector …`.
///
/// `--rm` and `--init` are not conveniences: with the node as PID 1 (ADR-0003 §10) the container
/// ends when the process does, and `--init` gives delivered scripts a reaper that is not the
/// node itself.
pub fn docker_argv(image: &str, mount: &str, extra: &[String]) -> Vec<String> {
    let mut argv: Vec<String> = vec![
        "docker".into(),
        "run".into(),
        "--rm".into(),
        "--init".into(),
        "-i".into(),
    ];
    argv.extend(extra.iter().cloned());
    argv.push(image.to_string());
    argv.push("effector".into());
    argv.push("--control-plane".into());
    argv.push(mount.to_string());
    argv.push("--token-stdin".into());
    argv
}

/// Minimal JSON string escaping for the one field this binary writes.
fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Read `"token"` out of the mint response without pulling a JSON tree into this binary.
fn token_from_json(payload: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    value.get("token")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    // A test's failure IS its panic; the workspace's no-panic rule is about the shipped paths.
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn a_control_plane_url_becomes_the_mount_the_node_dials() {
        assert_eq!(
            mount_url("http://127.0.0.1:8080", "home-pc").unwrap(),
            "ws://127.0.0.1:8080/effector/home-pc"
        );
        assert_eq!(
            mount_url("http://plane.internal:9000/", "home-pc").unwrap(),
            "ws://plane.internal:9000/effector/home-pc"
        );
        assert_eq!(
            mount_url("http://plane.internal:9000/prism", "a").unwrap(),
            "ws://plane.internal:9000/prism/effector/a"
        );
        assert!(mount_url("https://plane.internal", "a").is_err());
        assert!(mount_url("http://user:pw@plane.internal", "a").is_err());
    }

    #[test]
    fn the_container_gets_no_token_on_its_command_line() {
        let argv = docker_argv(
            "img:tag",
            "ws://h:1/effector/a",
            &["--network".into(), "host".into()],
        );
        assert_eq!(argv[..5], ["docker", "run", "--rm", "--init", "-i"]);
        assert_eq!(&argv[5], "--network");
        assert_eq!(&argv[6], "host");
        assert_eq!(&argv[7], "img:tag");
        assert_eq!(&argv[8], "effector");
        assert!(argv.contains(&"--token-stdin".to_string()));
        assert!(
            !argv.iter().any(|a| a == "--token" || a.contains("token=")),
            "the token rides stdin, never argv: {argv:?}"
        );
    }

    #[test]
    fn a_mint_response_is_parsed_and_a_missing_token_is_an_error() {
        assert_eq!(
            token_from_json("{\"token\":\"abc\",\"status\":\"pending\"}").unwrap(),
            "abc"
        );
        assert!(token_from_json("{\"status\":\"pending\"}").is_none());
    }
}
