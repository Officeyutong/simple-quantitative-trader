use std::{
    net::SocketAddr,
    path::{Component, Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result};

const MAX_HEADER_BYTES: usize = 16 * 1024;

pub async fn run(
    address: SocketAddr,
    static_dir: PathBuf,
    rpc_address: SocketAddr,
    cancellation: CancellationToken,
) -> Result<()> {
    if !static_dir.join("index.html").is_file() {
        return Err(AppError::Config(format!(
            "web build is missing at {}; run `trunk build --release` in web/",
            static_dir.display()
        )));
    }
    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, root = %static_dir.display(), "Web UI server listening");
    loop {
        let (stream, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted?,
        };
        let static_dir = static_dir.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, &static_dir, rpc_address).await {
                tracing::debug!(%error, "Web UI request failed");
            }
        });
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    static_dir: &Path,
    rpc_address: SocketAddr,
) -> Result<()> {
    let mut buffer = vec![0_u8; MAX_HEADER_BYTES];
    let read = stream.read(&mut buffer).await?;
    if read == 0 {
        return Ok(());
    }
    let request = std::str::from_utf8(&buffer[..read])
        .map_err(|_| AppError::Config("invalid HTTP request encoding".into()))?;
    let Some(first_line) = request.lines().next() else {
        return Ok(());
    };
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let raw_path = parts.next().unwrap_or("/");
    if !matches!(method, "GET" | "HEAD") {
        return write_response(
            &mut stream,
            method,
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            b"method not allowed",
            rpc_address,
            false,
        )
        .await;
    }

    let relative = raw_path
        .split('?')
        .next()
        .unwrap_or("/")
        .trim_start_matches('/');
    let candidate =
        safe_asset_path(static_dir, relative).unwrap_or_else(|| static_dir.join("index.html"));
    let (path, spa_fallback) = if candidate.is_file() {
        (candidate, false)
    } else {
        (static_dir.join("index.html"), true)
    };
    let body = tokio::fs::read(&path).await?;
    write_response(
        &mut stream,
        method,
        "200 OK",
        content_type(&path),
        &body,
        rpc_address,
        !spa_fallback && path.file_name().is_some_and(|name| name != "index.html"),
    )
    .await
}

fn safe_asset_path(root: &Path, relative: &str) -> Option<PathBuf> {
    if relative.is_empty() {
        return Some(root.join("index.html"));
    }
    let path = Path::new(relative);
    if path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        Some(root.join(path))
    } else {
        None
    }
}

async fn write_response(
    stream: &mut TcpStream,
    method: &str,
    status: &str,
    content_type: &str,
    body: &[u8],
    _rpc_address: SocketAddr,
    immutable: bool,
) -> Result<()> {
    let cache_control = if immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let script_hashes = if content_type.starts_with("text/html") {
        inline_script_hashes(body)
    } else {
        String::new()
    };
    let headers = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: {cache_control}\r\n\
         X-Content-Type-Options: nosniff\r\n\
         X-Frame-Options: DENY\r\n\
         Referrer-Policy: no-referrer\r\n\
         Content-Security-Policy: default-src 'self'; script-src 'self' 'wasm-unsafe-eval'{script_hashes}; \
         style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; \
         frame-ancestors 'none'\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    if method != "HEAD" {
        stream.write_all(body).await?;
    }
    stream.shutdown().await?;
    Ok(())
}

fn inline_script_hashes(html: &[u8]) -> String {
    let Ok(html) = std::str::from_utf8(html) else {
        return String::new();
    };
    let mut remaining = html;
    let mut hashes = Vec::new();
    while let Some(script_start) = remaining.find("<script") {
        remaining = &remaining[script_start + "<script".len()..];
        let Some(tag_end) = remaining.find('>') else {
            break;
        };
        let attributes = &remaining[..tag_end];
        remaining = &remaining[tag_end + 1..];
        let Some(script_end) = remaining.find("</script>") else {
            break;
        };
        let script = &remaining[..script_end];
        remaining = &remaining[script_end + "</script>".len()..];
        if !attributes.split_ascii_whitespace().any(|part| {
            part.eq_ignore_ascii_case("src") || part.to_ascii_lowercase().starts_with("src=")
        }) {
            let digest = Sha256::digest(script.as_bytes());
            hashes.push(format!(" 'sha256-{}'", STANDARD.encode(digest)));
        }
    }
    hashes.join("")
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_paths_cannot_escape_the_dist_directory() {
        let root = Path::new("/tmp/dist");
        assert!(safe_asset_path(root, "app.js").is_some());
        assert!(safe_asset_path(root, "../secret").is_none());
        assert!(safe_asset_path(root, "/etc/passwd").is_none());
    }

    #[test]
    fn csp_hashes_every_inline_script_but_not_external_scripts() {
        let html = br#"<script>console.log("one")</script><script src="/app.js"></script>
            <script type="module">console.log("two")</script>"#;
        let hashes = inline_script_hashes(html);
        assert_eq!(hashes.matches("'sha256-").count(), 2);
        assert!(!hashes.contains("app.js"));
    }
}
