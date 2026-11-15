//! The HTTP client of the CLI and the TUI.
//!
//! Both talk to the master only through its API. With a token the
//! connection is TLS: the token carries the fingerprint of the master's
//! CA, so the client can make sure it is talking to the right master
//! before it sends the token, with no certificate to install by hand.

use anyhow::{Context, Result, bail};
use reqwest::{Method, Response};
use serde_json::Value;

use crate::config::ClientConfig;
use crate::security::{self, MASTER_NAME, Token};

/// Where the API is when nothing says otherwise: a local standalone.
const DEFAULT_API: &str = "127.0.0.1:7070";

/// A connection to a master's API.
#[derive(Debug, Clone)]
pub(crate) struct Client {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    /// The address as the user knows it, for messages.
    pub(crate) address: String,
}

impl Client {
    /// Prepares the client. With a token this already contacts the
    /// master, to check its CA against the token.
    pub(crate) async fn connect(config: &ClientConfig) -> Result<Self> {
        let api = config.api.as_deref().unwrap_or(DEFAULT_API);
        let address = api
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let Some(token) = &config.token else {
            return Ok(Self {
                http: reqwest::Client::new(),
                base: format!("http://{address}"),
                token: None,
                address,
            });
        };
        let parsed = Token::parse(token)?;
        let ca_pem = security::fetch_ca(&address, &parsed.fingerprint).await?;
        let socket = tokio::net::lookup_host(&address)
            .await
            .with_context(|| format!("cannot resolve {address}"))?
            .next()
            .with_context(|| format!("{address} has no address"))?;
        // The master's certificate is for the name `lwwo-master`, not for
        // wherever it happens to listen: connect to the address, but check
        // the certificate against that name and only against its own CA.
        let http = reqwest::Client::builder()
            .tls_certs_only([reqwest::Certificate::from_pem(ca_pem.as_bytes())?])
            .resolve(MASTER_NAME, socket)
            .build()?;
        Ok(Self {
            http,
            base: format!("https://{MASTER_NAME}:{}", socket.port()),
            token: Some(token.trim().to_owned()),
            address,
        })
    }

    async fn send(&self, method: Method, path: &str, body: Option<Body<'_>>) -> Result<Response> {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request = match body {
            Some(Body::Json(value)) => request.json(value),
            Some(Body::Text(text)) => request.body(text.to_owned()),
            None => request,
        };
        let response = request.send().await.with_context(|| {
            format!(
                "cannot reach the master at {}: is it running? (set LWWO_API to change the address)",
                self.address
            )
        })?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let message = response
            .json::<Value>()
            .await
            .ok()
            .and_then(|body| body["error"].as_str().map(str::to_owned))
            .unwrap_or_else(|| status.to_string());
        bail!(message)
    }

    /// `GET path`, as JSON.
    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        Ok(self.send(Method::GET, path, None).await?.json().await?)
    }

    /// `POST path` with a JSON body.
    pub(crate) async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        let response = self
            .send(Method::POST, path, Some(Body::Json(body)))
            .await?;
        Ok(response.json().await?)
    }

    /// `POST path` with a text body.
    pub(crate) async fn post_text(&self, path: &str, body: &str) -> Result<Value> {
        let response = self
            .send(Method::POST, path, Some(Body::Text(body)))
            .await?;
        Ok(response.json().await?)
    }

    /// `DELETE path`.
    pub(crate) async fn delete(&self, path: &str) -> Result<Value> {
        Ok(self.send(Method::DELETE, path, None).await?.json().await?)
    }

    /// `GET path` as a stream: the logs of an attempt, or `/events`.
    pub(crate) async fn stream(&self, path: &str) -> Result<Response> {
        self.send(Method::GET, path, None).await
    }
}

enum Body<'a> {
    Json(&'a Value),
    Text(&'a str),
}

/// A line of a task's log, as the master stores it: milliseconds, stream
/// and text, separated by tabs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogLine {
    pub(crate) at: i64,
    /// `O` for stdout, `E` for stderr, `S` for lwwo itself.
    pub(crate) stream: char,
    pub(crate) text: String,
}

impl LogLine {
    /// Reads a stored line. A line in another shape is shown as it is.
    pub(crate) fn parse(line: &str) -> Self {
        let mut parts = line.splitn(3, '\t');
        let parsed = (|| {
            let at = parts.next()?.parse().ok()?;
            let stream = parts.next()?.chars().next()?;
            Some(Self {
                at,
                stream,
                text: parts.next()?.to_owned(),
            })
        })();
        parsed.unwrap_or_else(|| Self {
            at: 0,
            stream: 'O',
            text: line.to_owned(),
        })
    }

    /// The line for a terminal: time, a mark for errors, text.
    pub(crate) fn display(&self) -> String {
        let time = if self.at == 0 {
            "        ".to_owned()
        } else {
            crate::util::format_local(self.at)[11..].to_owned()
        };
        let mark = match self.stream {
            'E' => "ERR ",
            'S' => "lwwo: ",
            _ => "",
        };
        format!("{time}  {mark}{}", self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_lines() {
        let line = LogLine::parse("1792980001000\tE\ttimeout on the\tconnection");
        assert_eq!(line.at, 1_792_980_001_000);
        assert_eq!(line.stream, 'E');
        assert_eq!(line.text, "timeout on the\tconnection");
        assert!(line.display().ends_with("  ERR timeout on the\tconnection"));
        assert_eq!(LogLine::parse("no tabs here").text, "no tabs here");
    }
}
