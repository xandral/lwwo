//! The internal CA, certificates and tokens.
//!
//! The master is its own certificate authority, as in Docker Swarm. A
//! worker enters once with the join token, gets a certificate for a key
//! that never leaves its disk, and from then on master and worker
//! recognise each other with mutual TLS. Every token carries the
//! fingerprint of the CA, so whoever holds one can check that it is
//! talking to the right master before sending any secret.

use std::fmt;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SerialNumber,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::util::{hex, sha256_hex};

/// The name in the master's certificate. The CA never gives it to a
/// worker, so a worker cannot pass itself off as the master.
pub(crate) const MASTER_NAME: &str = "lwwo-master";

const CA_DAYS: i64 = 3_650;
const MASTER_CERT_DAYS: i64 = 365;

/// Makes ring the TLS provider of the process. Call once, at start.
pub(crate) fn install_crypto() {
    // An error means a provider is already installed, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Writes a file only its owner can read: keys and tokens.
pub(crate) fn write_private(path: &Path, content: &str) -> Result<()> {
    let _ = std::fs::remove_file(path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("cannot write {}", path.display()))?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

/// A random serial number, positive and without leading zeros.
fn new_serial() -> SerialNumber {
    let mut bytes = *Uuid::new_v4().as_bytes();
    bytes[0] = (bytes[0] & 0x7f) | 0x40;
    SerialNumber::from_slice(&bytes)
}

/// The fingerprint of a certificate: the SHA-256 of its DER form.
pub(crate) fn fingerprint(der: &[u8]) -> String {
    sha256_hex(der)
}

fn pem_to_der(pem_text: &str) -> Result<Vec<u8>> {
    Ok(pem::parse(pem_text)
        .context("not a PEM certificate")?
        .into_contents())
}

/// A certificate issued by the CA.
#[derive(Debug, Clone)]
pub(crate) struct Issued {
    pub(crate) cert_pem: String,
    pub(crate) serial: String,
}

/// The certificate authority of a master.
pub(crate) struct Ca {
    cert_pem: String,
    key_pem: String,
}

impl fmt::Debug for Ca {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the key.
        f.debug_struct("Ca").finish_non_exhaustive()
    }
}

impl Ca {
    /// Loads the CA from `dir`, creating it at the first start. The key
    /// stays in `ca.key`, readable by its owner only, and never in the
    /// database.
    pub(crate) fn load_or_create(dir: &Path) -> Result<Self> {
        let cert_path = dir.join("ca.crt");
        let key_path = dir.join("ca.key");
        if cert_path.exists() && key_path.exists() {
            return Ok(Self {
                cert_pem: std::fs::read_to_string(&cert_path)
                    .with_context(|| format!("cannot read {}", cert_path.display()))?,
                key_pem: std::fs::read_to_string(&key_path)
                    .with_context(|| format!("cannot read {}", key_path.display()))?,
            });
        }
        std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params
            .distinguished_name
            .push(DnType::CommonName, "lwwo internal CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.not_before = OffsetDateTime::now_utc() - Duration::days(1);
        params.not_after = OffsetDateTime::now_utc() + Duration::days(CA_DAYS);
        params.serial_number = Some(new_serial());
        let ca = Self {
            cert_pem: params.self_signed(&key)?.pem(),
            key_pem: key.serialize_pem(),
        };
        write_private(&key_path, &ca.key_pem)?;
        std::fs::write(&cert_path, &ca.cert_pem)
            .with_context(|| format!("cannot write {}", cert_path.display()))?;
        Ok(ca)
    }

    /// The CA certificate, which is public.
    pub(crate) fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// The fingerprint that goes into the tokens.
    pub(crate) fn fingerprint(&self) -> Result<String> {
        Ok(fingerprint(&pem_to_der(&self.cert_pem)?))
    }

    fn issuer(&self) -> Result<Issuer<'static, KeyPair>> {
        let key = KeyPair::from_pem(&self.key_pem)?;
        Ok(Issuer::from_ca_cert_pem(&self.cert_pem, key)?)
    }

    /// A fresh certificate and key for the master itself, named
    /// [`MASTER_NAME`].
    pub(crate) fn issue_master(&self) -> Result<(String, String)> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec![MASTER_NAME.to_owned()])?;
        params
            .distinguished_name
            .push(DnType::CommonName, MASTER_NAME);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.not_before = OffsetDateTime::now_utc() - Duration::days(1);
        params.not_after = OffsetDateTime::now_utc() + Duration::days(MASTER_CERT_DAYS);
        params.serial_number = Some(new_serial());
        let cert = params.signed_by(&key, &self.issuer()?)?;
        Ok((cert.pem(), key.serialize_pem()))
    }

    /// Signs the request of a worker named `name`, for `lifetime` from
    /// now. Whatever the request asks for, the certificate only says who
    /// the worker is.
    pub(crate) fn sign_worker(
        &self,
        csr_pem: &str,
        name: &str,
        lifetime: std::time::Duration,
    ) -> Result<Issued> {
        ensure!(
            name != MASTER_NAME,
            "`{MASTER_NAME}` is the name of the master: choose another name for the worker"
        );
        ensure!(!name.is_empty(), "the worker has no name");
        let mut request = CertificateSigningRequestParams::from_pem(csr_pem)
            .context("invalid certificate request")?;
        let serial = new_serial();
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        // Valid from a little before now, for a worker whose clock is
        // behind: a day, or less when the certificate itself is short.
        let lifetime =
            Duration::try_from(lifetime).context("the certificate lifetime is too long")?;
        let now = OffsetDateTime::now_utc();
        params.not_before = now - Duration::days(1).min(lifetime / 2);
        params.not_after = now + lifetime;
        params.serial_number = Some(serial.clone());
        request.params = params;
        let cert = request.signed_by(&self.issuer()?)?;
        Ok(Issued {
            cert_pem: cert.pem(),
            serial: serial_text(&serial.to_bytes()),
        })
    }
}

/// Checks the name a worker asks for when it joins. It is printed by the
/// CLI and the TUI and goes in URLs: letters, digits, `_`, `-` and `.`,
/// which host names have.
pub(crate) fn check_worker_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "the worker has no name");
    ensure!(
        name.len() <= 64,
        "the worker name is longer than 64 characters"
    );
    ensure!(
        !name.starts_with('.')
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')),
        "the worker name `{}` may only have letters, digits, `_`, `-` and `.`, \
         and cannot start with `.`: set another one with --name",
        name.escape_default()
    );
    Ok(())
}

/// A serial number as `4f:1a:…`, as stored in `workers.cert_serial`.
fn serial_text(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// A new key and a certificate request for a worker.
pub(crate) fn worker_request(name: &str) -> Result<(String, String)> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.distinguished_name.push(DnType::CommonName, name);
    let request = params.serialize_request(&key)?;
    Ok((request.pem()?, key.serialize_pem()))
}

/// Who a certificate belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) name: String,
    pub(crate) serial: String,
    /// When the certificate expires, UTC milliseconds.
    pub(crate) not_before: i64,
    pub(crate) not_after: i64,
}

/// Reads name, serial and validity of a certificate in DER form.
pub(crate) fn identity(der: &[u8]) -> Result<Identity> {
    let (_, cert) = X509Certificate::from_der(der).context("invalid certificate")?;
    let name = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|name| name.as_str().ok())
        .context("the certificate has no name")?
        .to_owned();
    Ok(Identity {
        name,
        serial: serial_text(cert.raw_serial()),
        not_before: cert.validity().not_before.timestamp() * 1_000,
        not_after: cert.validity().not_after.timestamp() * 1_000,
    })
}

/// Like [`identity`], for a certificate in PEM form.
pub(crate) fn identity_pem(cert_pem: &str) -> Result<Identity> {
    identity(&pem_to_der(cert_pem)?)
}

/// What a token is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    /// CLI, TUI and scripts calling the API.
    Api,
    /// A worker entering for the first time.
    Join,
}

impl TokenKind {
    /// The value of `tokens.kind`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Join => "join",
        }
    }
}

/// A token: `lwwo1_<CA fingerprint>.<id>.<secret>`. The database only
/// keeps the id and the hash of the secret.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) fingerprint: String,
    pub(crate) id: String,
    pub(crate) secret: String,
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the secret.
        f.debug_struct("Token")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Token {
    /// A new random token for the master whose CA has this fingerprint.
    pub(crate) fn generate(fingerprint: &str) -> Self {
        Self {
            fingerprint: fingerprint.to_owned(),
            id: format!("tok_{}", &hex(Uuid::new_v4().as_bytes())[..12]),
            secret: format!(
                "{}{}",
                hex(Uuid::new_v4().as_bytes()),
                hex(Uuid::new_v4().as_bytes())
            ),
        }
    }

    /// Reads a token from its text.
    pub(crate) fn parse(text: &str) -> Result<Self> {
        let Some(rest) = text.trim().strip_prefix("lwwo1_") else {
            bail!("not an lwwo token: it does not start with `lwwo1_`");
        };
        let parts: Vec<&str> = rest.split('.').collect();
        let [fingerprint, id, secret] = parts.as_slice() else {
            bail!("not an lwwo token: it is cut short or damaged");
        };
        Ok(Self {
            fingerprint: (*fingerprint).to_owned(),
            id: (*id).to_owned(),
            secret: (*secret).to_owned(),
        })
    }

    /// The text to hand out.
    pub(crate) fn text(&self) -> String {
        format!("lwwo1_{}.{}.{}", self.fingerprint, self.id, self.secret)
    }

    /// What the database stores in place of the secret.
    pub(crate) fn secret_hash(&self) -> String {
        format!("sha256:{}", sha256_hex(self.secret.as_bytes()))
    }
}

/// Accepts any certificate: used only to look at what a server presents,
/// on a connection that carries nothing.
#[derive(Debug)]
struct LookOnly(Arc<CryptoProvider>);

impl ServerCertVerifier for LookOnly {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Gets the CA certificate of the master at `address` (`host:port`), and
/// checks it against the fingerprint in a token.
///
/// The server sends its CA along with its own certificate. Nothing is
/// trusted on this first connection and nothing is sent on it: the
/// certificate is public, and it is accepted only if its fingerprint is
/// the one in the token. The caller then connects again, this time
/// verifying the master against the CA, and only then sends secrets.
pub(crate) async fn fetch_ca(address: &str, expected_fingerprint: &str) -> Result<String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(LookOnly(provider)))
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let tcp = tokio::net::TcpStream::connect(address)
        .await
        .with_context(|| format!("cannot connect to the master at {address}"))?;
    let name = ServerName::try_from(MASTER_NAME).expect("a valid DNS name");
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await
        .with_context(|| format!("TLS handshake with {address} failed"))?;
    let (_, session) = tls.get_ref();
    let chain = session.peer_certificates().unwrap_or_default();
    let ca = chain
        .iter()
        .find(|cert| fingerprint(cert) == expected_fingerprint)
        .with_context(|| {
            format!(
                "the master at {address} is not the one this token belongs to: \
                 its CA does not match the fingerprint in the token"
            )
        })?;
    Ok(pem::encode(&pem::Pem::new("CERTIFICATE", ca.to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lwwo-ca-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_ca_is_created_once_and_its_key_is_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir();
        let ca = Ca::load_or_create(&dir).unwrap();
        let again = Ca::load_or_create(&dir).unwrap();
        assert_eq!(ca.fingerprint().unwrap(), again.fingerprint().unwrap());
        let mode = std::fs::metadata(dir.join("ca.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_worker_gets_a_certificate_with_its_name_for_the_time_asked() {
        let ninety_days = std::time::Duration::from_secs(90 * 86_400);
        let dir = temp_dir();
        let ca = Ca::load_or_create(&dir).unwrap();
        let (request, _key) = worker_request("w3").unwrap();
        let issued = ca.sign_worker(&request, "w3", ninety_days).unwrap();
        let who = identity_pem(&issued.cert_pem).unwrap();
        assert_eq!(who.name, "w3");
        assert_eq!(who.serial, issued.serial);
        let days = (who.not_after - who.not_before) / 86_400_000;
        assert_eq!(days, 91, "one day of slack for clock differences");
        // A short certificate is backdated by half its life, not by a day.
        let short = std::time::Duration::from_secs(60);
        let who = identity_pem(&ca.sign_worker(&request, "w3", short).unwrap().cert_pem).unwrap();
        assert_eq!((who.not_after - who.not_before) / 1_000, 90);

        // The name comes from the master, not from the request.
        let (request, _key) = worker_request(MASTER_NAME).unwrap();
        assert_eq!(
            identity_pem(
                &ca.sign_worker(&request, "w4", ninety_days)
                    .unwrap()
                    .cert_pem
            )
            .unwrap()
            .name,
            "w4"
        );
        assert!(ca.sign_worker(&request, MASTER_NAME, ninety_days).is_err());

        for good in ["w3", "mac-mini.local", "gpu_01"] {
            assert!(check_worker_name(good).is_ok(), "{good}");
        }
        let long = "w".repeat(65);
        for bad in [
            "",
            ".hidden",
            "two words",
            "tab\there",
            "esc\u{1b}[31m",
            "a/b",
            &long,
        ] {
            assert!(check_worker_name(bad).is_err(), "{bad:?}");
        }
        assert!(ca.sign_worker("not a request", "w5", ninety_days).is_err());

        let (master_cert, _) = ca.issue_master().unwrap();
        assert_eq!(identity_pem(&master_cert).unwrap().name, MASTER_NAME);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn tokens_survive_their_text_and_hide_their_secret() {
        let token = Token::generate("ab12");
        let parsed = Token::parse(&token.text()).unwrap();
        assert_eq!(parsed, token);
        assert!(token.text().starts_with("lwwo1_ab12.tok_"));
        assert!(token.secret_hash().starts_with("sha256:"));
        assert!(!format!("{token:?}").contains(&token.secret));
        assert!(Token::parse("lwwo1_only.two").is_err());
        assert!(Token::parse("ghp_something").is_err());
    }
}
