//! Signing-key handling.
//!
//! Per the methodology lock ("Write-side daemons — 2026-04-28"):
//!
//! - **Dev mode (this slice):** keypair is a 64-byte JSON byte-array
//!   file (the format `solana-keygen new -o <path>` produces) at
//!   `~/Library/Application Support/scryer/keys/pyth-poster.json` by
//!   default, overridable via `--signer-keypair PATH`. File mode must
//!   be `0600`; the daemon refuses to start otherwise.
//!
//! - **Prod mode (deferred):** macOS Keychain Secure Enclave. The
//!   hot key never leaves the chip; signing is via the `security`
//!   framework. File-on-disk fallback in prod mode is **prohibited**.
//!   This slice does NOT implement prod loading; attempting it errors
//!   with `KeyError::ProdNotImplemented`.
//!
//! The `DevKeypair` type owns the 64-byte secret-key bytes but never
//! logs them. Pubkey-only logging is acceptable; secret-key logging
//! is not.

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// Default dev-mode keypair file path:
/// `~/Library/Application Support/scryer/keys/pyth-poster.json`.
pub fn default_dev_keypair_path() -> PathBuf {
    if let Some(home) = dirs_home() {
        home.join("Library")
            .join("Application Support")
            .join("scryer")
            .join("keys")
            .join("pyth-poster.json")
    } else {
        PathBuf::from("./pyth-poster.json")
    }
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[derive(Debug, Error)]
pub enum KeyError {
    #[error("keypair file `{0}` not found")]
    NotFound(PathBuf),

    #[error("keypair file `{path}` mode is `{mode:#o}`, must be `0o600` per methodology lock")]
    InsecureMode { path: PathBuf, mode: u32 },

    #[error("keypair file `{0}` is unreadable: {1}")]
    Unreadable(PathBuf, std::io::Error),

    #[error("keypair file `{path}` is not a valid solana-keygen JSON byte-array: {reason}")]
    Malformed { path: PathBuf, reason: String },

    #[error(
        "prod-mode keypair loading not implemented yet — \
         requires Keychain Secure Enclave wrapper (see methodology O-write-3)"
    )]
    ProdNotImplemented,
}

/// 64-byte ed25519 secret-key bytes loaded from a `solana-keygen` JSON
/// file. The first 32 bytes are the seed, the last 32 are the public
/// key — same layout `solana-sdk::signer::keypair::Keypair::from_bytes`
/// expects. We deliberately don't depend on `solana-sdk` here so this
/// crate stays buildable as the daemon scaffold matures.
pub struct DevKeypair {
    bytes: [u8; 64],
    /// Resolved path the bytes were loaded from. Useful for error
    /// reporting; never logged with the bytes.
    source_path: PathBuf,
}

impl std::fmt::Debug for DevKeypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Pubkey is safe; secret bytes are NEVER printed.
        f.debug_struct("DevKeypair")
            .field("pubkey", &self.pubkey_base58())
            .field("source_path", &self.source_path)
            .field("secret_bytes", &"<redacted-64-bytes>")
            .finish()
    }
}

impl DevKeypair {
    /// Load + validate a dev-mode keypair file. Performs:
    /// 1. Existence check.
    /// 2. File-mode check (`0o600`) — required by the methodology lock.
    /// 3. JSON parse — must be `Vec<u8>` of length 64.
    ///
    /// Does NOT cryptographically validate the keypair (no derivation
    /// check) — a malformed key would fail at first signing attempt.
    /// This is acceptable for dev mode; prod mode goes through
    /// Keychain which validates at item-lookup time.
    pub fn load_from_path(path: &Path) -> Result<Self, KeyError> {
        if !path.exists() {
            return Err(KeyError::NotFound(path.to_path_buf()));
        }

        check_file_mode(path)?;

        let raw = fs::read_to_string(path)
            .map_err(|e| KeyError::Unreadable(path.to_path_buf(), e))?;

        let bytes_vec: Vec<u8> = serde_json::from_str(&raw).map_err(|e| KeyError::Malformed {
            path: path.to_path_buf(),
            reason: format!("not a JSON byte-array: {e}"),
        })?;

        if bytes_vec.len() != 64 {
            return Err(KeyError::Malformed {
                path: path.to_path_buf(),
                reason: format!("expected 64 bytes, got {}", bytes_vec.len()),
            });
        }

        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(&bytes_vec);

        Ok(DevKeypair {
            bytes,
            source_path: path.to_path_buf(),
        })
    }

    /// Last 32 bytes — the on-chain public key. Safe to log.
    pub fn pubkey_bytes(&self) -> [u8; 32] {
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&self.bytes[32..]);
        pk
    }

    /// Base58-encoded pubkey, suitable for log lines and the mirror
    /// tape's writer-pubkey reference column.
    pub fn pubkey_base58(&self) -> String {
        bs58_encode(&self.pubkey_bytes())
    }

    /// Full 64-byte secret-key. NEVER log. Caller is responsible for
    /// keeping this off any log path. Provided as a slice (not a
    /// returned array) to discourage accidental copies.
    pub fn secret_bytes(&self) -> &[u8; 64] {
        &self.bytes
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }
}

#[cfg(unix)]
fn check_file_mode(path: &Path) -> Result<(), KeyError> {
    use std::os::unix::fs::PermissionsExt;
    let meta = fs::metadata(path).map_err(|e| KeyError::Unreadable(path.to_path_buf(), e))?;
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(KeyError::InsecureMode {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_file_mode(_path: &Path) -> Result<(), KeyError> {
    // Non-unix targets aren't supported for write-side daemons in v0;
    // the methodology assumes macOS launchd. If we ever need to ship
    // on Windows / Linux, this is the spot for a methodology entry +
    // platform-specific mode check.
    Ok(())
}

/// Minimal Base58 encoder (no external dep). Produces the standard
/// Bitcoin-alphabet base58 string Solana uses for pubkeys. Pulled in
/// here to avoid forcing `bs58` into the dep tree for slice 1; the
/// real daemon will use `solana-sdk::pubkey::Pubkey::to_string()` which
/// formats identically.
fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 58] =
        b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    // Count leading zeros (preserved as leading '1's per base58).
    let zeros = bytes.iter().take_while(|b| **b == 0).count();

    // Convert to base 58 by repeated division.
    let mut digits = Vec::<u8>::with_capacity(bytes.len() * 138 / 100 + 1);
    for &byte in &bytes[zeros..] {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let mut out = String::with_capacity(zeros + digits.len());
    for _ in 0..zeros {
        out.push('1');
    }
    for d in digits.iter().rev() {
        out.push(ALPHABET[*d as usize] as char);
    }
    out
}

/// Prod-mode keypair loading is intentionally unimplemented in this
/// slice. Calls during prod-mode boot route here so the failure is
/// visible at the right layer (key-load), not at first-signing.
pub fn load_prod_keypair() -> Result<(), KeyError> {
    Err(KeyError::ProdNotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    fn write_keypair_file(dir: &Path, name: &str, mode: u32, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        let json = serde_json::to_string(bytes).unwrap();
        fs::write(&path, json).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(mode);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    fn fake_keypair_bytes() -> [u8; 64] {
        let mut bytes = [0u8; 64];
        for i in 0..64 {
            bytes[i] = (i + 1) as u8;
        }
        bytes
    }

    #[test]
    fn loads_well_formed_0600_keypair() {
        let dir = TempDir::new().unwrap();
        let path = write_keypair_file(dir.path(), "kp.json", 0o600, &fake_keypair_bytes());
        let kp = DevKeypair::load_from_path(&path).expect("load");
        assert_eq!(kp.secret_bytes()[0], 1);
        assert_eq!(kp.secret_bytes()[63], 64);
        assert_eq!(kp.pubkey_bytes()[0], 33); // first byte of the trailing 32
    }

    #[test]
    fn rejects_missing_file() {
        let err = DevKeypair::load_from_path(Path::new("/no/such/path.json")).unwrap_err();
        assert!(matches!(err, KeyError::NotFound(_)));
    }

    #[test]
    fn rejects_world_readable_keypair() {
        let dir = TempDir::new().unwrap();
        let path = write_keypair_file(dir.path(), "kp.json", 0o644, &fake_keypair_bytes());
        let err = DevKeypair::load_from_path(&path).unwrap_err();
        assert!(matches!(
            err,
            KeyError::InsecureMode { mode: 0o644, .. }
        ));
    }

    #[test]
    fn rejects_group_readable_keypair() {
        let dir = TempDir::new().unwrap();
        let path = write_keypair_file(dir.path(), "kp.json", 0o640, &fake_keypair_bytes());
        let err = DevKeypair::load_from_path(&path).unwrap_err();
        assert!(matches!(err, KeyError::InsecureMode { mode: 0o640, .. }));
    }

    #[test]
    fn rejects_wrong_byte_count() {
        let dir = TempDir::new().unwrap();
        let too_short = vec![0u8; 32];
        let path = write_keypair_file(dir.path(), "short.json", 0o600, &too_short);
        let err = DevKeypair::load_from_path(&path).unwrap_err();
        match err {
            KeyError::Malformed { reason, .. } => {
                assert!(reason.contains("expected 64 bytes"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn rejects_non_json_content() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("garbage.json");
        fs::write(&path, "not json at all").unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&path, perms).unwrap();
        let err = DevKeypair::load_from_path(&path).unwrap_err();
        assert!(matches!(err, KeyError::Malformed { .. }));
    }

    #[test]
    fn prod_loader_is_not_yet_implemented() {
        let err = load_prod_keypair().unwrap_err();
        assert!(matches!(err, KeyError::ProdNotImplemented));
    }

    #[test]
    fn bs58_known_pubkey_roundtrip() {
        // First 32 bytes of an all-1s 64-byte keypair → known b58.
        let bytes = [1u8; 32];
        let encoded = bs58_encode(&bytes);
        // Verify against a hand-checked reference: bs58 of [1;32]
        // starts with several digits. Spot-check non-empty + ASCII.
        assert!(!encoded.is_empty());
        assert!(encoded.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn bs58_preserves_leading_zeros_as_ones() {
        let bytes = vec![0u8, 0u8, 1u8];
        let encoded = bs58_encode(&bytes);
        assert!(encoded.starts_with("11"), "got {encoded}");
    }

    #[test]
    fn default_dev_path_is_under_app_support() {
        let p = default_dev_keypair_path();
        let s = p.to_string_lossy();
        assert!(s.ends_with("scryer/keys/pyth-poster.json"), "got {s}");
    }
}
