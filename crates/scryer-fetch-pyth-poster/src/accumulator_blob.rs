//! Hermes accumulator-update binary blob parser.
//!
//! Hermes' `/v2/updates/price/latest?encoding=base64` endpoint returns
//! a `binary.data: [base64]` field whose decoded bytes are an
//! `AccumulatorUpdateData` (per `pythnet-sdk`'s wire module). Each
//! blob carries:
//!
//! - 4-byte `"PNAU"` magic
//! - 1-byte major version (must be `1`)
//! - 1-byte minor version (≥ 0)
//! - 1-byte trailing-payload length + that many trailing bytes
//!   (typically 0 — the trailing payload is reserved for future
//!   versions and is empty in production today)
//! - 1-byte proof-type tag (`0` = WormholeMerkle; only known variant)
//! - 2-byte big-endian VAA length + VAA bytes
//! - 1-byte `Vec<MerklePriceUpdate>` length + that many updates,
//!   where each update is:
//!     - 2-byte big-endian message length + message bytes
//!     - 1-byte proof-depth + that many 20-byte Keccak160 hashes
//!
//! All multi-byte integers in the wire format are **big-endian**
//! (per `pythnet-sdk::wire::v1::AccumulatorUpdateData::try_from_slice`
//! using `byteorder::BE`). The on-chain receiver instruction's
//! borsh-serialized `MerklePriceUpdate`, however, uses borsh's
//! standard u32-LE length prefix for both the message bytes and the
//! proof hashes. So this parser deliberately decodes to logical
//! `Vec<u8>` / `Vec<[u8; 20]>` and lets the on-chain encoder
//! re-emit under borsh framing.
//!
//! Source-truth reference: pyth-network/pyth-crosschain @ commit
//! `f8032d3`,
//! `pythnet-sdk/src/wire.rs::v1::AccumulatorUpdateData`. See
//! `methodology_log.md` "pyth-poster posting flow — 2026-04-29
//! (locked) §What the upstream sources lock #3".

use thiserror::Error;

/// 4-byte magic prefix identifying a Pythnet accumulator update.
pub const ACCUMULATOR_UPDATE_MAGIC: [u8; 4] = *b"PNAU";

/// The only `AccumulatorUpdateData.major_version` we accept. The
/// upstream parser rejects anything else as `Error::InvalidVersion`,
/// so we mirror that.
pub const ACCEPTED_MAJOR_VERSION: u8 = 1;

/// Proof-type tag for the WormholeMerkle variant — the only variant
/// the upstream `Proof` enum defines today.
pub const WORMHOLE_MERKLE_PROOF_TYPE: u8 = 0;

/// One Pyth merkle price update extracted from an accumulator blob.
/// Logical, not wire-encoded — the on-chain encoder re-emits this
/// under borsh framing for `pyth-solana-receiver::MerklePriceUpdate`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MerklePriceUpdate {
    /// The signed `PriceFeedMessage` bytes (Pyth's own format —
    /// header + price/conf/expo/publish_time/etc.). Caller does not
    /// decode these here; the receiver and push-oracle programs do.
    pub message: Vec<u8>,
    /// The merkle proof for `message` against the VAA's merkle root,
    /// as a sequence of 20-byte Keccak160 sibling hashes.
    pub proof: Vec<[u8; 20]>,
}

/// One decoded accumulator-update blob from Hermes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccumulatorUpdate {
    /// The signed Wormhole VAA — guardian-signed payload that
    /// commits to the merkle root the per-update `proof` fields
    /// hash against. Forwarded verbatim into Wormhole core's
    /// `WriteEncodedVaa` in chunks during the daemon's flow.
    pub vaa: Vec<u8>,
    /// Per-feed merkle price updates riding under the VAA. For
    /// single-feed Hermes responses (the daemon's typical case)
    /// this contains exactly one element; multi-feed responses
    /// pack several.
    pub updates: Vec<MerklePriceUpdate>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum BlobError {
    #[error("blob too short for header (got {0} bytes, need ≥ {1})")]
    Truncated(usize, usize),
    #[error("bad magic: expected {expected:?}, got {got:?}")]
    BadMagic { expected: [u8; 4], got: [u8; 4] },
    #[error(
        "unsupported major version: expected {expected}, got {got}"
    )]
    UnsupportedMajor { expected: u8, got: u8 },
    #[error(
        "unsupported proof-type tag: expected {expected}, got {got}"
    )]
    UnsupportedProofType { expected: u8, got: u8 },
    #[error("trailing bytes after fully decoded blob: {0} extra bytes")]
    TrailingBytes(usize),
}

/// Decode a Hermes accumulator-update binary blob (typically the
/// base64-decoded contents of `binary.data[i]`).
pub fn parse_accumulator_update(bytes: &[u8]) -> Result<AccumulatorUpdate, BlobError> {
    let mut cur = Cursor::new(bytes);

    // Magic (4 bytes).
    let magic = cur.take_array::<4>()?;
    if magic != ACCUMULATOR_UPDATE_MAGIC {
        return Err(BlobError::BadMagic {
            expected: ACCUMULATOR_UPDATE_MAGIC,
            got: magic,
        });
    }

    // Major + minor versions.
    let major = cur.take_byte()?;
    if major != ACCEPTED_MAJOR_VERSION {
        return Err(BlobError::UnsupportedMajor {
            expected: ACCEPTED_MAJOR_VERSION,
            got: major,
        });
    }
    let _minor = cur.take_byte()?; // Forward-compatible per upstream.

    // Trailing payload — `Vec<u8>` with a u8 length prefix in this
    // wire format. Typically empty; we skip past it.
    let trailing_len = cur.take_byte()? as usize;
    cur.advance(trailing_len)?;

    // Proof-type discriminant.
    let proof_type = cur.take_byte()?;
    if proof_type != WORMHOLE_MERKLE_PROOF_TYPE {
        return Err(BlobError::UnsupportedProofType {
            expected: WORMHOLE_MERKLE_PROOF_TYPE,
            got: proof_type,
        });
    }

    // VAA bytes (PrefixedVec<u16, u8> — u16 BE length).
    let vaa_len = cur.take_u16_be()? as usize;
    let vaa = cur.take_slice(vaa_len)?.to_vec();

    // Vec<MerklePriceUpdate> — u8 length prefix in this wire format.
    let updates_len = cur.take_byte()? as usize;
    let mut updates = Vec::with_capacity(updates_len);
    for _ in 0..updates_len {
        let msg_len = cur.take_u16_be()? as usize;
        let message = cur.take_slice(msg_len)?.to_vec();
        let proof_len = cur.take_byte()? as usize;
        let mut proof = Vec::with_capacity(proof_len);
        for _ in 0..proof_len {
            let h = cur.take_array::<20>()?;
            proof.push(h);
        }
        updates.push(MerklePriceUpdate { message, proof });
    }

    let remaining = cur.remaining();
    if remaining > 0 {
        return Err(BlobError::TrailingBytes(remaining));
    }

    Ok(AccumulatorUpdate { vaa, updates })
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn need(&self, n: usize) -> Result<(), BlobError> {
        if self.remaining() < n {
            Err(BlobError::Truncated(self.buf.len(), self.pos + n))
        } else {
            Ok(())
        }
    }

    fn take_byte(&mut self) -> Result<u8, BlobError> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn take_u16_be(&mut self) -> Result<u16, BlobError> {
        self.need(2)?;
        let bytes = [self.buf[self.pos], self.buf[self.pos + 1]];
        self.pos += 2;
        Ok(u16::from_be_bytes(bytes))
    }

    fn take_slice(&mut self, n: usize) -> Result<&'a [u8], BlobError> {
        self.need(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], BlobError> {
        let s = self.take_slice(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(s);
        Ok(out)
    }

    fn advance(&mut self, n: usize) -> Result<(), BlobError> {
        self.need(n)?;
        self.pos += n;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic accumulator blob with the given VAA and
    /// (message, proof) updates. Used to round-trip the parser
    /// without needing a captured live fixture.
    fn build_blob(
        vaa: &[u8],
        updates: &[(&[u8], &[[u8; 20]])],
        minor_version: u8,
        trailing: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&ACCUMULATOR_UPDATE_MAGIC);
        out.push(ACCEPTED_MAJOR_VERSION);
        out.push(minor_version);
        out.push(trailing.len() as u8);
        out.extend_from_slice(trailing);
        out.push(WORMHOLE_MERKLE_PROOF_TYPE);
        let vaa_len: u16 = vaa.len().try_into().unwrap();
        out.extend_from_slice(&vaa_len.to_be_bytes());
        out.extend_from_slice(vaa);
        out.push(updates.len() as u8);
        for (msg, proof) in updates {
            let msg_len: u16 = msg.len().try_into().unwrap();
            out.extend_from_slice(&msg_len.to_be_bytes());
            out.extend_from_slice(msg);
            out.push(proof.len() as u8);
            for h in proof.iter() {
                out.extend_from_slice(h);
            }
        }
        out
    }

    #[test]
    fn round_trips_single_update_blob() {
        let vaa = (0..400u16).map(|i| (i & 0xff) as u8).collect::<Vec<u8>>();
        let msg = vec![0xaa; 85]; // typical PriceFeedMessage size
        let proof = [[1u8; 20], [2u8; 20], [3u8; 20], [4u8; 20]];
        let blob = build_blob(&vaa, &[(&msg, &proof)], 0, &[]);

        let parsed = parse_accumulator_update(&blob).expect("parse");
        assert_eq!(parsed.vaa, vaa);
        assert_eq!(parsed.updates.len(), 1);
        assert_eq!(parsed.updates[0].message, msg);
        assert_eq!(parsed.updates[0].proof, proof.to_vec());
    }

    #[test]
    fn round_trips_multi_update_blob() {
        let vaa = vec![0xfeu8; 1024];
        let msg_a = vec![0x01; 85];
        let msg_b = vec![0x02; 85];
        let blob = build_blob(
            &vaa,
            &[
                (&msg_a, &[[1u8; 20], [2u8; 20]]),
                (&msg_b, &[[3u8; 20]]),
            ],
            0,
            &[],
        );

        let parsed = parse_accumulator_update(&blob).expect("parse");
        assert_eq!(parsed.vaa.len(), 1024);
        assert_eq!(parsed.updates.len(), 2);
        assert_eq!(parsed.updates[0].message, msg_a);
        assert_eq!(parsed.updates[0].proof.len(), 2);
        assert_eq!(parsed.updates[1].message, msg_b);
        assert_eq!(parsed.updates[1].proof.len(), 1);
    }

    #[test]
    fn tolerates_nonzero_minor_version() {
        // Per upstream's `try_from_slice`, only the major version is
        // strictly checked; minor versions ≥ CURRENT_MINOR_VERSION
        // are accepted (forward-compatible).
        let vaa = vec![0u8; 16];
        let blob = build_blob(&vaa, &[(&[1, 2, 3, 4], &[])], 7, &[]);
        let parsed = parse_accumulator_update(&blob).expect("parse");
        assert_eq!(parsed.vaa.len(), 16);
    }

    #[test]
    fn skips_trailing_payload_bytes() {
        // Real-world blobs typically have empty trailing; this
        // test pins the "PrefixedVec<u8, u8>" header behavior in
        // case upstream introduces a non-empty trailing payload.
        let vaa = vec![0u8; 16];
        let trailing = vec![0xde, 0xad, 0xbe, 0xef];
        let blob = build_blob(&vaa, &[(&[1, 2, 3, 4], &[])], 0, &trailing);
        let parsed = parse_accumulator_update(&blob).expect("parse");
        assert_eq!(parsed.vaa.len(), 16);
        assert_eq!(parsed.updates.len(), 1);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut blob = build_blob(&[0u8; 4], &[(&[1, 2], &[])], 0, &[]);
        blob[0] = b'X';
        let err = parse_accumulator_update(&blob).unwrap_err();
        match err {
            BlobError::BadMagic { got, .. } => assert_eq!(got[0], b'X'),
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_major_version() {
        let mut blob = build_blob(&[0u8; 4], &[(&[1, 2], &[])], 0, &[]);
        blob[4] = 2; // major version offset in the layout
        let err = parse_accumulator_update(&blob).unwrap_err();
        assert!(matches!(err, BlobError::UnsupportedMajor { got: 2, .. }));
    }

    #[test]
    fn rejects_unknown_proof_type() {
        let vaa = vec![0u8; 8];
        let mut blob = build_blob(&vaa, &[(&[1, 2], &[])], 0, &[]);
        // Find the proof-type byte: 4 magic + 1 major + 1 minor + 1
        // trailing-len + 0 trailing = offset 7.
        blob[7] = 99;
        let err = parse_accumulator_update(&blob).unwrap_err();
        assert!(matches!(err, BlobError::UnsupportedProofType { got: 99, .. }));
    }

    #[test]
    fn rejects_truncated_vaa_length() {
        // Construct a blob whose VAA-length field claims more bytes
        // than are present.
        let mut blob = build_blob(&[0u8; 4], &[(&[1, 2], &[])], 0, &[]);
        // Find the VAA-length offset: 7 (magic+versions+trail+proof) → 8/9
        // Actually offset 8/9 with our layout. Inflate to 0xFFFF.
        blob[8] = 0xff;
        blob[9] = 0xff;
        let err = parse_accumulator_update(&blob).unwrap_err();
        assert!(matches!(err, BlobError::Truncated(_, _)));
    }

    #[test]
    fn rejects_trailing_bytes_after_blob() {
        let vaa = vec![0u8; 8];
        let mut blob = build_blob(&vaa, &[(&[1, 2, 3, 4], &[])], 0, &[]);
        blob.extend_from_slice(&[0xab, 0xcd]); // tack on extra bytes
        let err = parse_accumulator_update(&blob).unwrap_err();
        assert!(matches!(err, BlobError::TrailingBytes(2)));
    }

    #[test]
    fn parses_zero_proof_depth_update() {
        // Edge case: a single-leaf merkle tree has zero proof
        // siblings. Pyth doesn't ship this in production, but the
        // parser should not panic.
        let vaa = vec![0u8; 8];
        let blob = build_blob(&vaa, &[(&[1, 2, 3, 4], &[])], 0, &[]);
        let parsed = parse_accumulator_update(&blob).expect("parse");
        assert_eq!(parsed.updates[0].proof.len(), 0);
    }
}
