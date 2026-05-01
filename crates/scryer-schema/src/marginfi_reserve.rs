//! MarginFi-v2 Bank-account snapshots.
//!
//! `v1` is locked. Counterpart to `kamino_reserve.v1` (item 4) for
//! soothsayer Paper-3's cross-protocol parameter table. MarginFi
//! calls these "Banks", one per asset within a Group; `bank` is the
//! account-PDA pubkey returned from `getProgramAccounts`.
//!
//! Layout pinned 2026-04-30 against `idl/marginfi/marginfi-v2.json`
//! (anchor IDL fetched live from
//! `MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA`) and verified
//! against a real on-chain Bank account (1864-byte total, 1856-byte
//! body after 8-byte Anchor disc). MarginFi-v2 is a zero-copy
//! Anchor program (`#[zero_copy]` + bytemuck repr=C); decode is
//! offset-based, same approach Kamino + Fluid take.
//!
//! # Bank top-level layout (after the 8-byte Anchor discriminator)
//!
//! ```text
//!     8  mint: pubkey (32)
//!    40  mint_decimals: u8 (1)
//!    41  group: pubkey (32)
//!    73  _pad0: [u8; 7]
//!    80  asset_share_value: WrappedI80F48 (16)
//!    96  liability_share_value: WrappedI80F48 (16)
//!   112  liquidity_vault: pubkey (32)
//!   144  liquidity_vault_bump: u8
//!   145  liquidity_vault_authority_bump: u8
//!   146  insurance_vault: pubkey (32)
//!   178  insurance_vault_bump: u8
//!   179  insurance_vault_authority_bump: u8
//!   180  _pad1: [u8; 4]
//!   184  collected_insurance_fees_outstanding: WrappedI80F48 (16)
//!   200  fee_vault: pubkey (32)
//!   232  fee_vault_bump: u8
//!   233  fee_vault_authority_bump: u8
//!   234  _pad2: [u8; 6]
//!   240  collected_group_fees_outstanding: WrappedI80F48 (16)
//!   256  total_liability_shares: WrappedI80F48 (16)
//!   272  total_asset_shares: WrappedI80F48 (16)
//!   288  last_update: i64 (8)
//!   296  config: BankConfig (544)
//!   840  flags: u64 (8)
//!   ...
//! ```
//!
//! # BankConfig nested layout (within the Bank, starting at byte 296)
//!
//! ```text
//!   296  asset_weight_init: WrappedI80F48 (16)
//!   312  asset_weight_maint: WrappedI80F48 (16)
//!   328  liability_weight_init: WrappedI80F48 (16)
//!   344  liability_weight_maint: WrappedI80F48 (16)
//!   360  deposit_limit: u64 (8)
//!   368  interest_rate_config: InterestRateConfig (240)
//!   608  operational_state: BankOperationalState (1, rust-repr enum)
//!   609  oracle_setup: OracleSetup (1, rust-repr enum)
//!   610  oracle_keys: [pubkey; 5] (160)
//!   770  _pad0: [u8; 6]
//!   776  borrow_limit: u64 (8)
//!   784  risk_tier: RiskTier (1)
//!   785  asset_tag: u8
//!   786  config_flags: u8
//!   787  _pad1: [u8; 5]
//!   792  total_asset_value_init_limit: u64 (8)
//!   800  oracle_max_age: u16 (2)
//!   802  _padding0: [u8; 2]
//!   804  oracle_max_confidence: u32 (4)
//!   808  fixed_price: WrappedI80F48 (16)
//!   824  _padding1: [u8; 16]
//! ```
//!
//! # WrappedI80F48 → f64
//!
//! 16 bytes little-endian, signed I80F48 fixed-point: `f64 =
//! i128::from_le_bytes(bytes16) as f64 * 2f64.powi(-48)`. Same Q-style
//! conversion Kamino uses at Q60; MarginFi pins at Q48.
//!
//! # `oracle_keys` is a fixed array of 5 pubkeys
//!
//! Many Banks only populate slot 0. Zero-pubkey
//! (`11111111111111111111111111111111`) entries are filtered out at
//! decode time; the schema column carries only the populated keys.
//! Order is preserved for downstream consumer dispatch (per the
//! methodology lock).
//!
//! # What's NOT in this schema (vs. the original `docs/schemas.md` spec)
//!
//! The locked spec listed `liquidator_fee_pct` and `insurance_fee_pct`
//! as Bank-level fields. The actual marginfi-v2 IDL puts liquidation
//! fees in the GLOBAL `FeeState` account
//! (`liquidation_max_fee` / `liquidation_flat_sol_fee`), not per-Bank.
//! This v1 schema captures only what's actually wire-encoded per-Bank;
//! a future consumer that needs the global liquidation-fee config
//! reads `FeeState` separately.
//!
//! The four interest-rate-related fees (`insurance_fee_fixed_apr`,
//! `insurance_ir_fee`, `protocol_fixed_fee_apr`, `protocol_ir_fee`)
//! ARE per-Bank — they live inside `interest_rate_config`. v1 captures
//! them in `ir_curve_points_json` alongside the curve points + util
//! rates.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "marginfi_reserve.v1";

    /// Solana System Program — sentinel for unused `oracle_keys` slots.
    pub const SYSTEM_PROGRAM_NULL: &str = "11111111111111111111111111111111";

    /// One MarginFi-v2 Bank snapshot.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reserve {
        /// Bank account pubkey (the PDA returned from
        /// `getProgramAccounts`). `_dedup_key` source.
        pub bank: String,
        /// MarginfiGroup pubkey this Bank belongs to.
        pub group: String,
        /// SPL mint of the Bank's asset.
        pub asset_mint: String,
        /// Symbol resolved from caller-supplied xStock/SPL registry.
        /// `"?"` for mints not in the registry.
        pub asset_symbol: String,
        /// Decimals of `asset_mint` (per Bank config).
        pub asset_decimals: u8,

        // === Oracle wiring ===
        /// Snake-case enum variant of `OracleSetup` (e.g.
        /// `"switchboard_pull"`, `"pyth_push_oracle"`, `"fixed"`,
        /// `"none"`, `"kamino_pyth_push"`, …). 18 variants total per
        /// IDL; consumers should match against the lowercased
        /// snake-case form to be robust to additions.
        pub oracle_setup: String,
        /// Up to 5 base58-encoded pubkeys, with zero-pubkey entries
        /// filtered out. Order preserved.
        pub oracle_keys: Vec<String>,
        /// Staleness cap in seconds (oracle's max age before MarginFi
        /// rejects the price).
        pub oracle_max_age_seconds: u16,
        /// Max-confidence cap. `0` = unset / no cap.
        pub oracle_max_confidence: u32,

        // === Risk weights (Q48 → f64) ===
        pub asset_weight_init: f64,
        pub asset_weight_maint: f64,
        pub liability_weight_init: f64,
        pub liability_weight_maint: f64,

        // === Limits ===
        /// Asset-native units.
        pub deposit_limit: u64,
        /// Asset-native units.
        pub borrow_limit: u64,
        /// Caps `Σ(asset value × weight_init)` at Bank level.
        pub total_asset_value_init_limit: u64,

        // === State + tags ===
        /// Snake-case `BankOperationalState`: `"paused"` |
        /// `"operational"` | `"reduce_only"` | `"killed_by_bankruptcy"`.
        pub operational_state: String,
        /// Snake-case `RiskTier`: `"collateral"` | `"isolated"`.
        pub risk_tier: String,
        /// Per-Bank classification tag (used by some risk-engine
        /// pathways).
        pub asset_tag: u8,
        /// Bitfield of per-Bank flags. Documented in
        /// `idl/marginfi/marginfi-v2.json` and the marginfi-v2 source.
        pub config_flags: u8,

        // === Interest-rate config (key fields surfaced; full curve
        // is in `ir_curve_points_json`) ===
        pub optimal_utilization_rate: f64,
        pub plateau_interest_rate: f64,
        pub max_interest_rate: f64,
        pub insurance_fee_fixed_apr: f64,
        pub insurance_ir_fee: f64,
        pub protocol_fixed_fee_apr: f64,
        pub protocol_ir_fee: f64,
        pub protocol_origination_fee: f64,
        /// `0` (legacy 3-point) or `1` (multi-point). Selects which
        /// curve implementation is active.
        pub curve_type: u8,
        /// JSON-serialized list of `(util_rate_u32, rate_u32)` pairs
        /// — `[zero_util_rate, hundred_util_rate, points[0..5]]`. Up
        /// to 7 entries; small enough that JSON is more honest than
        /// parallel-array fixed-arity columns.
        pub ir_curve_points_json: String,

        // === Bank cache (last oracle read + spot rates) ===
        /// USD price from the last instruction that consumed an
        /// oracle. `0.0` if never consumed.
        pub cache_last_oracle_price: f64,
        /// Confidence at the same observation. `0.0` if never.
        pub cache_last_oracle_price_confidence: f64,
        /// Unix-seconds when `cache_last_oracle_price` was last
        /// updated. `0` if never.
        pub cache_last_oracle_price_timestamp: i64,
        /// APR (0–1000%) cached from utilization-driven derivation;
        /// `cache.base_rate / u32::MAX × 1000`.
        pub cache_base_rate_pct: f64,
        pub cache_lending_rate_pct: f64,
        pub cache_borrowing_rate_pct: f64,

        /// `bank.last_update` — unix-seconds of the last bank-state
        /// mutation (interest accrual, etc.).
        pub last_update: i64,
        /// Full account data, base64-encoded. Preserved for forensic
        /// re-decode of fields the typed columns don't surface (the
        /// 304-byte trailer `integration_acc_*` / `rate_limiter` /
        /// padding) without re-fetching.
        pub raw_account_b64: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reserve {
        /// `(bank, _fetched_at)` is a unique snapshot key. Same shape
        /// as `kamino_reserve.v1` so weekly snapshots accumulate as
        /// distinct rows for parameter-drift analysis.
        pub fn dedup_key(&self) -> String {
            format!("marginfi_reserve:{}:{}", self.bank, self.meta.fetched_at)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("bank", DataType::LargeUtf8, false),
            Field::new("group", DataType::LargeUtf8, false),
            Field::new("asset_mint", DataType::LargeUtf8, false),
            Field::new("asset_symbol", DataType::LargeUtf8, false),
            Field::new("asset_decimals", DataType::UInt8, false),
            Field::new("oracle_setup", DataType::LargeUtf8, false),
            // oracle_keys serialized as comma-joined string; consumers
            // split on `,`. Arrow ListArray would be more typed but adds
            // surface area — stay compatible with the simpler
            // LargeUtf8 column convention used elsewhere in the repo.
            Field::new("oracle_keys", DataType::LargeUtf8, false),
            Field::new("oracle_max_age_seconds", DataType::UInt16, false),
            Field::new("oracle_max_confidence", DataType::UInt32, false),
            Field::new("asset_weight_init", DataType::Float64, false),
            Field::new("asset_weight_maint", DataType::Float64, false),
            Field::new("liability_weight_init", DataType::Float64, false),
            Field::new("liability_weight_maint", DataType::Float64, false),
            Field::new("deposit_limit", DataType::UInt64, false),
            Field::new("borrow_limit", DataType::UInt64, false),
            Field::new("total_asset_value_init_limit", DataType::UInt64, false),
            Field::new("operational_state", DataType::LargeUtf8, false),
            Field::new("risk_tier", DataType::LargeUtf8, false),
            Field::new("asset_tag", DataType::UInt8, false),
            Field::new("config_flags", DataType::UInt8, false),
            Field::new("optimal_utilization_rate", DataType::Float64, false),
            Field::new("plateau_interest_rate", DataType::Float64, false),
            Field::new("max_interest_rate", DataType::Float64, false),
            Field::new("insurance_fee_fixed_apr", DataType::Float64, false),
            Field::new("insurance_ir_fee", DataType::Float64, false),
            Field::new("protocol_fixed_fee_apr", DataType::Float64, false),
            Field::new("protocol_ir_fee", DataType::Float64, false),
            Field::new("protocol_origination_fee", DataType::Float64, false),
            Field::new("curve_type", DataType::UInt8, false),
            Field::new("ir_curve_points_json", DataType::LargeUtf8, false),
            Field::new("cache_last_oracle_price", DataType::Float64, false),
            Field::new("cache_last_oracle_price_confidence", DataType::Float64, false),
            Field::new("cache_last_oracle_price_timestamp", DataType::Int64, false),
            Field::new("cache_base_rate_pct", DataType::Float64, false),
            Field::new("cache_lending_rate_pct", DataType::Float64, false),
            Field::new("cache_borrowing_rate_pct", DataType::Float64, false),
            Field::new("last_update", DataType::Int64, false),
            Field::new("raw_account_b64", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Reserve]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let bank = LargeStringArray::from_iter_values(rows.iter().map(|r| r.bank.as_str()));
        let group = LargeStringArray::from_iter_values(rows.iter().map(|r| r.group.as_str()));
        let asset_mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_mint.as_str()));
        let asset_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_symbol.as_str()));
        let asset_decimals = UInt8Array::from_iter_values(rows.iter().map(|r| r.asset_decimals));
        let oracle_setup =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.oracle_setup.as_str()));
        let oracle_keys_joined: Vec<String> =
            rows.iter().map(|r| r.oracle_keys.join(",")).collect();
        let oracle_keys =
            LargeStringArray::from_iter_values(oracle_keys_joined.iter().map(|s| s.as_str()));
        let oracle_max_age_seconds =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.oracle_max_age_seconds));
        let oracle_max_confidence =
            UInt32Array::from_iter_values(rows.iter().map(|r| r.oracle_max_confidence));
        let asset_weight_init =
            Float64Array::from_iter_values(rows.iter().map(|r| r.asset_weight_init));
        let asset_weight_maint =
            Float64Array::from_iter_values(rows.iter().map(|r| r.asset_weight_maint));
        let liability_weight_init =
            Float64Array::from_iter_values(rows.iter().map(|r| r.liability_weight_init));
        let liability_weight_maint =
            Float64Array::from_iter_values(rows.iter().map(|r| r.liability_weight_maint));
        let deposit_limit = UInt64Array::from_iter_values(rows.iter().map(|r| r.deposit_limit));
        let borrow_limit = UInt64Array::from_iter_values(rows.iter().map(|r| r.borrow_limit));
        let total_asset_value_init_limit =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.total_asset_value_init_limit));
        let operational_state =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.operational_state.as_str()));
        let risk_tier =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.risk_tier.as_str()));
        let asset_tag = UInt8Array::from_iter_values(rows.iter().map(|r| r.asset_tag));
        let config_flags = UInt8Array::from_iter_values(rows.iter().map(|r| r.config_flags));
        let optimal_utilization_rate =
            Float64Array::from_iter_values(rows.iter().map(|r| r.optimal_utilization_rate));
        let plateau_interest_rate =
            Float64Array::from_iter_values(rows.iter().map(|r| r.plateau_interest_rate));
        let max_interest_rate =
            Float64Array::from_iter_values(rows.iter().map(|r| r.max_interest_rate));
        let insurance_fee_fixed_apr =
            Float64Array::from_iter_values(rows.iter().map(|r| r.insurance_fee_fixed_apr));
        let insurance_ir_fee =
            Float64Array::from_iter_values(rows.iter().map(|r| r.insurance_ir_fee));
        let protocol_fixed_fee_apr =
            Float64Array::from_iter_values(rows.iter().map(|r| r.protocol_fixed_fee_apr));
        let protocol_ir_fee =
            Float64Array::from_iter_values(rows.iter().map(|r| r.protocol_ir_fee));
        let protocol_origination_fee =
            Float64Array::from_iter_values(rows.iter().map(|r| r.protocol_origination_fee));
        let curve_type = UInt8Array::from_iter_values(rows.iter().map(|r| r.curve_type));
        let ir_curve_points_json = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.ir_curve_points_json.as_str()),
        );
        let cache_last_oracle_price =
            Float64Array::from_iter_values(rows.iter().map(|r| r.cache_last_oracle_price));
        let cache_last_oracle_price_confidence = Float64Array::from_iter_values(
            rows.iter().map(|r| r.cache_last_oracle_price_confidence),
        );
        let cache_last_oracle_price_timestamp = Int64Array::from_iter_values(
            rows.iter().map(|r| r.cache_last_oracle_price_timestamp),
        );
        let cache_base_rate_pct =
            Float64Array::from_iter_values(rows.iter().map(|r| r.cache_base_rate_pct));
        let cache_lending_rate_pct =
            Float64Array::from_iter_values(rows.iter().map(|r| r.cache_lending_rate_pct));
        let cache_borrowing_rate_pct =
            Float64Array::from_iter_values(rows.iter().map(|r| r.cache_borrowing_rate_pct));
        let last_update = Int64Array::from_iter_values(rows.iter().map(|r| r.last_update));
        let raw_account_b64 =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_account_b64.as_str()));
        let schema_version = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.meta.schema_version.as_str()),
        );
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(bank),
            Arc::new(group),
            Arc::new(asset_mint),
            Arc::new(asset_symbol),
            Arc::new(asset_decimals),
            Arc::new(oracle_setup),
            Arc::new(oracle_keys),
            Arc::new(oracle_max_age_seconds),
            Arc::new(oracle_max_confidence),
            Arc::new(asset_weight_init),
            Arc::new(asset_weight_maint),
            Arc::new(liability_weight_init),
            Arc::new(liability_weight_maint),
            Arc::new(deposit_limit),
            Arc::new(borrow_limit),
            Arc::new(total_asset_value_init_limit),
            Arc::new(operational_state),
            Arc::new(risk_tier),
            Arc::new(asset_tag),
            Arc::new(config_flags),
            Arc::new(optimal_utilization_rate),
            Arc::new(plateau_interest_rate),
            Arc::new(max_interest_rate),
            Arc::new(insurance_fee_fixed_apr),
            Arc::new(insurance_ir_fee),
            Arc::new(protocol_fixed_fee_apr),
            Arc::new(protocol_ir_fee),
            Arc::new(protocol_origination_fee),
            Arc::new(curve_type),
            Arc::new(ir_curve_points_json),
            Arc::new(cache_last_oracle_price),
            Arc::new(cache_last_oracle_price_confidence),
            Arc::new(cache_last_oracle_price_timestamp),
            Arc::new(cache_base_rate_pct),
            Arc::new(cache_lending_rate_pct),
            Arc::new(cache_borrowing_rate_pct),
            Arc::new(last_update),
            Arc::new(raw_account_b64),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reserve>, FromArrowError> {
        let bank = downcast_column::<LargeStringArray>(batch, "bank")?;
        let group = downcast_column::<LargeStringArray>(batch, "group")?;
        let asset_mint = downcast_column::<LargeStringArray>(batch, "asset_mint")?;
        let asset_symbol = downcast_column::<LargeStringArray>(batch, "asset_symbol")?;
        let asset_decimals = downcast_column::<UInt8Array>(batch, "asset_decimals")?;
        let oracle_setup = downcast_column::<LargeStringArray>(batch, "oracle_setup")?;
        let oracle_keys = downcast_column::<LargeStringArray>(batch, "oracle_keys")?;
        let oracle_max_age_seconds = downcast_column::<UInt16Array>(batch, "oracle_max_age_seconds")?;
        let oracle_max_confidence = downcast_column::<UInt32Array>(batch, "oracle_max_confidence")?;
        let asset_weight_init = downcast_column::<Float64Array>(batch, "asset_weight_init")?;
        let asset_weight_maint = downcast_column::<Float64Array>(batch, "asset_weight_maint")?;
        let liability_weight_init = downcast_column::<Float64Array>(batch, "liability_weight_init")?;
        let liability_weight_maint =
            downcast_column::<Float64Array>(batch, "liability_weight_maint")?;
        let deposit_limit = downcast_column::<UInt64Array>(batch, "deposit_limit")?;
        let borrow_limit = downcast_column::<UInt64Array>(batch, "borrow_limit")?;
        let total_asset_value_init_limit =
            downcast_column::<UInt64Array>(batch, "total_asset_value_init_limit")?;
        let operational_state = downcast_column::<LargeStringArray>(batch, "operational_state")?;
        let risk_tier = downcast_column::<LargeStringArray>(batch, "risk_tier")?;
        let asset_tag = downcast_column::<UInt8Array>(batch, "asset_tag")?;
        let config_flags = downcast_column::<UInt8Array>(batch, "config_flags")?;
        let optimal_utilization_rate =
            downcast_column::<Float64Array>(batch, "optimal_utilization_rate")?;
        let plateau_interest_rate =
            downcast_column::<Float64Array>(batch, "plateau_interest_rate")?;
        let max_interest_rate = downcast_column::<Float64Array>(batch, "max_interest_rate")?;
        let insurance_fee_fixed_apr =
            downcast_column::<Float64Array>(batch, "insurance_fee_fixed_apr")?;
        let insurance_ir_fee = downcast_column::<Float64Array>(batch, "insurance_ir_fee")?;
        let protocol_fixed_fee_apr =
            downcast_column::<Float64Array>(batch, "protocol_fixed_fee_apr")?;
        let protocol_ir_fee = downcast_column::<Float64Array>(batch, "protocol_ir_fee")?;
        let protocol_origination_fee =
            downcast_column::<Float64Array>(batch, "protocol_origination_fee")?;
        let curve_type = downcast_column::<UInt8Array>(batch, "curve_type")?;
        let ir_curve_points_json =
            downcast_column::<LargeStringArray>(batch, "ir_curve_points_json")?;
        let cache_last_oracle_price =
            downcast_column::<Float64Array>(batch, "cache_last_oracle_price")?;
        let cache_last_oracle_price_confidence =
            downcast_column::<Float64Array>(batch, "cache_last_oracle_price_confidence")?;
        let cache_last_oracle_price_timestamp =
            downcast_column::<Int64Array>(batch, "cache_last_oracle_price_timestamp")?;
        let cache_base_rate_pct = downcast_column::<Float64Array>(batch, "cache_base_rate_pct")?;
        let cache_lending_rate_pct =
            downcast_column::<Float64Array>(batch, "cache_lending_rate_pct")?;
        let cache_borrowing_rate_pct =
            downcast_column::<Float64Array>(batch, "cache_borrowing_rate_pct")?;
        let last_update = downcast_column::<Int64Array>(batch, "last_update")?;
        let raw_account_b64 = downcast_column::<LargeStringArray>(batch, "raw_account_b64")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            let oracle_keys_str = oracle_keys.value(i);
            let oracle_keys_vec: Vec<String> = if oracle_keys_str.is_empty() {
                Vec::new()
            } else {
                oracle_keys_str.split(',').map(String::from).collect()
            };
            out.push(Reserve {
                bank: bank.value(i).to_string(),
                group: group.value(i).to_string(),
                asset_mint: asset_mint.value(i).to_string(),
                asset_symbol: asset_symbol.value(i).to_string(),
                asset_decimals: asset_decimals.value(i),
                oracle_setup: oracle_setup.value(i).to_string(),
                oracle_keys: oracle_keys_vec,
                oracle_max_age_seconds: oracle_max_age_seconds.value(i),
                oracle_max_confidence: oracle_max_confidence.value(i),
                asset_weight_init: asset_weight_init.value(i),
                asset_weight_maint: asset_weight_maint.value(i),
                liability_weight_init: liability_weight_init.value(i),
                liability_weight_maint: liability_weight_maint.value(i),
                deposit_limit: deposit_limit.value(i),
                borrow_limit: borrow_limit.value(i),
                total_asset_value_init_limit: total_asset_value_init_limit.value(i),
                operational_state: operational_state.value(i).to_string(),
                risk_tier: risk_tier.value(i).to_string(),
                asset_tag: asset_tag.value(i),
                config_flags: config_flags.value(i),
                optimal_utilization_rate: optimal_utilization_rate.value(i),
                plateau_interest_rate: plateau_interest_rate.value(i),
                max_interest_rate: max_interest_rate.value(i),
                insurance_fee_fixed_apr: insurance_fee_fixed_apr.value(i),
                insurance_ir_fee: insurance_ir_fee.value(i),
                protocol_fixed_fee_apr: protocol_fixed_fee_apr.value(i),
                protocol_ir_fee: protocol_ir_fee.value(i),
                protocol_origination_fee: protocol_origination_fee.value(i),
                curve_type: curve_type.value(i),
                ir_curve_points_json: ir_curve_points_json.value(i).to_string(),
                cache_last_oracle_price: cache_last_oracle_price.value(i),
                cache_last_oracle_price_confidence: cache_last_oracle_price_confidence.value(i),
                cache_last_oracle_price_timestamp: cache_last_oracle_price_timestamp.value(i),
                cache_base_rate_pct: cache_base_rate_pct.value(i),
                cache_lending_rate_pct: cache_lending_rate_pct.value(i),
                cache_borrowing_rate_pct: cache_borrowing_rate_pct.value(i),
                last_update: last_update.value(i),
                raw_account_b64: raw_account_b64.value(i).to_string(),
                meta: Meta {
                    schema_version: sver.to_string(),
                    fetched_at: fetched_at.value(i),
                    source: source.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    /// Convert a 16-byte WrappedI80F48 little-endian array to f64.
    /// MarginFi's I80F48 is 80 bits integer + 48 bits fraction, stored
    /// as a 128-bit signed little-endian integer.
    pub fn wrapped_i80f48_to_f64(bytes: &[u8; 16]) -> f64 {
        let raw = i128::from_le_bytes(*bytes);
        raw as f64 * 2f64.powi(-48)
    }

    /// Convert MarginFi's u32-rate-out-of-1000% to a plain f64
    /// percentage. Used by `BankCache.{base,lending,borrowing}_rate`
    /// per the IDL docstring (`u32::MAX = 1000%`).
    pub fn u32_rate_to_pct(raw: u32) -> f64 {
        (raw as f64 / u32::MAX as f64) * 1000.0
    }

    /// Convert `OracleSetup` enum byte to snake_case string.
    pub fn oracle_setup_to_string(byte: u8) -> String {
        match byte {
            0 => "none",
            1 => "pyth_legacy",
            2 => "switchboard_v2",
            3 => "pyth_push_oracle",
            4 => "switchboard_pull",
            5 => "staked_with_pyth_push",
            6 => "kamino_pyth_push",
            7 => "kamino_switchboard_pull",
            8 => "fixed",
            9 => "drift_pyth_pull",
            10 => "drift_switchboard_pull",
            11 => "solend_pyth_pull",
            12 => "solend_switchboard_pull",
            13 => "fixed_kamino",
            14 => "fixed_drift",
            15 => "juplend_pyth_pull",
            16 => "juplend_switchboard_pull",
            17 => "fixed_juplend",
            _ => "unknown",
        }
        .to_string()
    }

    /// Convert `BankOperationalState` enum byte to snake_case string.
    pub fn operational_state_to_string(byte: u8) -> String {
        match byte {
            0 => "paused",
            1 => "operational",
            2 => "reduce_only",
            3 => "killed_by_bankruptcy",
            _ => "unknown",
        }
        .to_string()
    }

    /// Convert `RiskTier` enum byte to snake_case string.
    pub fn risk_tier_to_string(byte: u8) -> String {
        match byte {
            0 => "collateral",
            1 => "isolated",
            _ => "unknown",
        }
        .to_string()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn sample(bank: &str) -> Reserve {
            Reserve {
                bank: bank.to_string(),
                group: "1c731b2fe9c165b8e0ac33e8295f4c0e7c23fe622ec8a54e48abe2b4a6917a66".to_string(),
                asset_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                asset_symbol: "USDC".to_string(),
                asset_decimals: 6,
                oracle_setup: "switchboard_pull".to_string(),
                oracle_keys: vec!["2bc11b6c8390c54e62277468e2dc34f2e6f91c8dda9ccdc899716ae27e9a27d8".to_string()],
                oracle_max_age_seconds: 300,
                oracle_max_confidence: 0,
                asset_weight_init: 0.0,
                asset_weight_maint: 0.8,
                liability_weight_init: 1.3,
                liability_weight_maint: 1.2,
                deposit_limit: 986136984195825,
                borrow_limit: 986136984195825,
                total_asset_value_init_limit: 0,
                operational_state: "operational".to_string(),
                risk_tier: "collateral".to_string(),
                asset_tag: 0,
                config_flags: 0,
                optimal_utilization_rate: 0.8,
                plateau_interest_rate: 0.1,
                max_interest_rate: 1.5,
                insurance_fee_fixed_apr: 0.0,
                insurance_ir_fee: 0.0,
                protocol_fixed_fee_apr: 0.0,
                protocol_ir_fee: 0.0,
                protocol_origination_fee: 0.0,
                curve_type: 0,
                ir_curve_points_json: "[]".to_string(),
                cache_last_oracle_price: 1.0,
                cache_last_oracle_price_confidence: 0.0001,
                cache_last_oracle_price_timestamp: 1734971661,
                cache_base_rate_pct: 5.0,
                cache_lending_rate_pct: 4.0,
                cache_borrowing_rate_pct: 6.0,
                last_update: 1734971661,
                raw_account_b64: "BASE64HERE".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "marginfi_reserve.v1");
        }

        #[test]
        fn dedup_key_uses_bank_and_fetched_at() {
            let r = sample("14pCPReiear5V7viGVtdafwm6yCfBoz7pTkigGzcrdQm");
            assert_eq!(
                r.dedup_key(),
                "marginfi_reserve:14pCPReiear5V7viGVtdafwm6yCfBoz7pTkigGzcrdQm:1777300000"
            );
        }

        #[test]
        fn round_trip_preserves_all_fields_including_oracle_keys_list() {
            let r1 = sample("BANK1");
            let r2 = Reserve {
                oracle_keys: vec![
                    "key_a".to_string(),
                    "key_b".to_string(),
                    "key_c".to_string(),
                ],
                ..sample("BANK2")
            };
            let rows = vec![r1, r2];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 42);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn empty_oracle_keys_round_trips_as_empty_vec() {
            let r = Reserve {
                oracle_keys: vec![],
                ..sample("EMPTY")
            };
            let batch = to_record_batch(&[r.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered.len(), 1);
            assert!(recovered[0].oracle_keys.is_empty());
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut r = sample("BANK");
            r.meta.schema_version = "marginfi_reserve.v2".to_string();
            let batch = to_record_batch(&[r]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn wrapped_i80f48_decodes_known_values() {
            // 0.0
            assert_eq!(wrapped_i80f48_to_f64(&[0u8; 16]), 0.0);
            // 1.0 = 0x1_0000_0000_0000 (1 << 48) at byte 6 LSB-first
            let mut one = [0u8; 16];
            one[6] = 1;
            assert!((wrapped_i80f48_to_f64(&one) - 1.0).abs() < 1e-9);
            // 0.8 = 0xCCCCCCCCCCCC at the 6 lowest bytes (matches the
            // real on-chain `asset_weight_maint = 0.8` we verified
            // against bank `14pCPReiear...`).
            let v = [
                0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ];
            assert!((wrapped_i80f48_to_f64(&v) - 0.8).abs() < 1e-3);
        }

        #[test]
        fn oracle_setup_byte_to_string_covers_known_variants() {
            assert_eq!(oracle_setup_to_string(0), "none");
            assert_eq!(oracle_setup_to_string(1), "pyth_legacy");
            assert_eq!(oracle_setup_to_string(2), "switchboard_v2");
            assert_eq!(oracle_setup_to_string(4), "switchboard_pull");
            assert_eq!(oracle_setup_to_string(8), "fixed");
            assert_eq!(oracle_setup_to_string(17), "fixed_juplend");
            assert_eq!(oracle_setup_to_string(99), "unknown");
        }

        #[test]
        fn u32_rate_pct_corner_cases() {
            assert_eq!(u32_rate_to_pct(0), 0.0);
            assert!((u32_rate_to_pct(u32::MAX) - 1000.0).abs() < 1e-9);
            // half = 500%
            assert!((u32_rate_to_pct(u32::MAX / 2) - 500.0).abs() < 1.0);
        }
    }
}
