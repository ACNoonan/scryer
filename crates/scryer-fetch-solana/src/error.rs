use thiserror::Error;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("rate limited (429); giving up after {attempts} attempts")]
    RateLimitGiveUp { attempts: u32 },

    #[error("signature pagination cursor failed to advance")]
    CursorStuck,

    #[error("signature pagination hit safety cap of {cap} pages")]
    SignaturePageCap { cap: u32 },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
