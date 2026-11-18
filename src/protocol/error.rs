use crate::protocol::endpoints::IdFormatError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

/// Error codes indicating that an Epic OAuth bearer token is expired, revoked, or invalid.
pub const INVALID_TOKEN_CODES: [&str; 3] = [
    "errors.com.epicgames.common.oauth.invalid_token",
    "errors.com.epicgames.common.authentication.token_verification_failed",
    "errors.com.epicgames.unauthorized",
];

pub mod error_codes {
    pub const STALE_REVISION: &str = "errors.com.epicgames.social.party.stale_revision";
    pub const CORRECTIVE_ACTION_REQUIRED: &str =
        "errors.com.epicgames.oauth.corrective_action_required";
    pub const INVALID_ACCOUNT_CREDENTIALS: &str =
        "errors.com.epicgames.account.invalid_account_credentials";
    pub const INVALID_REFRESH_TOKEN: &str =
        "errors.com.epicgames.account.auth_token.invalid_refresh_token";
    pub const AUTHORIZATION_CODE_NOT_FOUND: &str =
        "errors.com.epicgames.account.oauth.authorization_code_not_found";
    pub const AUTHORIZATION_PENDING: &str =
        "errors.com.epicgames.account.oauth.authorization_pending";
    pub const NOT_FOUND: &str = "errors.com.epicgames.not_found";
    pub const CONCURRENT_MODIFICATION: &str =
        "errors.com.epicgames.common.concurrent_modification_error";
    pub const THROTTLED: &str = "errors.com.epicgames.common.throttled";
    pub const PARTY_NOT_FOUND: &str = "errors.com.epicgames.social.party.party_not_found";
    pub const USER_HAS_NO_PARTY: &str = "errors.com.epicgames.social.party.user_has_no_party";
    pub const USER_HAS_PARTY: &str = "errors.com.epicgames.social.party.user_has_party";
}

/// Per-field validation details returned by Epic REST APIs inside `validationFailures`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationFailure {
    #[serde(default)]
    pub field_name: String,
    #[serde(default)]
    pub invalid_value: Option<serde_json::Value>,
    #[serde(default)]
    pub error_message: String,
    #[serde(default)]
    pub error_code: String,
    #[serde(default)]
    pub message_vars: Vec<String>,
}

/// Structured error payload returned by Epic Games Account, Party, Friends, and EOS HTTP services.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EpicgamesAPIErrorData {
    #[serde(rename = "errorCode", default)]
    pub error_code: String,
    #[serde(rename = "errorMessage", default)]
    pub error_message: String,
    #[serde(rename = "messageVars", default)]
    pub message_vars: Vec<String>,
    #[serde(rename = "numericErrorCode", default)]
    pub numeric_error_code: Option<i64>,
    #[serde(rename = "originatingService", default)]
    pub originating_service: Option<String>,
    #[serde(default)]
    pub intent: Option<String>,
    #[serde(default)]
    pub error_description: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(rename = "errorStatus", default)]
    pub error_status: Option<u16>,
    #[serde(alias = "continuation", alias = "continuationToken", default)]
    pub continuation_token: Option<String>,
    #[serde(rename = "correctiveAction", default)]
    pub corrective_action: Option<String>,
    #[serde(rename = "validationFailures", default)]
    pub validation_failures: Option<HashMap<String, ValidationFailure>>,
}

impl EpicgamesAPIErrorData {
    pub fn effective_code(&self) -> &str {
        if !self.error_code.is_empty() {
            &self.error_code
        } else {
            self.error.as_deref().unwrap_or_default()
        }
    }

    pub fn effective_message(&self) -> &str {
        if !self.error_message.is_empty() {
            &self.error_message
        } else {
            self.error_description
                .as_deref()
                .unwrap_or_else(|| self.effective_code())
        }
    }

    pub fn is_invalid_token(&self) -> bool {
        let code = self.effective_code();
        INVALID_TOKEN_CODES.contains(&code)
    }

    pub fn is_invalid_refresh_token(&self) -> bool {
        self.effective_code() == error_codes::INVALID_REFRESH_TOKEN
    }

    pub fn is_stale_revision(&self) -> bool {
        self.effective_code() == error_codes::STALE_REVISION
    }

    pub fn is_corrective_action_required(&self) -> bool {
        self.effective_code() == error_codes::CORRECTIVE_ACTION_REQUIRED
    }

    pub fn is_concurrent_modification(&self) -> bool {
        self.effective_code() == error_codes::CONCURRENT_MODIFICATION
    }

    pub fn is_throttled(&self) -> bool {
        self.effective_code() == error_codes::THROTTLED
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self.effective_code(),
            error_codes::NOT_FOUND
                | error_codes::PARTY_NOT_FOUND
                | error_codes::USER_HAS_NO_PARTY
        )
    }

    pub fn is_user_has_party(&self) -> bool {
        self.effective_code() == error_codes::USER_HAS_PARTY
            || self.numeric_error_code == Some(51012)
    }

    /// Parses `(client_revision, server_revision)` from `messageVars` when `errorCode` is `stale_revision`.
    pub fn parse_stale_revisions(&self) -> Option<(u64, u64)> {
        if !self.is_stale_revision() || self.message_vars.len() < 2 {
            return None;
        }
        let client_rev = self.message_vars[0].parse::<u64>().ok()?;
        let server_rev = self.message_vars[1].parse::<u64>().ok()?;
        Some((client_rev, server_rev))
    }
}

impl fmt::Display for EpicgamesAPIErrorData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = self.effective_code();
        let msg = self.effective_message();
        match (code.is_empty(), msg.is_empty()) {
            (false, false) if code != msg => write!(f, "{code}: {msg}"),
            (false, _) => f.write_str(code),
            (true, false) => f.write_str(msg),
            (true, true) => f.write_str("unknown Epic Games API error"),
        }
    }
}

impl std::error::Error for EpicgamesAPIErrorData {}

#[derive(Debug, Error)]
pub enum RustfnError {
    #[error("Epic API error (HTTP {status}): {body}")]
    EpicApi {
        status: u16,
        body: Box<EpicgamesAPIErrorData>,
    },

    #[error("unexpected HTTP status {status}: {body}")]
    HttpStatus { status: u16, body: String },

    #[error(transparent)]
    IdFormat(#[from] IdFormatError),

    #[error("HTTP transport failure: {0}")]
    Transport(String),

    #[error("JSON serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("authentication failure: {0}")]
    Auth(String),

    #[error("party state failure: {0}")]
    PartyState(String),

    #[error("wire protocol failure: {0}")]
    Protocol(String),

    #[error("operation timed out: {0}")]
    Timeout(String),

    #[error("client session cancelled")]
    Cancelled,
}

impl RustfnError {
    pub fn as_epic_api(&self) -> Option<(u16, &EpicgamesAPIErrorData)> {
        match self {
            Self::EpicApi { status, body } => Some((*status, body)),
            _ => None,
        }
    }

    pub fn is_invalid_token(&self) -> bool {
        matches!(self, Self::EpicApi { status: 401, .. })
            || self
                .as_epic_api()
                .is_some_and(|(_, err)| err.is_invalid_token())
    }

    pub fn is_invalid_refresh_token(&self) -> bool {
        self.as_epic_api()
            .is_some_and(|(_, err)| err.is_invalid_refresh_token())
    }

    pub fn is_stale_revision(&self) -> bool {
        self.as_epic_api()
            .is_some_and(|(_, err)| err.is_stale_revision())
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::EpicApi { status: 404, .. } | Self::HttpStatus { status: 404, .. }
        ) || self.as_epic_api().is_some_and(|(_, err)| err.is_not_found())
    }

    pub fn is_user_has_party(&self) -> bool {
        self.as_epic_api()
            .is_some_and(|(_, err)| err.is_user_has_party())
    }
}
