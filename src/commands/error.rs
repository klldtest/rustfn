use crate::commands::cooldown::BucketType;
use crate::protocol::RustfnError;
use std::fmt;

/// Command parsing, check, cooldown, concurrency, and invocation errors.
#[derive(Debug)]
pub enum CommandError {
    CommandNotFound(String),
    MissingRequiredArgument(String),
    BadArgument(String),
    CheckFailure(String),
    PrivateMessageOnly,
    PartyMessageOnly,
    NotOwner,
    DisabledCommand(String),
    CommandOnCooldown {
        retry_after: f64,
        bucket: BucketType,
    },
    MaxConcurrencyReached {
        number: usize,
        per: BucketType,
    },
    UnexpectedQuote(char),
    InvalidEndOfQuotedString(Option<char>),
    ExpectedClosingQuote(char),
    DuplicateCommand(String),
    Client(RustfnError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandNotFound(name) => write!(f, "Command \"{name}\" is not found"),
            Self::MissingRequiredArgument(arg) => {
                write!(f, "{arg} is a required argument that is missing.")
            }
            Self::BadArgument(msg) => write!(f, "Bad argument: {msg}"),
            Self::CheckFailure(msg) => write!(f, "Check failed: {msg}"),
            Self::PrivateMessageOnly => {
                write!(f, "This command can only be used in private messages.")
            }
            Self::PartyMessageOnly => write!(f, "This command can only be used in a party."),
            Self::NotOwner => write!(f, "You do not own this bot."),
            Self::DisabledCommand(name) => write!(f, "{name} command is disabled."),
            Self::CommandOnCooldown { retry_after, .. } => {
                write!(f, "You are on cooldown. Try again in {retry_after:.2}s")
            }
            Self::MaxConcurrencyReached { number, per } => {
                write!(f, "Too many people using this command. Limit is {number} per {per:?}")
            }
            Self::UnexpectedQuote(ch) => {
                write!(f, "Unexpected quote mark, {ch:?}, in non-quoted string")
            }
            Self::InvalidEndOfQuotedString(ch) => {
                write!(f, "Expected space after closing quotation, got {ch:?}")
            }
            Self::ExpectedClosingQuote(ch) => {
                write!(f, "Expected closing {ch} quotation")
            }
            Self::DuplicateCommand(name) => {
                write!(f, "Command \"{name}\" or one of its aliases is already registered.")
            }
            Self::Client(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for CommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(err) => Some(err),
            _ => None,
        }
    }
}

impl From<RustfnError> for CommandError {
    fn from(err: RustfnError) -> Self {
        Self::Client(err)
    }
}
