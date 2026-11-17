use crate::context::{BoxBotFuture, Context, IncomingMessage, IntoBotResult};
use crate::cooldown::{BucketType, CooldownMapping, MaxConcurrency};
use crate::error::CommandError;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type BoxCheckFuture = Pin<Box<dyn Future<Output = Result<(), CommandError>> + Send>>;
pub type CheckFn = Arc<dyn Fn(&Context) -> BoxCheckFuture + Send + Sync>;
pub type CommandCallback = Arc<dyn Fn(Context) -> BoxBotFuture + Send + Sync>;
pub type CommandErrorFn =
    Arc<dyn Fn(Context, Arc<CommandError>) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;
pub type CommandHookFn = Arc<dyn Fn(Context) -> BoxBotFuture + Send + Sync>;

/// Prefix specification matching string, multi-prefix list, or dynamic resolver.
#[derive(Clone)]
pub enum PrefixConfig {
    Single(String),
    Multiple(Vec<String>),
    Dynamic(Arc<dyn Fn(&IncomingMessage) -> Vec<String> + Send + Sync>),
}

impl Default for PrefixConfig {
    fn default() -> Self {
        Self::Single("!".to_owned())
    }
}

impl From<&str> for PrefixConfig {
    fn from(value: &str) -> Self {
        Self::Single(value.to_owned())
    }
}

impl From<String> for PrefixConfig {
    fn from(value: String) -> Self {
        Self::Single(value)
    }
}

impl From<Vec<String>> for PrefixConfig {
    fn from(values: Vec<String>) -> Self {
        Self::Multiple(values)
    }
}

impl<const N: usize> From<[&str; N]> for PrefixConfig {
    fn from(values: [&str; N]) -> Self {
        Self::Multiple(values.into_iter().map(ToOwned::to_owned).collect())
    }
}

impl PrefixConfig {
    pub fn resolve(&self, msg: &IncomingMessage) -> Vec<String> {
        match self {
            Self::Single(s) => vec![s.clone()],
            Self::Multiple(v) => v.clone(),
            Self::Dynamic(func) => func(msg),
        }
    }
}

/// Built-in check requiring the command to originate in a private DM.
pub fn dm_only() -> CheckFn {
    Arc::new(|ctx: &Context| {
        let is_dm = ctx.is_dm();
        Box::pin(async move {
            if is_dm {
                Ok(())
            } else {
                Err(CommandError::PrivateMessageOnly)
            }
        })
    })
}

/// Built-in check requiring the command to originate in party chat.
pub fn party_only() -> CheckFn {
    Arc::new(|ctx: &Context| {
        let is_party = ctx.is_party();
        Box::pin(async move {
            if is_party {
                Ok(())
            } else {
                Err(CommandError::PartyMessageOnly)
            }
        })
    })
}

/// Built-in check requiring the author to match the bot's `owner_id` / `owner_ids` (or bot itself).
pub fn is_owner() -> CheckFn {
    Arc::new(|ctx: &Context| {
        let bot = ctx.bot.clone();
        let author_id = ctx.author_id().clone();
        Box::pin(async move {
            if bot.is_owner(&author_id).await {
                Ok(())
            } else {
                Err(CommandError::NotOwner)
            }
        })
    })
}

/// Declarative command and optional subcommand group.
#[derive(Clone)]
pub struct Command {
    pub name: String,
    pub aliases: Vec<String>,
    pub brief: Option<String>,
    pub help: Option<String>,
    pub description: String,
    pub usage: Option<String>,
    pub hidden: bool,
    pub enabled: bool,
    pub case_insensitive: bool,
    pub invoke_without_command: bool,
    pub cog_name: Option<String>,
    pub parent_name: Option<String>,
    pub checks: Vec<CheckFn>,
    pub cooldown: Option<Arc<CooldownMapping>>,
    pub max_concurrency: Option<Arc<MaxConcurrency>>,
    pub on_error: Option<CommandErrorFn>,
    pub before_invoke: Option<CommandHookFn>,
    pub after_invoke: Option<CommandHookFn>,
    pub subcommands: CommandMap,
    pub callback: CommandCallback,
}

impl Command {
    pub fn new<F, Fut, R>(name: impl Into<String>, callback: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        let boxed: CommandCallback = Arc::new(move |ctx| {
            let fut = callback(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        });
        Self {
            name: name.into(),
            aliases: Vec::new(),
            brief: None,
            help: None,
            description: String::new(),
            usage: None,
            hidden: false,
            enabled: true,
            case_insensitive: false,
            invoke_without_command: false,
            cog_name: None,
            parent_name: None,
            checks: Vec::new(),
            cooldown: None,
            max_concurrency: None,
            on_error: None,
            before_invoke: None,
            after_invoke: None,
            subcommands: CommandMap::new(false),
            callback: boxed,
        }
    }

    /// Creates a group command that defaults `invoke_without_command = false` unless overridden.
    pub fn group<F, Fut, R>(name: impl Into<String>, callback: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        Self::new(name, callback)
    }

    pub fn aliases<I, S>(mut self, aliases: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.aliases = aliases.into_iter().map(Into::into).collect();
        self
    }

    pub fn brief(mut self, brief: impl Into<String>) -> Self {
        self.brief = Some(brief.into());
        self
    }

    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn usage(mut self, usage: impl Into<String>) -> Self {
        self.usage = Some(usage.into());
        self
    }

    pub fn hidden(mut self, hidden: bool) -> Self {
        self.hidden = hidden;
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn case_insensitive(mut self, insensitive: bool) -> Self {
        self.case_insensitive = insensitive;
        self.subcommands.set_case_insensitive(insensitive);
        self
    }

    pub fn invoke_without_command(mut self, value: bool) -> Self {
        self.invoke_without_command = value;
        self
    }

    pub fn cooldown(mut self, rate: usize, per: Duration, bucket: BucketType) -> Self {
        self.cooldown = Some(CooldownMapping::new(rate, per, bucket));
        self
    }

    pub fn max_concurrency(mut self, number: usize, per: BucketType, wait: bool) -> Self {
        self.max_concurrency = Some(MaxConcurrency::new(number, per, wait));
        self
    }

    pub fn check<F, Fut>(mut self, check: F) -> Self
    where
        F: Fn(&Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CommandError>> + Send + 'static,
    {
        self.checks.push(Arc::new(move |ctx| Box::pin(check(ctx))));
        self
    }

    pub fn with_check(mut self, check: CheckFn) -> Self {
        self.checks.push(check);
        self
    }

    pub fn dm_only(self) -> Self {
        self.with_check(dm_only())
    }

    pub fn party_only(self) -> Self {
        self.with_check(party_only())
    }

    pub fn is_owner(self) -> Self {
        self.with_check(is_owner())
    }

    /// Registers a local error handler. Returning `true` marks the error handled; `false` bubbles up to `Bot::on_command_error`.
    pub fn on_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, Arc<CommandError>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.on_error = Some(Arc::new(move |ctx, err| Box::pin(handler(ctx, err))));
        self
    }

    pub fn before_invoke<F, Fut, R>(mut self, hook: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.before_invoke = Some(Arc::new(move |ctx| {
            let fut = hook(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn after_invoke<F, Fut, R>(mut self, hook: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.after_invoke = Some(Arc::new(move |ctx| {
            let fut = hook(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    /// Attaches a child subcommand onto this group command.
    pub fn subcommand(mut self, mut child: Command) -> Result<Self, CommandError> {
        child.parent_name = Some(self.qualified_name());
        if self.case_insensitive && !child.case_insensitive {
            child = child.case_insensitive(true);
        }
        self.subcommands.add(child)?;
        Ok(self)
    }

    /// Convenience helper attaching a subcommand by name and closure directly.
    pub fn add_subcommand<F, Fut, R>(
        self,
        name: impl Into<String>,
        callback: F,
    ) -> Result<Self, CommandError>
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.subcommand(Command::new(name, callback))
    }

    pub fn qualified_name(&self) -> String {
        match &self.parent_name {
            Some(parent) => format!("{parent} {}", self.name),
            None => self.name.clone(),
        }
    }

    pub fn short_doc(&self) -> &str {
        if let Some(brief) = &self.brief {
            return brief;
        }
        if let Some(help) = &self.help {
            return help.lines().next().unwrap_or("");
        }
        ""
    }

    pub fn signature(&self) -> String {
        match &self.usage {
            Some(usage) => format!("{} {usage}", self.qualified_name()),
            None if !self.subcommands.is_empty() => format!("{} <subcommand>", self.qualified_name()),
            None => self.qualified_name(),
        }
    }
}

/// Alias highlighting command group semantics.
pub type Group = Command;

/// Command lookup table supporting aliases, nested groups, and case-insensitive normalization.
#[derive(Clone, Default)]
pub struct CommandMap {
    case_insensitive: bool,
    by_key: HashMap<String, Arc<Command>>,
    primary: Vec<Arc<Command>>,
}

impl CommandMap {
    pub fn new(case_insensitive: bool) -> Self {
        Self {
            case_insensitive,
            by_key: HashMap::new(),
            primary: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.primary.is_empty()
    }

    pub fn set_case_insensitive(&mut self, insensitive: bool) {
        if self.case_insensitive == insensitive {
            return;
        }
        self.case_insensitive = insensitive;
        let commands = std::mem::take(&mut self.primary);
        self.by_key.clear();
        for mut cmd in commands.into_iter().map(|c| (*c).clone()) {
            cmd.subcommands.set_case_insensitive(insensitive);
            let _ = self.add(cmd);
        }
    }

    fn normalize(&self, key: &str) -> String {
        if self.case_insensitive {
            key.to_ascii_lowercase()
        } else {
            key.to_owned()
        }
    }

    pub fn add(&mut self, mut command: Command) -> Result<(), CommandError> {
        if self.case_insensitive {
            command.subcommands.set_case_insensitive(true);
        }
        let primary_key = self.normalize(&command.name);
        if self.by_key.contains_key(&primary_key) {
            return Err(CommandError::DuplicateCommand(command.name));
        }

        let mut alias_keys = Vec::with_capacity(command.aliases.len());
        for alias in &command.aliases {
            let norm = self.normalize(alias);
            if norm == primary_key || self.by_key.contains_key(&norm) {
                return Err(CommandError::DuplicateCommand(alias.clone()));
            }
            alias_keys.push(norm);
        }

        let shared = Arc::new(command);
        self.by_key.insert(primary_key, Arc::clone(&shared));
        for alias_key in alias_keys {
            self.by_key.insert(alias_key, Arc::clone(&shared));
        }
        self.primary.push(shared);
        Ok(())
    }

    pub fn remove(&mut self, name: &str) -> Option<Arc<Command>> {
        let key = self.normalize(name);
        let removed = self.by_key.remove(&key)?;
        if removed
            .aliases
            .iter()
            .any(|alias| self.normalize(alias) == key)
        {
            return Some(removed);
        }
        let primary_key = self.normalize(&removed.name);
        self.by_key.remove(&primary_key);
        for alias in &removed.aliases {
            self.by_key.remove(&self.normalize(alias));
        }
        self.primary
            .retain(|existing| existing.name != removed.name);
        Some(removed)
    }

    pub fn get(&self, name: &str) -> Option<Arc<Command>> {
        if !name.contains(' ') {
            return self.by_key.get(&self.normalize(name)).cloned();
        }
        let mut parts = name.split_whitespace();
        let first = parts.next()?;
        let mut current = self.by_key.get(&self.normalize(first))?.clone();
        for part in parts {
            current = current.subcommands.get(part)?;
        }
        Some(current)
    }

    pub fn commands(&self) -> &[Arc<Command>] {
        &self.primary
    }

    pub fn walk_commands(&self) -> Vec<Arc<Command>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for cmd in &self.primary {
            if seen.insert(cmd.qualified_name()) {
                out.push(Arc::clone(cmd));
            }
            for sub in cmd.subcommands.walk_commands() {
                if seen.insert(sub.qualified_name()) {
                    out.push(sub);
                }
            }
        }
        out
    }
}
