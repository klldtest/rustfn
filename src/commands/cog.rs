use crate::commands::bot::Bot;
use crate::commands::command::Command;
use crate::commands::error::CommandError;
use std::sync::Arc;

/// Modular command container.
pub trait BotCog: Send + Sync + 'static {
    fn name(&self) -> &str;

    fn description(&self) -> &str {
        ""
    }

    /// Registers this cog's commands onto `registrar`. Commands added here are tagged with `cog_name`.
    fn register(self: Arc<Self>, commands: &mut CogRegistrar<'_>) -> Result<(), CommandError>;
}

/// Scoped registrar passed to [`BotCog::register`] that tags every added command with the cog's name.
pub struct CogRegistrar<'a> {
    cog_name: String,
    bot: &'a mut Bot,
}

impl<'a> CogRegistrar<'a> {
    pub(crate) fn new(cog_name: String, bot: &'a mut Bot) -> Self {
        Self { cog_name, bot }
    }

    pub fn add_command(&mut self, mut command: Command) -> Result<(), CommandError> {
        command.cog_name = Some(self.cog_name.clone());
        self.bot.try_add_command(command)
    }
}
