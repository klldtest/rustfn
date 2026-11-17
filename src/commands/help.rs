use crate::commands::context::{BoxBotFuture, Context, HelpResponder};
use crate::commands::command::{Command, CommandMap};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Configuration for the built-in `!help` formatter.
#[derive(Debug, Clone)]
pub struct HelpOptions {
    pub command_name: String,
    pub dm_help: bool,
    pub no_category: String,
    pub show_hidden: bool,
}

impl Default for HelpOptions {
    fn default() -> Self {
        Self {
            command_name: "help".to_owned(),
            dm_help: false,
            no_category: "Commands".to_owned(),
            show_hidden: false,
        }
    }
}

/// Default text-formatted help command responder.
pub struct DefaultHelpCommand {
    options: HelpOptions,
    description: String,
    commands: Arc<RwLock<CommandMap>>,
}

impl DefaultHelpCommand {
    pub fn new(
        options: HelpOptions,
        description: String,
        commands: Arc<RwLock<CommandMap>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            description,
            commands,
        })
    }

    pub fn build_command(responder: Arc<Self>) -> Command {
        let name = responder.options.command_name.clone();
        Command::new(name, move |ctx| {
            let responder = Arc::clone(&responder);
            async move {
                let target = if ctx.raw_args.is_empty() {
                    None
                } else {
                    Some(ctx.raw_args.clone())
                };
                responder.send_help(ctx, target).await
            }
        })
        .usage("[command]")
        .brief("Shows help for commands or a specific command.")
    }

    fn format_bot_help(&self, prefix: &str) -> String {
        let guard = self
            .commands
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for cmd in guard.commands() {
            if !cmd.enabled || (cmd.hidden && !self.options.show_hidden) {
                continue;
            }
            let category = cmd
                .cog_name
                .clone()
                .unwrap_or_else(|| self.options.no_category.clone());
            let doc = cmd.short_doc();
            let line = if doc.is_empty() {
                format!("{prefix}{}", cmd.name)
            } else {
                format!("{prefix}{} - {doc}", cmd.name)
            };
            grouped.entry(category).or_default().push(line);
        }

        let mut lines = Vec::new();
        if !self.description.is_empty() {
            lines.push(self.description.clone());
        }
        for (category, entries) in grouped {
            lines.push(format!("{category}:"));
            for entry in entries {
                lines.push(format!("  {entry}"));
            }
        }
        lines.push(format!(
            "Type {prefix}{} <command> for details on a command.",
            self.options.command_name
        ));
        lines.join("\n")
    }

    fn format_command_help(&self, prefix: &str, cmd: &Command) -> String {
        let mut lines = vec![format!("Usage: {prefix}{}", cmd.signature())];
        if !cmd.aliases.is_empty() {
            lines.push(format!("Aliases: {}", cmd.aliases.join(", ")));
        }
        if !cmd.description.is_empty() {
            lines.push(cmd.description.clone());
        } else if let Some(help) = &cmd.help {
            lines.push(help.clone());
        } else if let Some(brief) = &cmd.brief {
            lines.push(brief.clone());
        }

        let subs: Vec<_> = cmd
            .subcommands
            .commands()
            .iter()
            .filter(|sub| sub.enabled && (!sub.hidden || self.options.show_hidden))
            .collect();
        if !subs.is_empty() {
            lines.push("Subcommands:".to_owned());
            for sub in subs {
                let doc = sub.short_doc();
                if doc.is_empty() {
                    lines.push(format!("  {}", sub.name));
                } else {
                    lines.push(format!("  {} - {doc}", sub.name));
                }
            }
        }

        lines.join("\n")
    }
}

impl HelpResponder for DefaultHelpCommand {
    fn send_help(&self, ctx: Context, entity: Option<String>) -> BoxBotFuture {
        let prefix = ctx.prefix.clone().unwrap_or_else(|| "!".to_owned());
        let output = match entity {
            None => self.format_bot_help(&prefix),
            Some(query) => {
                let guard = self
            .commands
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
                match guard.get(&query) {
                    Some(cmd) => self.format_command_help(&prefix, &cmd),
                    None => format!("No command called \"{query}\" found."),
                }
            }
        };
        let dm_help = self.options.dm_help;
        Box::pin(async move {
            if dm_help {
                ctx.send_dm(output).await
            } else {
                ctx.send(output).await
            }
        })
    }
}
