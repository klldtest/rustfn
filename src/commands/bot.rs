use crate::client::config::ClientConfig;
use crate::commands::cog::{BotCog, CogRegistrar};
use crate::commands::context::{
    BotHandle, BotUser, BoxBotFuture, Context, FriendRequestContext, HelpResponder,
    IncomingMessage, IntoBotResult, MessageChannel, PartyInviteContext, PartyMemberContext,
    ReadyContext,
};
use crate::commands::command::{
    CheckFn, Command, CommandHookFn, CommandMap, PrefixConfig,
};
use crate::commands::error::CommandError;
use crate::commands::help::{DefaultHelpCommand, HelpOptions};
use crate::commands::view::StringView;
use crate::client::client::Client;
use crate::auth::{
    AuthEvent, AuthMethod, DeviceAuthCredentials, DeviceCodeAuth, JsonFileDeviceAuthStore,
};
use crate::events::ClientEvent;
use crate::friends::FriendRecord;
use crate::protocol::{AccountId, RustfnError};
use secrecy::SecretString;
use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

type ReadyHandler = Arc<dyn Fn(ReadyContext) -> BoxBotFuture + Send + Sync>;
type FriendRequestHandler = Arc<dyn Fn(FriendRequestContext) -> BoxBotFuture + Send + Sync>;
type FriendRecordHandler = Arc<dyn Fn(BotHandle, FriendRecord) -> BoxBotFuture + Send + Sync>;
type PartyInviteHandler = Arc<dyn Fn(PartyInviteContext) -> BoxBotFuture + Send + Sync>;
type PartyMemberHandler = Arc<dyn Fn(PartyMemberContext) -> BoxBotFuture + Send + Sync>;
type MessageHandler = Arc<dyn Fn(Context) -> BoxBotFuture + Send + Sync>;
type CommandEventHandler = Arc<dyn Fn(Context) -> BoxBotFuture + Send + Sync>;
type GlobalCommandErrorHandler =
    Arc<dyn Fn(Context, Arc<CommandError>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
type RawEventHandler = Arc<dyn Fn(BotHandle, Arc<ClientEvent>) -> BoxBotFuture + Send + Sync>;

#[derive(Default)]
struct BotHandlers {
    on_ready: Option<ReadyHandler>,
    on_friend_request: Option<FriendRequestHandler>,
    on_friend_add: Option<FriendRecordHandler>,
    on_friend_remove: Option<FriendRecordHandler>,
    on_party_invite: Option<PartyInviteHandler>,
    on_party_member_join: Option<PartyMemberHandler>,
    on_party_member_leave: Option<PartyMemberHandler>,
    on_message: Option<MessageHandler>,
    on_command: Option<CommandEventHandler>,
    on_command_completion: Option<CommandEventHandler>,
    on_command_error: Option<GlobalCommandErrorHandler>,
    on_event: Option<RawEventHandler>,
    global_checks: Vec<CheckFn>,
    before_invoke: Option<CommandHookFn>,
    after_invoke: Option<CommandHookFn>,
}

/// Declarative command bot runner.
pub struct Bot {
    prefix: PrefixConfig,
    case_insensitive: bool,
    description: String,
    owner_ids: HashSet<AccountId>,
    config: ClientConfig,
    auth: Option<AuthMethod>,
    device_auth_dir: PathBuf,
    help_options: Option<HelpOptions>,
    commands: Arc<RwLock<CommandMap>>,
    handlers: BotHandlers,
}

impl Bot {
    /// Creates a new `Bot` with the given command prefix (string, slice of prefixes, or `PrefixConfig`).
    pub fn new(command_prefix: impl Into<PrefixConfig>) -> Self {
        Self {
            prefix: command_prefix.into(),
            case_insensitive: false,
            description: String::new(),
            owner_ids: HashSet::new(),
            config: ClientConfig {
                create_device_auth: true,
                ..ClientConfig::default()
            },
            auth: None,
            device_auth_dir: PathBuf::from("./device_auth"),
            help_options: Some(HelpOptions::default()),
            commands: Arc::new(RwLock::new(CommandMap::new(false))),
            handlers: BotHandlers::default(),
        }
    }

    fn read_commands(&self) -> std::sync::RwLockReadGuard<'_, CommandMap> {
        self.commands.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_commands(&self) -> std::sync::RwLockWriteGuard<'_, CommandMap> {
        self.commands.write().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn case_insensitive(mut self, insensitive: bool) -> Self {
        self.case_insensitive = insensitive;
        self.write_commands().set_case_insensitive(insensitive);
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn owner_id(mut self, owner_id: AccountId) -> Self {
        self.owner_ids.insert(owner_id);
        self
    }

    pub fn owner_ids<I>(mut self, owner_ids: I) -> Self
    where
        I: IntoIterator<Item = AccountId>,
    {
        self.owner_ids.extend(owner_ids);
        self
    }

    pub fn auth(mut self, auth: impl Into<AuthMethod>) -> Self {
        self.auth = Some(auth.into());
        self
    }

    pub fn device_auth_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.device_auth_dir = dir.into();
        self
    }

    pub fn config(mut self, config: ClientConfig) -> Self {
        self.config = config;
        self
    }

    pub fn help_options(mut self, options: HelpOptions) -> Self {
        self.help_options = Some(options);
        self
    }

    pub fn no_help_command(mut self) -> Self {
        self.help_options = None;
        self
    }

    /// Registers a global check executed before every command invocation.
    pub fn add_check<F, Fut>(mut self, check: F) -> Self
    where
        F: Fn(&Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CommandError>> + Send + 'static,
    {
        self.handlers
            .global_checks
            .push(Arc::new(move |ctx| Box::pin(check(ctx))));
        self
    }

    pub fn with_check(mut self, check: CheckFn) -> Self {
        self.handlers.global_checks.push(check);
        self
    }

    pub fn before_invoke<F, Fut, R>(mut self, hook: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.before_invoke = Some(Arc::new(move |ctx| {
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
        self.handlers.after_invoke = Some(Arc::new(move |ctx| {
            let fut = hook(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    /// Registers a command builder fluently (panics on duplicate name/alias; use `try_add_command` for fallible addition).
    pub fn add_command(mut self, command: Command) -> Self {
        self.try_add_command(command)
            .expect("duplicate command name or alias");
        self
    }

    pub fn try_add_command(&mut self, command: Command) -> Result<(), CommandError> {
        self.write_commands().add(command)
    }

    pub fn remove_command(&mut self, name: &str) -> Option<Arc<Command>> {
        self.write_commands().remove(name)
    }

    pub fn get_command(&self, name: &str) -> Option<Arc<Command>> {
        self.read_commands().get(name)
    }

    pub fn commands(&self) -> Vec<Arc<Command>> {
        self.read_commands().commands().to_vec()
    }

    pub fn walk_commands(&self) -> Vec<Arc<Command>> {
        self.read_commands().walk_commands()
    }

    /// Registers a [`BotCog`] whose commands are tagged with `cog.name()`.
    pub fn add_cog(mut self, cog: impl BotCog) -> Result<Self, CommandError> {
        let name = cog.name().to_owned();
        let arc_cog = Arc::new(cog);
        let mut registrar = CogRegistrar::new(name, &mut self);
        arc_cog.register(&mut registrar)?;
        Ok(self)
    }

    /// Shorthand for registering a simple prefix command matching party chat and DMs.
    pub fn command<F, Fut, R>(self, name: impl Into<String>, callback: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.add_command(Command::new(name, callback))
    }

    pub fn on_ready<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(ReadyContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_ready = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_friend_request<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(FriendRequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_friend_request = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_friend_add<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(BotHandle, FriendRecord) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_friend_add = Some(Arc::new(move |bot, friend| {
            let fut = handler(bot, friend);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_friend_remove<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(BotHandle, FriendRecord) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_friend_remove = Some(Arc::new(move |bot, friend| {
            let fut = handler(bot, friend);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_party_invite<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(PartyInviteContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_party_invite = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_party_member_join<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(PartyMemberContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_party_member_join = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_party_member_leave<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(PartyMemberContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_party_member_leave = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_message<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_message = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_command<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_command = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_command_completion<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_command_completion = Some(Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    pub fn on_command_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, Arc<CommandError>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.handlers.on_command_error = Some(Arc::new(move |ctx, err| Box::pin(handler(ctx, err))));
        self
    }

    pub fn on_event<F, Fut, R>(mut self, handler: F) -> Self
    where
        F: Fn(BotHandle, Arc<ClientEvent>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoBotResult + 'static,
    {
        self.handlers.on_event = Some(Arc::new(move |bot, ev| {
            let fut = handler(bot, ev);
            Box::pin(async move { fut.await.into_bot_result() })
        }));
        self
    }

    /// Builds [`Client`], authenticates, processes events until `Ctrl+C`, and logs out cleanly.
    pub async fn run(self) -> Result<(), RustfnError> {
        self.run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    /// Runs the bot until `shutdown` resolves, then logs out cleanly.
    pub async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), RustfnError> {
        let auth = match self.auth.take() {
            Some(explicit) => explicit,
            None => default_bot_auth(&self.device_auth_dir)?,
        };

        let client = Client::builder()
            .config(self.config.clone())
            .device_store(Arc::new(JsonFileDeviceAuthStore::new(&self.device_auth_dir)))
            .build()?;

        let handle = BotHandle::new(client.clone(), self.owner_ids.clone());
        let help_responder = self.install_default_help();
        handle.set_help_responder(help_responder).await;

        let runtime = Arc::new(BotRuntime {
            handle,
            prefix: self.prefix,
            commands: self.commands,
            handlers: self.handlers,
        });

        let events = client.subscribe_events();
        let loop_cancel = CancellationToken::new();
        let loop_task = spawn_bot_event_loop(events, runtime, loop_cancel.clone());

        let login_res = match auth {
            AuthMethod::DeviceCode => {
                Box::pin(client.authenticate_device_code(DeviceCodeAuth::new())).await
            }
            AuthMethod::DeviceCodeAuth(cfg) => {
                Box::pin(client.authenticate_device_code(cfg)).await
            }
            other => Box::pin(client.login(other)).await,
        };

        if let Err(err) = login_res {
            loop_cancel.cancel();
            let _ = loop_task.await;
            return Err(err);
        }

        info!("bot running");
        shutdown.await;
        info!("bot shutting down");
        let logout_res = client.logout().await;
        loop_cancel.cancel();
        let _ = loop_task.await;
        logout_res
    }

    fn install_default_help(&mut self) -> Option<Arc<dyn HelpResponder>> {
        let options = self.help_options.take()?;
        let help_name = options.command_name.clone();
        let responder = DefaultHelpCommand::new(
            options,
            self.description.clone(),
            Arc::clone(&self.commands),
        );
        let mut guard = self.write_commands();
        if guard.get(&help_name).is_none() {
            let _ = guard.add(DefaultHelpCommand::build_command(Arc::clone(&responder)));
        }
        Some(responder)
    }
}

struct BotRuntime {
    handle: BotHandle,
    prefix: PrefixConfig,
    commands: Arc<RwLock<CommandMap>>,
    handlers: BotHandlers,
}

impl BotRuntime {
    fn get_prefix(&self, message: &IncomingMessage) -> Vec<String> {
        self.prefix.resolve(message)
    }

    fn get_context(&self, message: IncomingMessage) -> Context {
        let mut view = StringView::new(&message.content);
        let mut ctx = Context::new(self.handle.clone(), message.clone(), view.clone());

        let mut prefixes = self.get_prefix(&message);
        prefixes.sort_by_key(|p| std::cmp::Reverse(p.len()));
        let matched_prefix = prefixes
            .into_iter()
            .find(|prefix| !prefix.is_empty() && view.skip_string(prefix)
                || (prefix.is_empty() && !message.content.is_empty()));

        let Some(prefix) = matched_prefix else {
            return ctx;
        };

        let invoker = view.get_word().to_owned();
        if invoker.is_empty() {
            return ctx;
        }

        let cmd = self
            .commands
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&invoker);

        ctx.prefix = Some(prefix);
        ctx.invoked_with = Some(invoker);
        ctx.view = view;
        if let Some(found) = cmd {
            ctx.command_name = Some(found.name.clone());
            ctx.cog_name = found.cog_name.clone();
        }
        ctx
    }

    async fn process_message(&self, message: IncomingMessage) {
        if self.is_self(&message.author_id).await {
            return;
        }

        let ctx = self.get_context(message);
        if let Some(on_message) = &self.handlers.on_message {
            spawn_bot_future(on_message(ctx.clone()), "on_message");
        }

        self.invoke_context(ctx).await;
    }

    async fn invoke_context(&self, mut ctx: Context) {
        let Some(invoked_with) = ctx.invoked_with.clone() else {
            return;
        };

        let root = {
            let guard = self
                .commands
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ctx.command_name
                .as_deref()
                .and_then(|name| guard.get(name))
                .or_else(|| guard.get(&invoked_with))
        };

        let Some(root_cmd) = root else {
            let err = Arc::new(CommandError::CommandNotFound(invoked_with));
            self.handle_command_error(ctx, None, err).await;
            return;
        };

        if let Some(on_command) = &self.handlers.on_command {
            spawn_bot_future(on_command(ctx.clone()), "on_command");
        }

        match self.execute_command_chain(&mut ctx, root_cmd.clone()).await {
            Ok(()) => {
                if let Some(on_complete) = &self.handlers.on_command_completion {
                    spawn_bot_future(on_complete(ctx), "on_command_completion");
                }
            }
            Err((failing_cmd, err)) => {
                ctx.command_failed = true;
                self.handle_command_error(ctx, Some(failing_cmd), Arc::new(err))
                    .await;
            }
        }
    }

    async fn execute_command_chain(
        &self,
        ctx: &mut Context,
        root: Arc<Command>,
    ) -> Result<(), (Arc<Command>, CommandError)> {
        let mut current = root;
        loop {
            if current.subcommands.is_empty() {
                return self
                    .invoke_single_command(ctx, &current)
                    .await
                    .map_err(|err| (Arc::clone(&current), err));
            }

            let saved_index = ctx.view.index;
            let saved_prev = ctx.view.previous;
            ctx.view.skip_ws();
            let trigger = ctx.view.get_word().to_owned();
            let sub = if trigger.is_empty() {
                None
            } else {
                ctx.subcommand_passed = Some(trigger.clone());
                current.subcommands.get(&trigger)
            };

            let early_invoke = !current.invoke_without_command;
            if early_invoke {
                let mut parent_ctx = ctx.clone();
                if let Some(sub_cmd) = &sub {
                    parent_ctx.invoked_subcommand = Some(sub_cmd.qualified_name());
                } else {
                    parent_ctx.view.index = saved_index;
                    parent_ctx.view.previous = saved_prev;
                }
                self.invoke_single_command(&mut parent_ctx, &current)
                    .await
                    .map_err(|err| (Arc::clone(&current), err))?;
            } else if sub.is_some() {
                self.verify_parent_checks(ctx, &current)
                    .await
                    .map_err(|err| (Arc::clone(&current), err))?;
            }

            match sub {
                Some(sub_cmd) => {
                    ctx.invoked_with = Some(trigger);
                    ctx.invoked_subcommand = Some(sub_cmd.qualified_name());
                    ctx.command_name = Some(sub_cmd.qualified_name());
                    current = sub_cmd;
                }
                None => {
                    ctx.view.index = saved_index;
                    ctx.view.previous = saved_prev;
                    if !early_invoke {
                        return self
                            .invoke_single_command(ctx, &current)
                            .await
                            .map_err(|err| (Arc::clone(&current), err));
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn verify_parent_checks(
        &self,
        ctx: &Context,
        cmd: &Arc<Command>,
    ) -> Result<(), CommandError> {
        if !cmd.enabled {
            return Err(CommandError::DisabledCommand(cmd.qualified_name()));
        }
        for check in &cmd.checks {
            check(ctx).await?;
        }
        Ok(())
    }

    async fn invoke_single_command(
        &self,
        ctx: &mut Context,
        cmd: &Arc<Command>,
    ) -> Result<(), CommandError> {
        if !cmd.enabled {
            return Err(CommandError::DisabledCommand(cmd.qualified_name()));
        }

        let mut rest_view = ctx.view.clone();
        rest_view.skip_ws();
        ctx.raw_args = rest_view.read_rest().trim().to_owned();
        let mut args_view = StringView::new(&ctx.raw_args);
        ctx.args = args_view.parse_all_quoted_args()?;

        for check in &self.handlers.global_checks {
            check(ctx).await?;
        }
        for check in &cmd.checks {
            check(ctx).await?;
        }

        let _concurrency_permit = match &cmd.max_concurrency {
            Some(limiter) => Some(limiter.acquire(ctx.author_id()).await?),
            None => None,
        };

        if let Some(cooldown) = &cmd.cooldown
            && let Some(retry_after) = cooldown.update_rate_limit(ctx.author_id()).await
        {
            return Err(CommandError::CommandOnCooldown {
                retry_after: retry_after.as_secs_f64(),
                bucket: cooldown.bucket(),
            });
        }

        if let Some(global_before) = &self.handlers.before_invoke {
            global_before(ctx.clone()).await?;
        }
        if let Some(local_before) = &cmd.before_invoke {
            local_before(ctx.clone()).await?;
        }

        let invoke_res = (cmd.callback)(ctx.clone()).await;

        if let Some(local_after) = &cmd.after_invoke
            && let Err(after_err) = local_after(ctx.clone()).await
        {
            warn!(command = %cmd.qualified_name(), error = %after_err, "command after_invoke hook failed");
        }
        if let Some(global_after) = &self.handlers.after_invoke
            && let Err(after_err) = global_after(ctx.clone()).await
        {
            warn!(command = %cmd.qualified_name(), error = %after_err, "global after_invoke hook failed");
        }

        invoke_res
    }

    async fn handle_command_error(
        &self,
        ctx: Context,
        cmd: Option<Arc<Command>>,
        err: Arc<CommandError>,
    ) {
        if let Some(command) = cmd
            && let Some(local_error) = &command.on_error
            && local_error(ctx.clone(), Arc::clone(&err)).await
        {
            return;
        }

        if let Some(global_error) = &self.handlers.on_command_error {
            global_error(ctx, err).await;
        } else if !matches!(&*err, CommandError::CommandNotFound(_)) {
            warn!(command = ?ctx.command_name, error = %err, "unhandled command error");
        }
    }

    async fn is_self(&self, author_id: &AccountId) -> bool {
        if let Some(user) = self.handle.user().await {
            return &user.id == author_id;
        }
        false
    }
}

fn spawn_bot_event_loop(
    mut events: broadcast::Receiver<Arc<ClientEvent>>,
    runtime: Arc<BotRuntime>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                recv = events.recv() => match recv {
                    Ok(event) => {
                        let disconnected = matches!(&*event, ClientEvent::Disconnected);
                        handle_event(&runtime, event).await;
                        if disconnected {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "bot event loop lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    })
}

async fn handle_event(runtime: &Arc<BotRuntime>, event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::DeviceCodePrompt {
            verification_uri_complete,
            user_code,
            ..
        } => {
            info!(
                user_code = %user_code,
                verification_uri = %verification_uri_complete,
                "open browser to complete device-code sign-in"
            );
        }
        ClientEvent::Auth(AuthEvent::DeviceAuthCreated(creds)) => {
            info!(account_id = %creds.account_id, "saved DeviceAuth credentials");
        }
        ClientEvent::Ready {
            account_id,
            display_name,
        } => {
            let user = BotUser {
                id: account_id.clone(),
                display_name: display_name.clone(),
            };
            runtime.handle.set_user(user.clone()).await;
            if let Some(cb) = &runtime.handlers.on_ready {
                let ctx = ReadyContext {
                    user,
                    bot: runtime.handle.clone(),
                };
                spawn_bot_future(cb(ctx), "on_ready");
            }
        }
        ClientEvent::FriendRequestReceived(request) => {
            if let Some(cb) = &runtime.handlers.on_friend_request {
                let ctx = FriendRequestContext {
                    request: request.clone(),
                    bot: runtime.handle.clone(),
                };
                spawn_bot_future(cb(ctx), "on_friend_request");
            }
        }
        ClientEvent::FriendAdded(friend) => {
            if let Some(cb) = &runtime.handlers.on_friend_add {
                spawn_bot_future(cb(runtime.handle.clone(), friend.clone()), "on_friend_add");
            }
        }
        ClientEvent::FriendRemoved(friend) => {
            if let Some(cb) = &runtime.handlers.on_friend_remove {
                spawn_bot_future(
                    cb(runtime.handle.clone(), friend.clone()),
                    "on_friend_remove",
                );
            }
        }
        ClientEvent::PartyInviteReceived(invite) => {
            if let Some(cb) = &runtime.handlers.on_party_invite {
                let ctx = PartyInviteContext {
                    invite: invite.clone(),
                    bot: runtime.handle.clone(),
                };
                spawn_bot_future(cb(ctx), "on_party_invite");
            }
        }
        ClientEvent::PartyMemberJoined { party_id, member } => {
            if let Some(cb) = &runtime.handlers.on_party_member_join {
                let ctx = PartyMemberContext {
                    party_id: party_id.clone(),
                    member: member.clone(),
                    bot: runtime.handle.clone(),
                };
                spawn_bot_future(cb(ctx), "on_party_member_join");
            }
        }
        ClientEvent::PartyMemberLeft { party_id, member } => {
            if let Some(cb) = &runtime.handlers.on_party_member_leave {
                let ctx = PartyMemberContext {
                    party_id: party_id.clone(),
                    member: member.clone(),
                    bot: runtime.handle.clone(),
                };
                spawn_bot_future(cb(ctx), "on_party_member_leave");
            }
        }
        ClientEvent::FriendMessage(msg) => {
            let normalized = IncomingMessage {
                message_id: msg.message_id.clone(),
                author_id: msg.author_id.clone(),
                author_display_name: msg.author_display_name.clone(),
                content: msg.content.clone(),
                channel: MessageChannel::DirectMessage,
            };
            let runtime_clone = Arc::clone(runtime);
            tokio::spawn(async move {
                runtime_clone.process_message(normalized).await;
            });
        }
        ClientEvent::PartyMessage(msg) => {
            let author_display_name = if let Some(party) = runtime.handle.client().party().await {
                party.snapshot().await.and_then(|snap| {
                    snap.members
                        .get(&msg.author_id)
                        .and_then(|m| m.display_name.clone())
                })
            } else {
                None
            };
            let normalized = IncomingMessage {
                message_id: msg.id.clone().unwrap_or_else(|| msg.timestamp_ms.to_string()),
                author_id: msg.author_id.clone(),
                author_display_name,
                content: msg.content.clone(),
                channel: MessageChannel::Party(msg.party_id.clone()),
            };
            let runtime_clone = Arc::clone(runtime);
            tokio::spawn(async move {
                runtime_clone.process_message(normalized).await;
            });
        }
        _ => {}
    }

    if let Some(cb) = &runtime.handlers.on_event {
        spawn_bot_future(cb(runtime.handle.clone(), event), "on_event");
    }
}

fn spawn_bot_future(fut: BoxBotFuture, label: &'static str) {
    tokio::spawn(async move {
        if let Err(err) = fut.await {
            warn!(handler = label, error = %err, "bot callback returned error");
        }
    });
}

fn default_bot_auth(device_dir: &Path) -> Result<AuthMethod, RustfnError> {
    if let (Ok(account_id), Ok(device_id), Ok(secret)) = (
        std::env::var("FN_ACCOUNT_ID"),
        std::env::var("FN_DEVICE_ID"),
        std::env::var("FN_DEVICE_SECRET"),
    ) {
        let parsed = AccountId::parse(account_id)?;
        return Ok(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            parsed,
            device_id,
            SecretString::from(secret),
        )));
    }

    if let Some(creds) = load_first_device_auth(device_dir) {
        info!(account_id = %creds.account_id, "loaded saved DeviceAuth");
        return Ok(AuthMethod::DeviceAuth(creds));
    }

    if let Ok(code) = std::env::var("FN_AUTH_CODE") {
        return Ok(AuthMethod::AuthorizationCode(code));
    }

    Ok(AuthMethod::from(DeviceCodeAuth::new()))
}

fn load_first_device_auth(dir: &Path) -> Option<DeviceAuthCredentials> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut paths: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    paths.sort();

    for path in paths {
        if let Ok(bytes) = std::fs::read(&path)
            && let Ok(creds) = serde_json::from_slice::<DeviceAuthCredentials>(&bytes)
            && path.file_stem().and_then(|stem| stem.to_str())
                == Some(creds.account_id.as_str())
        {
            return Some(creds);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::cooldown::BucketType;
    use crate::chat::{ChatTransportOrigin, ReceivedFriendMessage};
    use crate::party::PartyChatMessage;
    use crate::protocol::{LobbyId, PartyId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[test]
    fn load_first_device_auth_checks_account_id() {
        let dir = std::env::temp_dir().join(format!(
            "rustfn_commands_device_auth_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let account_id = AccountId::from_raw("11111111111111111111111111111111");
        let creds = DeviceAuthCredentials::new(
            account_id.clone(),
            "device".to_owned(),
            SecretString::from("secret".to_owned()),
        );
        let wrong_path = dir.join("00000000000000000000000000000000.json");
        std::fs::write(&wrong_path, creds.to_export_json().unwrap().as_bytes()).unwrap();
        assert!(load_first_device_auth(&dir).is_none());

        let correct_path = dir.join(format!("{}.json", account_id.as_str()));
        std::fs::rename(&wrong_path, &correct_path).unwrap();
        assert_eq!(load_first_device_auth(&dir).unwrap().account_id, account_id);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    async fn test_runtime(mut bot: Bot) -> Arc<BotRuntime> {
        let client = Client::builder().build().expect("client");
        let handle = BotHandle::new(client, bot.owner_ids.clone());
        let help = bot.install_default_help();
        handle.set_help_responder(help).await;
        Arc::new(BotRuntime {
            handle,
            prefix: bot.prefix,
            commands: bot.commands,
            handlers: bot.handlers,
        })
    }

    #[test]
    fn string_view_parses_quoted_words_and_escapes() {
        let mut view = StringView::new("hello \"multi word\\\"arg\"  tail ");
        let args = view.parse_all_quoted_args().expect("quoted args parse");
        assert_eq!(args, vec!["hello", "multi word\"arg", "tail"]);

        let mut unclosed = StringView::new("\"oops");
        assert!(matches!(
            unclosed.get_quoted_word(),
            Err(CommandError::ExpectedClosingQuote('"'))
        ));
    }

    #[tokio::test]
    async fn runtime_supports_aliases_groups_checks_cooldowns_and_cogs() {
        struct SocialCog {
            hits: Arc<AtomicUsize>,
        }

        impl BotCog for SocialCog {
            fn name(&self) -> &str {
                "Social"
            }

            fn register(
                self: Arc<Self>,
                commands: &mut CogRegistrar<'_>,
            ) -> Result<(), CommandError> {
                let hits = Arc::clone(&self.hits);
                commands.add_command(
                    Command::new("wave", move |ctx| {
                        let hits = Arc::clone(&hits);
                        async move {
                            let target: String = ctx.arg(0)?;
                            assert_eq!(target, "big world");
                            hits.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                    })
                    .aliases(["hi"])
                    .cooldown(1, Duration::from_secs(60), BucketType::User),
                )
            }
        }

        let owner_id = AccountId::from_raw("11111111111111111111111111111111");
        let guest_id = AccountId::from_raw("22222222222222222222222222222222");
        let self_id = AccountId::from_raw("33333333333333333333333333333333");

        let cog_hits = Arc::new(AtomicUsize::new(0));
        let group_log = Arc::new(Mutex::new(Vec::<String>::new()));
        let group_log_parent = Arc::clone(&group_log);
        let group_log_sub = Arc::clone(&group_log);
        let errors_seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let errors_clone = Arc::clone(&errors_seen);

        let party_group = Command::group("party", move |_ctx| {
            let group_log_parent = Arc::clone(&group_log_parent);
            async move {
                group_log_parent.lock().await.push("parent".to_owned());
            }
        })
        .invoke_without_command(true)
        .add_subcommand("lock", move |_ctx| {
            let group_log_sub = Arc::clone(&group_log_sub);
            async move {
                group_log_sub.lock().await.push("lock".to_owned());
            }
        })
        .expect("subcommand")
        .party_only();

        let owner_cmd = Command::new("admin", |_ctx| async move { Ok(()) }).is_owner();

        let bot = Bot::new(["!", "?"])
            .case_insensitive(true)
            .owner_id(owner_id.clone())
            .add_cog(SocialCog {
                hits: Arc::clone(&cog_hits),
            })
            .expect("cog")
            .add_command(party_group)
            .add_command(owner_cmd)
            .on_command_error(move |_ctx, err| {
                let errors_clone = Arc::clone(&errors_clone);
                async move {
                    errors_clone.lock().await.push(err.to_string());
                }
            });

        let runtime = test_runtime(bot).await;
        runtime
            .handle
            .set_user(BotUser {
                id: self_id.clone(),
                display_name: "BotSelf".to_owned(),
            })
            .await;

        runtime
            .process_message(IncomingMessage {
                message_id: "m1".to_owned(),
                author_id: guest_id.clone(),
                author_display_name: Some("Guest".to_owned()),
                content: "?HI \"big world\"".to_owned(),
                channel: MessageChannel::DirectMessage,
            })
            .await;
        assert_eq!(cog_hits.load(Ordering::SeqCst), 1);

        runtime
            .process_message(IncomingMessage {
                message_id: "m2".to_owned(),
                author_id: guest_id.clone(),
                author_display_name: Some("Guest".to_owned()),
                content: "!wave \"big world\"".to_owned(),
                channel: MessageChannel::DirectMessage,
            })
            .await;

        runtime
            .process_message(IncomingMessage {
                message_id: "m3".to_owned(),
                author_id: guest_id.clone(),
                author_display_name: Some("Guest".to_owned()),
                content: "!admin".to_owned(),
                channel: MessageChannel::DirectMessage,
            })
            .await;

        runtime
            .process_message(IncomingMessage {
                message_id: "m4".to_owned(),
                author_id: owner_id,
                author_display_name: None,
                content: "!party lock".to_owned(),
                channel: MessageChannel::Party(LobbyId::from_raw(
                    "44444444444444444444444444444444-1-default",
                )),
            })
            .await;
        assert_eq!(*group_log.lock().await, vec!["lock".to_owned()]);

        runtime
            .process_message(IncomingMessage {
                message_id: "m5".to_owned(),
                author_id: guest_id,
                author_display_name: None,
                content: "!party lock".to_owned(),
                channel: MessageChannel::DirectMessage,
            })
            .await;

        runtime
            .process_message(IncomingMessage {
                message_id: "m6".to_owned(),
                author_id: AccountId::from_raw("55555555555555555555555555555555"),
                author_display_name: None,
                content: "!doesnotexist".to_owned(),
                channel: MessageChannel::DirectMessage,
            })
            .await;

        handle_event(
            &runtime,
            Arc::new(ClientEvent::FriendMessage(ReceivedFriendMessage {
                message_id: "self".to_owned(),
                author_id: self_id.clone(),
                author_display_name: None,
                conversation_id: None,
                content: "!admin".to_owned(),
                sent_at: chrono::Utc::now(),
                origin: ChatTransportOrigin::EOSConversation,
            })),
        )
        .await;
        handle_event(
            &runtime,
            Arc::new(ClientEvent::PartyMessage(PartyChatMessage {
                id: None,
                party_id: LobbyId::from_raw("44444444444444444444444444444444-1-default"),
                eos_party_id: PartyId::from_raw("44444444444444444444444444444444"),
                author_id: self_id,
                content: "!party lock".to_owned(),
                timestamp_ms: 0,
            })),
        )
        .await;

        let errs = errors_seen.lock().await;
        assert!(errs.iter().any(|e| e.contains("on cooldown")));
        assert!(errs.iter().any(|e| e.contains("do not own")));
        assert!(errs.iter().any(|e| e.contains("only be used in a party")));
        assert!(errs.iter().any(|e| e.contains("not found")));
    }
}
