use crate::commands::error::CommandError;
use crate::commands::view::StringView;
use crate::client::client::Client;
use crate::friends::{FriendDirection, PendingFriend};
use crate::party::{ClientParty, PartyMemberSnapshot, ReceivedPartyInvitation};
use crate::protocol::{AccountId, LobbyId, RustfnError};
use std::collections::HashSet;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type BoxBotFuture = Pin<Box<dyn Future<Output = Result<(), CommandError>> + Send>>;

/// Allows bot event hooks and commands to return `()` or `Result<T, CommandError>`.
pub trait IntoBotResult {
    fn into_bot_result(self) -> Result<(), CommandError>;
}

impl IntoBotResult for () {
    fn into_bot_result(self) -> Result<(), CommandError> {
        Ok(())
    }
}

impl<T> IntoBotResult for Result<T, CommandError> {
    fn into_bot_result(self) -> Result<(), CommandError> {
        self.map(|_| ())
    }
}

#[doc(hidden)]
pub trait HelpResponder: Send + Sync + 'static {
    fn send_help(&self, ctx: Context, entity: Option<String>) -> BoxBotFuture;
}

/// Identity snapshot for the logged-in bot account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotUser {
    pub id: AccountId,
    pub display_name: String,
}

/// Channel where an incoming message or command originated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageChannel {
    DirectMessage,
    Party(LobbyId),
}

/// Normalized incoming message shared between DM and Party chat transports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingMessage {
    pub message_id: String,
    pub author_id: AccountId,
    pub author_display_name: Option<String>,
    pub content: String,
    pub channel: MessageChannel,
}

impl IncomingMessage {
    pub fn author_name(&self) -> &str {
        self.author_display_name
            .as_deref()
            .unwrap_or(self.author_id.as_str())
    }

    pub fn is_dm(&self) -> bool {
        matches!(self.channel, MessageChannel::DirectMessage)
    }

    pub fn is_party(&self) -> bool {
        matches!(self.channel, MessageChannel::Party(_))
    }
}

/// Cloneable runtime handle exposed to every event callback and command `Context`.
#[derive(Clone)]
pub struct BotHandle {
    client: Client,
    user: Arc<RwLock<Option<BotUser>>>,
    owner_ids: Arc<HashSet<AccountId>>,
    help_responder: Arc<RwLock<Option<Arc<dyn HelpResponder>>>>,
}

impl BotHandle {
    pub(crate) fn new(client: Client, owner_ids: HashSet<AccountId>) -> Self {
        Self {
            client,
            user: Arc::new(RwLock::new(None)),
            owner_ids: Arc::new(owner_ids),
            help_responder: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) async fn set_user(&self, user: BotUser) {
        *self.user.write().await = Some(user);
    }

    pub(crate) async fn set_help_responder(&self, responder: Option<Arc<dyn HelpResponder>>) {
        *self.help_responder.write().await = responder;
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn owner_ids(&self) -> &HashSet<AccountId> {
        &self.owner_ids
    }

    /// Checks whether `account_id` belongs to the bot itself or configured `owner_id`/`owner_ids`.
    pub async fn is_owner(&self, account_id: &AccountId) -> bool {
        if self.owner_ids.contains(account_id) {
            return true;
        }
        if self.owner_ids.is_empty()
            && let Some(self_user) = self.user().await
        {
            return &self_user.id == account_id;
        }
        false
    }

    pub async fn user(&self) -> Option<BotUser> {
        if let Some(user) = self.user.read().await.clone() {
            return Some(user);
        }
        let id = self.client.account_id().await?;
        let display_name = self
            .client
            .display_name()
            .await
            .unwrap_or_else(|| id.as_str().to_owned());
        Some(BotUser { id, display_name })
    }

    pub async fn party(&self) -> Option<ClientParty> {
        self.client.party().await
    }

    pub async fn accept_friend_request(&self, id_or_name: &str) -> Result<(), CommandError> {
        self.client.add_friend(id_or_name).await?;
        Ok(())
    }

    pub async fn decline_friend_request(&self, id_or_name: &str) -> Result<(), CommandError> {
        self.client.remove_friend(id_or_name).await?;
        Ok(())
    }

    pub async fn join_party(&self, party_id: &str) -> Result<ClientParty, CommandError> {
        Ok(self.client.join_party(party_id).await?)
    }

    pub async fn accept_party_invite(
        &self,
        invite: &ReceivedPartyInvitation,
    ) -> Result<ClientParty, CommandError> {
        Ok(self.client.accept_invite(invite).await?)
    }

    pub async fn decline_party_invite(
        &self,
        invite: &ReceivedPartyInvitation,
    ) -> Result<(), CommandError> {
        self.client.decline_invite(invite).await?;
        Ok(())
    }

    pub async fn send_dm(
        &self,
        friend_id_or_name: &str,
        content: &str,
    ) -> Result<(), CommandError> {
        self.client
            .send_friend_message(friend_id_or_name, content)
            .await?;
        Ok(())
    }
}

/// Passed into `Bot::on_ready`. Derefs to [`BotHandle`] and carries `bot.user.id` / `bot.user.display_name`.
#[derive(Clone)]
pub struct ReadyContext {
    pub user: BotUser,
    pub bot: BotHandle,
}

impl Deref for ReadyContext {
    type Target = BotHandle;

    fn deref(&self) -> &Self::Target {
        &self.bot
    }
}

/// Incoming or outgoing pending friend request wrapper passed into `Bot::on_friend_request`.
#[derive(Clone)]
pub struct FriendRequestContext {
    pub request: PendingFriend,
    pub bot: BotHandle,
}

impl FriendRequestContext {
    pub fn id(&self) -> &AccountId {
        &self.request.id
    }

    pub fn display_name(&self) -> &str {
        self.request
            .display_name
            .as_deref()
            .unwrap_or(self.request.id.as_str())
    }

    pub fn is_inbound(&self) -> bool {
        self.request.direction == FriendDirection::Inbound
    }

    pub async fn accept(&self) -> Result<(), CommandError> {
        self.bot
            .accept_friend_request(self.request.id.as_str())
            .await
    }

    pub async fn decline(&self) -> Result<(), CommandError> {
        self.bot
            .decline_friend_request(self.request.id.as_str())
            .await
    }
}

/// Incoming party invitation wrapper passed into `Bot::on_party_invite`.
#[derive(Clone)]
pub struct PartyInviteContext {
    pub invite: ReceivedPartyInvitation,
    pub bot: BotHandle,
}

impl PartyInviteContext {
    pub fn inviter_name(&self) -> &str {
        self.invite
            .inviter_display_name
            .as_deref()
            .unwrap_or(self.invite.inviter_id.as_str())
    }

    pub async fn accept(&self) -> Result<ClientParty, CommandError> {
        self.bot
            .join_party(self.invite.eos_party_id.as_str())
            .await
    }

    pub async fn decline(&self) -> Result<(), CommandError> {
        self.bot.decline_party_invite(&self.invite).await
    }
}

/// Context passed into `Bot::on_party_member_join` and `Bot::on_party_member_leave`.
#[derive(Clone)]
pub struct PartyMemberContext {
    pub party_id: LobbyId,
    pub member: PartyMemberSnapshot,
    pub bot: BotHandle,
}

impl PartyMemberContext {
    pub fn member_name(&self) -> &str {
        self.member
            .display_name
            .as_deref()
            .unwrap_or(self.member.account_id.as_str())
    }
}

/// Command invocation context passed to handlers.
#[derive(Clone)]
pub struct Context {
    pub bot: BotHandle,
    pub message: IncomingMessage,
    pub view: StringView,
    pub prefix: Option<String>,
    pub command_name: Option<String>,
    pub invoked_with: Option<String>,
    pub invoked_subcommand: Option<String>,
    pub subcommand_passed: Option<String>,
    pub args: Vec<String>,
    pub raw_args: String,
    pub cog_name: Option<String>,
    pub command_failed: bool,
}

impl Context {
    pub(crate) fn new(bot: BotHandle, message: IncomingMessage, view: StringView) -> Self {
        Self {
            bot,
            message,
            view,
            prefix: None,
            command_name: None,
            invoked_with: None,
            invoked_subcommand: None,
            subcommand_passed: None,
            args: Vec::new(),
            raw_args: String::new(),
            cog_name: None,
            command_failed: false,
        }
    }

    pub fn valid(&self) -> bool {
        self.prefix.is_some() && self.command_name.is_some()
    }

    pub fn author_id(&self) -> &AccountId {
        &self.message.author_id
    }

    pub fn author_name(&self) -> &str {
        self.message.author_name()
    }

    pub fn is_dm(&self) -> bool {
        self.message.is_dm()
    }

    pub fn is_party(&self) -> bool {
        self.message.is_party()
    }

    pub async fn party(&self) -> Option<ClientParty> {
        self.bot.party().await
    }

    /// Parses positional argument `index` using `FromStr`, returning `MissingRequiredArgument` or `BadArgument` on error.
    pub fn arg<T>(&self, index: usize) -> Result<T, CommandError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let raw = self
            .args
            .get(index)
            .ok_or_else(|| CommandError::MissingRequiredArgument(format!("arg[{index}]")))?;
        raw.parse::<T>()
            .map_err(|err| CommandError::BadArgument(format!("arg[{index}] ({raw:?}): {err}")))
    }

    /// Parses optional positional argument `index` without failing when absent.
    pub fn opt_arg<T>(&self, index: usize) -> Result<Option<T>, CommandError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let Some(raw) = self.args.get(index) else {
            return Ok(None);
        };
        raw.parse::<T>()
            .map(Some)
            .map_err(|err| CommandError::BadArgument(format!("arg[{index}] ({raw:?}): {err}")))
    }

    /// Sends a response back to the party chat or DM channel that triggered this context.
    pub async fn send(&self, content: impl AsRef<str>) -> Result<(), CommandError> {
        let text = content.as_ref();
        match &self.message.channel {
            MessageChannel::DirectMessage => {
                self.bot.send_dm(self.author_id().as_str(), text).await
            }
            MessageChannel::Party(_) => {
                let party = self.bot.party().await.ok_or_else(|| {
                    RustfnError::PartyState("bot is not currently in a party".to_owned())
                })?;
                party.send_message(text).await?;
                Ok(())
            }
        }
    }

    /// Sends a DM directly to the message author even if triggered from party chat.
    pub async fn send_dm(&self, content: impl AsRef<str>) -> Result<(), CommandError> {
        self.bot
            .send_dm(self.author_id().as_str(), content.as_ref())
            .await
    }

    /// Triggers the configured help output for the bot or a specific command/cog name.
    pub async fn send_help(&self, entity: Option<&str>) -> Result<(), CommandError> {
        let responder = self.bot.help_responder.read().await.clone();
        if let Some(help) = responder {
            help.send_help(self.clone(), entity.map(ToOwned::to_owned))
                .await
        } else {
            Ok(())
        }
    }
}
