use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::{OsRng, RngCore};
use rustfn_friends::FriendManager;
use rustfn_http::{
    ChatMessageMetadata, ChatSendMessageBody, ChatSendMessageRequest, HTTP,
    PublicKeyResponse,
};
use rustfn_protocol::{AccountId, PartyId, Platform, RustfnError};
use rustfn_xmpp::{XMPP, chat_message_frame};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const ERR_CHAT_IS_REPORTABLE_MISMATCH: &str =
    "errors.com.epicgames.social.chat.is_reportable_mismatch";

/// Transport channel that delivered an incoming whisper or DM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChatTransportOrigin {
    EOSConversation,
    EOSWhisper,
    Xmpp,
}

/// Incoming direct/friend chat message received from a friend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceivedFriendMessage {
    pub message_id: String,
    pub author_id: AccountId,
    pub author_display_name: Option<String>,
    pub conversation_id: Option<String>,
    pub content: String,
    pub sent_at: DateTime<Utc>,
    pub origin: ChatTransportOrigin,
}

/// Metadata for a friend direct message sent by the local client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentFriendMessage {
    pub message_id: String,
    pub recipient_id: AccountId,
    pub conversation_id: String,
    pub content: String,
    pub sent_at: DateTime<Utc>,
}

pub const MAX_CHAT_MESSAGE_LEN: usize = 2047;

/// Conversation signing category placed in the `cty` field of Epic signed chat bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatSignedType {
    Persistent,
    Party,
}

impl ChatSignedType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "Persistent",
            Self::Party => "Party",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SignedChatInnerPayload<'a> {
    mid: &'a str,
    sid: &'a str,
    rid: &'a str,
    msg: &'a str,
    tst: u64,
    seq: u64,
    rec: bool,
    mts: [u8; 0],
    cty: &'a str,
}

/// Output of Ed25519 chat message signing: base64 payload, base64 signature, and message metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedChatPayload {
    pub message_id: String,
    pub body_base64: String,
    pub signature_base64: String,
    pub pub_jwt: String,
    pub timestamp_secs: u64,
}

/// Ephemeral per-session Ed25519 keypair registered against Epic's PublicKey service.
pub struct SignedChatKeypair {
    signing_key: SigningKey,
    public_key_b64: String,
    registration: Option<PublicKeyResponse>,
}

impl SignedChatKeypair {
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        OsRng.fill_bytes(&mut secret);
        let signing_key = SigningKey::from_bytes(&secret);
        let public_key_b64 = BASE64_STANDARD.encode(signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            public_key_b64,
            registration: None,
        }
    }

    pub fn from_secret_bytes(secret: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(secret);
        let public_key_b64 = BASE64_STANDARD.encode(signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            public_key_b64,
            registration: None,
        }
    }

    pub fn public_key_b64(&self) -> &str {
        &self.public_key_b64
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    pub fn registration(&self) -> Option<&PublicKeyResponse> {
        self.registration.as_ref()
    }

    pub fn is_registered(&self) -> bool {
        self.registration.is_some()
    }

    pub async fn ensure_registered(
        &mut self,
        http: &HTTP,
        allow_fallback: bool,
    ) -> Result<&PublicKeyResponse, RustfnError> {
        if self.registration.is_none() {
            let registered = http
                .register_public_key(&self.public_key_b64, allow_fallback)
                .await?;
            return Ok(self.registration.insert(registered));
        }
        match self.registration.as_ref() {
            Some(reg) => Ok(reg),
            None => Err(RustfnError::Protocol(
                "public key registration missing".to_owned(),
            )),
        }
    }

    /// Signs `base64(json_payload) || 0x00` with the session's Ed25519 private key.
    pub fn sign_message(
        &self,
        sender_id: &AccountId,
        conversation_id: &str,
        content: &str,
        chat_type: ChatSignedType,
        sequence: u64,
    ) -> Result<SignedChatPayload, RustfnError> {
        if content.len() > MAX_CHAT_MESSAGE_LEN {
            return Err(RustfnError::Protocol(format!(
                "chat message length {} exceeds maximum {MAX_CHAT_MESSAGE_LEN}",
                content.len()
            )));
        }

        let reg = self.registration.as_ref().ok_or_else(|| {
            RustfnError::Auth("Ed25519 chat keypair has not been registered yet".to_owned())
        })?;

        let message_id = Uuid::new_v4().simple().to_string();
        let timestamp_secs = Utc::now().timestamp().max(0) as u64;

        let inner = SignedChatInnerPayload {
            mid: &message_id,
            sid: sender_id.as_str(),
            rid: conversation_id,
            msg: content,
            tst: timestamp_secs,
            seq: sequence,
            rec: false,
            mts: [],
            cty: chat_type.as_str(),
        };

        let json_bytes = serde_json::to_vec(&inner)?;
        let body_base64 = BASE64_STANDARD.encode(&json_bytes);

        let mut signed_input = Vec::with_capacity(body_base64.len() + 1);
        signed_input.extend_from_slice(body_base64.as_bytes());
        signed_input.push(0x00);

        let signature: Signature = self.signing_key.sign(&signed_input);
        let signature_base64 = BASE64_STANDARD.encode(signature.to_bytes());

        Ok(SignedChatPayload {
            message_id,
            body_base64,
            signature_base64,
            pub_jwt: reg.jwt.clone(),
            timestamp_secs,
        })
    }
}

/// Verifies an incoming `base64(payload)` + `0x00` signature against a raw 32-byte Ed25519 public key.
pub fn verify_signed_message(
    raw_public_key_b64: &str,
    body_base64: &str,
    signature_base64: &str,
) -> Result<(), RustfnError> {
    let key_bytes = BASE64_STANDARD
        .decode(raw_public_key_b64)
        .map_err(|err| RustfnError::Protocol(format!("invalid base64 public key: {err}")))?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| RustfnError::Protocol("public key must be 32 bytes".to_owned()))?;
    let verifying_key = VerifyingKey::from_bytes(&key_array)
        .map_err(|err| RustfnError::Protocol(format!("invalid Ed25519 key: {err}")))?;

    let sig_bytes = BASE64_STANDARD
        .decode(signature_base64)
        .map_err(|err| RustfnError::Protocol(format!("invalid base64 signature: {err}")))?;
    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| RustfnError::Protocol("Ed25519 signature must be 64 bytes".to_owned()))?;
    let signature = Signature::from_bytes(&sig_array);

    let mut message = Vec::with_capacity(body_base64.len() + 1);
    message.extend_from_slice(body_base64.as_bytes());
    message.push(0x00);

    verifying_key
        .verify(&message, &signature)
        .map_err(|err| RustfnError::Protocol(format!("Ed25519 signature verification failed: {err}")))
}

/// Decodes Epic Connect STOMP chat message bodies.
///
/// Base64-encoded JSON payloads unwrap `.msg`; plain base64 UTF-8 payloads trim trailing NULs;
/// unencoded legacy strings pass through cleanly.
pub fn decode_stomp_message_body(raw_body: &str) -> String {
    if raw_body.is_empty() {
        return String::new();
    }

    let Ok(decoded_bytes) = BASE64_STANDARD.decode(raw_body.trim()) else {
        return raw_body.to_owned();
    };
    let Ok(decoded_text) = String::from_utf8(decoded_bytes) else {
        return raw_body.to_owned();
    };
    let trimmed = decoded_text.trim_end_matches('\0');
    if let Ok(json_val) = serde_json::from_str::<Value>(trimmed)
        && let Some(msg) = json_val.get("msg").and_then(Value::as_str)
    {
        return msg.to_owned();
    }
    trimmed.to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        ChatSignedType, SignedChatKeypair, decode_stomp_message_body, verify_signed_message,
    };
    use rustfn_http::PublicKeyResponse;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use rustfn_protocol::AccountId;
    use serde_json::Value;

    #[test]
    fn signs_with_trailing_nul_and_verifies_ed25519_payload() {
        let mut kp = SignedChatKeypair::from_secret_bytes(&[7u8; 32]);
        kp.registration = Some(PublicKeyResponse {
            key: kp.public_key_b64().to_owned(),
            algorithm: "ed25519".to_owned(),
            jwt: "jwt-token".to_owned(),
            key_guid: Some("guid-1".to_owned()),
        });

        let sender = AccountId::parse("11111111111111111111111111111111").expect("valid account id");
        let signed = kp
            .sign_message(
                &sender,
                "ep-abcdef1234",
                "hello squad",
                ChatSignedType::Persistent,
                1,
            )
            .expect("sign succeeds");

        verify_signed_message(kp.public_key_b64(), &signed.body_base64, &signed.signature_base64)
            .expect("signature verifies against body || 0x00");

        let decoded_bytes = BASE64_STANDARD
            .decode(&signed.body_base64)
            .expect("valid base64 body");
        let parsed: Value = serde_json::from_slice(&decoded_bytes).expect("valid json payload");
        assert_eq!(parsed["sid"], "11111111111111111111111111111111");
        assert_eq!(parsed["rid"], "ep-abcdef1234");
        assert_eq!(parsed["msg"], "hello squad");
        assert_eq!(parsed["cty"], "Persistent");
        assert_eq!(parsed["seq"], 1);
        assert_eq!(parsed["rec"], false);

        assert_eq!(decode_stomp_message_body(&signed.body_base64), "hello squad");
    }

    #[test]
    fn decodes_plain_and_legacy_message_bodies() {
        assert_eq!(decode_stomp_message_body(""), "");
        let plain_b64 = BASE64_STANDARD.encode("plain message\0\0");
        assert_eq!(decode_stomp_message_body(&plain_b64), "plain message");
        assert_eq!(decode_stomp_message_body("legacy raw text!"), "legacy raw text!");
    }
}

#[derive(Debug, Clone)]
struct DmConversationEntry {
    conversation_id: String,
    is_reportable: bool,
}

/// Result returned when delivering an outbound EOS party chat message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentPartyChatResult {
    pub message_id: String,
    pub conversation_id: String,
    pub resolved_is_reportable: bool,
}

/// Manages Ed25519 key registration, DM conversation caching, signed EOS messages, and XMPP whispers.
pub struct ChatManager {
    self_id: AccountId,
    platform: Platform,
    deployment_id: String,
    xmpp_domain: String,
    allow_publickey_fallback: bool,
    http: HTTP,
    friends: Arc<FriendManager>,
    keypair: Mutex<SignedChatKeypair>,
    dm_conversations: DashMap<AccountId, DmConversationEntry>,
}

impl ChatManager {
    pub fn new(
        self_id: AccountId,
        platform: Platform,
        deployment_id: String,
        xmpp_domain: String,
        allow_publickey_fallback: bool,
        http: HTTP,
        friends: Arc<FriendManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            self_id,
            platform,
            deployment_id,
            xmpp_domain,
            allow_publickey_fallback,
            http,
            friends,
            keypair: Mutex::new(SignedChatKeypair::generate()),
            dm_conversations: DashMap::new(),
        })
    }

    pub async fn ensure_keypair_registered(&self) -> Result<(), RustfnError> {
        let mut guard = self.keypair.lock().await;
        let _ = guard
            .ensure_registered(&self.http, self.allow_publickey_fallback)
            .await?;
        Ok(())
    }

    /// Caches a known DM conversation ID discovered via STOMP `ConversationCreated`.
    pub fn cache_dm_conversation(
        &self,
        peer_id: AccountId,
        conversation_id: String,
        is_reportable: bool,
    ) {
        self.dm_conversations.insert(
            peer_id,
            DmConversationEntry {
                conversation_id,
                is_reportable,
            },
        );
    }

    /// Sends a signed EOS DM to a friend (creating/fetching the DM conversation as needed).
    pub async fn send_friend_message(
        &self,
        friend_id_or_name: &str,
        content: &str,
    ) -> Result<SentFriendMessage, RustfnError> {
        if content.len() > MAX_CHAT_MESSAGE_LEN {
            return Err(RustfnError::Protocol(format!(
                "chat message length {} exceeds maximum {MAX_CHAT_MESSAGE_LEN}",
                content.len()
            )));
        }

        let recipient_id = self.friends.resolve_friend_id(friend_id_or_name).await?;
        let mut entry = self.resolve_dm_conversation(&recipient_id, false).await?;

        match self
            .post_dm_message(&recipient_id, &entry, content)
            .await
        {
            Ok(sent) => Ok(sent),
            Err(err) if is_reportable_mismatch(&err) => {
                self.dm_conversations.remove(&recipient_id);
                entry = self.resolve_dm_conversation(&recipient_id, true).await?;
                if entry.is_reportable == is_reportable_expected(&err).unwrap_or(entry.is_reportable) {
                    self.post_dm_message(&recipient_id, &entry, content).await
                } else {
                    entry.is_reportable = !entry.is_reportable;
                    self.dm_conversations.insert(recipient_id.clone(), entry.clone());
                    self.post_dm_message(&recipient_id, &entry, content).await
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Sends a signed EOS message inside the current persistent or scoped party chat conversation.
    pub async fn send_party_message(
        &self,
        fn_party_id: &PartyId,
        eos_party_id: Option<&PartyId>,
        recipient_ids: &[AccountId],
        is_reportable_hint: Option<bool>,
        content: &str,
    ) -> Result<SentPartyChatResult, RustfnError> {
        if content.len() > MAX_CHAT_MESSAGE_LEN {
            return Err(RustfnError::Protocol(format!(
                "chat message length {} exceeds maximum {MAX_CHAT_MESSAGE_LEN}",
                content.len()
            )));
        }

        let (scope, addressed_conv, signed_conv, chat_type, default_reportable) =
            if let Some(eos_id) = eos_party_id {
                let addressed = format!("ep-{}", eos_id.as_str());
                (
                    "_".to_owned(),
                    addressed.clone(),
                    addressed,
                    ChatSignedType::Persistent,
                    true,
                )
            } else {
                (
                    self.deployment_id.clone(),
                    format!("p-{}", fn_party_id.as_str()),
                    fn_party_id.as_str().to_owned(),
                    ChatSignedType::Party,
                    false,
                )
            };

        let is_reportable = is_reportable_hint.unwrap_or(default_reportable);
        let allowed_recipients: Vec<AccountId> = recipient_ids
            .iter()
            .filter(|id| *id != &self.self_id)
            .cloned()
            .collect();

        match self
            .post_party_message(
                &scope,
                &addressed_conv,
                &signed_conv,
                chat_type,
                &allowed_recipients,
                is_reportable,
                content,
            )
            .await
        {
            Ok(msg_id) => Ok(SentPartyChatResult {
                message_id: msg_id,
                conversation_id: addressed_conv,
                resolved_is_reportable: is_reportable,
            }),
            Err(err) if is_reportable_mismatch(&err) => {
                let expected = is_reportable_expected(&err).unwrap_or(!is_reportable);
                let msg_id = self
                    .post_party_message(
                        &scope,
                        &addressed_conv,
                        &signed_conv,
                        chat_type,
                        &allowed_recipients,
                        expected,
                        content,
                    )
                    .await?;
                Ok(SentPartyChatResult {
                    message_id: msg_id,
                    conversation_id: addressed_conv,
                    resolved_is_reportable: expected,
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Sends a lightweight EOS whisper (`POST /epic/chat/v1/public/{ns}/whispers/{sender}/{recipient}`).
    pub async fn send_eos_whisper(
        &self,
        recipient_id: &AccountId,
        content: &str,
    ) -> Result<String, RustfnError> {
        let correlation_id = format!("EOS-{}-{}", Utc::now().timestamp_millis(), Uuid::new_v4());
        let body = json!({
            "message": {
                "body": content,
            },
            "correlationId": correlation_id,
        });
        self.http
            .send_eos_whisper(&self.deployment_id, &self.self_id, recipient_id, &body)
            .await?;
        Ok(correlation_id)
    }

    /// Sends an XMPP direct chat stanza (`<message type="chat">`) for legacy clients.
    pub async fn send_xmpp_whisper(
        &self,
        xmpp: &XMPP,
        recipient_id: &AccountId,
        content: &str,
    ) -> Result<(), RustfnError> {
        let to_jid = format!("{}@{}", recipient_id.as_str(), self.xmpp_domain);
        let msg_id = Uuid::new_v4().simple().to_string();
        let xml = chat_message_frame(&to_jid, &msg_id, content);
        xmpp.send_stanza(xml).await
    }

    async fn resolve_dm_conversation(
        &self,
        recipient_id: &AccountId,
        force_refresh: bool,
    ) -> Result<DmConversationEntry, RustfnError> {
        if !force_refresh && let Some(cached) = self.dm_conversations.get(recipient_id) {
            return Ok(cached.value().clone());
        }

        let conv = self
            .http
            .create_dm_conversation(&self.self_id, recipient_id, false)
            .await?;
        let entry = DmConversationEntry {
            conversation_id: conv.conversation_id,
            is_reportable: conv.is_reportable,
        };
        self.dm_conversations
            .insert(recipient_id.clone(), entry.clone());
        Ok(entry)
    }

    async fn post_dm_message(
        &self,
        recipient_id: &AccountId,
        entry: &DmConversationEntry,
        content: &str,
    ) -> Result<SentFriendMessage, RustfnError> {
        let signed = self
            .sign_with_registered_key(&entry.conversation_id, content, ChatSignedType::Persistent, 0)
            .await?;

        let request = ChatSendMessageRequest {
            allowed_recipients: vec![recipient_id.clone(), self.self_id.clone()],
            message: ChatSendMessageBody {
                body: signed.body_base64,
            },
            is_reportable: entry.is_reportable,
            metadata: ChatMessageMetadata {
                tm_v: "2".to_owned(),
                pub_jwt: signed.pub_jwt,
                sig: signed.signature_base64,
                npm: None,
                plf_nm: self.platform.as_str().to_owned(),
                plf_id: self.self_id.as_str().to_owned(),
            },
        };

        let resp = self
            .http
            .send_eos_conversation_message("_", &entry.conversation_id, &self.self_id, &request)
            .await?;

        Ok(SentFriendMessage {
            message_id: resp.message_id.unwrap_or(signed.message_id),
            recipient_id: recipient_id.clone(),
            conversation_id: entry.conversation_id.clone(),
            content: content.to_owned(),
            sent_at: Utc::now(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn post_party_message(
        &self,
        scope: &str,
        addressed_conv: &str,
        signed_conv: &str,
        chat_type: ChatSignedType,
        allowed_recipients: &[AccountId],
        is_reportable: bool,
        content: &str,
    ) -> Result<String, RustfnError> {
        let signed = self
            .sign_with_registered_key(signed_conv, content, chat_type, 1)
            .await?;

        let request = ChatSendMessageRequest {
            allowed_recipients: allowed_recipients.to_vec(),
            message: ChatSendMessageBody {
                body: signed.body_base64,
            },
            is_reportable,
            metadata: ChatMessageMetadata {
                tm_v: "2".to_owned(),
                pub_jwt: signed.pub_jwt,
                sig: signed.signature_base64,
                npm: Some("1".to_owned()),
                plf_nm: self.platform.as_str().to_owned(),
                plf_id: self.self_id.as_str().to_owned(),
            },
        };

        let resp = self
            .http
            .send_eos_conversation_message(scope, addressed_conv, &self.self_id, &request)
            .await?;
        Ok(resp.message_id.unwrap_or(signed.message_id))
    }

    async fn sign_with_registered_key(
        &self,
        conversation_id: &str,
        content: &str,
        chat_type: ChatSignedType,
        sequence: u64,
    ) -> Result<SignedChatPayload, RustfnError> {
        let mut guard = self.keypair.lock().await;
        guard
            .ensure_registered(&self.http, self.allow_publickey_fallback)
            .await?;
        guard.sign_message(&self.self_id, conversation_id, content, chat_type, sequence)
    }
}

fn is_reportable_mismatch(err: &RustfnError) -> bool {
    matches!(
        err,
        RustfnError::EpicApi { body, .. }
            if body.effective_code() == ERR_CHAT_IS_REPORTABLE_MISMATCH
    )
}

fn is_reportable_expected(err: &RustfnError) -> Option<bool> {
    let RustfnError::EpicApi { body, .. } = err else {
        return None;
    };
    let lower = body.error_message.to_ascii_lowercase();
    if lower.contains(": true") {
        Some(true)
    } else if lower.contains(": false") {
        Some(false)
    } else {
        None
    }
}
