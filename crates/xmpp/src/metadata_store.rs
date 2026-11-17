use crate::message::XMPPPartyMemberDeltaNotification;
use dashmap::DashMap;
use rustfn_protocol::{AccountId, MetaMap, PartyId};
use std::time::{Duration, Instant};

/// Default lifetime for buffered XMPP member state deltas awaiting STOMP `MEMBER_JOINED`.
pub const DEFAULT_MEMBER_METADATA_TTL: Duration = Duration::from_secs(10);

/// Single buffered party member metadata delta from XMPP (`MEMBER_JOINED` or `MEMBER_STATE_UPDATED`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedMemberMetadataUpdate {
    pub account_id: AccountId,
    pub account_dn: Option<String>,
    pub revision: u64,
    pub member_state_updated: MetaMap,
    pub member_state_removed: Vec<String>,
}

impl From<XMPPPartyMemberDeltaNotification> for QueuedMemberMetadataUpdate {
    fn from(value: XMPPPartyMemberDeltaNotification) -> Self {
        Self {
            account_id: value.account_id,
            account_dn: value.account_dn,
            revision: value.revision,
            member_state_updated: value.member_state_updated,
            member_state_removed: value.member_state_removed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StoreKey {
    party_id: PartyId,
    account_id: AccountId,
}

#[derive(Debug, Clone)]
struct EntryBucket {
    updates: Vec<QueuedMemberMetadataUpdate>,
    expires_at: Instant,
}

/// 10-second TTL buffer holding out-of-order XMPP member deltas until STOMP creates the member slot.
///
/// Uses timestamped buckets and synchronous expiration sweeps to avoid per-entry timers.
#[derive(Debug)]
pub struct XMPPMetadataStore {
    ttl: Duration,
    entries: DashMap<StoreKey, EntryBucket>,
}

impl Default for XMPPMetadataStore {
    fn default() -> Self {
        Self::new(DEFAULT_MEMBER_METADATA_TTL)
    }
}

impl XMPPMetadataStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: DashMap::new(),
        }
    }

    /// Buffers a member metadata update under `(party_id, update.account_id)` and refreshes its TTL.
    pub fn push(&self, party_id: PartyId, update: QueuedMemberMetadataUpdate) {
        self.purge_expired();
        let now = Instant::now();
        let key = StoreKey {
            party_id,
            account_id: update.account_id.clone(),
        };
        let mut entry = self.entries.entry(key).or_insert_with(|| EntryBucket {
            updates: Vec::new(),
            expires_at: now + self.ttl,
        });
        if entry.expires_at <= now {
            entry.updates.clear();
        }
        entry.updates.push(update);
        entry.expires_at = now + self.ttl;
    }

    /// Drains all non-expired buffered deltas for `(party_id, account_id)`, sorted by `revision`
    /// ascending and discarding stale entries with `revision < min_revision`.
    pub fn drain_sorted(
        &self,
        party_id: &PartyId,
        account_id: &AccountId,
        min_revision: u64,
    ) -> Vec<QueuedMemberMetadataUpdate> {
        let key = StoreKey {
            party_id: party_id.clone(),
            account_id: account_id.clone(),
        };
        let Some((_, mut bucket)) = self.entries.remove(&key) else {
            return Vec::new();
        };
        if bucket.expires_at <= Instant::now() {
            return Vec::new();
        }
        bucket.updates.sort_by_key(|u| u.revision);
        bucket
            .updates
            .into_iter()
            .filter(|u| u.revision >= min_revision)
            .collect()
    }

    pub fn discard(&self, party_id: &PartyId, account_id: &AccountId) {
        let key = StoreKey {
            party_id: party_id.clone(),
            account_id: account_id.clone(),
        };
        self.entries.remove(&key);
    }

    pub fn clear(&self) {
        self.entries.clear();
    }

    pub fn purge_expired(&self) {
        let now = Instant::now();
        self.entries.retain(|_, bucket| bucket.expires_at > now);
    }

    pub fn active_entry_count(&self) -> usize {
        self.purge_expired();
        self.entries.len()
    }
}
