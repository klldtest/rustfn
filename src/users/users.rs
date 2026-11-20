use crate::http::{
    AccountRecordResponse, AvatarResponse, HTTP, GlobalProfileItemResponse,
    UserSearchEntryResponse,
};
use crate::protocol::{AccountId, CacheConfig, RustfnError, UserSearchPlatform};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;

const ACCOUNT_BATCH_CHUNK_SIZE: usize = 100;

/// Epic account profile, including fields available only to the account owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserProfile {
    pub id: AccountId,
    pub display_name: Option<String>,
    pub external_auths: BTreeMap<String, Value>,
    pub avatar_id: Option<String>,
    pub name: Option<String>,
    pub last_name: Option<String>,
    pub email: Option<String>,
    pub country: Option<String>,
    pub preferred_language: Option<String>,
    pub minor_verified: Option<bool>,
    pub minor_expected: Option<bool>,
    pub minor_status: Option<String>,
}

impl UserProfile {
    pub fn from_account_record(record: AccountRecordResponse) -> Self {
        Self {
            id: record.id,
            display_name: record.display_name,
            external_auths: record.external_auths,
            avatar_id: None,
            name: record.name,
            last_name: record.last_name,
            email: record.email,
            country: record.country,
            preferred_language: record.preferred_language,
            minor_verified: record.minor_verified,
            minor_expected: record.minor_expected,
            minor_status: record.minor_status,
        }
    }

    pub fn partial(id: AccountId, display_name: Option<String>) -> Self {
        Self {
            id,
            display_name,
            external_auths: BTreeMap::new(),
            avatar_id: None,
            name: None,
            last_name: None,
            email: None,
            country: None,
            preferred_language: None,
            minor_verified: None,
            minor_expected: None,
            minor_status: None,
        }
    }
}

pub type UserAvatar = AvatarResponse;
pub type UserGlobalProfile = GlobalProfileItemResponse;
pub type UserSearchResult = UserSearchEntryResponse;

#[derive(Debug, Clone)]
struct CachedUser {
    profile: UserProfile,
    fetched_at: Instant,
    is_partial: bool,
}

/// Account lookup and TTL cache.
pub struct UserManager {
    http: HTTP,
    config: CacheConfig,
    cache: DashMap<AccountId, CachedUser>,
    self_user: RwLock<Option<UserProfile>>,
}

impl UserManager {
    pub fn new(http: HTTP, config: CacheConfig) -> Arc<Self> {
        Arc::new(Self {
            http,
            config,
            cache: DashMap::new(),
            self_user: RwLock::new(None),
        })
    }

    pub async fn set_self_user(&self, profile: UserProfile) {
        self.insert_cached(profile.clone(), false);
        *self.self_user.write().await = Some(profile);
    }

    pub async fn self_user(&self) -> Option<UserProfile> {
        self.self_user.read().await.clone()
    }

    pub async fn fetch_self(&self, self_id: &AccountId) -> Result<UserProfile, RustfnError> {
        let record = self.http.fetch_account_by_id(self_id).await?;
        let profile = UserProfile::from_account_record(record);
        self.set_self_user(profile.clone()).await;
        Ok(profile)
    }

    pub fn get_cached(&self, account_id: &AccountId) -> Option<UserProfile> {
        let entry = self.cache.get(account_id)?;
        (!entry.is_partial && entry.fetched_at.elapsed() < self.config.user_ttl)
            .then(|| entry.profile.clone())
    }

    pub fn get_cached_by_display_name(&self, display_name: &str) -> Option<UserProfile> {
        let ttl = self.config.user_ttl;
        self.cache.iter().find_map(|entry| {
            let cached = entry.value();
            (!cached.is_partial
                && cached.fetched_at.elapsed() < ttl
                && cached
                    .profile
                    .display_name
                    .as_deref()
                    .is_some_and(|dn| dn.eq_ignore_ascii_case(display_name)))
            .then(|| cached.profile.clone())
        })
    }

    /// Seeds partial `(account_id, display_name)` data without overwriting fresh full entries.
    pub fn upsert_partial(&self, account_id: &AccountId, display_name: Option<String>) {
        if let Some(mut entry) = self.cache.get_mut(account_id) {
            if entry.profile.display_name.is_none() && display_name.is_some() {
                entry.profile.display_name = display_name;
            }
            return;
        }
        self.insert_cached(UserProfile::partial(account_id.clone(), display_name), true);
    }

    pub async fn fetch(&self, account_id: &AccountId) -> Result<UserProfile, RustfnError> {
        if let Some(cached) = self.get_cached(account_id) {
            return Ok(cached);
        }

        let record = self.http.fetch_account_by_id(account_id).await?;
        Ok(self.cache_full(UserProfile::from_account_record(record)))
    }

    pub async fn fetch_by_display_name(
        &self,
        display_name: &str,
    ) -> Result<UserProfile, RustfnError> {
        if let Some(cached) = self.get_cached_by_display_name(display_name) {
            return Ok(cached);
        }
        let record = self.http.fetch_account_by_display_name(display_name).await?;
        Ok(self.cache_full(UserProfile::from_account_record(record)))
    }

    pub async fn fetch_by_query(&self, id_or_display_name: &str) -> Result<UserProfile, RustfnError> {
        if let Ok(account_id) = AccountId::parse(id_or_display_name) {
            self.fetch(&account_id).await
        } else {
            self.fetch_by_display_name(id_or_display_name).await
        }
    }

    pub async fn resolve_id(&self, id_or_display_name: &str) -> Result<AccountId, RustfnError> {
        if let Ok(account_id) = AccountId::parse(id_or_display_name) {
            return Ok(account_id);
        }
        Ok(self.fetch_by_display_name(id_or_display_name).await?.id)
    }

    pub async fn fetch_multiple(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<UserProfile>, RustfnError> {
        let mut missing = Vec::new();
        let mut seen_missing = HashSet::new();
        for id in account_ids {
            if self.get_cached(id).is_none() && seen_missing.insert(id.clone()) {
                missing.push(id.clone());
            }
        }

        for chunk in missing.chunks(ACCOUNT_BATCH_CHUNK_SIZE) {
            for record in self.http.fetch_accounts_multiple(chunk).await? {
                self.cache_full(UserProfile::from_account_record(record));
            }
        }

        let mut results = Vec::with_capacity(account_ids.len());
        let mut seen = HashSet::new();
        for id in account_ids {
            if seen.insert(id.clone())
                && let Some(entry) = self.cache.get(id)
            {
                results.push(entry.profile.clone());
            }
        }
        Ok(results)
    }

    pub async fn search(
        &self,
        client_id: &AccountId,
        prefix: &str,
        platform: UserSearchPlatform,
    ) -> Result<Vec<UserSearchResult>, RustfnError> {
        self.http.search_users(client_id, prefix, platform).await
    }

    pub async fn fetch_avatars(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<UserAvatar>, RustfnError> {
        let mut all = Vec::with_capacity(account_ids.len());
        for chunk in account_ids.chunks(ACCOUNT_BATCH_CHUNK_SIZE) {
            let batch = self.http.fetch_avatars_multiple(chunk).await?;
            for avatar in &batch {
                if let Some(mut entry) = self.cache.get_mut(&avatar.account_id) {
                    entry.profile.avatar_id = avatar.avatar_id.clone();
                }
            }
            all.extend(batch);
        }
        Ok(all)
    }

    pub async fn fetch_global_profiles(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<UserGlobalProfile>, RustfnError> {
        let mut all = Vec::with_capacity(account_ids.len());
        for chunk in account_ids.chunks(ACCOUNT_BATCH_CHUNK_SIZE) {
            all.extend(self.http.fetch_global_profiles_multiple(chunk).await?);
        }
        Ok(all)
    }

    /// Removes expired and oldest entries to respect the cache limit.
    pub fn sweep_expired(&self) -> usize {
        let ttl = self.config.user_ttl;
        let before = self.cache.len();
        self.cache
            .retain(|_, entry| entry.fetched_at.elapsed() < ttl);

        if self.cache.len() > self.config.max_users {
            let mut entries: Vec<(AccountId, Instant)> = self
                .cache
                .iter()
                .map(|r| (r.key().clone(), r.value().fetched_at))
                .collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let to_remove = entries.len().saturating_sub(self.config.max_users);
            for (id, _) in entries.into_iter().take(to_remove) {
                self.cache.remove(&id);
            }
        }

        before.saturating_sub(self.cache.len())
    }

    fn cache_full(&self, mut profile: UserProfile) -> UserProfile {
        if let Some(existing) = self.cache.get(&profile.id)
            && profile.avatar_id.is_none()
        {
            profile.avatar_id = existing.profile.avatar_id.clone();
        }
        self.insert_cached(profile.clone(), false);
        profile
    }

    fn insert_cached(&self, profile: UserProfile, is_partial: bool) {
        let id = profile.id.clone();
        self.cache.insert(
            id,
            CachedUser {
                profile,
                fetched_at: Instant::now(),
                is_partial,
            },
        );
        // Hysteresis avoids an O(n log n) eviction sort on every insert at capacity.
        let slack = (self.config.max_users / 10).max(16);
        if self.cache.len() > self.config.max_users.saturating_add(slack) {
            let _ = self.sweep_expired();
        }
    }
}
