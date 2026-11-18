use reqwest::StatusCode;
use rustfn::commands::{Bot, CommandError, Context};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

const COSMETICS_ALL_URL: &str = "https://fortnite-api.com/v2/cosmetics";
const COSMETICS_BR_SEARCH_URL: &str = "https://fortnite-api.com/v2/cosmetics/br/search";

const BR_SEARCH_PARAM_KEYS: &[&str] = &[
    "language",
    "searchLanguage",
    "matchMethod",
    "id",
    "name",
    "description",
    "type",
    "displayType",
    "backendType",
    "rarity",
    "displayRarity",
    "backendRarity",
    "hasSeries",
    "series",
    "backendSeries",
    "hasSet",
    "set",
    "setText",
    "backendSet",
    "hasIntroduction",
    "backendIntroduction",
    "introductionChapter",
    "introductionSeason",
    "hasFeaturedImage",
    "hasVariants",
    "hasGameplayTags",
    "gameplayTag",
    "hasMetaTags",
    "metaTag",
    "hasDynamicPakId",
    "dynamicPakId",
    "added",
    "addedSince",
    "unseenFor",
    "lastAppearance",
];

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct AllCosmeticsData {
    #[serde(default)]
    br: Vec<BrCosmetic>,
}

#[derive(Debug, Clone, Deserialize)]
struct CosmeticType {
    value: String,
    #[serde(rename = "displayValue")]
    display_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BrCosmetic {
    id: String,
    name: String,
    description: Option<String>,
    #[serde(rename = "type")]
    cosmetic_type: Option<CosmeticType>,
    rarity: Option<CosmeticType>,
    introduction: Option<CosmeticIntroduction>,
}

#[derive(Debug, Clone, Deserialize)]
struct CosmeticIntroduction {
    chapter: Option<String>,
    season: Option<String>,
    text: Option<String>,
}

#[derive(Clone)]
struct CosmeticsApi {
    http: reqwest::Client,
    br_cache: Arc<RwLock<Option<Vec<BrCosmetic>>>>,
}

impl CosmeticsApi {
    fn new() -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?;
        Ok(Self {
            http,
            br_cache: Arc::new(RwLock::new(None)),
        })
    }

    async fn get_or_fetch_catalog(&self) -> Result<Vec<BrCosmetic>, String> {
        if let Some(cached) = self.br_cache.read().await.as_ref() {
            return Ok(cached.clone());
        }

        let resp = self
            .http
            .get(COSMETICS_ALL_URL)
            .send()
            .await
            .map_err(|e| format!("failed to fetch {COSMETICS_ALL_URL}: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!(
                "GET {COSMETICS_ALL_URL} returned status {}",
                resp.status()
            ));
        }

        let parsed: ApiResponse<AllCosmeticsData> = resp
            .json()
            .await
            .map_err(|e| format!("failed to parse {COSMETICS_ALL_URL}: {e}"))?;

        *self.br_cache.write().await = Some(parsed.data.br.clone());
        Ok(parsed.data.br)
    }

    async fn br_search(&self, params: &[(String, String)]) -> Result<Option<BrCosmetic>, String> {
        let resp = self
            .http
            .get(COSMETICS_BR_SEARCH_URL)
            .query(params)
            .send()
            .await
            .map_err(|e| format!("failed to query {COSMETICS_BR_SEARCH_URL}: {e}"))?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(format!(
                "GET {COSMETICS_BR_SEARCH_URL} returned status {}",
                resp.status()
            ));
        }

        let parsed: ApiResponse<BrCosmetic> = resp
            .json()
            .await
            .map_err(|e| format!("failed to parse {COSMETICS_BR_SEARCH_URL}: {e}"))?;

        Ok(Some(parsed.data))
    }

    async fn find_cosmetic(
        &self,
        expected_type: &str,
        raw_args: &str,
        parsed_args: &[String],
    ) -> Result<Option<BrCosmetic>, String> {
        if let Some(mut params) = parse_search_params(parsed_args) {
            if !params.iter().any(|(k, _)| k == "type") {
                params.push(("type".to_owned(), expected_type.to_owned()));
            }
            if !params.iter().any(|(k, _)| k == "language") {
                params.push(("language".to_owned(), "en".to_owned()));
            }
            if !params.iter().any(|(k, _)| k == "searchLanguage") {
                params.push(("searchLanguage".to_owned(), "en".to_owned()));
            }
            return self.br_search(&params).await;
        }

        let query = raw_args.trim();
        if query.is_empty() {
            return Ok(None);
        }

        if looks_like_cosmetic_id(query) {
            let params = vec![
                ("id".to_owned(), query.to_owned()),
                ("type".to_owned(), expected_type.to_owned()),
                ("matchMethod".to_owned(), "full".to_owned()),
                ("language".to_owned(), "en".to_owned()),
                ("searchLanguage".to_owned(), "en".to_owned()),
            ];
            if let Ok(Some(found)) = self.br_search(&params).await {
                return Ok(Some(found));
            }
        }

        for match_method in ["full", "contains"] {
            let params = vec![
                ("name".to_owned(), query.to_owned()),
                ("type".to_owned(), expected_type.to_owned()),
                ("matchMethod".to_owned(), match_method.to_owned()),
                ("language".to_owned(), "en".to_owned()),
                ("searchLanguage".to_owned(), "en".to_owned()),
            ];
            if let Ok(Some(found)) = self.br_search(&params).await {
                return Ok(Some(found));
            }
        }

        let catalog = self.get_or_fetch_catalog().await?;
        Ok(search_catalog(&catalog, expected_type, query))
    }
}

fn parse_search_params(args: &[String]) -> Option<Vec<(String, String)>> {
    if args.is_empty() {
        return None;
    }
    let mut params = Vec::with_capacity(args.len());
    for token in args {
        let (key, val) = token.split_once('=')?;
        let canonical = BR_SEARCH_PARAM_KEYS.iter().find(|&&k| k == key)?;
        if val.is_empty() {
            return None;
        }
        params.push(((*canonical).to_owned(), val.to_owned()));
    }
    Some(params)
}

fn looks_like_cosmetic_id(query: &str) -> bool {
    if query.contains(char::is_whitespace) {
        return false;
    }
    let upper = query.to_ascii_uppercase();
    upper.starts_with("CID_")
        || upper.starts_with("CHARACTER_")
        || upper.starts_with("EID_")
        || upper.starts_with("EMOTE_")
}

fn search_catalog(catalog: &[BrCosmetic], expected_type: &str, query: &str) -> Option<BrCosmetic> {
    let q_lower = query.to_ascii_lowercase();
    let mut starts_match: Option<&BrCosmetic> = None;
    let mut contains_match: Option<&BrCosmetic> = None;

    for item in catalog {
        let matches_type = item
            .cosmetic_type
            .as_ref()
            .is_some_and(|t| t.value.eq_ignore_ascii_case(expected_type));
        if !matches_type {
            continue;
        }

        if item.id.eq_ignore_ascii_case(query) || item.name.eq_ignore_ascii_case(query) {
            return Some(item.clone());
        }

        let name_lower = item.name.to_ascii_lowercase();
        if starts_match.is_none() && name_lower.starts_with(&q_lower) {
            starts_match = Some(item);
        } else if contains_match.is_none() && name_lower.contains(&q_lower) {
            contains_match = Some(item);
        }
    }

    starts_match.or(contains_match).cloned()
}

fn format_cosmetic_summary(item: &BrCosmetic) -> String {
    let rarity = item
        .rarity
        .as_ref()
        .and_then(|r| r.display_value.as_deref())
        .unwrap_or("Unknown");
    let type_label = item
        .cosmetic_type
        .as_ref()
        .and_then(|t| t.display_value.as_deref())
        .unwrap_or("Cosmetic");
    let intro = item
        .introduction
        .as_ref()
        .and_then(|i| {
            i.text.clone().or_else(|| match (&i.chapter, &i.season) {
                (Some(c), Some(s)) => Some(format!("C{c}S{s}")),
                _ => None,
            })
        })
        .unwrap_or_else(|| "Unknown season".to_owned());
    let desc = item.description.as_deref().unwrap_or("");
    if desc.is_empty() {
        format!("{} ({}) [{} | {} | {}]", item.name, item.id, type_label, rarity, intro)
    } else {
        format!(
            "{} ({}) [{} | {} | {}] - {}",
            item.name, item.id, type_label, rarity, intro, desc
        )
    }
}

async fn handle_skin_command(ctx: Context, api: Arc<CosmeticsApi>) -> Result<(), CommandError> {
    if ctx.raw_args.trim().is_empty() {
        return ctx
            .send("Usage: !skin <name or id> (or key=value search params)")
            .await;
    }

    let cosmetic = match api.find_cosmetic("outfit", &ctx.raw_args, &ctx.args).await {
        Ok(Some(item)) => item,
        Ok(None) => return ctx.send(format!("Skin not found: {}", ctx.raw_args)).await,
        Err(err) => return ctx.send(format!("Cosmetic lookup failed: {err}")).await,
    };

    let Some(party) = ctx.party().await else {
        return ctx.send("Bot is not currently in a party.").await;
    };

    party
        .me()
        .set_outfit(&cosmetic.id, Vec::new(), None)
        .await?;

    ctx.send(format!("Equipped skin: {}", format_cosmetic_summary(&cosmetic)))
        .await
}

async fn handle_emote_command(ctx: Context, api: Arc<CosmeticsApi>) -> Result<(), CommandError> {
    let query = ctx.raw_args.trim();
    if query.is_empty() {
        return ctx
            .send("Usage: !emote <name or id | clear> (or key=value search params)")
            .await;
    }

    let Some(party) = ctx.party().await else {
        return ctx.send("Bot is not currently in a party.").await;
    };

    if query.eq_ignore_ascii_case("clear") || query.eq_ignore_ascii_case("stop") {
        party.me().clear_emote().await?;
        return ctx.send("Cleared emote.").await;
    }

    let cosmetic = match api.find_cosmetic("emote", &ctx.raw_args, &ctx.args).await {
        Ok(Some(item)) => item,
        Ok(None) => return ctx.send(format!("Emote not found: {}", ctx.raw_args)).await,
        Err(err) => return ctx.send(format!("Cosmetic lookup failed: {err}")).await,
    };

    party.me().set_emote(&cosmetic.id, None).await?;

    ctx.send(format!("Playing emote: {}", format_cosmetic_summary(&cosmetic)))
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cosmetics_api = Arc::new(CosmeticsApi::new()?);
    let ready_api = Arc::clone(&cosmetics_api);
    let skin_api = Arc::clone(&cosmetics_api);
    let emote_api = Arc::clone(&cosmetics_api);

    Bot::new("!")
        .on_ready(move |bot| {
            let api = Arc::clone(&ready_api);
            async move {
                println!("Bot ready as {} ({})", bot.user.display_name, bot.user.id);
                tokio::spawn(async move {
                    if let Err(err) = api.get_or_fetch_catalog().await {
                        eprintln!("Warning: failed to warm cosmetics catalog: {err}");
                    }
                });
            }
        })
        .on_friend_request(|req| async move {
            if req.is_inbound() {
                req.accept().await?;
            }
            Ok(())
        })
        .on_message(|ctx| async move {
            println!("Message from {}: {}", ctx.author_name(), ctx.message.content);
            Ok(())
        })
        .command("hello", |ctx| async move { ctx.send("Hello!").await })
        .command("ping", |ctx| async move { ctx.send("Pong!").await })
        .command("skin", move |ctx| {
            let api = Arc::clone(&skin_api);
            async move { handle_skin_command(ctx, api).await }
        })
        .command("emote", move |ctx| {
            let api = Arc::clone(&emote_api);
            async move { handle_emote_command(ctx, api).await }
        })
        .run()
        .await?;

    Ok(())
}
