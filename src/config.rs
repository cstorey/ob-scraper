use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::Environment;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MainConfig {
    pub client_credentials: PathBuf,
    pub environment: Environment,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderConfig {
    pub user_token: PathBuf,
    pub target_dir: PathBuf,
    #[serde(default)]
    pub scrape_accounts: bool,
    #[serde(default)]
    pub scrape_cards: bool,
    #[serde(default)]
    pub scrape_info: bool,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScraperConfig {
    pub main: MainConfig,
    pub providers: HashMap<String, ProviderConfig>,
}