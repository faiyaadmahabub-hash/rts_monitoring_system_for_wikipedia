//! Data model structs: MinEdit (ingestion), WikiEdit<'a> (zero-copy hot path), WikiEditOwned (benchmarks).

use serde::Deserialize;

// Parsed at ingestion time; String fields are allocated once before the queue.
#[derive(Deserialize)]
pub struct MinEdit {
    #[serde(rename = "type")]
    pub event_type:  Option<String>,
    pub bot:         Option<bool>,
    pub title:       Option<String>,
    pub server_name: Option<String>,
    pub user:        Option<String>,
}

impl MinEdit {
    pub fn is_edit(&self) -> bool {
        self.event_type.as_deref() == Some("edit")
    }

    pub fn is_human(&self) -> bool {
        !self.bot.unwrap_or(false)
    }
}

// Zero-copy: #[serde(borrow)] produces &'a str slices from the raw buffer — no heap alloc.
#[derive(Debug, Deserialize)]
pub struct WikiEdit<'a> {
    #[serde(borrow)] pub user:        Option<&'a str>,
    pub bot:                          Option<bool>,
    #[serde(borrow)] pub server_name: Option<&'a str>,
    #[serde(borrow)] pub title:       Option<&'a str>,
}

// Owned equivalent used only in benchmarks to measure heap allocation cost.
#[derive(Deserialize)]
pub struct WikiEditOwned {
    pub user:        Option<String>,
    pub bot:         Option<bool>,
    pub server_name: Option<String>,
    pub title:       Option<String>,
}
