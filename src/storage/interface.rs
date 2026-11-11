use std::{collections::HashMap, sync::RwLock};

use once_cell::sync::Lazy;
use std::path::{Path, PathBuf};
use url::{ParseError, Url};

use super::local::LocalFileStorageProvider;

pub trait FileStorage: std::io::Read + std::io::Write {}

pub trait StorageProvider: Send + Sync {
    fn scheme(&self) -> String;
    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>>;
    fn list_paths(&self, input: &str) -> Vec<String>;
}

static STORAGE_REGISTRY: Lazy<RwLock<HashMap<String, Box<dyn StorageProvider>>>> =
    Lazy::new(|| {
        let mut registry = HashMap::new();

        let providers: Vec<Box<dyn StorageProvider>> = vec![Box::new(LocalFileStorageProvider)];

        for provider in providers {
            registry.insert(provider.scheme(), provider);
        }

        RwLock::new(registry)
    });

pub fn get_file_storage(url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    let scheme = url.scheme();

    let provider = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    provider.create(url)
}

pub fn list_schemas() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    Ok(registry.keys().map(|x| x.to_string()).collect())
}

pub fn list_paths_for_url(input: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let url = str_to_url(input).map_err(|_| "Invalid path")?;
    let scheme = url.scheme();

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    let provider = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    Ok(provider.list_paths(input))
}

pub fn str_to_url(input: &str) -> Result<Url, String> {
    match Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let absolute_path = if Path::new(input).is_absolute() {
                PathBuf::from(input)
            } else {
                std::env::current_dir()
                    .map_err(|e| format!("Can't get current directory: {}", e))?
                    .join(input)
            };
            Url::from_file_path(absolute_path).map_err(|_| "Failed to create file URL".to_string())
        }
        Err(other) => Err(other.to_string()),
    }
}
