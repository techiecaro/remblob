use std::{collections::HashMap, sync::RwLock};

use once_cell::sync::Lazy;
use url::Url;

use super::local::LocalFileStorageProvider;

pub trait FileStorage: std::io::Read + std::io::Write {}

pub trait StorageProvider: Send + Sync {
    fn scheme(&self) -> String;
    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>>;
    fn list_paths(&self, url: Url) -> Vec<String>;
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

    let reg_info = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    reg_info.create(url)
}
