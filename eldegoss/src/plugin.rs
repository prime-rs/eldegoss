use std::{collections::HashMap, fs::read_dir, path::Path, sync::Arc};

use color_eyre::Result;
use extism::{Manifest, Plugin, Wasm};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::RwLock;
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../extensions/"]
struct Extensions;

#[derive(Clone)]
pub(crate) struct Plugins {
    pub(crate) plugins: Arc<RwLock<HashMap<String, Plugin>>>,
}

impl Plugins {
    pub(crate) fn init(path: &str) -> Result<Self> {
        let plugins = Self {
            plugins: Arc::new(RwLock::new(build_plugins(path)?)),
        };
        plugins_hot_reload(plugins.clone(), path.to_string())?;
        Ok(plugins)
    }
}

fn build_plugins(path: &str) -> Result<HashMap<String, Plugin>> {
    let dir = read_dir(path)?;
    let mut plugins = HashMap::new();
    dir.for_each(|entry| {
        if let Ok(entry) = entry {
            if entry.path().extension().unwrap_or_default().eq("wasm") {
                let wasm = Wasm::file(entry.path());
                let manifest = Manifest::new([wasm]);
                let plugin = Plugin::new(manifest, [], true)
                    .map_err(|err| {
                        error!("{err:?}");
                    })
                    .unwrap();
                plugins.insert(
                    entry
                        .path()
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                    plugin,
                );
            }
        }
    });
    Ok(plugins)
}

pub(crate) fn plugins_hot_reload(plugins: Plugins, path: String) -> Result<()> {
    let path_c = path.clone();
    let mut watcher = RecommendedWatcher::new(
        move |result: Result<Event, notify::Error>| {
            let event = result.unwrap();

            if event.kind.is_modify() {
                match build_plugins(&path_c) {
                    Ok(new_plugins) => {
                        println!("reloading config");
                        *plugins.plugins.write() = new_plugins;
                    }
                    Err(error) => error!("Error reloading config: {:?}", error),
                }
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(Path::new(&path), RecursiveMode::Recursive)?;
    Ok(())
}

#[tokio::test]
async fn test() {
    let plugins = Plugins::init("../plugins").unwrap();
    let mut inteval = tokio::time::interval(std::time::Duration::from_secs(1));
    for _ in 0..10 {
        inteval.tick().await;
        let mut binding = plugins.plugins.write();
        let plugin = binding.get_mut("dispatch").unwrap();
        let res = plugin.call::<&str, &str>("dispatch", "JLer").unwrap();
        println!("{}", res);
    }
}
