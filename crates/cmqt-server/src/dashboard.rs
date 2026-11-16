//! Dashboards (cgrf output, `corbet.obs.dashboard/1`) and manifest loading.

use std::collections::BTreeMap;
use std::path::Path;

use cmqt::{Grid, Tree};
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
pub struct Step {
    pub color: String,
    #[serde(default)]
    pub value: Option<f64>,
}

#[derive(Deserialize)]
struct RawManifest {
    generation: String,
    #[serde(default)]
    screens: Vec<RawManifestScreen>,
}

#[derive(Deserialize)]
struct RawManifestScreen {
    name: String,
    door: String,
    #[serde(default)]
    file: Option<String>,
}

#[derive(Deserialize)]
struct RawDashboard {
    uid: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    panels: Vec<RawPanel>,
    corbet: RawCorbet,
}

#[derive(Deserialize)]
struct RawCorbet {
    generation: String,
    screen: String,
    door: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    greyed: bool,
    #[serde(default)]
    topology: Option<Value>,
}

#[derive(Deserialize)]
struct RawPanel {
    #[serde(default)]
    title: String,
    #[serde(default, rename = "gridPos")]
    grid_pos: Option<RawGrid>,
    #[serde(default, rename = "fieldConfig")]
    field_config: Option<RawFieldConfig>,
    #[serde(default)]
    corbet: Option<RawPanelCorbet>,
}

#[derive(Deserialize)]
struct RawGrid {
    x: u32,
    y: u32,
    w: u32,
    h: u32,
}

#[derive(Deserialize)]
struct RawFieldConfig {
    #[serde(default)]
    defaults: RawDefaults,
}

#[derive(Default, Deserialize)]
struct RawDefaults {
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    decimals: Option<u32>,
    #[serde(default)]
    thresholds: Option<RawThresholds>,
}

#[derive(Deserialize)]
struct RawThresholds {
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct RawPanelCorbet {
    #[serde(default)]
    tile: Option<String>,
    #[serde(default)]
    alerts: Vec<String>,
    #[serde(default)]
    link: Option<String>,
    #[serde(default)]
    topology: Option<Value>,
    #[serde(default)]
    record: Option<RawRecord>,
    #[serde(default)]
    scope: BTreeMap<String, OneOrMany>,
}

/// A scope label value: one string, or a list meaning "any of".
#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
struct RawRecord {
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

/// One tile of a screen.
#[derive(Clone, Debug)]
pub struct TileDef {
    pub id: String,
    pub title: String,
    pub grid: Grid,
    pub unit: Option<String>,
    pub decimals: Option<u32>,
    pub steps: Vec<Step>,
    pub alerts: Vec<String>,
    pub link: Option<String>,
    /// Labels the firing alert must carry (every key; a value matches if it is
    /// any of the listed ones).
    pub scope: BTreeMap<String, Vec<String>>,
    /// The `corbet:tile:value` series this tile reads: `(screen, tile)`.
    /// Public panels carry the labels of the internal panel they mirror.
    pub record: (String, String),
}

#[derive(Clone, Debug)]
pub struct Screen {
    pub name: String,
    pub title: String,
    pub kind: String,
    pub greyed: bool,
    pub tiles: Vec<TileDef>,
    pub topology: Option<Value>,
}

/// All screens of one tree.
#[derive(Clone, Debug, Default)]
pub struct TreeBoard {
    pub screens: Vec<Screen>,
}

#[derive(Clone, Debug)]
pub struct Board {
    pub generation: String,
    pub public: TreeBoard,
    pub internal: TreeBoard,
}

impl Board {
    pub fn tree(&self, t: Tree) -> &TreeBoard {
        match t {
            Tree::Public => &self.public,
            Tree::Internal => &self.internal,
        }
    }
}

/// Read only the generation of `dir/manifest.json`.
pub fn read_generation(manifest: &Path) -> Result<String, String> {
    let text =
        std::fs::read_to_string(manifest).map_err(|e| format!("{}: {e}", manifest.display()))?;
    let m: Value =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", manifest.display()))?;
    m.get("generation")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{}: no generation", manifest.display()))
}

/// Load the manifest and every dashboard it lists. All-or-nothing: a manifest
/// whose files disagree on the generation is rejected (the caller keeps the
/// previous board, contract §6).
pub fn load(dir: &Path) -> Result<Board, String> {
    let mpath = dir.join("manifest.json");
    let text = std::fs::read_to_string(&mpath).map_err(|e| format!("{}: {e}", mpath.display()))?;
    let manifest: RawManifest =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", mpath.display()))?;
    let mut board = Board {
        generation: manifest.generation.clone(),
        public: TreeBoard::default(),
        internal: TreeBoard::default(),
    };
    for entry in &manifest.screens {
        let tree = match entry.door.as_str() {
            "public" => Tree::Public,
            "internal" => Tree::Internal,
            other => return Err(format!("screen {}: unknown door {other}", entry.name)),
        };
        let rel = entry
            .file
            .clone()
            .unwrap_or_else(|| format!("{}/{}.json", entry.door, entry.name));
        let path = dir.join(&rel);
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let raw: RawDashboard =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        if raw.corbet.generation != manifest.generation {
            return Err(format!(
                "{}: generation {} differs from manifest {}",
                path.display(),
                raw.corbet.generation,
                manifest.generation
            ));
        }
        if raw.corbet.door != entry.door || raw.corbet.screen != entry.name || raw.uid != entry.name
        {
            return Err(format!(
                "{}: door/screen/uid disagree with the manifest",
                path.display()
            ));
        }
        let screen = convert(raw);
        match tree {
            Tree::Public => board.public.screens.push(screen),
            Tree::Internal => board.internal.screens.push(screen),
        }
    }
    Ok(board)
}

fn convert(raw: RawDashboard) -> Screen {
    let mut topology = raw.corbet.topology;
    let mut tiles = Vec::new();
    let screen_name = raw.corbet.screen.clone();
    for p in raw.panels {
        let Some(c) = p.corbet else { continue };
        if topology.is_none() {
            topology = c.topology.clone();
        }
        let Some(id) = c.tile else { continue };
        let g = p.grid_pos.unwrap_or(RawGrid {
            x: 0,
            y: 0,
            w: 24,
            h: 4,
        });
        let d = p.field_config.map(|f| f.defaults).unwrap_or_default();
        let label = |k: &str, dflt: &str| {
            c.record
                .as_ref()
                .and_then(|r| r.labels.get(k).cloned())
                .unwrap_or_else(|| dflt.to_string())
        };
        let record = (label("screen", &screen_name), label("tile", &id));
        let mut scope: BTreeMap<String, Vec<String>> = c
            .scope
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    match v {
                        OneOrMany::One(x) => vec![x],
                        OneOrMany::Many(x) => x,
                    },
                )
            })
            .collect();
        if scope.is_empty() && raw.corbet.kind == "device" {
            scope.insert("host_name".into(), vec![screen_name.clone()]);
        }
        tiles.push(TileDef {
            id,
            title: p.title,
            grid: Grid {
                x: g.x,
                y: g.y,
                w: g.w,
                h: g.h,
            },
            unit: d.unit,
            decimals: d.decimals,
            steps: d.thresholds.map(|t| t.steps).unwrap_or_default(),
            alerts: c.alerts,
            link: c.link,
            scope,
            record,
        });
    }
    Screen {
        name: raw.corbet.screen,
        title: raw.title,
        kind: raw.corbet.kind,
        greyed: raw.corbet.greyed,
        tiles,
        topology,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cgrf")
    }

    #[test]
    fn loads_real_cgrf_board() {
        let b = load(&fixtures()).unwrap();
        assert_eq!(b.public.screens.len(), 23);
        assert_eq!(b.internal.screens.len(), 26);
        let srv = b.internal.screens.iter().find(|s| s.name == "srv").unwrap();
        // alive, jobs (srv's heartbeats), cpu, ram, disk, io, net
        assert_eq!(srv.tiles.len(), 7);
        assert_eq!(srv.tiles[0].scope["host_name"], ["srv"]);
        let ov = b
            .public
            .screens
            .iter()
            .find(|s| s.name == "overview")
            .unwrap();
        let fra = ov.tiles.iter().find(|t| t.id == "fra_brh").unwrap();
        assert!(!fra.scope.contains_key("site"), "scoped by host_name only");
        assert_eq!(fra.scope["host_name"].len(), 5, "list scope");
        assert_eq!(fra.alerts, ["*"]);
        let topo = b
            .internal
            .screens
            .iter()
            .find(|s| s.name == "topology")
            .unwrap();
        assert!(topo.topology.is_some());
        assert!(
            b.internal
                .screens
                .iter()
                .any(|s| s.name == "elitebook" && s.greyed)
        );
    }

    #[test]
    fn rejects_mixed_generations() {
        let d = std::env::temp_dir().join(format!("cmqt-gen-{}", std::process::id()));
        for sub in ["internal", "public"] {
            std::fs::create_dir_all(d.join(sub)).unwrap();
        }
        let dash = std::fs::read_to_string(fixtures().join("internal/srv.json")).unwrap();
        std::fs::write(d.join("internal/srv.json"), dash).unwrap();
        let man = r#"{"generation":"other","screens":[{"name":"srv","door":"internal","file":"internal/srv.json"}]}"#;
        std::fs::write(d.join("manifest.json"), man).unwrap();
        let e = load(&d).unwrap_err();
        assert!(e.contains("differs from manifest"), "{e}");
        std::fs::remove_file(d.join("internal/srv.json")).unwrap();
        std::fs::remove_file(d.join("manifest.json")).unwrap();
        std::fs::remove_dir(d.join("internal")).unwrap();
        std::fs::remove_dir(d.join("public")).unwrap();
        std::fs::remove_dir(&d).unwrap();
    }
}
