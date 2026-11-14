//! Pure board logic: tile state, playlist, layout, change detection.

use std::collections::{BTreeMap, HashMap};

use cmqt::{Layout, LayoutTile, Playlist, PlaylistEntry, TileMsg, TileState, Tree};

use crate::dashboard::{Screen, TileDef, TreeBoard};
use crate::format::{format_value, threshold_colour};

/// Latest values of `corbet:tile:value`, keyed by `(screen, tile)`.
#[derive(Debug, Default)]
pub struct Samples {
    map: HashMap<(String, String), f64>,
}

impl Samples {
    pub fn insert(&mut self, screen: &str, tile: &str, v: f64) {
        self.map.insert((screen.into(), tile.into()), v);
    }

    pub fn get(&self, screen: &str, tile: &str) -> Option<f64> {
        self.map
            .get(&(screen.to_string(), tile.to_string()))
            .copied()
    }
}

/// One firing alert with all its labels (`alertname` included).
#[derive(Debug, Default, Clone)]
pub struct Alert {
    pub labels: BTreeMap<String, String>,
}

impl Alert {
    pub fn new(name: &str, labels: &[(&str, &str)]) -> Alert {
        let mut l: BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        l.insert("alertname".into(), name.into());
        Alert { labels: l }
    }
}

/// Firing alerts.
#[derive(Debug, Default)]
pub struct Firing {
    pub alerts: Vec<Alert>,
}

impl Firing {
    /// `alertname` in `names` AND every scope label matches (a scope value
    /// matches when the alert's label equals any of the listed values).
    pub fn matches(&self, names: &[String], scope: &BTreeMap<String, Vec<String>>) -> bool {
        self.alerts.iter().any(|a| {
            a.labels.get("alertname").is_some_and(|n| names.contains(n))
                && scope
                    .iter()
                    .all(|(k, vals)| a.labels.get(k).is_some_and(|v| vals.contains(v)))
        })
    }

    /// Any firing alert whose `host_name` is `device` (topology nodes).
    pub fn device_has_alert(&self, device: &str) -> bool {
        self.alerts
            .iter()
            .any(|a| a.labels.get("host_name").is_some_and(|h| h == device))
    }
}

fn state_colour(s: TileState) -> &'static str {
    match s {
        TileState::Ok => "green",
        TileState::Issue => "red",
        TileState::Unknown | TileState::Greyed => "grey",
    }
}

/// The state word shown on the public tree (and on topology nodes).
pub fn state_word(s: TileState) -> &'static str {
    match s {
        TileState::Ok => "OK",
        TileState::Issue => "Issue",
        TileState::Unknown | TileState::Greyed => "No data",
    }
}

fn state_only(s: TileState, now: u64) -> TileMsg {
    TileMsg {
        v: None,
        d: state_word(s).into(),
        c: state_colour(s).into(),
        s,
        t: now,
    }
}

/// Compute one tile. `samples`/`firing` are `None` when VictoriaMetrics could
/// not be queried.
///
/// * Device tiles: `issue` when a scoped alert fires, `ok` with a value,
///   otherwise `unknown`.
/// * Aggregate tiles (site, overview, topology): the state comes from the
///   alert match alone; the recorded value is shown on the internal tree only.
/// * Public tree: state only (`v` null, `d` = OK / Issue / No data).
pub fn compute_tile(
    tree: Tree,
    screen: &Screen,
    tile: &TileDef,
    samples: Option<&Samples>,
    firing: Option<&Firing>,
    now: u64,
) -> TileMsg {
    if screen.greyed {
        return if tree == Tree::Public {
            state_only(TileState::Greyed, now)
        } else {
            TileMsg {
                v: None,
                d: "n/a".into(),
                c: "grey".into(),
                s: TileState::Greyed,
                t: now,
            }
        };
    }
    let value = samples
        .and_then(|s| s.get(&tile.record.0, &tile.record.1))
        .filter(|v| v.is_finite());
    let issue = firing.is_some_and(|f| f.matches(&tile.alerts, &tile.scope));
    let aggregate = screen.kind != "device";
    let state = if issue {
        TileState::Issue
    } else if aggregate {
        if firing.is_some() {
            TileState::Ok
        } else {
            TileState::Unknown
        }
    } else if value.is_some() {
        TileState::Ok
    } else {
        TileState::Unknown
    };
    if tree == Tree::Public {
        return state_only(state, now);
    }
    match value {
        Some(v) => TileMsg {
            v: Some(v),
            d: format_value(v, tile.unit.as_deref(), tile.decimals),
            c: threshold_colour(v, &tile.steps),
            s: state,
            t: now,
        },
        None => TileMsg {
            v: None,
            d: "n/a".into(),
            c: state_colour(state).into(),
            s: state,
            t: now,
        },
    }
}

/// One tile per topology node (`topology/<device>`): state from the firing
/// alerts of that device; greyed nodes are `greyed`, planned nodes get none.
pub fn topology_tiles(
    topology: &serde_json::Value,
    firing: Option<&Firing>,
    now: u64,
) -> Vec<(String, TileMsg)> {
    let Some(nodes) = topology.get("nodes").and_then(serde_json::Value::as_array) else {
        return vec![];
    };
    nodes
        .iter()
        .filter_map(|n| {
            let device = n.get("device")?.as_str()?;
            let flag = |k: &str| n.get(k).and_then(serde_json::Value::as_bool) == Some(true);
            if flag("planned") {
                return None;
            }
            let state = if flag("greyed") {
                TileState::Greyed
            } else {
                match firing {
                    None => TileState::Unknown,
                    Some(f) if f.device_has_alert(device) => TileState::Issue,
                    Some(_) => TileState::Ok,
                }
            };
            Some((device.to_string(), state_only(state, now)))
        })
        .collect()
}

/// Does the screen have an issue, given the computed tiles?
pub fn screen_has_issue(tiles: &[TileMsg]) -> bool {
    tiles.iter().any(|t| t.s == TileState::Issue)
}

/// Playlist: screens with issues first, otherwise in dashboard order.
pub fn playlist(interval_s: u64, screens: &[(String, bool)]) -> Playlist {
    let mut entries: Vec<PlaylistEntry> = screens
        .iter()
        .map(|(name, issue)| PlaylistEntry {
            name: name.clone(),
            issue: *issue,
        })
        .collect();
    entries.sort_by_key(|e| !e.issue); // stable
    Playlist {
        interval_s,
        screens: entries,
    }
}

/// `_layout` for a screen. Links to screens that do not exist in this tree
/// are dropped, which keeps public layouts free of internal screen names.
pub fn layout(generation: &str, screen: &Screen, tree: &TreeBoard) -> Layout {
    let exists = |n: &str| tree.screens.iter().any(|s| s.name == n);
    let tiles = screen
        .tiles
        .iter()
        .map(|t| LayoutTile {
            id: t.id.clone(),
            title: t.title.clone(),
            grid: t.grid,
            unit: t.unit.clone(),
            link: t.link.clone().filter(|l| exists(l)),
        })
        .collect();
    Layout {
        schema: cmqt::LAYOUT_SCHEMA.into(),
        generation: generation.into(),
        screen: screen.name.clone(),
        title: screen.title.clone(),
        kind: screen.kind.clone(),
        greyed: screen.greyed,
        tiles,
        topology: screen.topology.as_ref().map(|t| scrub_topology(t, tree)),
    }
}

/// Keep only topology nodes/edges/bracket members whose device has a screen in
/// this tree.
fn scrub_topology(t: &serde_json::Value, tree: &TreeBoard) -> serde_json::Value {
    use serde_json::Value;
    let exists = |n: &str| tree.screens.iter().any(|s| s.name == n);
    let mut out = t.clone();
    let keep_node = |v: &Value| v.get("device").and_then(Value::as_str).is_some_and(exists);
    if let Some(a) = out.get_mut("nodes").and_then(Value::as_array_mut) {
        a.retain(keep_node);
    }
    if let Some(a) = out.get_mut("edges").and_then(Value::as_array_mut) {
        a.retain(|e| {
            ["from", "to"]
                .iter()
                .all(|k| e.get(*k).and_then(Value::as_str).is_some_and(exists))
        });
    }
    if let Some(a) = out.get_mut("brackets").and_then(Value::as_array_mut) {
        for b in a.iter_mut() {
            if let Some(m) = b.get_mut("members").and_then(Value::as_array_mut) {
                m.retain(|x| x.as_str().is_some_and(exists));
            }
        }
    }
    out
}

/// Publish decisions. Tracks the last published payload per topic.
#[derive(Debug, Default)]
pub struct Tracker {
    last: HashMap<String, (String, u64)>,
}

impl Tracker {
    /// Forget everything: the next evaluation republishes all topics.
    pub fn clear(&mut self) {
        self.last.clear();
    }

    /// Publish when `fingerprint` differs from the last published one, or the
    /// last publish is `refresh_s` old (so retained messages never expire
    /// while the board is quiet).
    pub fn should_publish(
        &mut self,
        topic: &str,
        fingerprint: &str,
        now: u64,
        refresh_s: u64,
    ) -> bool {
        let publish = match self.last.get(topic) {
            None => true,
            Some((f, at)) => f != fingerprint || now.saturating_sub(*at) >= refresh_s,
        };
        if publish {
            self.last.insert(topic.into(), (fingerprint.into(), now));
        }
        publish
    }
}

/// Fingerprint of the part of a tile that triggers a publish: `d`, `c`, `s`.
pub fn tile_fingerprint(m: &TileMsg) -> String {
    format!("{}\u{1}{}\u{1}{:?}", m.d, m.c, m.s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::Step;
    use cmqt::Grid;
    use serde_json::json;

    fn tile(id: &str, alerts: &[&str], scope: &[(&str, &[&str])], link: Option<&str>) -> TileDef {
        TileDef {
            id: id.into(),
            title: id.into(),
            grid: Grid {
                x: 0,
                y: 0,
                w: 8,
                h: 4,
            },
            unit: Some("percentunit".into()),
            decimals: Some(0),
            steps: vec![
                Step {
                    color: "green".into(),
                    value: None,
                },
                Step {
                    color: "red".into(),
                    value: Some(0.9),
                },
            ],
            alerts: alerts.iter().map(|s| s.to_string()).collect(),
            link: link.map(Into::into),
            scope: scope
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(|x| x.to_string()).collect()))
                .collect(),
            record: ("".into(), id.into()),
        }
    }

    fn screen(name: &str, kind: &str, greyed: bool, mut tiles: Vec<TileDef>) -> Screen {
        for t in &mut tiles {
            t.record.0 = name.into();
        }
        Screen {
            name: name.into(),
            title: name.into(),
            kind: kind.into(),
            greyed,
            tiles,
            topology: None,
        }
    }

    fn samples(screen: &str, tile: &str, v: f64) -> Samples {
        let mut s = Samples::default();
        s.insert(screen, tile, v);
        s
    }

    fn firing(a: &[Alert]) -> Firing {
        Firing { alerts: a.to_vec() }
    }

    fn calc(tree: Tree, sc: &Screen, sm: Option<&Samples>, f: Option<&Firing>) -> TileMsg {
        compute_tile(tree, sc, &sc.tiles[0], sm, f, 100)
    }

    #[test]
    fn ok_value_with_colour() {
        let sc = screen(
            "srv",
            "device",
            false,
            vec![tile(
                "ram",
                &["DeviceMemoryLow"],
                &[("host_name", &["srv"])],
                None,
            )],
        );
        let f = Firing::default();
        let m = calc(
            Tree::Internal,
            &sc,
            Some(&samples("srv", "ram", 0.63)),
            Some(&f),
        );
        assert_eq!(
            (m.d.as_str(), m.c.as_str(), m.s),
            ("63 %", "green", TileState::Ok)
        );
        let m = calc(
            Tree::Internal,
            &sc,
            Some(&samples("srv", "ram", 0.95)),
            Some(&f),
        );
        assert_eq!(m.c, "red");
    }

    #[test]
    fn issue_needs_alertname_and_scope() {
        let sc = screen(
            "srv",
            "device",
            false,
            vec![tile(
                "ram",
                &["DeviceMemoryLow"],
                &[("host_name", &["srv"])],
                None,
            )],
        );
        let sm = samples("srv", "ram", 0.5);
        let go = |a: Alert| calc(Tree::Internal, &sc, Some(&sm), Some(&firing(&[a]))).s;
        assert_eq!(
            go(Alert::new("DeviceMemoryLow", &[("host_name", "srv")])),
            TileState::Issue
        );
        assert_eq!(
            go(Alert::new("DeviceMemoryLow", &[("host_name", "fra-0")])),
            TileState::Ok
        );
        assert_eq!(
            go(Alert::new("DeviceDiskFull", &[("host_name", "srv")])),
            TileState::Ok
        );
    }

    #[test]
    fn scope_lists_mean_any_of_and_all_labels_must_match() {
        let sc = screen(
            "overview",
            "overview",
            false,
            vec![tile(
                "fra",
                &["DeviceDown"],
                &[("site", &["0x20"]), ("host_name", &["fra-0", "fra-1"])],
                None,
            )],
        );
        let go = |a: Alert| calc(Tree::Internal, &sc, None, Some(&firing(&[a]))).s;
        assert_eq!(
            go(Alert::new(
                "DeviceDown",
                &[("site", "0x20"), ("host_name", "fra-1")]
            )),
            TileState::Issue
        );
        assert_eq!(
            go(Alert::new(
                "DeviceDown",
                &[("site", "0x20"), ("host_name", "fra-9")]
            )),
            TileState::Ok
        );
        assert_eq!(
            go(Alert::new(
                "DeviceDown",
                &[("site", "0x21"), ("host_name", "fra-0")]
            )),
            TileState::Ok
        );
        assert_eq!(
            go(Alert::new("DeviceDown", &[("host_name", "fra-0")])),
            TileState::Ok,
            "missing site label"
        );
    }

    #[test]
    fn aggregate_state_ignores_the_value() {
        let sc = screen(
            "overview",
            "overview",
            false,
            vec![tile("a", &["DeviceDown"], &[("site", &["0x20"])], None)],
        );
        // no recorded value at all: still OK, because the alert match decides
        let m = calc(
            Tree::Internal,
            &sc,
            Some(&Samples::default()),
            Some(&Firing::default()),
        );
        assert_eq!((m.s, m.v, m.d.as_str()), (TileState::Ok, None, "n/a"));
        // VictoriaMetrics down: unknown
        assert_eq!(calc(Tree::Internal, &sc, None, None).s, TileState::Unknown);
    }

    #[test]
    fn public_tree_is_state_only() {
        let sc = screen(
            "srv",
            "device",
            false,
            vec![tile(
                "ram",
                &["DeviceMemoryLow"],
                &[("host_name", &["srv"])],
                None,
            )],
        );
        let sm = samples("srv", "ram", 0.95);
        let ok = calc(Tree::Public, &sc, Some(&sm), Some(&Firing::default()));
        assert_eq!(
            (ok.v, ok.d.as_str(), ok.c.as_str(), ok.s),
            (None, "OK", "green", TileState::Ok)
        );
        let f = firing(&[Alert::new("DeviceMemoryLow", &[("host_name", "srv")])]);
        let bad = calc(Tree::Public, &sc, Some(&sm), Some(&f));
        assert_eq!(
            (bad.v, bad.d.as_str(), bad.c.as_str(), bad.s),
            (None, "Issue", "red", TileState::Issue)
        );
        let none = calc(
            Tree::Public,
            &sc,
            Some(&Samples::default()),
            Some(&Firing::default()),
        );
        assert_eq!((none.d.as_str(), none.s), ("No data", TileState::Unknown));
    }

    #[test]
    fn public_panel_reads_the_internal_series() {
        let mut t = tile("ram", &[], &[], None);
        t.record = ("srv".into(), "ram".into());
        let sc = Screen {
            name: "other".into(),
            title: "".into(),
            kind: "device".into(),
            greyed: false,
            tiles: vec![t],
            topology: None,
        };
        let m = calc(
            Tree::Public,
            &sc,
            Some(&samples("srv", "ram", 0.1)),
            Some(&Firing::default()),
        );
        assert_eq!(m.s, TileState::Ok);
    }

    #[test]
    fn unknown_without_value_or_without_victoria() {
        let sc = screen("srv", "device", false, vec![tile("ram", &[], &[], None)]);
        let m = calc(
            Tree::Internal,
            &sc,
            Some(&Samples::default()),
            Some(&Firing::default()),
        );
        assert_eq!((m.s, m.v, m.c.as_str()), (TileState::Unknown, None, "grey"));
        assert_eq!(calc(Tree::Internal, &sc, None, None).s, TileState::Unknown);
        let nan = samples("srv", "ram", f64::NAN);
        assert_eq!(
            calc(Tree::Internal, &sc, Some(&nan), Some(&Firing::default())).s,
            TileState::Unknown
        );
    }

    #[test]
    fn greyed_screen_ignores_everything() {
        let sc = screen(
            "laptop",
            "device",
            true,
            vec![tile(
                "ram",
                &["DeviceMemoryLow"],
                &[("host_name", &["laptop"])],
                None,
            )],
        );
        let f = firing(&[Alert::new("DeviceMemoryLow", &[("host_name", "laptop")])]);
        let m = calc(
            Tree::Internal,
            &sc,
            Some(&samples("laptop", "ram", 0.5)),
            Some(&f),
        );
        assert_eq!((m.s, m.v), (TileState::Greyed, None));
        assert_eq!(calc(Tree::Public, &sc, None, None).d, "No data");
    }

    #[test]
    fn topology_node_tiles() {
        let topo = json!({"nodes":[
            {"device":"srv"},{"device":"fra-0"},{"device":"fra-3","planned":true},{"device":"office","greyed":true}]});
        let f = firing(&[Alert::new("DeviceDown", &[("host_name", "fra-0")])]);
        let t = topology_tiles(&topo, Some(&f), 5);
        let get = |d: &str| t.iter().find(|(x, _)| x == d).map(|(_, m)| m.clone());
        assert_eq!(get("srv").unwrap().d, "OK");
        assert_eq!(get("fra-0").unwrap().s, TileState::Issue);
        assert!(get("fra-3").is_none(), "planned nodes get no tile");
        assert_eq!(get("office").unwrap().s, TileState::Greyed);
        let none = topology_tiles(&topo, None, 5);
        assert_eq!(
            none.iter().find(|(x, _)| x == "srv").unwrap().1.s,
            TileState::Unknown
        );
    }

    #[test]
    fn playlist_puts_issues_first_keeping_order() {
        let p = playlist(
            30,
            &[
                ("a".into(), false),
                ("b".into(), true),
                ("c".into(), false),
                ("d".into(), true),
            ],
        );
        let names: Vec<_> = p.screens.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["b", "d", "a", "c"]);
        assert!(p.screens[0].issue && !p.screens[2].issue);
    }

    #[test]
    fn tracker_publishes_on_change_and_refresh_only() {
        let mut t = Tracker::default();
        assert!(t.should_publish("a", "x", 100, 60));
        assert!(!t.should_publish("a", "x", 110, 60), "unchanged");
        assert!(t.should_publish("a", "y", 111, 60), "changed");
        assert!(!t.should_publish("a", "y", 150, 60));
        assert!(
            t.should_publish("a", "y", 171, 60),
            "refresh keeps the message alive"
        );
        t.clear();
        assert!(
            t.should_publish("a", "y", 172, 60),
            "after reconnect everything goes out"
        );
    }

    #[test]
    fn fingerprint_ignores_value_and_time() {
        let a = TileMsg {
            v: Some(0.5),
            d: "50 %".into(),
            c: "green".into(),
            s: TileState::Ok,
            t: 1,
        };
        let b = TileMsg {
            v: Some(0.501),
            t: 9,
            ..a.clone()
        };
        assert_eq!(tile_fingerprint(&a), tile_fingerprint(&b));
        let c = TileMsg {
            s: TileState::Issue,
            ..a.clone()
        };
        assert_ne!(tile_fingerprint(&a), tile_fingerprint(&c));
    }

    #[test]
    fn public_layout_drops_links_to_missing_screens() {
        let a = screen(
            "overview",
            "overview",
            false,
            vec![
                tile("x", &[], &[], Some("srv")),
                tile("y", &[], &[], Some("secret")),
            ],
        );
        let tree = TreeBoard {
            screens: vec![a.clone(), screen("srv", "device", false, vec![])],
        };
        let l = layout("g", &a, &tree);
        assert_eq!(l.tiles[0].link.as_deref(), Some("srv"));
        assert_eq!(l.tiles[1].link, None);
        assert_eq!(l.schema, "corbet.obs.layout/1");
    }

    #[test]
    fn topology_is_scrubbed_to_known_devices() {
        let mut topo = screen("topology", "topology", false, vec![]);
        topo.topology = Some(json!({
            "nodes":[{"device":"srv"},{"device":"secret"}],
            "edges":[{"from":"srv","to":"secret","kind":"x"},{"from":"srv","to":"srv","kind":"y"}],
            "brackets":[{"name":"b","members":["srv","secret"]}]
        }));
        let tree = TreeBoard {
            screens: vec![topo.clone(), screen("srv", "device", false, vec![])],
        };
        let l = layout("g", &topo, &tree);
        let t = l.topology.unwrap();
        assert_eq!(t["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(t["edges"].as_array().unwrap().len(), 1);
        assert_eq!(t["brackets"][0]["members"], json!(["srv"]));
    }
}
