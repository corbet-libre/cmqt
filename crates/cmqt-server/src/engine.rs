//! Pure board logic: tile state, playlist, layout, change detection.

use std::collections::HashMap;

use cmqt::{Layout, LayoutTile, Playlist, PlaylistEntry, TileMsg, TileState, Tree};

use crate::dashboard::{Screen, TileDef, TreeBoard};
use crate::format::{format_value, threshold_colour};

/// Latest values of `corbet:tile:value`, keyed by `(door label, screen, tile)`.
/// The door label is empty when cprm sets none.
#[derive(Debug, Default)]
pub struct Samples {
    map: HashMap<(String, String, String), f64>,
}

impl Samples {
    pub fn insert(&mut self, door: &str, screen: &str, tile: &str, v: f64) {
        self.map
            .insert((door.into(), screen.into(), tile.into()), v);
    }

    /// Look up a tile: the series labelled with this tree's door first, then
    /// the one without a `door` label. See README (CONTRACT QUESTION 3).
    pub fn get(&self, tree: Tree, screen: &str, tile: &str) -> Option<f64> {
        let k = |d: &str| (d.to_string(), screen.to_string(), tile.to_string());
        self.map
            .get(&k(tree.name()))
            .or_else(|| self.map.get(&k("")))
            .copied()
    }
}

/// Firing alerts as `(alertname, host_name)`.
#[derive(Debug, Default)]
pub struct Firing {
    pub alerts: Vec<(String, Option<String>)>,
}

impl Firing {
    fn matches(&self, names: &[String], host: Option<&str>) -> bool {
        self.alerts
            .iter()
            .any(|(n, h)| names.contains(n) && (host.is_none() || h.as_deref() == host))
    }
}

/// The `host_name` a tile's alerts are matched against: the screen itself on a
/// device screen, the linked device elsewhere, otherwise any host.
/// See README (CONTRACT QUESTION 2).
fn alert_host<'a>(screen: &'a Screen, tile: &'a TileDef) -> Option<&'a str> {
    if screen.kind == "device" {
        Some(&screen.name)
    } else {
        tile.link.as_deref()
    }
}

/// Compute one tile. `samples` is `None` when VictoriaMetrics could not be
/// queried.
pub fn compute_tile(
    tree: Tree,
    screen: &Screen,
    tile: &TileDef,
    samples: Option<&Samples>,
    firing: Option<&Firing>,
    now: u64,
) -> TileMsg {
    let unknown = |s: TileState| TileMsg {
        v: None,
        d: "n/a".into(),
        c: "grey".into(),
        s,
        t: now,
    };
    if screen.greyed {
        return unknown(TileState::Greyed);
    }
    let value = samples
        .and_then(|s| s.get(tree, &screen.name, &tile.id))
        .filter(|v| v.is_finite());
    let issue = firing.is_some_and(|f| f.matches(&tile.alerts, alert_host(screen, tile)));
    match value {
        None if issue => unknown(TileState::Issue),
        None => unknown(TileState::Unknown),
        Some(v) => TileMsg {
            v: Some(v),
            d: format_value(v, tile.unit.as_deref(), tile.decimals),
            c: threshold_colour(v, &tile.steps),
            s: if issue {
                TileState::Issue
            } else {
                TileState::Ok
            },
            t: now,
        },
    }
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

    fn tile(id: &str, alerts: &[&str], link: Option<&str>) -> TileDef {
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
            record_door: None,
        }
    }

    fn screen(name: &str, kind: &str, greyed: bool, tiles: Vec<TileDef>) -> Screen {
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
        s.insert("", screen, tile, v);
        s
    }

    #[test]
    fn ok_value_with_colour() {
        let sc = screen(
            "srv",
            "device",
            false,
            vec![tile("ram", &["DeviceMemoryLow"], None)],
        );
        let m = compute_tile(
            Tree::Internal,
            &sc,
            &sc.tiles[0],
            Some(&samples("srv", "ram", 0.63)),
            Some(&Firing::default()),
            100,
        );
        assert_eq!(
            (m.d.as_str(), m.c.as_str(), m.s),
            ("63 %", "green", TileState::Ok)
        );
        let m = compute_tile(
            Tree::Internal,
            &sc,
            &sc.tiles[0],
            Some(&samples("srv", "ram", 0.95)),
            Some(&Firing::default()),
            100,
        );
        assert_eq!(m.c, "red");
    }

    #[test]
    fn issue_only_for_this_host_and_alert() {
        let sc = screen(
            "srv",
            "device",
            false,
            vec![tile("ram", &["DeviceMemoryLow"], None)],
        );
        let sm = samples("srv", "ram", 0.5);
        let f = |n: &str, h: &str| Firing {
            alerts: vec![(n.into(), Some(h.into()))],
        };
        let go =
            |f: &Firing| compute_tile(Tree::Internal, &sc, &sc.tiles[0], Some(&sm), Some(f), 1).s;
        assert_eq!(go(&f("DeviceMemoryLow", "srv")), TileState::Issue);
        assert_eq!(go(&f("DeviceMemoryLow", "fra-0")), TileState::Ok);
        assert_eq!(go(&f("DeviceDiskFull", "srv")), TileState::Ok);
    }

    #[test]
    fn non_device_screens_match_on_link_or_any_host() {
        let sc = screen(
            "overview",
            "overview",
            false,
            vec![
                tile("a", &["DeviceDown"], Some("fra-0")),
                tile("b", &["DeviceDown"], None),
            ],
        );
        let mut sm = samples("overview", "a", 1.0);
        sm.insert("", "overview", "b", 1.0);
        let f = Firing {
            alerts: vec![("DeviceDown".into(), Some("fra-0".into()))],
        };
        let go =
            |i: usize| compute_tile(Tree::Internal, &sc, &sc.tiles[i], Some(&sm), Some(&f), 1).s;
        assert_eq!(go(0), TileState::Issue);
        assert_eq!(go(1), TileState::Issue);
        let other = Firing {
            alerts: vec![("DeviceDown".into(), Some("srv".into()))],
        };
        let go2 = |i: usize| {
            compute_tile(
                Tree::Internal,
                &sc,
                &sc.tiles[i],
                Some(&sm),
                Some(&other),
                1,
            )
            .s
        };
        assert_eq!(go2(0), TileState::Ok);
    }

    #[test]
    fn unknown_without_value_or_without_victoria() {
        let sc = screen("srv", "device", false, vec![tile("ram", &[], None)]);
        let m = compute_tile(
            Tree::Internal,
            &sc,
            &sc.tiles[0],
            Some(&Samples::default()),
            Some(&Firing::default()),
            5,
        );
        assert_eq!((m.s, m.v, m.c.as_str()), (TileState::Unknown, None, "grey"));
        let m = compute_tile(Tree::Internal, &sc, &sc.tiles[0], None, None, 5);
        assert_eq!(m.s, TileState::Unknown);
        let nan = samples("srv", "ram", f64::NAN);
        let m = compute_tile(
            Tree::Internal,
            &sc,
            &sc.tiles[0],
            Some(&nan),
            Some(&Firing::default()),
            5,
        );
        assert_eq!(m.s, TileState::Unknown);
    }

    #[test]
    fn greyed_screen_ignores_everything() {
        let sc = screen(
            "laptop",
            "device",
            true,
            vec![tile("ram", &["DeviceMemoryLow"], None)],
        );
        let f = Firing {
            alerts: vec![("DeviceMemoryLow".into(), Some("laptop".into()))],
        };
        let m = compute_tile(
            Tree::Internal,
            &sc,
            &sc.tiles[0],
            Some(&samples("laptop", "ram", 0.5)),
            Some(&f),
            1,
        );
        assert_eq!(m.s, TileState::Greyed);
        assert_eq!(m.v, None);
    }

    #[test]
    fn door_label_wins_over_unlabelled_series() {
        let mut s = Samples::default();
        s.insert("", "overview", "x", 0.1);
        s.insert("public", "overview", "x", 0.2);
        assert_eq!(s.get(Tree::Public, "overview", "x"), Some(0.2));
        assert_eq!(s.get(Tree::Internal, "overview", "x"), Some(0.1));
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
            vec![tile("x", &[], Some("srv")), tile("y", &[], Some("secret"))],
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
        topo.topology = Some(serde_json::json!({
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
        assert_eq!(t["brackets"][0]["members"], serde_json::json!(["srv"]));
    }
}
