//! Payload types, topic names and the staleness rule of the Corbet status
//! board (contract `corbet.obs` §5). No server dependencies: a kiosk renderer
//! or any other client can depend on this crate alone.
//!
//! Topics are shown as a client sees them (below the mount point). The server
//! writes the same topics below a `public/` or `internal/` tree prefix; see
//! [`Tree`].

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(feature = "client")]
pub mod client;

/// Schema string carried by every `_layout` message.
pub const LAYOUT_SCHEMA: &str = "corbet.obs.layout/1";

/// A stale tile is one older than this many intervals.
pub const STALE_INTERVALS: u64 = 3;

/// The two topic trees the server writes (D26).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tree {
    Public,
    Internal,
}

impl Tree {
    pub const ALL: [Tree; 2] = [Tree::Public, Tree::Internal];

    /// Topic prefix on the local listener, with the trailing slash.
    pub fn prefix(self) -> &'static str {
        match self {
            Tree::Public => "public/",
            Tree::Internal => "internal/",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Tree::Public => "public",
            Tree::Internal => "internal",
        }
    }
}

/// Topic builders (client view, no tree prefix).
pub mod topic {
    pub const ONLINE: &str = "_online";
    pub const PLAYLIST: &str = "_playlist";

    pub fn tile(screen: &str, tile: &str) -> String {
        format!("{screen}/{tile}")
    }

    pub fn layout(screen: &str) -> String {
        format!("{screen}/_layout")
    }

    /// Filter a screen client subscribes to: all of the screen's topics.
    pub fn screen_filter(screen: &str) -> String {
        format!("{screen}/#")
    }
}

/// Tile state `s`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TileState {
    Ok,
    Issue,
    Unknown,
    Greyed,
}

/// `<screen>/<tile>` payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TileMsg {
    /// Raw value; `null` when unknown or greyed.
    pub v: Option<f64>,
    /// The value as displayed (unit, decimals).
    pub d: String,
    /// Threshold colour name.
    pub c: String,
    pub s: TileState,
    /// Evaluation time, unix seconds.
    pub t: u64,
}

impl TileMsg {
    /// The part that triggers a publish when it changes (`d`, `c`, `s`).
    pub fn same_display(&self, other: &TileMsg) -> bool {
        self.d == other.d && self.c == other.c && self.s == other.s
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grid {
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LayoutTile {
    pub id: String,
    pub title: String,
    pub grid: Grid,
    #[serde(default)]
    pub unit: Option<String>,
    /// Another screen's name.
    #[serde(default)]
    pub link: Option<String>,
}

/// `<screen>/_layout` payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Layout {
    pub schema: String,
    pub generation: String,
    pub screen: String,
    pub title: String,
    pub kind: String,
    pub greyed: bool,
    pub tiles: Vec<LayoutTile>,
    /// Topology description (`nodes`, `edges`, `brackets`), passed through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlaylistEntry {
    pub name: String,
    pub issue: bool,
}

/// `_playlist` payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Playlist {
    pub interval_s: u64,
    pub screens: Vec<PlaylistEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnlineState {
    #[serde(rename = "online")]
    Online,
    #[serde(rename = "offline")]
    Offline,
    #[serde(rename = "generation-mismatch")]
    GenerationMismatch,
}

/// `_online` payload. The last will carries only `state`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Online {
    pub state: OnlineState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_s: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
}

impl Online {
    /// The last-will payload.
    pub fn offline() -> Online {
        Online {
            state: OnlineState::Offline,
            t: None,
            interval_s: None,
            generation: None,
        }
    }
}

/// What a client should show for a tile (contract §5, B4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// Show "observability unreachable".
    Unreachable,
    /// Generation mismatch: show "updating".
    Updating,
}

/// A tile is stale when `now - t > 3 * interval_s`.
pub fn is_tile_stale(now: u64, t: u64, interval_s: u64) -> bool {
    now.saturating_sub(t) > STALE_INTERVALS * interval_s
}

/// The staleness rule: stale when `_online` is not `online` or the tile's `t`
/// is older than three intervals. A missing `_online` counts as unreachable.
/// The interval comes from `_online` itself.
pub fn freshness(now: u64, tile_t: u64, online: Option<&Online>) -> Freshness {
    let Some(o) = online else {
        return Freshness::Unreachable;
    };
    match o.state {
        OnlineState::Offline => Freshness::Unreachable,
        OnlineState::GenerationMismatch => Freshness::Updating,
        OnlineState::Online => {
            let interval = o.interval_s.unwrap_or(0);
            if interval == 0 || is_tile_stale(now, tile_t, interval) {
                Freshness::Unreachable
            } else {
                Freshness::Fresh
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn online(state: OnlineState, interval: u64) -> Online {
        Online {
            state,
            t: Some(0),
            interval_s: Some(interval),
            generation: None,
        }
    }

    #[test]
    fn staleness_boundary_is_three_intervals() {
        assert!(!is_tile_stale(1090, 1000, 30));
        assert!(is_tile_stale(1091, 1000, 30));
        assert!(!is_tile_stale(900, 1000, 30), "future t is not stale");
    }

    #[test]
    fn freshness_follows_online_and_age() {
        let on = online(OnlineState::Online, 30);
        assert_eq!(freshness(1010, 1000, Some(&on)), Freshness::Fresh);
        assert_eq!(freshness(1100, 1000, Some(&on)), Freshness::Unreachable);
        assert_eq!(freshness(1010, 1000, None), Freshness::Unreachable);
        assert_eq!(
            freshness(1010, 1000, Some(&Online::offline())),
            Freshness::Unreachable
        );
        let mm = online(OnlineState::GenerationMismatch, 30);
        assert_eq!(freshness(1010, 1000, Some(&mm)), Freshness::Updating);
    }

    #[test]
    fn tile_json_matches_contract() {
        let m: TileMsg =
            serde_json::from_str(r#"{"v":0.63,"d":"63 %","c":"green","s":"ok","t":1794400000}"#)
                .unwrap();
        assert_eq!(m.s, TileState::Ok);
        let back = serde_json::to_value(&m).unwrap();
        assert_eq!(back["v"], 0.63);
        assert_eq!(back["s"], "ok");
    }

    #[test]
    fn will_payload_roundtrips() {
        let s = serde_json::to_string(&Online::offline()).unwrap();
        assert_eq!(s, r#"{"state":"offline"}"#);
        let o: Online = serde_json::from_str(r#"{"state":"generation-mismatch"}"#).unwrap();
        assert_eq!(o.state, OnlineState::GenerationMismatch);
    }

    #[test]
    fn display_comparison_ignores_value_and_time() {
        let a = TileMsg {
            v: Some(0.5),
            d: "50 %".into(),
            c: "green".into(),
            s: TileState::Ok,
            t: 1,
        };
        let mut b = a.clone();
        b.v = Some(0.501);
        b.t = 99;
        assert!(a.same_display(&b));
        b.c = "red".into();
        assert!(!a.same_display(&b));
    }
}
