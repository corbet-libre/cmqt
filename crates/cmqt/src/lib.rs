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
use std::time::Duration;

#[cfg(feature = "client")]
pub mod client;

/// Schema string carried by every `_layout` message.
pub const LAYOUT_SCHEMA: &str = "corbet.obs.layout/1";

/// `_online` older than this many intervals means unreachable.
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

/// Why the box is online but cannot tell the truth (`_online.degraded`). The board stays live (its tiles carry their own `unknown`
/// state, never a stale green); the relay's box-silence watcher pages on it as on silence.
pub mod degraded {
    /// VictoriaMetrics did not answer (or answered an error): nothing could be evaluated.
    pub const QUERY: &str = "query";
    /// VictoriaMetrics answers but holds no recorded tile value: vmalert does not evaluate the rules.
    pub const RULES: &str = "rules";
    /// The configured pipeline check (cmqt-server `degraded_query`, e.g. Alertmanager down or vmalert send errors) returned a series.
    pub const CHECK: &str = "check";
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
    /// Present while the box publishes but its pipeline is broken: one of [`degraded`]'s words. An addition to the
    /// contract: `state` stays `online` (clients that do not know the field keep working), only the watcher reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded: Option<String>,
}

impl Online {
    /// The last-will payload.
    pub fn offline() -> Online {
        Online {
            state: OnlineState::Offline,
            t: None,
            interval_s: None,
            generation: None,
            degraded: None,
        }
    }
}

/// What a client should show for the whole board (contract §5, B4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// Show "observability unreachable".
    Unreachable,
    /// Generation mismatch: show "updating".
    Updating,
}

/// The staleness rule (contract §5): liveness comes from `_online` alone.
/// `since_online` is the time since the client last RECEIVED `_online`,
/// measured on its own monotonic clock (`Instant`); the server's clock is never
/// compared. Unreachable when `_online` is missing, not `online`, or older than
/// three intervals. The interval comes from `_online` itself.
pub fn freshness(since_online: Option<Duration>, online: Option<&Online>) -> Freshness {
    let (Some(since), Some(o)) = (since_online, online) else {
        return Freshness::Unreachable;
    };
    match o.state {
        OnlineState::Offline => Freshness::Unreachable,
        OnlineState::GenerationMismatch => Freshness::Updating,
        OnlineState::Online => {
            let interval = o.interval_s.unwrap_or(0);
            if interval == 0 || since > Duration::from_secs(STALE_INTERVALS * interval) {
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
            degraded: None,
        }
    }

    #[test]
    fn freshness_follows_online_and_receipt_age() {
        let on = online(OnlineState::Online, 30);
        let secs = |n| Some(Duration::from_secs(n));
        assert_eq!(freshness(secs(10), Some(&on)), Freshness::Fresh);
        assert_eq!(freshness(secs(90), Some(&on)), Freshness::Fresh);
        assert_eq!(freshness(secs(91), Some(&on)), Freshness::Unreachable);
        assert_eq!(freshness(None, Some(&on)), Freshness::Unreachable);
        assert_eq!(freshness(secs(1), None), Freshness::Unreachable);
        assert_eq!(
            freshness(secs(1), Some(&Online::offline())),
            Freshness::Unreachable
        );
        let mm = online(OnlineState::GenerationMismatch, 30);
        assert_eq!(freshness(secs(1), Some(&mm)), Freshness::Updating);
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
    fn degraded_is_an_optional_field_of_an_online_state() {
        // an old payload (no field) still parses; a degraded one stays `online` for every client that ignores the field
        let old: Online =
            serde_json::from_str(r#"{"state":"online","t":1,"interval_s":30,"generation":"g"}"#)
                .unwrap();
        assert_eq!(old.degraded, None);
        let mut d = old.clone();
        d.degraded = Some(degraded::QUERY.into());
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["state"], "online");
        assert_eq!(v["degraded"], "query");
        assert!(
            serde_json::to_value(&old)
                .unwrap()
                .get("degraded")
                .is_none()
        );
        assert_eq!(
            freshness(Some(Duration::from_secs(1)), Some(&d)),
            Freshness::Fresh
        );
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
