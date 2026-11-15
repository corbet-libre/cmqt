//! The running service: ONE broker connection (both trees on it), one evaluation loop.
//!
//! Outgoing bytes are the budget of the box (1 GB/month free egress, charter D50): a tile is published only when its displayed value or state
//! changes (retained, so nothing is resent), `_online` is the only heartbeat (every 120 s), `_focus` goes out only when the set changes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cmqt::{Online, OnlineState, Tree, topic};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use rumqttc::v5::mqttbytes::v5::{LastWill, LastWillProperties};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions, mqttbytes::v5::Packet};

use crate::config::Config;
use crate::dashboard::{self, Board};
use crate::engine::{self, Tracker};
use crate::vm;

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// One tree on the shared broker connection.
struct Link {
    tree: Tree,
    client: AsyncClient,
    /// Counts connects (ConnAck): a Link that has not seen the latest one republishes everything.
    connects: Arc<AtomicU64>,
    seen_connects: u64,
    tracker: Tracker,
    expiry: u32,
    last_online: Option<(Instant, String)>,
    online_every: Duration,
}

/// The single connection (it carries both trees). Its last will marks the INTERNAL tree offline; the public tree's `_online` is taken down by the
/// relay's box-silence watcher (a connection has only one will).
fn connect(cfg: &Config) -> Vec<Link> {
    let expiry = (3 * cfg.online_every_s).min(u32::MAX as u64) as u32;
    let tree = Tree::Internal;
    {
        let mut opts = MqttOptions::new(
            format!("cmqt-server-{}", std::process::id()),
            &cfg.broker_host,
            cfg.broker_port,
        );
        opts.set_keep_alive(Duration::from_secs(120));
        if let Some(url) = &cfg.broker_url {
            opts = MqttOptions::new(opts.client_id(), url, 443);
            // Measured 2026-11-15: alwaysdata's proxy closes a WebSocket after 60 s without traffic
            // (keep-alive 120 and 60 both dropped every ~61 s and republished everything each time,
            // about 0.7 MB per hour); 30 s holds the connection (idle cost about 0.4 MB per day).
            opts.set_keep_alive(Duration::from_secs(30));
            opts.set_transport(rumqttc::Transport::wss_with_default_config());
        }
        if let (Some(u), Some(f)) = (&cfg.broker_user, &cfg.broker_password_file) {
            let pw = std::fs::read_to_string(f).unwrap_or_default();
            opts.set_credentials(u, pw.lines().next().unwrap_or("").trim());
        }
        let will_topic = format!("{}{}", tree.prefix(), topic::ONLINE);
        let will = serde_json::to_vec(&Online::offline()).unwrap_or_default();
        opts.set_last_will(LastWill::new(
            will_topic,
            will,
            QoS::AtLeastOnce,
            true,
            Some(LastWillProperties {
                delay_interval: None,
                payload_format_indicator: None,
                message_expiry_interval: Some(expiry),
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            }),
        ));
        let (client, eventloop) = AsyncClient::new(opts, 1024);
        let connects = Arc::new(AtomicU64::new(1));
        tokio::spawn(drive(eventloop, connects.clone()));
        Tree::ALL
            .iter()
            .map(|t| Link {
                tree: *t,
                client: client.clone(),
                connects: connects.clone(),
                seen_connects: 0,
                tracker: Tracker::default(),
                expiry,
                last_online: None,
                online_every: Duration::from_secs(cfg.online_every_s),
            })
            .collect()
    }
}

impl Link {
    /// Queue a retained QoS 1 publish. Only `_online` carries a message expiry
    /// (3 x interval); everything else lives until it changes. A full queue
    /// (broker down) drops the message: the reconnect republishes everything.
    fn publish(&self, rel_topic: &str, payload: Vec<u8>) {
        let props = PublishProperties {
            message_expiry_interval: (rel_topic == topic::ONLINE).then_some(self.expiry),
            ..Default::default()
        };
        let topic = format!("{}{}", self.tree.prefix(), rel_topic);
        if let Err(e) =
            self.client
                .try_publish_with_properties(topic, QoS::AtLeastOnce, true, payload, props)
        {
            eprintln!("cmqt-server: {} publish dropped: {e}", self.tree.name());
        }
    }

    /// A live (not retained) QoS 0 message: the focus report; a late joiner must not see an old one.
    fn publish_live(&self, rel_topic: &str, payload: Vec<u8>) {
        let topic = format!("{}{}", self.tree.prefix(), rel_topic);
        if let Err(e) = self
            .client
            .try_publish(topic, QoS::AtLeastOnce, true, payload) // retained: a restarted relay reads the current set at once
        {
            eprintln!(
                "cmqt-server: {} focus publish dropped: {e}",
                self.tree.name()
            );
        }
    }

    fn put<T: serde::Serialize>(&mut self, rel_topic: &str, value: &T, fingerprint: &str) {
        if self.tracker.should_publish(rel_topic, fingerprint)
            && let Ok(p) = serde_json::to_vec(value)
        {
            self.publish(rel_topic, p);
        }
    }
}

async fn drive(mut ev: EventLoop, connects: Arc<AtomicU64>) {
    loop {
        match ev.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                eprintln!("cmqt-server: connected");
                connects.fetch_add(1, Ordering::SeqCst);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("cmqt-server: connection: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

enum Gen {
    Match,
    Mismatch,
}

fn check_generation(cfg: &Config, board: &Board) -> Gen {
    let Some(p) = &cfg.prometheus_manifest else {
        return Gen::Match;
    };
    match dashboard::read_generation(p) {
        Ok(g) if g == board.generation => Gen::Match,
        Ok(g) => {
            eprintln!(
                "cmqt-server: generation mismatch: dashboards {} vs rules {g}",
                board.generation
            );
            Gen::Mismatch
        }
        Err(e) => {
            eprintln!("cmqt-server: rules manifest unreadable: {e}");
            Gen::Mismatch
        }
    }
}

/// One evaluation for one tree.
fn evaluate(
    cfg: &Config,
    link: &mut Link,
    board: &Board,
    gen_state: &Gen,
    vmres: Option<&(engine::Samples, engine::Firing)>,
    now: u64,
) {
    let tree_board = board.tree(link.tree);
    let mismatch = matches!(gen_state, Gen::Mismatch);
    let mut statuses = Vec::new();
    for screen in &tree_board.screens {
        let lay = engine::layout(&board.generation, screen, tree_board);
        if let Ok(fp) = serde_json::to_string(&lay) {
            link.put(&topic::layout(&screen.name), &lay, &fp);
        }
        let mut msgs = Vec::new();
        for tile in &screen.tiles {
            let m = engine::compute_tile(
                link.tree,
                screen,
                tile,
                vmres.map(|r| &r.0),
                vmres.map(|r| &r.1),
                engine::aggregate_coverage(tree_board, tile, vmres.map(|r| &r.0)),
                now,
            );
            if !mismatch {
                let fp = engine::tile_fingerprint(&m);
                link.put(&topic::tile(&screen.name, &tile.id), &m, &fp);
            }
            msgs.push(m);
        }
        if let Some(topo) = &lay.topology {
            for (device, m) in engine::topology_tiles(
                topo,
                vmres.map(|r| &r.1),
                &|d| engine::node_lacks_data(tree_board, d, vmres.map(|r| &r.0)),
                now,
            ) {
                if !mismatch {
                    let fp = engine::tile_fingerprint(&m);
                    link.put(&topic::tile(&screen.name, &device), &m, &fp);
                }
                msgs.push(m);
            }
        }
        statuses.push((screen.name.clone(), engine::screen_has_issue(&msgs)));
    }
    let pl = engine::playlist(cfg.interval_s, &statuses);
    if let Ok(fp) = serde_json::to_string(&pl) {
        link.put(topic::PLAYLIST, &pl, &fp);
    }
    let online = Online {
        state: if mismatch {
            OnlineState::GenerationMismatch
        } else {
            OnlineState::Online
        },
        t: Some(now),
        interval_s: Some(cfg.interval_s),
        generation: Some(board.generation.clone()),
    };
    // The heartbeat: when the state or the generation changed, else every online_every_s (default 120; not every evaluation: each message costs outgoing bytes on the box).
    let key = format!("{:?}{:?}", online.state, online.generation);
    let due = match &link.last_online {
        Some((at, k)) => *k != key || at.elapsed() >= link.online_every,
        None => true,
    };
    if due && let Ok(p) = serde_json::to_vec(&online) {
        link.publish(topic::ONLINE, p);
        link.last_online = Some((Instant::now(), key));
    }
}

pub async fn run(cfg: Config) -> Result<(), String> {
    let manifest = cfg.dashboards_dir.join("manifest.json");
    let mut board = dashboard::load(&cfg.dashboards_dir)?;
    eprintln!("cmqt-server: generation {}", board.generation);
    let mut links: Vec<Link> = connect(&cfg);
    let mut tick = tokio::time::interval(Duration::from_secs(cfg.interval_s));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| e.to_string())?;
    // The foveal focus (contract section 8): the (device, subsystem) pairs whose DeviceAttention alert is firing, as a live message on `internal/_focus`.
    // VictoriaMetrics is asked every 15 s (local, free), but a report goes out only when the set CHANGED (and once after every (re)connect): the box pays for every
    // message. The relay must not treat silence as "nothing is firing" (the heartbeat is `internal/_online`, every 120 s).
    let mut last_focus: Option<(String, u64)> = None;
    let mut focus = tokio::time::interval(Duration::from_secs(15));
    focus.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = focus.tick() => {
                let url = cfg.victoria_url.clone();
                if let Ok(Ok(pairs)) = tokio::task::spawn_blocking(move || vm::attention(&url)).await
                    && let Some(link) = links.iter().find(|l| l.tree.name() == "internal")
                {
                    let connects = link.connects.load(Ordering::SeqCst);
                    let key = serde_json::to_string(&pairs).unwrap_or_default();
                    if last_focus.as_ref() != Some(&(key.clone(), connects))
                        && let Ok(p) = serde_json::to_vec(&serde_json::json!({"t": now_s(), "firing": pairs}))
                    {
                        link.publish_live("_focus", p);
                        last_focus = Some((key, connects));
                    }
                }
                continue;
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
        }
        // A new generation replaces the board; a broken one keeps the old (§6).
        match dashboard::read_generation(&manifest) {
            Ok(g) if g != board.generation => match dashboard::load(&cfg.dashboards_dir) {
                Ok(b) => {
                    eprintln!("cmqt-server: new generation {}", b.generation);
                    board = b;
                    links.iter_mut().for_each(|l| l.tracker.clear());
                }
                Err(e) => eprintln!("cmqt-server: new dashboards rejected, keeping old: {e}"),
            },
            Ok(_) => {}
            Err(e) => eprintln!("cmqt-server: manifest: {e}"),
        }
        let gen_state = check_generation(&cfg, &board);
        let vmres = if matches!(gen_state, Gen::Match) {
            let url = cfg.victoria_url.clone();
            match tokio::task::spawn_blocking(move || vm::fetch(&url)).await {
                Ok(Ok(r)) => Some(r),
                Ok(Err(e)) => {
                    eprintln!("cmqt-server: victoriametrics: {e}");
                    None
                }
                Err(e) => {
                    eprintln!("cmqt-server: query task: {e}");
                    None
                }
            }
        } else {
            None
        };
        let now = now_s();
        for link in &mut links {
            let connects = link.connects.load(Ordering::SeqCst);
            if connects != link.seen_connects {
                link.seen_connects = connects;
                link.tracker.clear();
                link.last_online = None;
            }
            evaluate(&cfg, link, &board, &gen_state, vmres.as_ref(), now);
        }
    }
    // Clean shutdown: a normal DISCONNECT does not fire the will, so say it.
    for link in &links {
        if let Ok(p) = serde_json::to_vec(&Online::offline()) {
            link.publish(topic::ONLINE, p);
        }
        let _ = link.client.disconnect().await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(())
}
