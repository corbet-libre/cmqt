//! The running service: two broker connections, one evaluation loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cmqt::{Online, OnlineState, Tree, topic};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{LastWill, LastWillProperties};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions, mqttbytes::v5::Packet};
use rumqttc::v5::mqttbytes::v5::PublishProperties;

use crate::config::Config;
use crate::dashboard::{self, Board};
use crate::engine::{self, Tracker};
use crate::vm;

fn now_s() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// One broker connection for one tree.
struct Link {
    tree: Tree,
    client: AsyncClient,
    /// Set on every (re)connect: the next evaluation republishes everything.
    reconnected: Arc<AtomicBool>,
    tracker: Tracker,
    expiry: u32,
}

impl Link {
    fn connect(cfg: &Config, tree: Tree) -> Link {
        let expiry = (3 * cfg.interval_s).min(u32::MAX as u64) as u32;
        let mut opts = MqttOptions::new(
            format!("cmqt-server-{}-{}", tree.name(), std::process::id()),
            &cfg.broker_host,
            cfg.broker_port,
        );
        opts.set_keep_alive(Duration::from_secs(10));
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
        let reconnected = Arc::new(AtomicBool::new(true));
        tokio::spawn(drive(eventloop, reconnected.clone(), tree));
        Link { tree, client, reconnected, tracker: Tracker::default(), expiry }
    }

    /// Queue a retained QoS 1 publish with message expiry. A full queue (broker
    /// down) drops the message: the reconnect republishes everything.
    fn publish(&self, rel_topic: &str, payload: Vec<u8>) {
        let props = PublishProperties { message_expiry_interval: Some(self.expiry), ..Default::default() };
        let topic = format!("{}{}", self.tree.prefix(), rel_topic);
        if let Err(e) =
            self.client.try_publish_with_properties(topic, QoS::AtLeastOnce, true, payload, props)
        {
            eprintln!("cmqt-server: {} publish dropped: {e}", self.tree.name());
        }
    }

    fn put<T: serde::Serialize>(&mut self, rel_topic: &str, value: &T, fingerprint: &str, now: u64, refresh: u64) {
        if self.tracker.should_publish(rel_topic, fingerprint, now, refresh) {
            if let Ok(p) = serde_json::to_vec(value) {
                self.publish(rel_topic, p);
            }
        }
    }
}

async fn drive(mut ev: EventLoop, reconnected: Arc<AtomicBool>, tree: Tree) {
    loop {
        match ev.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                eprintln!("cmqt-server: {} connected", tree.name());
                reconnected.store(true, Ordering::SeqCst);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("cmqt-server: {} connection: {e}", tree.name());
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
    let Some(p) = &cfg.prometheus_manifest else { return Gen::Match };
    match dashboard::read_generation(p) {
        Ok(g) if g == board.generation => Gen::Match,
        Ok(g) => {
            eprintln!("cmqt-server: generation mismatch: dashboards {} vs rules {g}", board.generation);
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
    let refresh = cfg.refresh_intervals * cfg.interval_s;
    let tree_board = board.tree(link.tree);
    let mismatch = matches!(gen_state, Gen::Mismatch);
    let mut statuses = Vec::new();
    for screen in &tree_board.screens {
        let lay = engine::layout(&board.generation, screen, tree_board);
        if let Ok(fp) = serde_json::to_string(&lay) {
            link.put(&topic::layout(&screen.name), &lay, &fp, now, refresh);
        }
        let mut msgs = Vec::new();
        for tile in &screen.tiles {
            let m = engine::compute_tile(
                link.tree,
                screen,
                tile,
                vmres.map(|r| &r.0),
                vmres.map(|r| &r.1),
                now,
            );
            if !mismatch {
                let fp = engine::tile_fingerprint(&m);
                link.put(&topic::tile(&screen.name, &tile.id), &m, &fp, now, refresh);
            }
            msgs.push(m);
        }
        statuses.push((screen.name.clone(), engine::screen_has_issue(&msgs)));
    }
    let pl = engine::playlist(cfg.interval_s, &statuses);
    if let Ok(fp) = serde_json::to_string(&pl) {
        link.put(topic::PLAYLIST, &pl, &fp, now, refresh);
    }
    let online = Online {
        state: if mismatch { OnlineState::GenerationMismatch } else { OnlineState::Online },
        t: Some(now),
        interval_s: Some(cfg.interval_s),
        generation: Some(board.generation.clone()),
    };
    if let Ok(p) = serde_json::to_vec(&online) {
        link.publish(topic::ONLINE, p); // every interval, unconditionally
    }
}

pub async fn run(cfg: Config) -> Result<(), String> {
    let manifest = cfg.dashboards_dir.join("manifest.json");
    let mut board = dashboard::load(&cfg.dashboards_dir)?;
    eprintln!("cmqt-server: generation {}", board.generation);
    let mut links: Vec<Link> = Tree::ALL.iter().map(|t| Link::connect(&cfg, *t)).collect();
    let mut tick = tokio::time::interval(Duration::from_secs(cfg.interval_s));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| e.to_string())?;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
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
            if link.reconnected.swap(false, Ordering::SeqCst) {
                link.tracker.clear();
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
