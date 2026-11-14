//! End-to-end: a real Mosquitto, a fake VictoriaMetrics and the real binary.
//!
//! Needs `mosquitto` on PATH, or `MOSQUITTO_BIN`, or Nix (`nix build
//! nixpkgs#mosquitto`, resolved once).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use serde_json::{Value, json};

// ---------------------------------------------------------------- helpers

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn mosquitto_bin() -> &'static PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        if let Ok(p) = std::env::var("MOSQUITTO_BIN") {
            return p.into();
        }
        let out = Command::new("nix")
            .args([
                "build",
                "nixpkgs#mosquitto",
                "--no-link",
                "--print-out-paths",
            ])
            .output()
            .expect("nix is needed to get mosquitto (or set MOSQUITTO_BIN)");
        let path = String::from_utf8(out.stdout).unwrap();
        PathBuf::from(path.lines().next().expect("nix build printed no path")).join("bin/mosquitto")
    })
}

fn scratch(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("cmqt-it-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct Broker {
    child: Child,
    port: u16,
    mounted_port: u16,
    conf: PathBuf,
}

impl Broker {
    fn start(dir: &Path, port: u16, mounted_port: u16) -> Broker {
        // local listener for the server; a second one mounts public/ and is
        // what a browser would reach. No persistence, like production.
        let conf = dir.join("mosquitto.conf");
        // The real ACL of the public listener (deploy/public.acl).
        let acl = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/public.acl");
        let acl = acl.display();
        std::fs::write(
            &conf,
            format!(
                "per_listener_settings true\npersistence false\n\
                 listener {port} 127.0.0.1\nallow_anonymous true\n\
                 listener {mounted_port} 127.0.0.1\nallow_anonymous true\nmount_point public/\nacl_file {acl}\n"
            ),
        )
        .unwrap();
        let child = Command::new(mosquitto_bin())
            .args(["-c"])
            .arg(&conf)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start mosquitto");
        wait_port(port);
        Broker {
            child,
            port,
            mounted_port,
            conf,
        }
    }

    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = Command::new(mosquitto_bin())
            .args(["-c"])
            .arg(&self.conf)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_port(self.port);
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_port(port: u16) {
    let end = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < end, "port {port} never opened");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Canned VictoriaMetrics.
#[derive(Clone)]
struct Vm {
    state: Arc<Mutex<VmState>>,
    port: u16,
}

struct VmState {
    ram: f64,
    fail: bool,
    /// Firing alerts: `(alertname, [(label, value)])`.
    firing: Vec<(String, Vec<(String, String)>)>,
}

impl Vm {
    fn start() -> Vm {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(VmState {
            ram: 63.0,
            fail: false,
            firing: vec![],
        }));
        let st = state.clone();
        std::thread::spawn(move || {
            for conn in l.incoming() {
                let Ok(mut c) = conn else { continue };
                let mut buf = [0u8; 4096];
                let n = c.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let line = req.lines().next().unwrap_or("").to_string();
                let s = st.lock().unwrap();
                let (status, body) = if s.fail {
                    ("500 Internal Server Error", "{}".to_string())
                } else if line.contains("ALERTS") {
                    let r: Vec<Value> = s
                        .firing
                        .iter()
                        .map(|(a, labels)| {
                            let mut m = json!({"alertname":a,"alertstate":"firing"});
                            for (k, v) in labels {
                                m[k] = json!(v);
                            }
                            json!({"metric":m,"value":[1,"1"]})
                        })
                        .collect();
                    (
                        "200 OK",
                        json!({"status":"success","data":{"resultType":"vector","result":r}})
                            .to_string(),
                    )
                } else {
                    let m = |sc: &str, t: &str, v: &str| json!({"metric":{"screen":sc,"tile":t},"value":[1,v]});
                    let r = vec![
                        m("srv", "alive", "1"),
                        m("srv", "cpu", "5"),
                        m("srv", "ram", &s.ram.to_string()),
                        m("srv", "disk", "50"),
                        m("srv", "io", "1"),
                        m("archlxc", "ram", "40"),
                        m("overview", "home", "0"),
                    ];
                    (
                        "200 OK",
                        json!({"status":"success","data":{"resultType":"vector","result":r}})
                            .to_string(),
                    )
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = c.write_all(resp.as_bytes());
            }
        });
        Vm { state, port }
    }
}

#[derive(Clone, Debug)]
struct Msg {
    topic: String,
    payload: Value,
    retain: bool,
    expiry: Option<u32>,
}

/// A subscriber collecting everything it receives.
struct Watcher {
    msgs: Arc<Mutex<Vec<Msg>>>,
    _client: AsyncClient,
}

impl Watcher {
    async fn new(port: u16, filter: &str) -> Watcher {
        let mut o = MqttOptions::new(format!("watch-{}-{port}", free_port()), "127.0.0.1", port);
        o.set_keep_alive(Duration::from_secs(5));
        let (client, mut ev) = AsyncClient::new(o, 100);
        client.subscribe(filter, QoS::AtLeastOnce).await.unwrap();
        let msgs = Arc::new(Mutex::new(Vec::new()));
        let m = msgs.clone();
        tokio::spawn(async move {
            loop {
                match ev.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let payload = serde_json::from_slice(&p.payload).unwrap_or(Value::Null);
                        m.lock().unwrap().push(Msg {
                            topic: String::from_utf8_lossy(&p.topic).to_string(),
                            payload,
                            retain: p.retain,
                            expiry: p
                                .properties
                                .as_ref()
                                .and_then(|x| x.message_expiry_interval),
                        });
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        });
        Watcher {
            msgs,
            _client: client,
        }
    }

    fn all(&self) -> Vec<Msg> {
        self.msgs.lock().unwrap().clone()
    }

    fn on(&self, topic: &str) -> Vec<Msg> {
        self.all()
            .into_iter()
            .filter(|m| m.topic == topic)
            .collect()
    }

    fn last(&self, topic: &str) -> Option<Msg> {
        self.on(topic).pop()
    }

    async fn wait_for(&self, what: &str, f: impl Fn(&Watcher) -> bool) {
        let end = Instant::now() + Duration::from_secs(15);
        while !f(self) {
            assert!(
                Instant::now() < end,
                "timed out waiting for: {what}; have {:?}",
                self.all().iter().map(|m| &m.topic).collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path, broker_port: u16, vm: &Vm, interval_s: u64, extra: &str) -> Server {
        let cfg = dir.join("cmqt.toml");
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cgrf");
        std::fs::write(
            &cfg,
            format!(
                "dashboards_dir = '{}'\nvictoria_url = 'http://127.0.0.1:{}'\nbroker_port = {broker_port}\ninterval_s = {interval_s}\n{extra}\n",
                fixtures.display(),
                vm.port
            ),
        )
        .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_cmqt-server"))
            .arg(&cfg)
            .spawn()
            .unwrap();
        Server { child }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

fn online_count(w: &Watcher, tree: &str) -> usize {
    w.on(&format!("{tree}/_online"))
        .iter()
        .filter(|m| m.payload["state"] == "online")
        .count()
}

// ------------------------------------------------------------------ tests

fn alert(name: &str, labels: &[(&str, &str)]) -> (String, Vec<(String, String)>) {
    (
        name.into(),
        labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn topics_retention_and_public_isolation() {
    let dir = scratch("topics");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    // archlxc is internal-only (home site); fra-0 is public (Frankfurt site)
    vm.state.lock().unwrap().firing = vec![
        alert("DeviceDown", &[("host_name", "archlxc"), ("site", "0x2A")]),
        alert("DeviceDown", &[("host_name", "fra-0"), ("site", "0x20")]),
    ];
    let _server = Server::start(&dir, broker.port, &vm, 2, "");

    // Wait until the server has published, then subscribe LATE: everything
    // must arrive as retained messages.
    let probe = Watcher::new(broker.port, "internal/_online").await;
    probe
        .wait_for("first _online", |w| online_count(w, "internal") >= 1)
        .await;
    let all = Watcher::new(broker.port, "#").await;
    all.wait_for("retained board", |w| {
        w.last("public/_playlist").is_some()
            && w.last("internal/_playlist").is_some()
            && w.last("internal/topology/fra-0").is_some()
            && w.last("public/topology/fra-0").is_some()
            && w.last("internal/srv/disk").is_some()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // internal tile payload: value, display, state
    let ram = all.last("internal/srv/ram").unwrap();
    assert!(ram.retain, "tiles are retained");
    assert_eq!(ram.payload["v"], 63.0);
    assert_eq!(ram.payload["d"], "63.0 %");
    assert_eq!(ram.payload["s"], "ok");
    assert!(ram.payload["t"].as_u64().unwrap() > 1_700_000_000);
    // message expiry = 3 x interval (remaining time may already be lower)
    assert!(
        ram.expiry.is_some_and(|e| (1..=6).contains(&e)),
        "expiry {:?}",
        ram.expiry
    );
    // aggregate state comes from the alert match, scoped by site and host
    assert_eq!(
        all.last("internal/overview/home").unwrap().payload["s"],
        "issue"
    );
    assert_eq!(
        all.last("internal/overview/fra_brh").unwrap().payload["s"],
        "issue"
    );
    assert_eq!(
        all.last("internal/overview/hel_pmd").unwrap().payload["s"],
        "ok"
    );
    assert_eq!(
        all.last("internal/archlxc/alive").unwrap().payload["s"],
        "issue"
    );
    assert_eq!(
        all.last("internal/elitebook/ram").unwrap().payload["s"],
        "greyed"
    );

    // public tree: state only, and archlxc never colours a public tile
    let pub_home = all.last("public/overview/home").unwrap().payload;
    assert!(pub_home["v"].is_null());
    assert_eq!(
        (pub_home["d"].as_str(), pub_home["s"].as_str()),
        (Some("OK"), Some("ok"))
    );
    let pub_fra = all.last("public/overview/fra_brh").unwrap().payload;
    assert_eq!(
        (pub_fra["d"].as_str(), pub_fra["c"].as_str()),
        (Some("Issue"), Some("red"))
    );
    let pub_ram = all.last("public/srv/ram").unwrap().payload;
    assert!(pub_ram["v"].is_null());
    assert_eq!(pub_ram["d"], "OK");
    assert_eq!(
        all.last("public/overview/hel_pmd").unwrap().payload["d"],
        "OK"
    );

    // topology: one tile per node, none for planned nodes
    assert_eq!(
        all.last("internal/topology/fra-0").unwrap().payload["s"],
        "issue"
    );
    assert_eq!(
        all.last("internal/topology/srv").unwrap().payload["d"],
        "OK"
    );
    assert_eq!(
        all.last("internal/topology/elitebook").unwrap().payload["s"],
        "greyed"
    );
    assert!(
        all.last("internal/topology/fra-3").is_none(),
        "planned node has no tile"
    );
    assert_eq!(
        all.last("internal/topology/archlxc").unwrap().payload["s"],
        "issue"
    );
    assert!(all.last("public/topology/archlxc").is_none());
    assert_eq!(
        all.last("public/topology/fra-0").unwrap().payload["s"],
        "issue"
    );

    // layout
    let lay = all.last("internal/srv/_layout").unwrap().payload;
    assert_eq!(lay["schema"], "corbet.obs.layout/1");
    assert_eq!(lay["tiles"][0]["id"], "alive");
    let online = all.last("internal/_online").unwrap().payload;
    assert_eq!(
        (online["state"].as_str(), online["interval_s"].as_u64()),
        (Some("online"), Some(2))
    );
    assert_eq!(lay["generation"], online["generation"]);
    // playlist: screens with issues come first
    let pl = all.last("internal/_playlist").unwrap().payload;
    let screens = pl["screens"].as_array().unwrap();
    let issues = screens.iter().take_while(|s| s["issue"] == true).count();
    assert!(issues >= 3, "{pl}");
    assert!(screens.iter().skip(issues).all(|s| s["issue"] == false));

    // the public tree never contains internal screens or their names
    let public: Vec<Msg> = all
        .all()
        .into_iter()
        .filter(|m| m.topic.starts_with("public/"))
        .collect();
    assert!(!public.is_empty());
    for m in &public {
        for leak in ["archlxc", "office.", "internal"] {
            assert!(!m.topic.contains(leak), "leak {leak} in topic {}", m.topic);
            assert!(
                !m.payload.to_string().contains(leak),
                "leak {leak} in {} {}",
                m.topic,
                m.payload
            );
        }
    }

    // a browser on the mounted listener sees client topics, public only
    let web = Watcher::new(broker.mounted_port, "#").await;
    web.wait_for("mounted view", |w| {
        w.last("overview/_layout").is_some()
            && w.last("_online").is_some()
            && w.last("srv/ram").is_some()
    })
    .await;
    assert!(
        web.all()
            .iter()
            .all(|m| !m.topic.starts_with("internal/") && !m.topic.contains("archlxc"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_on_change_only_and_refreshes() {
    let dir = scratch("change");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    // interval 2 s, refresh after 2 intervals (4 s): a change must go out at
    // the next tick, an unchanged tile must not.
    let _server = Server::start(&dir, broker.port, &vm, 2, "");
    let w = Watcher::new(broker.port, "internal/#").await;
    w.wait_for("first board", |w| {
        online_count(w, "internal") >= 1 && w.last("internal/srv/ram").is_some()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(w.on("internal/srv/ram").len(), 1);
    vm.state.lock().unwrap().ram = 95.0;
    w.wait_for("second _online", |w| online_count(w, "internal") >= 2)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let ram = w.on("internal/srv/ram");
    assert_eq!(ram.len(), 2, "the change is published");
    assert_eq!(ram[1].payload["d"], "95.0 %");
    assert_ne!(ram[0].payload["c"], ram[1].payload["c"]);
    assert_eq!(
        w.on("internal/srv/disk").len(),
        1,
        "unchanged tile is not re-sent"
    );
    assert_eq!(
        w.on("internal/srv/_layout").len(),
        1,
        "unchanged layout is not re-sent"
    );
    // _online goes out every interval
    assert!(online_count(&w, "internal") >= 2);
    // after the refresh period the unchanged tile is re-sent with a new t
    w.wait_for("refresh of the unchanged tile", |w| {
        w.on("internal/srv/disk").len() >= 2
    })
    .await;
    let disk = w.on("internal/srv/disk");
    assert!(disk[1].payload["t"].as_u64() > disk[0].payload["t"].as_u64());
}

#[tokio::test(flavor = "multi_thread")]
async fn last_will_on_kill_and_expiry() {
    let dir = scratch("will");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    let mut server = Server::start(&dir, broker.port, &vm, 1, "");
    let w = Watcher::new(broker.port, "#").await;
    w.wait_for("online", |w| {
        online_count(w, "internal") >= 1 && online_count(w, "public") >= 1
    })
    .await;

    server.kill(); // SIGKILL: no clean shutdown, the broker must publish the wills
    w.wait_for("internal will", |w| {
        w.last("internal/_online")
            .is_some_and(|m| m.payload["state"] == "offline")
    })
    .await;
    w.wait_for("public will", |w| {
        w.last("public/_online")
            .is_some_and(|m| m.payload["state"] == "offline")
    })
    .await;
    // retained will for a late subscriber
    let late = Watcher::new(broker.port, "public/_online").await;
    late.wait_for("retained will", |w| {
        w.last("public/_online")
            .is_some_and(|m| m.payload["state"] == "offline" && m.retain)
    })
    .await;

    // everything carries a 3 s expiry (interval 1 s): after 5 s the board is gone
    tokio::time::sleep(Duration::from_secs(5)).await;
    let gone = Watcher::new(broker.port, "#").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        gone.all().is_empty(),
        "expired messages must not survive: {:?}",
        gone.all().iter().map(|m| &m.topic).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn victoriametrics_down_gives_unknown_but_stays_online() {
    let dir = scratch("vmdown");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    let _server = Server::start(&dir, broker.port, &vm, 1, "");
    let w = Watcher::new(broker.port, "internal/#").await;
    w.wait_for("ok tile", |w| {
        w.last("internal/srv/ram")
            .is_some_and(|m| m.payload["s"] == "ok")
    })
    .await;
    vm.state.lock().unwrap().fail = true;
    w.wait_for("unknown tile", |w| {
        w.last("internal/srv/ram")
            .is_some_and(|m| m.payload["s"] == "unknown")
    })
    .await;
    let before = online_count(&w, "internal");
    w.wait_for("still online", |w| online_count(w, "internal") > before)
        .await;
    assert_eq!(
        w.last("internal/_online").unwrap().payload["state"],
        "online"
    );
    vm.state.lock().unwrap().fail = false;
    w.wait_for("recovered", |w| {
        w.last("internal/srv/ram")
            .is_some_and(|m| m.payload["s"] == "ok")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generation_mismatch_withholds_tiles() {
    let dir = scratch("genmm");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    let prom = dir.join("prom-manifest.json");
    std::fs::write(
        &prom,
        r#"{"schema":"corbet.obs.prometheus/1","generation":"deadbeef","files":[]}"#,
    )
    .unwrap();
    let _server = Server::start(
        &dir,
        broker.port,
        &vm,
        1,
        &format!("prometheus_manifest = '{}'", prom.display()),
    );
    let w = Watcher::new(broker.port, "internal/#").await;
    w.wait_for("mismatch", |w| {
        w.last("internal/_online")
            .is_some_and(|m| m.payload["state"] == "generation-mismatch")
    })
    .await;
    let n = online_count(&w, "internal");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(n, 0);
    assert!(
        w.on("internal/srv/ram").is_empty(),
        "no tile values while generations disagree"
    );
    assert!(
        w.last("internal/srv/_layout").is_some(),
        "layouts still go out"
    );
    // rules catch up: tiles flow and the board is online again
    let gm = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cgrf/manifest.json"),
    )
    .unwrap();
    let gm: Value = serde_json::from_str(&gm).unwrap();
    std::fs::write(
        &prom,
        json!({"generation": gm["generation"], "files": []}).to_string(),
    )
    .unwrap();
    w.wait_for("recovered", |w| {
        w.last("internal/srv/ram").is_some() && online_count(w, "internal") >= 1
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn republishes_everything_after_broker_restart() {
    let dir = scratch("restart");
    let (p, mp) = (free_port(), free_port());
    let mut broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    let _server = Server::start(&dir, broker.port, &vm, 1, "refresh_intervals = 2");
    let w = Watcher::new(broker.port, "internal/srv/_layout").await;
    w.wait_for("layout", |w| w.last("internal/srv/_layout").is_some())
        .await;
    drop(w);
    broker.restart(); // no persistence: the retained board is gone
    let w2 = Watcher::new(broker.port, "internal/#").await;
    w2.wait_for("board is back", |w| {
        w.last("internal/srv/_layout").is_some()
            && w.last("internal/srv/ram").is_some()
            && w.last("internal/_playlist").is_some()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn public_listener_is_read_only() {
    let dir = scratch("acl");
    let (p, mp) = (free_port(), free_port());
    let broker = Broker::start(&dir, p, mp);
    let vm = Vm::start();
    let _server = Server::start(&dir, broker.port, &vm, 1, "");
    let local = Watcher::new(broker.port, "#").await;
    let web = Watcher::new(broker.mounted_port, "#").await;
    web.wait_for("public board", |w| w.last("srv/ram").is_some())
        .await;
    // an anonymous visitor tries to write: to a tile, and to a new topic
    let mut o = MqttOptions::new("intruder", "127.0.0.1", broker.mounted_port);
    o.set_keep_alive(Duration::from_secs(5));
    let (c, mut ev) = AsyncClient::new(o, 10);
    tokio::spawn(async move { while ev.poll().await.is_ok() {} });
    c.publish(
        "srv/ram",
        QoS::AtLeastOnce,
        true,
        r#"{"v":1,"d":"HACKED","c":"green","s":"ok","t":1}"#,
    )
    .await
    .unwrap();
    c.publish("evil", QoS::AtLeastOnce, true, "x")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        local
            .all()
            .iter()
            .all(|m| m.topic != "public/evil" && !m.payload.to_string().contains("HACKED"))
    );
}
