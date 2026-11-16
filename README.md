# cmqt

<!-- ccid:security:begin (rendered by ccid render; do not edit by hand) -->
**Security and dependencies:** watched by Renovate (with OSV vulnerability alerts); an update is merged only when its tests pass. See the [security policy](SECURITY.md).
<!-- ccid:security:end -->

The Corbet status board over MQTT: a server that turns cgrf dashboards and
VictoriaMetrics results into retained MQTT topics, a small library with the
payload types, and a static browser client.

Contract: `corbet.obs` contracts v1 (§1 generation, §3 dashboards, §5 MQTT,
§6 failure matrix). English only.

| Part | Path | What |
|---|---|---|
| `cmqt` | `crates/cmqt` | Payload types, topic names, staleness rule, optional subscribe helper (feature `client`). No server dependencies. |
| `cmqt-server` | `crates/cmqt-server` | `cmqt-server <config.toml>`: two broker connections (`public/`, `internal/`), evaluation every `interval_s`. |
| web | `web/` | TypeScript, mqtt.js over WebSocket, esbuild. `npm run typecheck`, `npm test`, `npm run build`. |
| deploy | `deploy/` | `mosquitto.conf`, `public.acl`, `internal.acl`, `cmqt.example.toml`. |

## What the server does

Every `interval_s` it re-reads the manifest (a new generation replaces the
board, a broken one is rejected and the old one keeps running), runs two
instant queries (`corbet:tile:value`, `ALERTS{alertstate="firing"}`), and per
tile computes `v`, `d` (unit and decimals), `c` (threshold colour) and `s`:

* `issue`: one of the panel's `corbet.alerts` fires with the panel's `corbet.scope` labels (`*` in `alerts`: any alert name);
  an attention rule (`kind="attention"`) is never an issue, not on a tile and not on a topology node
* `greyed`: greyed screens, never queried
* `unknown`: no value, or VictoriaMetrics unreachable (then `_online` stays `online`, with `degraded`)

Publishing: retained, QoS 1, only when `d`, `c` or `s` changed (a tile's `t` is
the time of its last change; nothing is re-sent periodically). Only `_online`
carries a message expiry (3 x `interval_s`) and goes out every interval. Everything is re-sent after a (re)connect. Each tree
has its own connection and its own retained last will on `_online`.
`_layout` (grid from `gridPos`, links, topology), `_playlist` (issues first)
and `_online` (every interval, with the generation id) are published as in §5.
On a generation mismatch (dashboards vs `prometheus_manifest`) `_online` says
`generation-mismatch` and no tile values go out.

`_online` carries `degraded` while the box publishes but cannot tell the truth (`cmqt::degraded`): `query` (VictoriaMetrics does
not answer), `rules` (it answers but holds no `corbet:tile:value` although the board has tiles: vmalert does not evaluate), `check`
(the optional `degraded_query` returned a series: a check of the alert path itself, such as Alertmanager down or vmalert send
errors). `state` stays `online`: screens stay live and their tiles carry their own `unknown`. A change of `degraded` publishes
`_online` at once. The relay's box-silence watcher (`cmqt flush`) treats a box degraded for `silence_s` like a silent one: one ntfy
message, one when it is back.

The public tree is built from `dashboards/public/` only. As defence in depth,
layout links and topology members that point at screens missing from the
public tree are removed before publishing.

## Relay: `cmqt flush` and `cmqt unseal`

Binary `cmqt` (crate `cmqt-relay`) carries OTLP metrics one way from a
durable RabbitMQ queue to VictoriaMetrics over sealed UDP/IPv6. Format:
[docs/relay-datagram.md](docs/relay-datagram.md).

| Command | Runs on | Config |
|---|---|---|
| `cmqt flush <flush.toml>` | relay (no root, 256 MiB) | `deploy/flush.example.toml` |
| `cmqt unseal <unseal.toml>` | box (IPv6 only) | `deploy/unseal.example.toml` |

Both expose Prometheus text counters on `metrics_listen`. The receive buffer
is clamped by `net.core.rmem_max`; the box must raise it to at least `rcvbuf`
(default 8 MiB), and `cmqt unseal` warns at start when it is clamped. The
queue must be declared durable beforehand (the Collector's `rabbitmqexporter`
creates none). Decoding is automatic: protobuf or OTLP JSON.

## Web client

Screen and door come from `location.hostname` (`<screen>.status.corbet.<ch|internal>`,
bare name = `overview`); it connects to `wss://<same host>`, subscribes to
`<screen>/#` and `_online`, draws a 24-column CSS grid (one column below 600 px),
real `<a href>` links, topology as SVG with every box linking to its device
screen. Stale (`now - t > 3 x interval_s`), offline or never connected: a
full-board "observability unreachable"; generation mismatch: "updating". For
local work `?screen=srv&tld=internal&broker=ws://127.0.0.1:9001` replaces the host name.

Bundle: see the build output (`dist/app.js`, about 344 KB, 108 KB gzipped, almost
all of it mqtt.js).

## Tests

```
systemd-run --user --scope -p CPUWeight=50 -- cargo test --workspace   # needs Nix for Mosquitto, or MOSQUITTO_BIN
cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings
cd web && npm run typecheck && npm test && npm run build
```

The integration test starts a real Mosquitto and a fake VictoriaMetrics and
checks retained topics, publish-on-change, refresh, last will on SIGKILL,
expiry, VictoriaMetrics down, generation mismatch, republish after a broker
restart, public/internal separation and the read-only public ACL.

## State rules (contract §3, §3b)

* Alerts match on `alertname` in the panel's `alerts` AND every label of its `scope` (a value may be a list, any of).
* Device tiles: `issue` on a match, `ok` with a value, else `unknown`. Aggregate tiles (site, overview, topology): the state comes from the match alone; their value is shown on the internal tree only.
* Public tree: state only. `v` null, `d` is `OK`, `Issue` or `No data`, colour from the state. Public panels mirror the internal series through their `record` labels.
* An aggregate tile whose devices are all greyed is `greyed`. Never green without data (contract section 3): an aggregate tile or topology node is `unknown`, not `ok`, while any non-greyed device it stands for (its `host_name` scope, or the devices of the site screen it links to) has no live `alive` value, until an alert fires (`issue`). Device tiles without a value are `unknown`.
* Topology: one tile `topology/<device>` per node, from the firing alerts with that `host_name`; `greyed` for greyed nodes, none for planned nodes. The web client colours the SVG boxes with them.
* The integration tests run against the real cgrf golden dashboards (`crates/cmqt-server/tests/fixtures/cgrf`, copied from cgrf commit in `FROM_CGRF_COMMIT`).

## Liveness (contract §5)

Liveness comes from `_online` alone. A client measures the time since it last
RECEIVED `_online` on its own monotonic clock (`performance.now()`, or `Instant`
in `cmqt::freshness`); more than 3 x `interval_s`, no `_online`, or a state other
than `online` means "observability unreachable" over the whole board;
`generation-mismatch` shows "updating". The server's clock is never compared. `degraded` does not change liveness (the tiles
already say `unknown` where data is missing); it is for the relay's watcher.

## Browser check

`web/shots.mjs` runs the real client in headless Chromium against a real Mosquitto,
the real `cmqt-server` and a fake VictoriaMetrics on the cgrf golden dashboards, and
saves screenshots (1920x1080 and 390x844: device screen, overview, topology, public
overview, and the board after the server is killed):

```
cargo build -p cmqt-server && (cd web && npm run build && node shots.mjs <out-dir>)
```

Notes: the page caps very tall panels at 12 grid rows; topology rows are squeezed to
ranks within each group; on a phone the topology SVG scales down (small text).
mqtt.js ships no smaller browser build (`mqtt.min.js` is the same size as the ESM build).

## CONTRACT QUESTIONs

All earlier questions were answered by the contract (§5 liveness, §3 scope, §3b public
panels, topology tiles). Open: none beyond these small choices.

| # | Gap | Smallest choice made |
|---|---|---|
| 1 | `d`, `c` and `v` for internal `unknown`/`greyed` tiles. | `v` null, `d` `n/a`, `c` `grey` (or the state colour when only the value is missing). |
| 2 | A device tile without `scope`. | Falls back to `host_name` = the screen name. |
| 3 | An alert that lacks a scope label. | A missing label never matches. |
