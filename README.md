# cmqt

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

* `issue`: one of the panel's `corbet.alerts` fires for the screen's `host_name`
* `greyed`: greyed screens, never queried
* `unknown`: no value, or VictoriaMetrics unreachable (then `_online` stays `online`)

Publishing: retained, QoS 1, MQTT 5 message expiry 3 x `interval_s`, only when
`d`, `c` or `s` changed. Everything is re-sent after a (re)connect. Each tree
has its own connection and its own retained last will on `_online`.
`_layout` (grid from `gridPos`, links, topology), `_playlist` (issues first)
and `_online` (every interval, with the generation id) are published as in §5.
On a generation mismatch (dashboards vs `prometheus_manifest`) `_online` says
`generation-mismatch` and no tile values go out.

The public tree is built from `dashboards/public/` only. As defence in depth,
layout links and topology members that point at screens missing from the
public tree are removed before publishing.

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

## CONTRACT QUESTIONs

| # | Gap | Smallest choice made |
|---|---|---|
| 1 | §5 says a tile is published only on change and every message expires after 3 x `interval_s`; a quiet tile would expire from the broker and go stale in the client after 90 s. | Unchanged messages (tiles, layouts, playlist) are re-sent every `refresh_intervals` (default 2, max 2) intervals with a fresh `t`. `_online` is sent every interval as specified. |
| 2 | "fires for that screen's `host_name`" is clear on device screens only. | Device screens match on the screen name. Other kinds (site, overview, topology) match on the panel's `link` device, or on any host when there is no link. |
| 3 | Recording rule labels are `screen` and `tile` only, but `overview` and `topology` exist in both doors with different content, so two panels would write the same series. | cmqt reads an optional `door` label on the series first (`record.labels.door`), then the unlabelled series. cgrf/cprm should add `door`. |
| 4 | How cmqt learns the rules' generation is not defined. | Config `prometheus_manifest` points at cprm's `prometheus/manifest.json`; its `generation` is compared each interval. An unreadable file counts as a mismatch. |
| 5 | The topology canvas has no per-device state, so its boxes cannot be coloured. | Boxes link to the device screens and show planned (dashed) and greyed; no state colour. cgrf could add one tile per device. |
| 6 | `d` and `c` for `unknown`/`greyed` and the `v` type are not defined. | `v` is `null`, `d` is `n/a`, `c` is `grey`. |
| 7 | Staleness compares the client clock with the server's `t`. | Implemented as written; a client clock off by more than 90 s shows "observability unreachable". |
| 8 | The web client also treats an old `_online.t` as unreachable (retained `online` would otherwise outlive a dead server by up to 3 intervals only via expiry). | Added: `now - _online.t > 3 x interval_s` is stale. |
