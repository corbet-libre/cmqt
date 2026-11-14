// Browser check: runs the REAL web client in headless Chromium against a real
// Mosquitto, the real cmqt-server and a fake VictoriaMetrics, on the real cgrf
// dashboards, and saves screenshots. Local instances only.
//
//   cargo build -p cmqt-server && npm run build && node shots.mjs <out-dir>
//
// Env: MOSQUITTO_BIN (default: nix build nixpkgs#mosquitto), CHROMIUM (default /usr/bin/chromium).
import { spawn, execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { createServer as netServer } from "node:net";
import { join, resolve } from "node:path";
import { chromium } from "playwright-core";

const out = resolve(process.argv[2] ?? "shots");
mkdirSync(out, { recursive: true });
const root = resolve(import.meta.dirname, "..");
const fixtures = join(root, "crates/cmqt-server/tests/fixtures/cgrf");
const tmp = join(out, "work");
mkdirSync(tmp, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function freePort() {
  return new Promise((res) => {
    const s = netServer().listen(0, "127.0.0.1", () => {
      const p = s.address().port;
      s.close(() => res(p));
    });
  });
}

const mosquitto =
  process.env.MOSQUITTO_BIN ??
  join(execFileSync("nix", ["build", "nixpkgs#mosquitto", "--no-link", "--print-out-paths"]).toString().split("\n")[0], "bin/mosquitto");

const [pBroker, pPub, pInt, pVm, pWeb] = await Promise.all([1, 2, 3, 4, 5].map(freePort));
const kids = [];
const run = (cmd, args, opts = {}) => {
  const c = spawn(cmd, args, { stdio: "ignore", ...opts });
  kids.push(c);
  return c;
};

// --- broker: local listener plus one WebSocket listener per door
writeFileSync(
  join(tmp, "mosquitto.conf"),
  `per_listener_settings true
persistence false
listener ${pBroker} 127.0.0.1
allow_anonymous true
listener ${pPub} 127.0.0.1
protocol websockets
allow_anonymous true
mount_point public/
acl_file ${join(root, "deploy/public.acl")}
listener ${pInt} 127.0.0.1
protocol websockets
allow_anonymous true
mount_point internal/
acl_file ${join(root, "deploy/internal.acl")}
`,
);
run(mosquitto, ["-c", join(tmp, "mosquitto.conf")]);

// --- fake VictoriaMetrics: a value for every internal recorded series
const manifest = JSON.parse(readFileSync(join(fixtures, "manifest.json"), "utf8"));
const series = [];
for (const s of manifest.screens.filter((x) => x.door === "internal")) {
  const d = JSON.parse(readFileSync(join(fixtures, s.file), "utf8"));
  let i = 0;
  for (const p of d.panels) {
    const c = p.corbet ?? {};
    if (!c.record) continue;
    const unit = p.fieldConfig?.defaults?.unit;
    const v = unit === "percent" ? 20 + ((s.name.length * 7 + i * 13) % 55) : unit === "none" ? 1 : unit === "ops" ? 0.02 : 0;
    series.push({ metric: { screen: c.record.labels.screen, tile: c.record.labels.tile }, value: [0, String(v)] });
    i++;
  }
}
const alerts = [
  { alertname: "DeviceDiskFull", host_name: "srv", site: "0x2A", alertstate: "firing" },
  { alertname: "DeviceMemoryLow", host_name: "fra-1", site: "0x20", alertstate: "firing" },
];
createServer((req, res) => {
  const q = new URL(req.url, "http://x").searchParams.get("query") ?? "";
  const result = q.includes("ALERTS") ? alerts.map((metric) => ({ metric, value: [0, "1"] })) : series;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify({ status: "success", data: { resultType: "vector", result } }));
}).listen(pVm, "127.0.0.1");

// --- static client
const mime = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" };
createServer((req, res) => {
  const f = join(root, "web/dist", req.url.split("?")[0] === "/" ? "index.html" : req.url.split("?")[0]);
  if (!existsSync(f)) return res.writeHead(404).end();
  res.setHeader("content-type", mime[f.slice(f.lastIndexOf("."))] ?? "application/octet-stream");
  res.end(readFileSync(f));
}).listen(pWeb, "127.0.0.1");

await sleep(1000);
writeFileSync(
  join(tmp, "cmqt.toml"),
  `dashboards_dir = '${fixtures}'\nvictoria_url = 'http://127.0.0.1:${pVm}'\nbroker_port = ${pBroker}\ninterval_s = 2\n`,
);
const server = run(join(root, "target/debug/cmqt-server"), [join(tmp, "cmqt.toml")]);
await sleep(3500);

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM ?? "/usr/bin/chromium", args: ["--no-sandbox"] });
const sizes = { desktop: { width: 1920, height: 1080 }, phone: { width: 390, height: 844 } };
const url = (screen, door) =>
  `http://127.0.0.1:${pWeb}/?screen=${encodeURIComponent(screen)}&tld=${door === "public" ? "ch" : "internal"}&broker=ws://127.0.0.1:${door === "public" ? pPub : pInt}`;

async function shoot(name, screen, door, size, extra) {
  const ctx = await browser.newContext({ viewport: sizes[size], colorScheme: "light" });
  const page = await ctx.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  await page.goto(url(screen, door));
  if (extra) await extra(page);
  else await page.waitForFunction(() => document.querySelector(".tile-value, .topology") && document.getElementById("overlay").hidden, null, { timeout: 15000 });
  await sleep(600);
  const file = join(out, `${name}-${size}.png`);
  await page.screenshot({ path: file, fullPage: true });
  const info = await page.evaluate(() => ({
    overlay: document.getElementById("overlay").hidden ? null : document.getElementById("overlay").textContent,
    tiles: document.querySelectorAll(".tile").length,
    links: document.querySelectorAll("a.tile").length,
    nodeLinks: document.querySelectorAll("a.node-link").length,
    issues: document.querySelectorAll(".s-issue").length,
    hScroll: document.documentElement.scrollWidth > document.documentElement.clientWidth,
  }));
  console.log(name, size, JSON.stringify(info), errors.length ? errors : "");
  await ctx.close();
}

for (const size of Object.keys(sizes)) {
  await shoot("device-srv", "srv", "internal", size);
  await shoot("overview", "overview", "internal", size);
  await shoot("topology", "topology", "internal", size);
  await shoot("public-overview", "overview", "public", size);
}

// --- server killed: open a live page first, then SIGKILL the server
for (const size of Object.keys(sizes)) {
  const ctx = await browser.newContext({ viewport: sizes[size] });
  const page = await ctx.newPage();
  await page.goto(url("srv", "internal"));
  if (size === "desktop") {
    await page.waitForFunction(() => document.querySelector(".tile-value") && document.getElementById("overlay").hidden, null, { timeout: 15000 });
    server.kill("SIGKILL");
    await sleep(1000);
  }
  await page.waitForFunction(() => !document.getElementById("overlay").hidden, null, { timeout: 15000 });
  await sleep(300);
  await page.screenshot({ path: join(out, `server-killed-${size}.png`) });
  console.log("server-killed", size, await page.evaluate(() => document.getElementById("overlay").textContent));
  await ctx.close();
}
await browser.close();
for (const k of kids) k.kill("SIGKILL");
process.exit(0);
