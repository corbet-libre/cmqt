import mqtt from "mqtt";

import {
  colourClass,
  freshness,
  parseHost,
  screenUrl,
  stackOrder,
  type HostInfo,
  type Layout,
  type Online,
  type TileMsg,
} from "./logic.ts";
import { drawTopology, paintNode } from "./topology.ts";

const app = document.getElementById("app") as HTMLElement;
const overlay = document.getElementById("overlay") as HTMLElement;

// The browser derives everything from its own host name. For local work the
// query string can stand in: ?screen=srv&tld=internal&broker=ws://127.0.0.1:9001
const q = new URLSearchParams(location.search);
const fromHost = parseHost(location.hostname);
const host: HostInfo | null =
  fromHost ?? (q.get("screen") ? { screen: q.get("screen") as string, tld: q.get("tld") === "ch" ? "ch" : "internal" } : null);
const brokerUrl = q.get("broker") ?? `wss://${location.host}`;

let online: Online | null = null;
let onlineAt: number | null = null; // performance.now() at the last _online
let layout: Layout | null = null;
const tiles = new Map<string, TileMsg>();
const nodes = new Map<string, HTMLElement>();
let connected = false;
let topologySvg: SVGSVGElement | null = null;

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function showOverlay(text: string | null): void {
  overlay.hidden = text === null;
  overlay.textContent = text ?? "";
  app.classList.toggle("blanked", text !== null);
}

function buildTile(t: Layout["tiles"][number], tld: HostInfo["tld"]): HTMLElement {
  const href = t.link ? screenUrl(t.link, tld) : null;
  const root = document.createElement(href ? "a" : "div");
  if (href) (root as HTMLAnchorElement).href = href;
  root.className = "tile";
  root.style.setProperty("--x", String(t.grid.x));
  root.style.setProperty("--y", String(t.grid.y));
  root.style.setProperty("--w", String(t.grid.w));
  // Very tall panels are capped: the board is a status view, not a canvas.
  root.style.setProperty("--h", String(Math.min(t.grid.h, 12)));
  const title = document.createElement("span");
  title.className = "tile-title";
  title.textContent = t.title;
  const value = document.createElement("span");
  value.className = "tile-value";
  value.textContent = "…";
  const mark = document.createElement("span");
  mark.className = "tile-mark";
  mark.hidden = true;
  mark.textContent = "issue";
  root.append(title, value, mark);
  return root;
}

function renderLayout(l: Layout, h: HostInfo): void {
  app.replaceChildren();
  nodes.clear();
  document.title = l.title;
  const board = document.createElement("main");
  board.className = l.greyed ? "board greyed" : "board";
  const h1 = document.createElement("h1");
  h1.textContent = l.title;
  app.append(h1);
  topologySvg = null;
  if (l.topology) {
    topologySvg = drawTopology(l.topology, h.tld);
    app.append(topologySvg);
  }
  for (const t of stackOrder(l.tiles)) {
    const n = buildTile(t, h.tld);
    nodes.set(t.id, n);
    board.append(n);
  }
  app.append(board);
  for (const id of nodes.keys()) paintTile(id);
  for (const [id, m] of tiles) paintNodeTile(id, m);
}

function paintNodeTile(id: string, m: TileMsg): void {
  if (topologySvg && !nodes.has(id)) paintNode(topologySvg, id, m.s, m.d);
}

function paintTile(id: string): void {
  const n = nodes.get(id);
  const m = tiles.get(id);
  if (!n) return;
  const value = n.querySelector(".tile-value") as HTMLElement;
  const mark = n.querySelector(".tile-mark") as HTMLElement;
  n.className = n.className.replace(/\b(c-\w+|s-\w+)\b/g, "").replace(/\s+/g, " ").trim();
  if (!m) {
    value.textContent = "…";
    mark.hidden = true;
    return;
  }
  n.classList.add(colourClass(m.c), `s-${m.s}`);
  value.textContent = m.d;
  mark.hidden = m.s !== "issue";
}

/** Re-evaluate liveness; runs on every message and once a second. */
function evaluate(): void {
  if (!connected) return showOverlay("observability unreachable");
  const since = onlineAt === null ? null : performance.now() - onlineAt;
  const f = freshness(since, online);
  if (f === "unreachable") showOverlay("observability unreachable");
  else if (f === "updating") showOverlay("updating");
  else showOverlay(null);
}

function main(): void {
  if (!host) {
    showOverlay("unknown screen");
    return;
  }
  showOverlay("observability unreachable");
  const client = mqtt.connect(brokerUrl, { protocolVersion: 5, reconnectPeriod: 2000, connectTimeout: 8000, clean: true });
  client.on("connect", () => {
    connected = true;
    client.subscribe([`${host.screen}/#`, "_online"], { qos: 1 });
    evaluate();
  });
  const down = (): void => {
    connected = false;
    onlineAt = null; // a retained _online after a reconnect must not look live
    evaluate();
  };
  client.on("close", down);
  client.on("offline", down);
  client.on("error", down);
  client.on("message", (topic, payload) => {
    let data: unknown;
    try {
      data = JSON.parse(payload.toString());
    } catch {
      return;
    }
    if (topic === "_online") {
      online = data as Online;
      onlineAt = performance.now();
    } else if (topic === `${host.screen}/_layout`) {
      layout = data as Layout;
      renderLayout(layout, host);
    } else if (topic.startsWith(`${host.screen}/`)) {
      const id = topic.slice(host.screen.length + 1);
      tiles.set(id, data as TileMsg);
      paintTile(id);
      paintNodeTile(id, data as TileMsg);
    }
    evaluate();
  });
  setInterval(evaluate, 1000);
}

main();
