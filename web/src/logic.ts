// Pure logic of the status board client: no DOM, no network. Mirrors the Rust
// crate `cmqt` (payload types, staleness rule).

export type TileState = "ok" | "issue" | "unknown" | "greyed";

export interface TileMsg {
  v: number | null;
  d: string;
  c: string;
  s: TileState;
  t: number;
}

export interface Grid {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface LayoutTile {
  id: string;
  title: string;
  grid: Grid;
  unit?: string | null;
  link?: string | null;
}

export interface TopologyNode {
  device: string;
  group: string;
  row: number;
  col: number;
  planned?: boolean;
  greyed?: boolean;
}

export interface Topology {
  nodes: TopologyNode[];
  edges: { from: string; to: string; kind?: string }[];
  brackets: { name: string; members: string[] }[];
}

export interface Layout {
  schema: string;
  generation: string;
  screen: string;
  title: string;
  kind: string;
  greyed: boolean;
  tiles: LayoutTile[];
  topology?: Topology;
}

export interface Online {
  state: "online" | "offline" | "generation-mismatch";
  t?: number;
  interval_s?: number;
  generation?: string;
}

export type Tld = "ch" | "internal";

export interface HostInfo {
  screen: string;
  tld: Tld;
}

const SCREEN_NAME = /^[a-z0-9][a-z0-9.-]*$/;

/** `status.corbet.<ch|internal>/<screen>`; the bare path is the overview. */
export function parseHost(hostname: string, pathname = "/"): HostInfo | null {
  const m = /^status\.corbet\.(ch|internal)$/.exec(hostname.toLowerCase());
  if (!m) return null;
  const screen = pathname.replace(/^\/+|\/+$/g, "") || "overview";
  if (!SCREEN_NAME.test(screen)) return null;
  return { screen, tld: m[1] as Tld };
}

/** URL of another screen on the same door (a path); null for names that are not screens. */
export function screenUrl(screen: string, _tld: Tld): string | null {
  if (!SCREEN_NAME.test(screen)) return null;
  return screen === "overview" ? "/" : `/${screen}`;
}

export type Freshness = "fresh" | "unreachable" | "updating";

/**
 * Liveness comes from `_online` alone (contract §5). `sinceOnlineMs` is the time
 * since `_online` was last RECEIVED, on the browser's monotonic clock
 * (performance.now()); the server's clock is never compared.
 */
export function freshness(sinceOnlineMs: number | null, online: Online | null): Freshness {
  if (!online || sinceOnlineMs === null) return "unreachable";
  if (online.state === "offline") return "unreachable";
  if (online.state === "generation-mismatch") return "updating";
  const interval = online.interval_s ?? 0;
  if (interval <= 0 || sinceOnlineMs > 3 * interval * 1000) return "unreachable";
  return "fresh";
}

const KNOWN_COLOURS = new Set(["green", "yellow", "orange", "red", "blue", "purple", "grey"]);

/** Map a Grafana colour name to one of our CSS classes. */
export function colourClass(c: string): string {
  const base = c.replace(/^(dark|light|semi-dark|super-light)-/, "");
  return KNOWN_COLOURS.has(base) ? `c-${base}` : "c-none";
}

/** Stack order for the single-column phone layout: top to bottom, left to right. */
export function stackOrder(tiles: LayoutTile[]): LayoutTile[] {
  return [...tiles].sort((a, b) => a.grid.y - b.grid.y || a.grid.x - b.grid.x);
}
