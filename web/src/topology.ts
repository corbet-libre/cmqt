// Topology geometry (pure) and SVG drawing.

import type { Topology } from "./logic.ts";
import { screenUrl, type Tld } from "./logic.ts";

export const BOX_W = 120;
export const BOX_H = 44;
const GAP_X = 24;
const GAP_Y = 24;
const PAD = 16;
const GROUP_HEAD = 22;
const GROUP_ORDER = ["helsinki", "frankfurt", "home", "outposts", "vultr"];

export interface Box {
  device: string;
  x: number;
  y: number;
  planned: boolean;
  greyed: boolean;
}

export interface Shape {
  width: number;
  height: number;
  boxes: Box[];
  groupLabels: { name: string; x: number; y: number }[];
  edges: { x1: number; y1: number; x2: number; y2: number; kind: string }[];
  brackets: { name: string; x: number; y: number; w: number; h: number }[];
}

/** Place every node by its group, row and column; groups are stacked in a fixed order. */
export function layoutTopology(t: Topology): Shape {
  const groups = [...new Set(t.nodes.map((n) => n.group))].sort((a, b) => {
    const ia = GROUP_ORDER.indexOf(a);
    const ib = GROUP_ORDER.indexOf(b);
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || a.localeCompare(b);
  });
  const boxes: Box[] = [];
  const groupLabels: Shape["groupLabels"] = [];
  let y = PAD;
  let width = 2 * PAD;
  for (const g of groups) {
    const nodes = t.nodes.filter((n) => n.group === g);
    const rows = Math.max(...nodes.map((n) => n.row)) + 1;
    groupLabels.push({ name: g, x: PAD, y: y + 14 });
    const top = y + GROUP_HEAD;
    for (const n of nodes) {
      boxes.push({
        device: n.device,
        x: PAD + n.col * (BOX_W + GAP_X),
        y: top + n.row * (BOX_H + GAP_Y),
        planned: n.planned === true,
        greyed: n.greyed === true,
      });
      width = Math.max(width, PAD + (n.col + 1) * (BOX_W + GAP_X) - GAP_X + PAD);
    }
    y = top + rows * (BOX_H + GAP_Y);
  }
  const at = new Map(boxes.map((b) => [b.device, b]));
  const edges: Shape["edges"] = [];
  for (const e of t.edges) {
    const a = at.get(e.from);
    const b = at.get(e.to);
    if (!a || !b) continue;
    edges.push({
      x1: a.x + BOX_W / 2,
      y1: a.y + BOX_H / 2,
      x2: b.x + BOX_W / 2,
      y2: b.y + BOX_H / 2,
      kind: e.kind ?? "",
    });
  }
  const brackets: Shape["brackets"] = [];
  for (const br of t.brackets) {
    const m = br.members.map((d) => at.get(d)).filter((b): b is Box => b !== undefined);
    if (m.length === 0) continue;
    const x1 = Math.min(...m.map((b) => b.x)) - 6;
    const y1 = Math.min(...m.map((b) => b.y)) - 6;
    const x2 = Math.max(...m.map((b) => b.x + BOX_W)) + 6;
    const y2 = Math.max(...m.map((b) => b.y + BOX_H)) + 6;
    brackets.push({ name: br.name, x: x1, y: y1, w: x2 - x1, h: y2 - y1 });
  }
  return { width, height: y + PAD, boxes, groupLabels, edges, brackets };
}

const NS = "http://www.w3.org/2000/svg";

function el(name: string, attrs: Record<string, string | number>): SVGElement {
  const e = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
  return e;
}

/** Draw the topology; every box is a real link to its device screen. */
export function drawTopology(t: Topology, tld: Tld): SVGSVGElement {
  const s = layoutTopology(t);
  const svg = el("svg", {
    viewBox: `0 0 ${s.width} ${s.height}`,
    class: "topology",
    role: "img",
    "aria-label": "Topology",
  }) as SVGSVGElement;
  for (const b of s.brackets) {
    svg.append(el("rect", { x: b.x, y: b.y, width: b.w, height: b.h, rx: 8, class: "bracket" }));
    const label = el("text", { x: b.x + 6, y: b.y - 2, class: "bracket-label" });
    label.textContent = b.name;
    svg.append(label);
  }
  for (const g of s.groupLabels) {
    const label = el("text", { x: g.x, y: g.y, class: "group-label" });
    label.textContent = g.name;
    svg.append(label);
  }
  for (const e of s.edges) svg.append(el("line", { x1: e.x1, y1: e.y1, x2: e.x2, y2: e.y2, class: "edge" }));
  for (const b of s.boxes) {
    const href = screenUrl(b.device, tld);
    const g = href ? el("a", { href, class: "node-link" }) : el("g", {});
    const cls = ["node", b.planned ? "planned" : "", b.greyed ? "greyed" : ""].join(" ").trim();
    g.append(el("rect", { x: b.x, y: b.y, width: BOX_W, height: BOX_H, rx: 6, class: cls }));
    const text = el("text", { x: b.x + BOX_W / 2, y: b.y + BOX_H / 2 + 5, "text-anchor": "middle", class: "node-label" });
    text.textContent = b.device;
    g.append(text);
    svg.append(g);
  }
  return svg;
}
