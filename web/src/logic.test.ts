import assert from "node:assert/strict";
import { test } from "node:test";

import { colourClass, freshness, parseHost, screenUrl, stackOrder } from "./logic.ts";
import { layoutTopology } from "./topology.ts";

test("screen comes from the path", () => {
  assert.deepEqual(parseHost("status.corbet.ch", "/"), { screen: "overview", tld: "ch" });
  assert.deepEqual(parseHost("status.corbet.internal", "/fra-0"), { screen: "fra-0", tld: "internal" });
  assert.deepEqual(parseHost("status.corbet.ch", "/site.fra.brh/"), { screen: "site.fra.brh", tld: "ch" });
  assert.equal(parseHost("example.com", "/"), null);
  assert.equal(parseHost("evil.status.corbet.ch", "/"), null);
  assert.equal(parseHost("status.corbet.ch", "/a/b"), null);
});

test("links are paths on the same door and never from odd names", () => {
  assert.equal(screenUrl("srv", "ch"), "/srv");
  assert.equal(screenUrl("overview", "internal"), "/");
  assert.equal(screenUrl("javascript:alert(1)", "ch"), null);
  assert.equal(screenUrl("a/b", "ch"), null);
});

test("liveness: offline, mismatch, time since _online was received", () => {
  const on = { state: "online" as const, t: 1000, interval_s: 30 };
  assert.equal(freshness(10_000, on), "fresh");
  assert.equal(freshness(90_000, on), "fresh");
  assert.equal(freshness(90_001, on), "unreachable");
  assert.equal(freshness(null, on), "unreachable", "never received");
  assert.equal(freshness(1000, null), "unreachable");
  assert.equal(freshness(1000, { state: "offline" }), "unreachable");
  assert.equal(freshness(1000, { state: "generation-mismatch" }), "updating");
});

test("colours map to classes", () => {
  assert.equal(colourClass("green"), "c-green");
  assert.equal(colourClass("dark-red"), "c-red");
  assert.equal(colourClass("#ff00ff"), "c-none");
});

test("phone stack order is top to bottom, left to right", () => {
  const g = (x: number, y: number) => ({ x, y, w: 8, h: 4 });
  const ids = stackOrder([
    { id: "c", title: "", grid: g(8, 4) },
    { id: "b", title: "", grid: g(8, 0) },
    { id: "a", title: "", grid: g(0, 0) },
  ]).map((t) => t.id);
  assert.deepEqual(ids, ["a", "b", "c"]);
});

test("topology places boxes by group, row and column and drops dangling edges", () => {
  const s = layoutTopology({
    nodes: [
      { device: "fra-0", group: "frankfurt", row: 0, col: 0 },
      { device: "fra-1", group: "frankfurt", row: 0, col: 1, planned: true },
      { device: "srv", group: "home", row: 0, col: 0 },
    ],
    edges: [
      { from: "fra-0", to: "fra-1" },
      { from: "fra-0", to: "ghost" },
    ],
    brackets: [{ name: "k8s", members: ["fra-0", "fra-1", "nobody"] }],
  });
  assert.equal(s.boxes.length, 3);
  const [a, b] = s.boxes;
  assert.ok(a && b && b.x > a.x && a.y === b.y);
  const srv = s.boxes.find((x) => x.device === "srv");
  assert.ok(srv && srv.y > a.y, "home is below frankfurt");
  assert.equal(s.edges.length, 1);
  assert.equal(s.brackets.length, 1);
  assert.equal(s.boxes.find((x) => x.device === "fra-1")?.planned, true);
});
