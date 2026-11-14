// Bundle src/main.ts with mqtt.js into dist/ next to index.html and style.css.
import { copyFileSync, mkdirSync, statSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { readFileSync } from "node:fs";
import { build } from "esbuild";

mkdirSync("dist", { recursive: true });
await build({
  entryPoints: ["src/main.ts"],
  bundle: true,
  minify: true,
  format: "esm",
  target: "es2022",
  platform: "browser",
  outfile: "dist/app.js",
  legalComments: "none",
});
for (const f of ["index.html", "style.css"]) copyFileSync(f, `dist/${f}`);
const raw = statSync("dist/app.js").size;
const gz = gzipSync(readFileSync("dist/app.js")).length;
console.log(`dist/app.js ${raw} bytes, ${gz} bytes gzipped`);
