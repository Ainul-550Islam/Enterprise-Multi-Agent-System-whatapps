#!/usr/bin/env node
"use strict";

const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const web = path.resolve(__dirname, "..");
const committed = path.join(web, "src/lib/api/schema.d.ts");
const script = path.join(web, "scripts/gen-api-client.sh");

if (!fs.existsSync(committed)) {
  console.error("check:api:up-to-date: src/lib/api/schema.d.ts is missing — run npm run gen-api-client");
  process.exit(1);
}

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "mas-api-schema-"));
const backup = path.join(tmpDir, "schema.d.ts");
fs.copyFileSync(committed, backup);
try {
  execFileSync("bash", [script], { stdio: "inherit", cwd: web });
  const expected = fs.readFileSync(committed, "utf8");
  const previous = fs.readFileSync(backup, "utf8");
  if (expected !== previous) {
    fs.copyFileSync(backup, committed);
    console.error("check:api:up-to-date: src/lib/api/schema.d.ts is stale. Run npm run gen-api-client and commit the result.");
    process.exit(1);
  }
  console.log("check:api:up-to-date: schema.d.ts matches openapi.yaml");
} catch (error) {
  if (fs.existsSync(backup)) {
    fs.copyFileSync(backup, committed);
  }
  throw error;
} finally {
  fs.rmSync(tmpDir, { recursive: true, force: true });
}
