#!/usr/bin/env node
/**
 * Hermy HQ ↔ Hermes bridge.
 *
 * Runs on the Mac mini where Hermes lives. Talks to the shared Postgres
 * (the same DATABASE_URL the website uses) — nothing is exposed to the
 * internet. Two jobs:
 *
 *   PULL  (Hermes → website): mirror the kanban board into HermesTask,
 *         cron list + health into DataStore, and emit activity events.
 *   PUSH  (website → Hermes): pick up AgentRequest rows that are `queued`
 *         (safe) or `approved` (human-approved side-effecting), send them to the
 *         profile as a chat completion, and write results back.
 *
 * Two transports, deliberately:
 *   - the `hermes` CLI for PULL/admin (kanban, cron, insights, profile list) —
 *     it only reads files under HERMES_HOME, which this container mounts;
 *   - the gateway's OpenAI-compatible HTTP api_server for chat, because
 *     `hermes -z` needs a gateway *in the same container*, while the live
 *     gateway lives in the `hermes` container. Each profile is mounted at
 *     /p/<profile>/v1 and authenticates with that profile's own scoped
 *     API_SERVER_KEY (gateway/platforms/api_server.py:_expected_api_key), which
 *     we read from the profile's secret scope on the shared HERMES_HOME mount.
 *   => the container must share the gateway's network namespace
 *      (`network_mode: "container:hermes"`): api_server binds 127.0.0.1:8642.
 *      The per-profile key lives in profiles/<profile>/.env — NOT in this
 *      stack's .env, so no gateway secret is duplicated here.
 *
 * Requires: the `hermes` binary on PATH, and env DATABASE_URL.
 * Optional env: HERMES_BOARD (default "default"), BRIDGE_POLL_MS (5000),
 *               BRIDGE_MIRROR_MS (30000), HERMES_BIN (default "hermes"),
 *               HERMES_API_BASE (default "http://127.0.0.1:8642").
 */
import pg from "pg";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

const execFileP = promisify(execFile);
const HERMES = process.env.HERMES_BIN || "hermes";
const BOARD = process.env.HERMES_BOARD || "default";
const POLL_MS = Number(process.env.BRIDGE_POLL_MS || 5000);
const MIRROR_MS = Number(process.env.BRIDGE_MIRROR_MS || 30000);
const RUN_TIMEOUT_MS = Number(process.env.BRIDGE_RUN_TIMEOUT_MS || 240000);
// The daily brief is a multi-tool agent turn (wiki + board + activity), not a one-shot reply;
// the interactive budget aborts it mid-work. The mirroring guard bounds its concurrency.
const BRIEF_TIMEOUT_MS = Number(process.env.BRIDGE_BRIEF_TIMEOUT_MS || 900000);
const WIKI_DIR = process.env.HERMES_WIKI || path.join(os.homedir(), ".hermes", "wiki");
const BRIEF_HOUR = Number(process.env.BRIEF_HOUR || 8);   // local hour to auto-generate the daily brief
const BRIEF_PROMPT =
  "You are the operator's chief of staff. Produce today's brief. Read your memory wiki open-loops " +
  "(~/.hermes/wiki), the kanban board, and recent activity. Output ONLY valid JSON (no prose, no code fences) " +
  'in exactly this shape: {"greeting":"one warm line","summary":"2-3 sentences on where things stand",' +
  '"sections":[{"label":"Needs your decision","items":["..."]},{"label":"Top priorities","items":["..."]},' +
  '{"label":"Recently shipped","items":["..."]},{"label":"Next actions","items":["..."]}]}. ' +
  "Keep every item short, concrete, and specific. Omit a section if it has nothing.";
let lastBriefDate = null;

const DB_URL = process.env.DATABASE_URL || "";
if (!DB_URL) { console.error("DATABASE_URL is required (use the direct postgres:// URL, not a prisma:// Accelerate URL)"); process.exit(1); }
if (DB_URL.startsWith("prisma://") || DB_URL.startsWith("prisma+")) {
  console.error("DATABASE_URL is a Prisma Accelerate URL; the bridge needs a DIRECT postgres:// connection string (e.g. POSTGRES_URL).");
  process.exit(1);
}
// Cloud Postgres (Prisma Postgres/Neon/Supabase/RDS) needs SSL; localhost doesn't.
const isLocal = /@(localhost|127\.0\.0\.1)/.test(DB_URL);
const pool = new pg.Pool({ connectionString: DB_URL, max: 4, ssl: isLocal ? undefined : { rejectUnauthorized: false } });

const log = (...a) => console.log(new Date().toISOString(), ...a);
const q = (text, params) => pool.query(text, params);

async function hermes(args, { timeout = 30000 } = {}) {
  const { stdout } = await execFileP(HERMES, args, { timeout, maxBuffer: 8 * 1024 * 1024 });
  return stdout;
}

/* ─────────────── Gateway HTTP (per-profile chat surface) ───────────────
 * The gateway mounts every profile at /p/<profile>/v1 (default profile at /v1)
 * and checks the request against the API_SERVER_KEY of *that profile's secret
 * scope* — profiles/<p>/.env, .env for default. Read it from the shared
 * HERMES_HOME mount instead of copying it into this stack's env. */
const GW_BASE = (process.env.HERMES_API_BASE || "http://127.0.0.1:8642").replace(/\/$/, "");
const HERMES_HOME = process.env.HERMES_HOME || os.homedir();
const PROFILE_RE = /^[a-z0-9][a-z0-9_-]*$/i;

function profileApiKey(profile) {
  const home = profile === "default" ? HERMES_HOME : path.join(HERMES_HOME, "profiles", profile);
  try {
    for (const line of fs.readFileSync(path.join(home, ".env"), "utf8").split("\n")) {
      const m = line.match(/^\s*API_SERVER_KEY\s*=\s*(.*)$/);
      if (m) return m[1].trim().replace(/^["'](.*)["']$/, "$1");
    }
  } catch { /* no scope file => no credential */ }
  return "";
}

function gwRoute(profile) {
  const p = String(profile || "default").toLowerCase();
  if (!PROFILE_RE.test(p)) throw new Error(`invalid profile name: ${profile}`);
  return { p, base: p === "default" ? `${GW_BASE}/v1` : `${GW_BASE}/p/${p}/v1` };
}

// Built by concatenation on purpose: a literal `Bearer <token>` in source gets
// mangled by secret-masking tooling (it once wrote "Bearer ***" into this file).
function authHeader(key) {
  return "Bearer " + key;
}

// True when the gateway is actually serving this profile over HTTP. This — not
// `hermes profile list` — is the authoritative liveness signal for a bridge that
// does not host the gateway: the CLI detects gateways through per-profile pid
// files in XDG_RUNTIME_DIR, which is private to each container.
async function gatewayServes(profile = "default") {
  let p, base;
  try { ({ p, base } = gwRoute(profile)); } catch { return false; }
  const key = profileApiKey(p);
  if (!key) return false;
  try {
    const res = await fetch(`${base}/models`, {
      headers: { Authorization: authHeader(key) },
      signal: AbortSignal.timeout(5000),
    });
    return res.status === 200;
  } catch { return false; }
}

async function chatCompletion(profile, prompt, { timeout = RUN_TIMEOUT_MS, session = null, toolChoice = null } = {}) {
  const { p, base } = gwRoute(profile);
  const key = profileApiKey(p);
  if (!key) throw new Error(`no API_SERVER_KEY in the "${p}" secret scope — set API_SERVER_KEY in ${p === "default" ? ".env" : `profiles/${p}/.env`}`);
  const headers = { "Content-Type": "application/json", Authorization: authHeader(key) };
  // Without an explicit id the gateway derives the session from the first user message
  // (api_server.py:_derive_chat_session_id), so identical prompts land in ONE session. A
  // session left ambiguous by a gateway restart then refuses every later turn with HTTP 409
  // unknown_execution, which is exactly how the daily brief died. Board work is one-shot per
  // request, so scope the session to the request.
  if (session) headers["X-Hermes-Session-Id"] = session;
  const res = await fetch(`${base}/chat/completions`, {
    method: "POST",
    headers,
    // The model alias is the profile itself; the default profile serves as "hermes-agent".
    body: JSON.stringify({
      model: p === "default" ? "hermes-agent" : p,
      messages: [{ role: "user", content: String(prompt || "") }],
      stream: false,
      ...(toolChoice ? { tool_choice: toolChoice } : {}),
    }),
    signal: AbortSignal.timeout(timeout),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`gateway HTTP ${res.status}: ${text.slice(0, 300)}`);
  const content = JSON.parse(text)?.choices?.[0]?.message?.content ?? "";
  if (!content) throw new Error("gateway returned no message content");
  return content;
}

async function emit(kind, title, { detail = null, agent = "hermes", level = "info", meta = null } = {}) {
  await q(
    `INSERT INTO "AgentEvent" (id, kind, title, detail, agent, level, meta, "createdAt")
     VALUES ($1,$2,$3,$4,$5,$6,$7, now())`,
    [randomUUID(), kind, title.slice(0, 200), detail, agent, level, meta ? JSON.stringify(meta) : null]
  );
}

async function setStore(key, data) {
  await q(
    `INSERT INTO "DataStore" (key, data, "updatedAt") VALUES ($1,$2, now())
     ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, "updatedAt" = now()`,
    [key, JSON.stringify(data)]
  );
}

/* ─────────────── PULL: mirror Hermes → Postgres ─────────────── */
async function mirrorKanban() {
  let tasks = [];
  try {
    // NB: this Hermes CLI wants --board BEFORE the subcommand.
    const out = await hermes(["kanban", "--board", BOARD, "list", "--json"], { timeout: 15000 });
    const parsed = JSON.parse(out || "[]");
    tasks = Array.isArray(parsed) ? parsed : parsed.tasks || [];
  } catch (e) { log("kanban list failed:", e.message.split("\n")[0]); return; }

  const seen = new Set();
  for (const t of tasks) {
    const id = String(t.id ?? t.task_id ?? "");
    if (!id) continue;
    seen.add(id);
    await q(
      `INSERT INTO "HermesTask" (id, board, title, assignee, status, priority, result, "updatedAt", "syncedAt")
       VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now())
       ON CONFLICT (id) DO UPDATE SET
         title=EXCLUDED.title, assignee=EXCLUDED.assignee, status=EXCLUDED.status,
         priority=EXCLUDED.priority, result=EXCLUDED.result, "syncedAt"=now()`,
      [id, BOARD, String(t.title ?? "untitled").slice(0, 300), t.assignee ?? null,
       String(t.status ?? "todo"), t.priority != null ? Number(t.priority) : null,
       t.result ? String(t.result).slice(0, 2000) : null]
    );
  }
  // prune tasks that vanished from the board
  if (seen.size) {
    await q(`DELETE FROM "HermesTask" WHERE board=$1 AND id <> ALL($2::text[])`, [BOARD, [...seen]]);
  } else {
    await q(`DELETE FROM "HermesTask" WHERE board=$1`, [BOARD]);
  }
}

async function mirrorCrons() {
  try {
    const out = await hermes(["cron", "list", "--all"], { timeout: 15000 });
    const lines = out.split("\n").map((l) => l.trimEnd()).filter(Boolean);
    await setStore("hermes-crons", { jobs: lines, raw: out.slice(0, 8000), syncedAt: new Date().toISOString() });
  } catch (e) { log("cron list failed:", e.message.split("\n")[0]); }
}

async function mirrorCost() {
  for (const args of [["insights", "--days", "7"], ["insights"]]) {
    try {
      const out = await hermes(args, { timeout: 15000 });
      await setStore("hermes-cost", { summary: out.slice(0, 4000), syncedAt: new Date().toISOString() });
      return;
    } catch { /* try next arg shape */ }
  }
}

async function mirrorHealth() {
  // Authoritative signal is the HTTP probe (see gatewayServes); the CLI output is
  // kept only as human-readable detail, since a sibling container cannot observe
  // the gateway's pid files.
  const served = await gatewayServes("default");
  let detail = "";
  try { detail = (await hermes(["status"], { timeout: 12000 })).slice(0, 4000); }
  catch (e) { detail = e.message.split("\n")[0]; }
  await setStore("hermes-health", { online: served, gateway: served ? "running" : "stopped", detail, lastSeen: new Date().toISOString() });
}

/* ─────────────── Profiles → AgentState (your Hermes profiles as agents) ─────────────── */
const PROFILE_ROLE = "Hermes"; // role prefix marking bridge-managed profile agents
const PROFILE_EMOJI = { orchestrator: "\u{1F9ED}", researcher: "\u{1F50E}", librarian: "\u{1F4DA}", hassio: "\u{1F3E0}", default: "\u{1F43A}" };

function parseProfiles(out) {
  const lines = out.split("\n");
  const headerIdx = lines.findIndex((l) => /^\s*Profile\s+Model\s+Gateway/.test(l));
  if (headerIdx < 0) return [];
  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim() || /^[\s\u2500-]+$/.test(line)) continue; // blank / rule separator
    const active = line.includes("\u25C6");
    const cols = line.replace(/^[\s\u25C6]*/, "").split(/\s+/);
    const name = cols[0];
    if (!name || !/^[a-z0-9][a-z0-9_-]*$/i.test(name)) continue;
    rows.push({ id: name.toLowerCase(), name, model: cols[1] || "", gateway: (cols[2] || "").toLowerCase(), active });
  }
  return rows;
}

async function mirrorProfiles() {
  let profiles = [];
  try {
    profiles = parseProfiles(await hermes(["profile", "list"], { timeout: 15000 }));
  } catch (e) {
    // execFileP rejects on non-zero exit, but the docker-managed Hermes "install out of sync"
    // warning prints to stderr while still emitting a valid table on stdout. Salvage stdout so a
    // transient non-zero exit doesn't drop a mirror tick; never proceed on an empty parse (no wipe).
    profiles = parseProfiles((e.stdout || "").toString());
    if (!profiles.length) {
      log("profile list failed:", (e.stderr || e.message || "").toString().split("\n").filter(Boolean).pop() || "error");
      return;
    }
  }
  if (!profiles.length) return;

  const running = await q(
    `SELECT "agentId", title FROM "AgentRequest" WHERE status='running' AND "agentId" IS NOT NULL ORDER BY "startedAt" DESC NULLS LAST`
  );
  const runningBy = {};
  for (const r of running.rows) if (r.agentId && runningBy[r.agentId] == null) runningBy[r.agentId] = r.title;

  const done = await q(
    `SELECT "agentId", count(*)::int AS n FROM "AgentRequest" WHERE "agentId" IS NOT NULL AND status='done' GROUP BY "agentId"`
  );
  const doneBy = Object.fromEntries(done.rows.map((r) => [r.agentId, r.n]));

  for (const p of profiles) {
    const isWorking = runningBy[p.id] != null;
    // HTTP probe first: `hermes profile list` reports the gateway column from
    // pid files that are private to the gateway container, so a sibling bridge
    // would wrongly see every profile as stopped.
    const served = (await gatewayServes(p.id)) || p.gateway === "running";
    const status = isWorking ? "working" : (served ? "online" : "offline");
    const recent = await q(
      `SELECT title, "createdAt" FROM "AgentEvent" WHERE agent=$1 ORDER BY "createdAt" DESC LIMIT 10`,
      [p.id]
    );
    const recentActivity = recent.rows.map((r) => ({ timestamp: r.createdAt.toISOString(), action: r.title }));
    await q(
      `INSERT INTO "AgentState" (id,name,emoji,role,status,"lastActive","tasksCompleted","totalCost","currentTask","recentActivity","updatedAt")
       VALUES ($1,$2,$3,$4,$5, now(), $6, 0, $7, $8::jsonb, now())
       ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, emoji=EXCLUDED.emoji, role=EXCLUDED.role,
         status=EXCLUDED.status, "lastActive"=now(), "tasksCompleted"=EXCLUDED."tasksCompleted",
         "currentTask"=EXCLUDED."currentTask", "recentActivity"=EXCLUDED."recentActivity", "updatedAt"=now()`,
      [p.id, p.name, PROFILE_EMOJI[p.id] || "\u{1F916}", `${PROFILE_ROLE} \u00B7 ${p.model || "profile"}`,
       status, doneBy[p.id] || 0, isWorking ? runningBy[p.id] : null, JSON.stringify(recentActivity)]
    );
  }
  // prune bridge-managed profile rows that vanished from the profile list (leaves hand-managed rows alone)
  const ids = profiles.map((p) => p.id);
  await q(`DELETE FROM "AgentState" WHERE role LIKE $1 AND id <> ALL($2::text[])`, [`${PROFILE_ROLE} \u00B7 %`, ids]);
}

/* ─────────────── Memory Wiki (warm tier: git-tracked markdown) ─────────────── */
function parseEntry(md) {
  const m = md.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
  const fm = {}; let body = md;
  if (m) {
    body = m[2];
    for (const line of m[1].split("\n")) {
      const kv = line.match(/^([A-Za-z_]+):\s*(.*)$/);
      if (!kv) continue;
      const v = kv[2].trim();
      if (v.startsWith("[") && v.endsWith("]")) fm[kv[1]] = v.slice(1, -1).split(",").map((s) => s.trim()).filter(Boolean);
      else fm[kv[1]] = v === "null" || v === "" ? null : v;
    }
  }
  return { fm, body: body.trim() };
}
function walkMd(dir, out = []) {
  let items = [];
  try { items = fs.readdirSync(dir, { withFileTypes: true }); } catch { return out; }
  for (const it of items) {
    const full = path.join(dir, it.name);
    if (it.isDirectory()) { if (it.name !== ".git") walkMd(full, out); }
    else if (it.name.endsWith(".md") && it.name !== "INDEX.md") out.push(full);
  }
  return out;
}
async function mirrorWiki() {
  if (!fs.existsSync(WIKI_DIR)) return;
  const seen = new Set();
  for (const file of walkMd(WIKI_DIR)) {
    const rel = path.relative(WIKI_DIR, file);
    const id = rel.replace(/\.md$/, "");
    seen.add(id);
    let raw = ""; try { raw = fs.readFileSync(file, "utf8"); } catch { continue; }
    const { fm, body } = parseEntry(raw);
    await q(
      `INSERT INTO "HermesMemory" (id, path, type, title, status, confidence, provenance, tags, links, body, "validFrom", "validTo", "updatedAt", "syncedAt")
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now(), now())
       ON CONFLICT (id) DO UPDATE SET path=EXCLUDED.path, type=EXCLUDED.type, title=EXCLUDED.title,
         status=EXCLUDED.status, confidence=EXCLUDED.confidence, provenance=EXCLUDED.provenance,
         tags=EXCLUDED.tags, links=EXCLUDED.links, body=EXCLUDED.body,
         "validFrom"=EXCLUDED."validFrom", "validTo"=EXCLUDED."validTo", "syncedAt"=now()`,
      [id, rel, fm.type || "fact", fm.title || id, fm.status || "active", fm.confidence || null,
       fm.provenance || null, Array.isArray(fm.tags) ? fm.tags : [], Array.isArray(fm.links) ? fm.links : [],
       body, fm.valid_from || null, fm.valid_to || null]
    );
  }
  if (seen.size) await q(`DELETE FROM "HermesMemory" WHERE id <> ALL($1::text[])`, [[...seen]]);
  else await q(`DELETE FROM "HermesMemory"`);
}
function writeWikiEntry(e) {
  const rel = e.path || `${e.type || "note"}s/${e.id}.md`;
  const full = path.join(WIKI_DIR, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  const now = new Date().toISOString().slice(0, 10);
  const lines = [
    "---", `id: ${e.id}`, `type: ${e.type || "note"}`, `title: ${e.title}`,
    `status: ${e.status || "active"}`,
    e.confidence ? `confidence: ${e.confidence}` : null,
    `provenance: ${e.provenance || "dashboard"}`,
    `tags: [${(e.tags || []).join(", ")}]`, `links: [${(e.links || []).join(", ")}]`,
    `updated: ${now}`, "---", "", e.body || "", "",
  ].filter((l) => l !== null);
  fs.writeFileSync(full, lines.join("\n"), "utf8");
  return rel;
}
async function gitCommitWiki(msg) {
  try {
    if (!fs.existsSync(path.join(WIKI_DIR, ".git"))) await execFileP("git", ["-C", WIKI_DIR, "init"]).catch(() => {});
    await execFileP("git", ["-C", WIKI_DIR, "add", "-A"]).catch(() => {});
    await execFileP("git", ["-C", WIKI_DIR, "commit", "-m", msg]).catch(() => {});
  } catch { /* ignore */ }
}

/* ─────────────── Chief-of-staff daily brief ─────────────── */
async function generateBriefing() {
  const day = new Date().toISOString().slice(0, 10);
  const raw = (await chatCompletion("default", BRIEF_PROMPT, { session: `hq-brief-${day}`, timeout: BRIEF_TIMEOUT_MS })).trim();
  let brief;
  try {
    const jsonStr = raw.replace(/^```(?:json)?/i, "").replace(/```$/, "").trim();
    const m = jsonStr.match(/\{[\s\S]*\}/);
    brief = JSON.parse(m ? m[0] : jsonStr);
  } catch { brief = { summary: raw.slice(0, 1500), sections: [] }; }
  brief.generatedAt = new Date().toISOString();
  await setStore("hermes-briefing", brief);
  await emit("status", "Daily brief generated", { level: "up" });
}
async function maybeDailyBrief() {
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  if (now.getHours() >= BRIEF_HOUR && lastBriefDate !== today) {
    lastBriefDate = today;
    try { await generateBriefing(); } catch (e) { log("daily brief err", e.message); }
  }
}

/* ─────────────── PUSH: run website requests via Hermes ─────────────── */
async function runRequest(r) {
  await q(`UPDATE "AgentRequest" SET status='running', "startedAt"=now(), "updatedAt"=now() WHERE id=$1`, [r.id]);
  await emit("run", `Started: ${r.title}`, { level: "info", agent: r.agentId || "hermes", meta: { requestId: r.id, kind: r.kind } });
  try {
    let result = "";
    if (r.kind === "oneshot" || r.kind === "chat") {
      // Address the profile the dashboard picked; an explicit `default` is also the
      // bridge's own board owner, so fall back to it when no agent was chosen.
      const profile = r.agentId && PROFILE_RE.test(r.agentId) ? r.agentId : "default";
      result = (await chatCompletion(profile, r.prompt || r.title, {
        session: `hq-req-${r.id}`,
        toolChoice: "none",
      })).trim();
    } else if (r.kind === "kanban") {
      result = (await hermes(["kanban", "--board", BOARD, "create", "--json", r.title], { timeout: 20000 })).trim();
    } else if (r.kind.startsWith("cron.")) {
      const op = r.kind.split(".")[1];
      const a = JSON.parse(r.prompt || "{}");
      const argv =
        op === "create" ? ["cron", "create", a.schedule, a.prompt || a.name].filter(Boolean)
        : op === "run"    ? ["cron", "run", a.id || a.name]
        : op === "pause"  ? ["cron", "pause", a.id || a.name]
        : op === "resume" ? ["cron", "resume", a.id || a.name]
        : op === "remove" ? ["cron", "remove", a.id || a.name]
        : op === "edit"   ? ["cron", "edit", a.id || a.name]
        : null;
      if (!argv) throw new Error(`unknown cron op ${op}`);
      result = (await hermes(argv, { timeout: 20000 })).trim();
      await mirrorCrons();
    } else if (r.kind === "memory.write") {
      const e = JSON.parse(r.prompt || "{}");
      const rel = writeWikiEntry(e);
      await gitCommitWiki(`wiki: update ${rel} (via dashboard)`);
      await mirrorWiki();
      result = `wrote ${rel}`;
    } else if (r.kind === "briefing.generate") {
      await generateBriefing();
      lastBriefDate = new Date().toISOString().slice(0, 10);
      result = "brief updated";
    } else {
      throw new Error(`unknown kind ${r.kind}`);
    }
    await q(`UPDATE "AgentRequest" SET status='done', result=$2, "finishedAt"=now(), "updatedAt"=now() WHERE id=$1`,
      [r.id, result.slice(0, 8000)]);
    await emit("run", `Done: ${r.title}`, { level: "up", agent: r.agentId || "hermes", detail: result.slice(0, 400), meta: { requestId: r.id } });
  } catch (e) {
    const msg = (e.stderr || e.message || "error").toString().split("\n")[0].slice(0, 600);
    await q(`UPDATE "AgentRequest" SET status='failed', error=$2, "finishedAt"=now(), "updatedAt"=now() WHERE id=$1`, [r.id, msg]);
    await emit("run", `Failed: ${r.title}`, { level: "down", agent: r.agentId || "hermes", detail: msg, meta: { requestId: r.id } });
    log("request failed:", r.id, msg);
  }
}

async function processQueue() {
  // Atomically claim a batch (FOR UPDATE SKIP LOCKED) so concurrent bridges can never
  // double-run the same request — required if a second bridge (e.g. the Mac mini) ever polls the same DB.
  const { rows } = await q(
    `UPDATE "AgentRequest" SET status='running', "startedAt"=now(), "updatedAt"=now()
     WHERE id IN (
       SELECT id FROM "AgentRequest"
       WHERE status IN ('queued','approved')
       ORDER BY "createdAt" ASC LIMIT 3
       FOR UPDATE SKIP LOCKED
     )
     RETURNING *`
  );
  for (const r of rows) await runRequest(r);
}

/* ─────────────── loops ─────────────── */
let mirroring = false;
async function mirrorTick() {
  // A turn-backed mirror step (daily brief) can outlive MIRROR_MS; overlapping ticks would
  // stack duplicate briefs and double-load the gateway.
  if (mirroring) return;
  mirroring = true;
  try {
    try { await mirrorKanban(); } catch (e) { log("mirrorKanban err", e.message); }
    try { await mirrorCrons(); } catch (e) { log("mirrorCrons err", e.message); }
    try { await mirrorHealth(); } catch (e) { log("mirrorHealth err", e.message); }
    try { await mirrorProfiles(); } catch (e) { log("mirrorProfiles err", e.message); }
    try { await mirrorWiki(); } catch (e) { log("mirrorWiki err", e.message); }
    try { await mirrorCost(); } catch (e) { log("mirrorCost err", e.message); }
    try { await maybeDailyBrief(); } catch (e) { log("maybeDailyBrief err", e.message); }
  } finally { mirroring = false; }
}

async function main() {
  log(`hermes-bridge up · board=${BOARD} · poll=${POLL_MS}ms · mirror=${MIRROR_MS}ms`);
  await emit("status", "Bridge connected", { level: "up" });
  // The queue loop starts on its own: an await on the first mirrorTick would starve board
  // work behind the daily brief (a turn-backed step can run for RUN_TIMEOUT_MS).
  const tick = async () => { try { await processQueue(); } catch (e) { log("queue loop", e.message); } finally { setTimeout(tick, POLL_MS); } };
  tick();
  mirrorTick().catch((e) => log("first mirror", e.message));
  setInterval(() => mirrorTick().catch((e) => log("mirror loop", e.message)), MIRROR_MS);
}
main().catch((e) => { console.error("fatal", e); process.exit(1); });
