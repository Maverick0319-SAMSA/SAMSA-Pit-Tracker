'use strict';
/**
 * SAMSA Pit Tracker server
 * - Zero dependencies (Node 18+)
 * - Serves the page, stores state in a JSON file, pushes live updates via SSE
 * - Everyone can view; only a logged-in admin can change anything
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'state.json');
const INDEX_FILE = path.join(__dirname, 'public', 'index.html');
const PIT_DURATION_MS = (Number(process.env.PIT_MINUTES) || 25) * 60 * 1000;
const SESSION_TTL_MS = 12 * 60 * 60 * 1000;

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'Maverick@SAMSA';
if (!process.env.ADMIN_PASSWORD) {
  console.warn('[warn] ADMIN_PASSWORD not set - using the default. Set it before going public!');
}

// ---------- state ----------
let state = { rev: 0, teams: [] };

function loadState() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (parsed && Array.isArray(parsed.teams)) state = { rev: parsed.rev || 0, teams: parsed.teams };
  } catch (e) { /* first run */ }
}

function persist() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmp = DATA_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(state));
  fs.renameSync(tmp, DATA_FILE); // atomic, so a crash can't leave a half-written file
}

function payload() {
  return { rev: state.rev, teams: state.teams, serverNow: Date.now(), pitDurationMs: PIT_DURATION_MS };
}

// ---------- live updates (Server-Sent Events) ----------
const clients = new Set();

function commit() {
  state.rev++;
  persist();
  const msg = `data: ${JSON.stringify(payload())}\n\n`;
  for (const res of clients) res.write(msg);
  return payload();
}

setInterval(() => { for (const res of clients) res.write(': keepalive\n\n'); }, 25000).unref();

// ---------- auth ----------
const sessions = new Map(); // token -> expiry
const failures = new Map(); // ip -> { count, resetAt }

function clientIp(req) {
  return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}

function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a)).digest();
  const hb = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function isAdmin(req) {
  const m = /^Bearer (.+)$/.exec(req.headers.authorization || '');
  if (!m) return false;
  const exp = sessions.get(m[1]);
  if (!exp) return false;
  if (exp < Date.now()) { sessions.delete(m[1]); return false; }
  return true;
}

// ---------- validation ----------
const cleanText = s => (typeof s === 'string' ? s.trim().slice(0, 40) : '');
const cleanColor = c => (typeof c === 'string' && /^#[0-9a-f]{6}$/i.test(c) ? c : '#94a3b8');
const uid = () => crypto.randomBytes(6).toString('hex');
const newDriver = name => ({ name: cleanText(name), pits: [] });

function makeTeam(name, color, gtls, gtsa, order) {
  return { id: uid(), name: cleanText(name), color: cleanColor(color), order, gtls: newDriver(gtls), gtsa: newDriver(gtsa) };
}

const DEMO = [
  ['Dutch Divebomb', '#f97316', 'vMxtics', 'RevexngeV2'],
  ['Coquette Racing', '#eab308', 'NikhilK_29', 'GrizzlyDust7222'],
  ['Los Santos Car Meet', '#a855f7', 'NJxyce', 'Colin_43'],
  ['The Diamond Lat', '#8b5cf6', 'Colin_43', 'CrownSkipper'],
  ['Chicken Crusaders', '#ef4444', 'Coolcarscarter', 'StrongVettell'],
  ['Scuderia Grotti', '#f43f5e', 'Bdogs_Gaming', 'ErrorSync-Omega'],
  ['Caramel & Vanilla', '#64748b', 'ArsxnalReece10', 'Cirrus5005'],
  ['Slow & Slower', '#65a30d', 'SmashCrash6', 'Hennessaint'],
  ['Get of the Frenchies', '#0ea5e9', 'Diglett4459', 'Unplugged'],
  ['The Nordictards', '#3b82f6', 'MogiPRO', 'Mulahnavatti'],
  ['Crowex', '#166534', 'KillaRampage504', 'Protay10'],
  ['Brexit Burritos', '#ec4899', 'Lynxy', 'CommandZ-'],
  ['', '#94a3b8', '', ''],
  ['', '#94a3b8', '', ''],
];

// ---------- http helpers ----------
function send(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > 100 * 1024) { reject(Object.assign(new Error('Body too large'), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reject(Object.assign(new Error('Invalid JSON'), { status: 400 })); }
    });
    req.on('error', reject);
  });
}

// ---------- API ----------
async function handleApi(req, res, p) {
  const method = req.method;

  if (method === 'GET' && p === '/api/state') return send(res, 200, payload());

  if (method === 'GET' && p === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // stop nginx from buffering the stream
    });
    res.write(`retry: 2000\ndata: ${JSON.stringify(payload())}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  if (method === 'POST' && p === '/api/login') {
    const ip = clientIp(req);
    const f = failures.get(ip);
    if (f && f.resetAt > Date.now() && f.count >= 10) return send(res, 429, { error: 'Too many attempts, try again later' });
    const { password } = await readBody(req);
    if (!safeEqual(password || '', ADMIN_PASSWORD)) {
      const cur = f && f.resetAt > Date.now() ? f : { count: 0, resetAt: Date.now() + 10 * 60 * 1000 };
      cur.count++;
      failures.set(ip, cur);
      return send(res, 401, { error: 'Wrong password' });
    }
    failures.delete(ip);
    const token = crypto.randomBytes(24).toString('hex');
    sessions.set(token, Date.now() + SESSION_TTL_MS);
    return send(res, 200, { token });
  }

  if (method === 'GET' && p === '/api/me') {
    return isAdmin(req) ? send(res, 200, { admin: true }) : send(res, 401, { error: 'Not logged in' });
  }

  // Everything below changes data -> admin only
  if (!isAdmin(req)) return send(res, 401, { error: 'Admin login required' });

  if (method === 'POST' && p === '/api/teams') {
    const b = await readBody(req);
    state.teams.push(makeTeam(b.name, b.color, b.gtls, b.gtsa, state.teams.length));
    return send(res, 200, commit());
  }

  if (method === 'POST' && p === '/api/demo') {
    state.teams = DEMO.map((d, i) => makeTeam(d[0], d[1], d[2], d[3], i));
    return send(res, 200, commit());
  }

  if (method === 'POST' && p === '/api/reset') {
    state.teams = [];
    return send(res, 200, commit());
  }

  const m = /^\/api\/teams\/([a-f0-9]+)(\/pit)?$/.exec(p);
  if (m) {
    const team = state.teams.find(t => t.id === m[1]);
    if (!team) return send(res, 404, { error: 'Team not found' });

    if (method === 'PUT' && !m[2]) {
      const b = await readBody(req);
      team.name = cleanText(b.name);
      team.color = cleanColor(b.color);
      team.gtls.name = cleanText(b.gtls);
      team.gtsa.name = cleanText(b.gtsa);
      return send(res, 200, commit());
    }

    if (method === 'DELETE' && !m[2]) {
      state.teams = state.teams.filter(t => t.id !== team.id);
      return send(res, 200, commit());
    }

    if (method === 'POST' && m[2]) {
      const b = await readBody(req);
      if (b.slot !== 'gtls' && b.slot !== 'gtsa') return send(res, 400, { error: 'slot must be gtls or gtsa' });
      const driver = team[b.slot];
      if (!driver.name) return send(res, 400, { error: 'No driver in that slot' });
      if (b.undo) driver.pits.pop(); else driver.pits.push(Date.now());
      return send(res, 200, commit());
    }
  }

  return send(res, 404, { error: 'Not found' });
}

// ---------- server ----------
const server = http.createServer(async (req, res) => {
  try {
    const p = new URL(req.url, 'http://localhost').pathname;
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return res.end(fs.readFileSync(INDEX_FILE));
    }
    if (p === '/healthz') return send(res, 200, { ok: true });
    if (p.startsWith('/api/')) return await handleApi(req, res, p);
    send(res, 404, { error: 'Not found' });
  } catch (e) {
    if (!res.headersSent) send(res, e.status || 500, { error: e.status ? e.message : 'Server error' });
    if (!e.status) console.error(e);
  }
});

loadState();
server.listen(PORT, () => console.log(`Pit tracker running on http://localhost:${PORT}`));
