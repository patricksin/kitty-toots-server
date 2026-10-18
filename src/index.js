/**
 * Kitty Toots multiplayer server (Cloudflare Worker + Durable Objects).
 *
 *   GET  /                 health check
 *   GET  /turn             short-lived ICE servers (STUN + TURN) for voice chat
 *   WS   /room/<garden>    one Durable Object per garden name
 *
 * Room protocol (JSON text frames):
 *   client -> server  {t:"p", p:{...}}            replace my presence (what I look like, where I am)
 *                     {t:"sig", to:id, d:{...}}   pass a voice-chat setup message to one player
 *   server -> client  {t:"welcome", id, peers:[{id,p}]}
 *                     {t:"p", id, p}  {t:"join", id}  {t:"leave", id}  {t:"sig", from, d}
 *
 * Secrets / vars (set with `wrangler secret put NAME` or in wrangler.toml [vars]):
 *   ALLOWED_ORIGINS     comma-separated site origins allowed to connect, e.g. https://kittytoots.netlify.app
 *   TURN_KEY_ID         Cloudflare Realtime TURN key id            (secret)
 *   TURN_KEY_API_TOKEN  Cloudflare Realtime TURN key API token     (secret)
 *   TURN_URLS + TURN_SECRET  alternative: your own coturn server using use-auth-secret
 *   TURN_TTL            credential lifetime in seconds (default 14400 = 4 hours)
 */

const MAX_PLAYERS = 8;
const MAX_PRESENCE_BYTES = 4096;
const MAX_SIGNAL_BYTES = 16384;
const MAX_MSGS_PER_SECOND = 60;
const ROOM_RE = /^[a-z0-9][a-z0-9-]{0,47}$/;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';
    const allowed = originAllowed(origin, env);

    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders(origin, allowed) });

    if (url.pathname === '/turn') {
      if (!allowed) return new Response('This site is not allowed to use this server.', { status: 403 });
      return json(await iceServers(env), 200, corsHeaders(origin, allowed));
    }

    const m = url.pathname.match(/^\/room\/([^/]+)$/);
    if (m) {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('Expected a WebSocket connection.', { status: 426 });
      if (!allowed) return new Response('This site is not allowed to use this server.', { status: 403 });
      const name = decodeURIComponent(m[1]);
      if (!ROOM_RE.test(name)) return new Response('Garden names use lowercase letters, numbers and dashes.', { status: 400 });
      const stub = env.GARDEN.get(env.GARDEN.idFromName(name));
      return stub.fetch(req);
    }

    if (url.pathname === '/') return new Response('Kitty Toots server is running.', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
    return new Response('Not found', { status: 404 });
  },
};

function originAllowed(origin, env) {
  const list = String(env.ALLOWED_ORIGINS || '*').split(',').map((s) => s.trim()).filter(Boolean);
  return list.includes('*') || list.includes(origin);
}
function corsHeaders(origin, allowed) {
  if (!allowed) return {};
  return { 'Access-Control-Allow-Origin': origin || '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS', Vary: 'Origin' };
}
function json(body, status, headers) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...headers } });
}

async function iceServers(env) {
  const ttl = Number(env.TURN_TTL || 14400);
  const stun = { urls: ['stun:stun.cloudflare.com:3478'] };
  try {
    if (env.TURN_KEY_ID && env.TURN_KEY_API_TOKEN) {
      const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${env.TURN_KEY_API_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl }),
      });
      if (r.ok) {
        const d = await r.json();
        // Port 53 is blocked by browsers and only causes a timeout, so drop it.
        const list = (d.iceServers || []).map((s) => ({ ...s, urls: [].concat(s.urls).filter((u) => !/:53(\?|$)/.test(u)) }));
        return { iceServers: list, ttl, relay: true };
      }
      console.warn('TURN credential request failed', r.status);
    }
    if (env.TURN_URLS && env.TURN_SECRET) {
      // Standard TURN REST credentials, as used by coturn's use-auth-secret option.
      const username = `${Math.floor(Date.now() / 1000) + ttl}:kittytoots`;
      const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(env.TURN_SECRET), { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
      const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(username)));
      let bin = ''; for (const b of sig) bin += String.fromCharCode(b);
      return { iceServers: [stun, { urls: env.TURN_URLS.split(',').map((s) => s.trim()), username, credential: btoa(bin) }], ttl, relay: true };
    }
  } catch (e) {
    console.warn('TURN error', e);
  }
  return { iceServers: [stun], ttl: 3600, relay: false };
}

export class Garden {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.presence = new Map(); // player id -> latest presence object (kept in memory; players resend it many times a second)
    this.rate = new Map();     // player id -> [second, count]
  }

  async fetch() {
    const others = this.ctx.getWebSockets();
    if (others.length >= MAX_PLAYERS) return new Response('This garden is full.', { status: 429 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const id = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id });
    const peers = [];
    for (const ws of others) {
      const a = ws.deserializeAttachment();
      if (a && a.id) peers.push({ id: a.id, p: this.presence.get(a.id) || null });
    }
    server.send(JSON.stringify({ t: 'welcome', id, peers }));
    this.broadcast(JSON.stringify({ t: 'join', id }), server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    const a = ws.deserializeAttachment();
    if (!a || typeof msg !== 'string' || msg.length > MAX_SIGNAL_BYTES) return;
    const sec = Math.floor(Date.now() / 1000), r = this.rate.get(a.id) || [sec, 0];
    if (r[0] !== sec) { r[0] = sec; r[1] = 0; }
    r[1]++; this.rate.set(a.id, r);
    if (r[1] > MAX_MSGS_PER_SECOND) return;
    let m;
    try { m = JSON.parse(msg); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'p' && m.p && typeof m.p === 'object' && !Array.isArray(m.p) && msg.length <= MAX_PRESENCE_BYTES + 32) {
      this.presence.set(a.id, m.p);
      this.broadcast(JSON.stringify({ t: 'p', id: a.id, p: m.p }), ws);
    } else if (m.t === 'sig' && typeof m.to === 'string' && m.d && typeof m.d === 'object') {
      for (const s of this.ctx.getWebSockets()) {
        const b = s.deserializeAttachment();
        if (b && b.id === m.to) { try { s.send(JSON.stringify({ t: 'sig', from: a.id, d: m.d })); } catch {} break; }
      }
    }
  }

  async webSocketClose(ws) { this.drop(ws); try { ws.close(1000, 'bye'); } catch {} }
  async webSocketError(ws) { this.drop(ws); }

  drop(ws) {
    const a = ws.deserializeAttachment();
    if (!a || a.gone) return;
    a.gone = true; try { ws.serializeAttachment(a); } catch {}
    this.presence.delete(a.id); this.rate.delete(a.id);
    this.broadcast(JSON.stringify({ t: 'leave', id: a.id }), ws);
  }

  broadcast(text, except) {
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(text); } catch {}
    }
  }
}
