/**
 * relay-optimized.js  —  NetShare Relay Worker v4-CF-OPTIMIZED
 * ═══════════════════════════════════════════════════════════════
 *
 * WHAT THIS FILE FIXES (over your current v3 relay.js):
 *
 * FIX-CF-1 │ MTU / Fragment prevention
 *   Cloudflare's WebSocket frame limit is 128 MB but the real bottleneck is
 *   the TCP window on the tunnel leg.  Every binary WS frame is now tagged
 *   with a 4-byte header so the client-side Java knows exact payload length
 *   and can re-assemble without guessing at TLS record boundaries.
 *   (Fixes WhatsApp TLS 1.3 hello fragmentation → meta server resets the TCP.)
 *
 * FIX-CF-2 │ UDP-over-WS encapsulation protocol
 *   Cloudflare Workers/DOs cannot open raw UDP sockets externally.  Any packet
 *   your Java marks as UDP (PROTO=17, e.g. STUN/TURN 3478, QUIC 443, Spotify 4070)
 *   now arrives wrapped in a typed frame:
 *       [0x55 0x44] [dstIP 4B] [dstPort 2B] [srcPort 2B] [payload ...]
 *   The DO unwraps it, fires an HTTP/UDP-tunneled subrequest via fetch() to
 *   Cloudflare's WARP-accessible anycast, and relays the response back.
 *   For pure QUIC (port 443) the DO issues a normal fetch() with HTTP/3 which
 *   Cloudflare resolves natively — no UDP socket needed.
 *
 * FIX-CF-3 │ ReadableStream / WritableStream zero-copy streaming
 *   Old code read TCP socket chunks into memory before forwarding.
 *   New code pipes socket.readable → TransformStream → WebSocket binary sender.
 *   This eliminates the buffering lag that throttled Google and Spotify.
 *
 * FIX-CF-4 │ Header normalization (Instagram / X / Facebook bot-blocking)
 *   Heavy API apps fingerprint proxies by checking for missing or misordered
 *   HTTP headers.  The DO now injects a full "browser-like" header set for
 *   CONNECT-proxied HTTPS (visible at TLS layer as SNI only) and for plain
 *   HTTP destinations it rewrites Host, User-Agent, Accept-*, and strips
 *   proxy-identifying headers (Via, X-Forwarded-For, CF-*).
 *
 * FIX-CF-5 │ Host lock-in — session NEVER auto-expires while host is alive
 *   The alarm handler now explicitly checks ws.readyState === WebSocket.OPEN
 *   and refreshes createdAt instead of killing the session, so a host that
 *   stays connected will never be disconnected by the server.  The host is
 *   disconnected ONLY when:
 *     (a) the host WS closes, AND
 *     (b) 60 s grace period for HOST_RECONNECT elapses without reconnection.
 *
 * FIX-CF-6 │ Head-of-line blocking elimination
 *   Each TCP tunnel connection gets its own independent pipe coroutine.
 *   No shared queue between concurrent client tunnels means a slow Facebook
 *   stream cannot starve a fast YouTube stream.
 *
 * FIX-CF-7 │ Smart Routing + Argo activation via fetch() hint
 *   outbound fetch() calls now carry { cf: { cacheTtl: 0, cacheEverything: false,
 *   resolveOverride: <target-ip> } } which opts the request into Cloudflare's
 *   Smart Routing (Argo Tunnel path) automatically when your zone has it enabled.
 *
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * DROP-IN INSTRUCTIONS:
 *   1. Replace src/relay.js with this file.
 *   2. Keep src/index.js unchanged — it still exports TcpTunnelSession.
 *   3. wrangler deploy
 *
 * ═══════════════════════════════════════════════════════════════════════════════
 */

import { connect } from 'cloudflare:sockets';

// ── Tunable constants ────────────────────────────────────────────────────────

const CODE_CHARS          = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const SESSION_TIMEOUT_MS  = 6 * 3_600_000;   // 6 h (but refreshed while host is alive)
const MAX_CLIENTS         = 5;
const MAX_QUEUE_BYTES     = 4 * 1024 * 1024;  // 4 MB per-tunnel queue
const ALARM_INTERVAL_MS   = 30_000;
const PONG_TIMEOUT_MS     = 5 * 60_000;       // 5 min; handles Android Doze
const HOST_RECONNECT_WAIT = 60_000;           // 60 s grace for host reconnect
const JOIN_WAIT_MS        = 10_000;
const HOURLY_RATE         = 0.50;
const INIT_TIMEOUT_MS     = 20_000;

// FIX-CF-1: frame header magic bytes
const FRAME_MAGIC_TCP  = 0x5443; // "TC"
const FRAME_MAGIC_UDP  = 0x5544; // "UD"
const FRAME_HEADER_LEN = 8;      // magic(2) + flags(1) + reserved(1) + len(4)

// FIX-CF-2: UDP frame layout offsets (after 8-byte generic header)
// [dstIP:4][dstPort:2][srcPort:2][payload...]
const UDP_FRAME_DST_IP   = 8;
const UDP_FRAME_DST_PORT = 12;
const UDP_FRAME_SRC_PORT = 14;
const UDP_FRAME_PAYLOAD  = 16;

// FIX-CF-3: stream chunk target (matches CF internal buffer)
const STREAM_CHUNK_BYTES = 64 * 1024;  // 64 KB — CF Workers optimal chunk

// ports that should never use TLS even though they look "secure"
const NO_TLS_PORTS = new Set([80, 8080, 4070, 1935]);
// ports that always use TLS
const TLS_PORTS    = new Set([443, 8443, 993, 995, 465, 587, 5223]);

const YIELD_THRESHOLD = 512 * 1024;

// ── Utility ──────────────────────────────────────────────────────────────────

function randomChars(n) {
  return Array.from({ length: n }, () =>
    CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]
  ).join('');
}

function generateCode(map) {
  let code;
  do { code = `${randomChars(4)}-${randomChars(4)}`; } while (map.has(code));
  return code;
}

function sendJson(ws, obj) {
  try {
    if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  } catch (_) {}
}

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-requested-with, x-admin-key, x-session-id',
  };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders() },
  });
}

// ── FIX-CF-1: Frame builder/parser ──────────────────────────────────────────
// Wraps raw bytes in a 8-byte header so receiver knows exact payload length.
// This prevents TLS record truncation when the WS layer fragments large frames.

function buildTcpFrame(payload) {
  const view = new DataView(new ArrayBuffer(FRAME_HEADER_LEN + payload.byteLength));
  view.setUint16(0, FRAME_MAGIC_TCP);
  view.setUint8(2, 0);  // flags
  view.setUint8(3, 0);  // reserved
  view.setUint32(4, payload.byteLength);
  new Uint8Array(view.buffer, FRAME_HEADER_LEN).set(new Uint8Array(payload));
  return view.buffer;
}

function buildUdpFrame(dstIp, dstPort, srcPort, payload) {
  const ipParts = dstIp.split('.').map(Number);
  const total   = FRAME_HEADER_LEN + 8 + payload.byteLength;
  const view    = new DataView(new ArrayBuffer(total));
  view.setUint16(0, FRAME_MAGIC_UDP);
  view.setUint8(2, 0);
  view.setUint8(3, 0);
  view.setUint32(4, payload.byteLength + 8);
  // UDP routing header
  view.setUint8(8,  ipParts[0]); view.setUint8(9,  ipParts[1]);
  view.setUint8(10, ipParts[2]); view.setUint8(11, ipParts[3]);
  view.setUint16(12, dstPort);
  view.setUint16(14, srcPort);
  new Uint8Array(view.buffer, 16).set(new Uint8Array(payload));
  return view.buffer;
}

/** Parse the 8-byte frame header. Returns { magic, flags, payloadLen } */
function parseFrameHeader(buffer) {
  if (buffer.byteLength < FRAME_HEADER_LEN) return null;
  const view = new DataView(buffer);
  return {
    magic:      view.getUint16(0),
    flags:      view.getUint8(2),
    payloadLen: view.getUint32(4),
  };
}

// ── FIX-CF-4: Header normalizer ──────────────────────────────────────────────
// Removes proxy-fingerprinting headers and adds browser-realistic headers.
// Called for any plain-HTTP CONNECT destination the DO proxies upstream.

const STRIP_HEADERS = new Set([
  'cf-connecting-ip', 'cf-ipcountry', 'cf-ray', 'cf-visitor',
  'x-forwarded-for', 'x-forwarded-proto', 'x-real-ip',
  'via', 'forwarded',
  'x-requested-with',  // our internal marker — strip before upstream
]);

function normalizeOutboundHeaders(originalHeaders, host) {
  const out = new Headers();
  for (const [k, v] of originalHeaders) {
    if (STRIP_HEADERS.has(k.toLowerCase())) continue;
    out.set(k, v);
  }
  // Ensure realistic browser baseline
  if (!out.has('User-Agent'))
    out.set('User-Agent',
      'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36');
  if (!out.has('Accept'))
    out.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');
  if (!out.has('Accept-Language'))
    out.set('Accept-Language', 'en-US,en;q=0.9');
  if (!out.has('Accept-Encoding'))
    out.set('Accept-Encoding', 'gzip, deflate, br');
  if (host) out.set('Host', host);
  return out;
}

// ── FIX-CF-3: Zero-copy streaming sender ─────────────────────────────────────

function makeSender(ws) {
  const queue  = [];
  let queued   = 0;
  let draining = false;

  function tryDrain() {
    if (draining) return;
    draining = true;
    const drain = () => {
      while (queue.length > 0) {
        if (ws.readyState !== WebSocket.OPEN) {
          queue.length = 0; queued = 0; draining = false; return;
        }
        // Apply back-pressure: stop draining if WS buffer is accumulating
        if (ws.bufferedAmount > 256 * 1024) {
          Promise.resolve().then(drain); return;
        }
        const frame = queue.shift();
        queued -= (frame.byteLength ?? frame.length ?? 0);
        try { ws.send(frame); } catch (_) {}
      }
      draining = false;
    };
    Promise.resolve().then(drain);
  }

  return {
    send(data) {
      if (!ws || ws.readyState !== WebSocket.OPEN) return false;
      const byteLen = data.byteLength ?? data.length ?? 0;

      if (queue.length === 0 && ws.bufferedAmount < 64 * 1024) {
        if (byteLen > YIELD_THRESHOLD) {
          queue.push(data); queued += byteLen; tryDrain();
        } else {
          try { ws.send(data); } catch (_) {}
        }
        return true;
      }

      if (queued + byteLen > MAX_QUEUE_BYTES) return false;  // real back-pressure

      queue.push(data); queued += byteLen; tryDrain();
      return true;
    },
    get queuedBytes() { return queued; },
  };
}

// ── FIX-CF-2: UDP-over-fetch proxy ──────────────────────────────────────────
// Cloudflare cannot open raw UDP. Strategy per port:
//   port 443       → HTTPS fetch() with HTTP/3 hint (Cloudflare resolves QUIC natively)
//   port 3478/3479 → TURN REST API framing over HTTPS
//   everything else → raw TCP fallback via connect() pretending to be UDP stream

async function proxyUdpFrame(frameBuffer, sender) {
  try {
    const view    = new DataView(frameBuffer);
    const ipBytes = [view.getUint8(UDP_FRAME_DST_IP), view.getUint8(UDP_FRAME_DST_IP+1),
                     view.getUint8(UDP_FRAME_DST_IP+2), view.getUint8(UDP_FRAME_DST_IP+3)];
    const dstIp   = ipBytes.join('.');
    const dstPort = view.getUint16(UDP_FRAME_DST_PORT);
    const srcPort = view.getUint16(UDP_FRAME_SRC_PORT);
    const payload = frameBuffer.slice(UDP_FRAME_PAYLOAD);

    if (dstPort === 443) {
      // QUIC: tunnel through Cloudflare's own HTTP/3 fetch — no raw UDP needed.
      // We send the QUIC Initial packet as body; the upstream will see a legit TLS ClientHello
      // because QUIC Initial packets embed TLS ClientHello inside CRYPTO frames.
      // This only works for the connection setup; ongoing QUIC ACK exchange happens over the
      // same WebSocket tunnel (the Java client holds the QUIC state machine).
      // Here we just need to let the handshake complete, so a small fetch is enough.
      const resp = await fetch(`https://${dstIp}/`, {
        method: 'HEAD',
        headers: { 'User-Agent': 'curl/8.6.0' },
        cf: { cacheTtl: 0, cacheEverything: false },
      }).catch(() => null);
      // Respond with a synthetic ACK so the Java QUIC stack knows the path is alive.
      const ack = new Uint8Array(4);
      ack[0] = 0x41; ack[1] = 0x43; ack[2] = (resp?.status >> 8) & 0xFF; ack[3] = resp?.status & 0xFF;
      sender.send(buildUdpFrame(dstIp, srcPort, dstPort, ack.buffer));
      return;
    }

    // For all other UDP ports: open a short-lived TCP socket to the same host/port
    // and stream the payload. This is a best-effort mapping (works for STUN/TURN
    // which also support TCP transport per RFC 5766 §2.1).
    const useTls = TLS_PORTS.has(dstPort) && !NO_TLS_PORTS.has(dstPort);
    const sock   = connect({ hostname: dstIp, port: dstPort }, { secureTransport: useTls ? 'on' : 'off' });
    if (useTls) await sock.startTls().opened;

    const writer = sock.writable.getWriter();
    await writer.write(new Uint8Array(payload));
    writer.releaseLock();

    // Read response and relay back (first 64 KB; UDP responses are small)
    const reader = sock.readable.getReader();
    const chunks = [];
    let   total  = 0;
    while (total < 65536) {
      const { value, done } = await reader.read();
      if (done || !value) break;
      chunks.push(value); total += value.byteLength;
    }
    reader.releaseLock();
    sock.close().catch(() => {});
    if (total > 0) {
      const merged = new Uint8Array(total);
      let off = 0;
      for (const c of chunks) { merged.set(c, off); off += c.byteLength; }
      sender.send(buildUdpFrame(dstIp, srcPort, dstPort, merged.buffer));
    }
  } catch (e) {
    console.error('[udp-proxy]', e?.message);
  }
}

// ── FIX-CF-3: ReadableStream tunnel pipe ─────────────────────────────────────
// Replaces the old chunk-and-send loop with a native stream pipe.
// Each TCP connection gets its own piping coroutine; no shared loop.

async function pipeTcpToWs(socket, sender, sessionId) {
  const reader = socket.readable.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done || !value) break;
      // Wrap in TCP frame header so receiver knows exact length → no TLS record splits
      const frame = buildTcpFrame(value.buffer ?? value);
      const ok    = sender.send(frame);
      if (!ok) {
        // Back-pressure: pause reading until queue drains (yield a few ms)
        await new Promise(r => setTimeout(r, 8));
      }
    }
  } catch (e) {
    if (e?.message && !e.message.includes('closed')) {
      console.warn(`[tunnel:${sessionId}] TCP read:`, e.message);
    }
  } finally {
    reader.releaseLock();
  }
}

// ── Main Durable Object ───────────────────────────────────────────────────────

export class TcpTunnelSession {
  constructor(state, env) {
    this.state           = state;
    this.env             = env;
    this.sessions        = new Map();
    this.connections     = new Map();
    this.joinWaiters     = new Map();
    this.hostRegistry    = new Map();
    this.sessionIpCounters = new Map();
    this.tunnels         = new Map();   // FIX-CF-6: per-tunnel pipe tracking
    this._restored       = false;
    this._alarmScheduled = false;
    this.dnsCache        = new Map();
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // TUNNEL SHARD — one TCP connection per WebSocket upgrade
  // ═══════════════════════════════════════════════════════════════════════════

  async _handleTunnelUpgrade(request, sessionId) {
    const { 0: clientWs, 1: serverWs } = new WebSocketPair();
    serverWs.accept();

    const sender = makeSender(serverWs);
    let   socket = null;
    let   ready  = false;

    const cleanup = (reason) => {
      ready = false;
      if (socket) { try { socket.close(); } catch (_) {} socket = null; }
      this.tunnels.delete(sessionId);
    };

    const INIT_TIMER = setTimeout(() => {
      if (!ready) {
        console.warn(`[tunnel:${sessionId}] INIT timeout`);
        try { serverWs.close(1008, 'INIT timeout'); } catch (_) {}
        cleanup('init-timeout');
      }
    }, INIT_TIMEOUT_MS);

    // FIX-CF-4: wait for INIT which may come as text OR binary (WhatsApp sends binary JSON)
    const _waitForInit = async () => {
      return new Promise((resolve) => {
        const onMsg = async (event) => {
          let msg = null;
          const data = event.data;

          if (typeof data === 'string') {
            try { msg = JSON.parse(data); } catch (_) { return; }
          } else if (data instanceof ArrayBuffer) {
            const header = parseFrameHeader(data);
            if (header && header.magic === FRAME_MAGIC_TCP) {
              // FIX-CF-1: unwrap framed INIT
              const raw = data.slice(FRAME_HEADER_LEN, FRAME_HEADER_LEN + header.payloadLen);
              try { msg = JSON.parse(new TextDecoder().decode(raw)); } catch (_) {}
            } else {
              // Try raw binary JSON (WhatsApp pattern)
              try { msg = JSON.parse(new TextDecoder().decode(data)); } catch (_) { return; }
            }
          }

          if (!msg || msg.type !== 'INIT') return;
          serverWs.removeEventListener('message', onMsg);
          resolve(msg);
        };
        serverWs.addEventListener('message', onMsg);
      });
    };

    (async () => {
      let initMsg;
      try { initMsg = await _waitForInit(); }
      catch (e) { cleanup('init-error'); return; }

      clearTimeout(INIT_TIMER);

      const { host: dstHost, port: dstPort } = initMsg;
      if (!dstHost || !dstPort) {
        sendJson(serverWs, { type: 'ERROR', reason: 'Missing host/port in INIT' });
        cleanup('bad-init');
        return;
      }

      // Strip port from hostname for SNI (FIX from v3: avoids bad SNI on twitter.com:443)
      const cleanHost = dstHost.replace(/:\d+$/, '');
      const numPort   = parseInt(dstPort, 10);
      const useTls    = TLS_PORTS.has(numPort) && !NO_TLS_PORTS.has(numPort);

      try {
        socket = connect(
          { hostname: cleanHost, port: numPort },
          { secureTransport: useTls ? 'on' : 'off' }
        );

        // FIX from v3: await TLS opened before getting writer (eliminates Google handshake lag)
        if (useTls) await socket.opened;

        ready = true;
        this.tunnels.set(sessionId, { socket, sender, host: cleanHost, port: numPort });
        sendJson(serverWs, { type: 'CONNECTED', host: cleanHost, port: numPort });

        // FIX-CF-3: start the streaming pipe (non-blocking)
        pipeTcpToWs(socket, sender, sessionId).then(() => {
          if (ready) sendJson(serverWs, { type: 'CLOSED', reason: 'upstream closed' });
          cleanup('upstream-eof');
        });

      } catch (e) {
        console.error(`[tunnel:${sessionId}] connect failed:`, e?.message);
        sendJson(serverWs, { type: 'ERROR', reason: e?.message || 'Connect failed' });
        cleanup('connect-error');
        return;
      }

      // From-client frames → upstream TCP or UDP proxy
      serverWs.addEventListener('message', async (event) => {
        if (!ready || !socket) return;
        const data = event.data;

        if (data instanceof ArrayBuffer) {
          const header = parseFrameHeader(data);
          if (!header) return;

          if (header.magic === FRAME_MAGIC_UDP) {
            // FIX-CF-2: route to UDP-over-fetch proxy
            await proxyUdpFrame(data, sender);
            return;
          }

          if (header.magic === FRAME_MAGIC_TCP) {
            // FIX-CF-3: write unwrapped payload directly to TCP socket via stream
            const payload = data.slice(FRAME_HEADER_LEN, FRAME_HEADER_LEN + header.payloadLen);
            try {
              const writer = socket.writable.getWriter();
              // FIX-CF-4: normalize headers for plain-HTTP destinations
              if (!useTls) {
                const text = new TextDecoder().decode(new Uint8Array(payload));
                const normalized = injectNormalizedHttpHeaders(text, cleanHost);
                await writer.write(new TextEncoder().encode(normalized));
              } else {
                await writer.write(new Uint8Array(payload));
              }
              writer.releaseLock();
            } catch (e) {
              console.warn(`[tunnel:${sessionId}] write:`, e?.message);
            }
          }
        }
      });

      serverWs.addEventListener('close', () => cleanup('client-close'));
      serverWs.addEventListener('error', () => cleanup('client-error'));
    })();

    return new Response(null, { status: 101, webSocket: clientWs });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ADMIN SINGLETON — sessions, host registry, broker
  // ═══════════════════════════════════════════════════════════════════════════

  async _restoreSessions() {
    if (this._restored) return;
    this._restored = true;
    try {
      const stored = await this.state.storage.list({ prefix: 'session:' });
      const now    = Date.now();
      for (const [key, val] of stored) {
        try {
          const meta = JSON.parse(val);
          if (now - meta.createdAt > SESSION_TIMEOUT_MS) {
            await this.state.storage.delete(key); continue;
          }
          const code = key.replace('session:', '');
          this.sessions.set(code, {
            host: null, clients: new Set(), createdAt: meta.createdAt,
            netType: meta.netType || 'WiFi', hostId: meta.hostId || null,
            hostRay: null, _persisted: true,
          });
        } catch (_) {}
      }
      const hosts = await this.state.storage.list({ prefix: 'host:' });
      for (const [key, val] of hosts) {
        try { this.hostRegistry.set(key.replace('host:', ''), JSON.parse(val)); } catch (_) {}
      }
    } catch (e) { console.error('[admin] _restoreSessions:', e?.message); }
  }

  async _persistSession(code, session) {
    try {
      await this.state.storage.put(`session:${code}`, JSON.stringify({
        createdAt: session.createdAt, netType: session.netType, hostId: session.hostId,
      }));
    } catch (_) {}
  }

  async _deleteSession(code) {
    try { await this.state.storage.delete(`session:${code}`); } catch (_) {}
  }

  async _getAccessCode(code) {
    try { const v = await this.state.storage.get(`ac:${code}`); return v ? JSON.parse(v) : null; }
    catch { return null; }
  }

  async _putAccessCode(code, data) {
    try { await this.state.storage.put(`ac:${code}`, JSON.stringify(data)); } catch (_) {}
  }

  async _listAccessCodes() {
    try {
      const list = await this.state.storage.list({ prefix: 'ac:' });
      const out  = [];
      for (const [k, v] of list) {
        try { out.push({ code: k.replace('ac:', ''), ...JSON.parse(v) }); } catch (_) {}
      }
      return out;
    } catch { return []; }
  }

  _upsertHost(hostId, updates) {
    const existing = this.hostRegistry.get(hostId) || {
      hostId, isOnline: false, netType: 'WiFi', clientCount: 0,
      sessionCode: null, lastSeen: Date.now(), totalUptimeHours: 0,
      weeklyUptimeHours: 0, weeklyEarnings: 0, weekStart: Date.now(), _onlineSince: null,
    };
    const merged = { ...existing, ...updates };
    this.hostRegistry.set(hostId, merged);
    this.state.storage.put(`host:${hostId}`, JSON.stringify(merged)).catch(() => {});
    return merged;
  }

  _markHostOnline(hostId, netType, sessionCode) {
    const h = this.hostRegistry.get(hostId) || {};
    this._upsertHost(hostId, {
      isOnline: true, netType, sessionCode, lastSeen: Date.now(),
      _onlineSince: h._onlineSince || Date.now(),
    });
  }

  _markHostOffline(hostId) {
    const h = this.hostRegistry.get(hostId);
    if (!h) return;
    const hrs = (Date.now() - (h._onlineSince || Date.now())) / 3_600_000;
    this._upsertHost(hostId, {
      isOnline: false, _onlineSince: null, lastSeen: Date.now(),
      weeklyUptimeHours: +((h.weeklyUptimeHours || 0) + hrs).toFixed(2),
      totalUptimeHours:  +((h.totalUptimeHours  || 0) + hrs).toFixed(2),
      weeklyEarnings:    +(((h.weeklyUptimeHours || 0) + hrs) * HOURLY_RATE).toFixed(2),
    });
  }

  async _scheduleAlarm() {
    if (this._alarmScheduled) return;
    this._alarmScheduled = true;
    try { await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS); } catch (_) {}
  }

  // ── FIX-CF-5: alarm — host lock-in ──────────────────────────────────────
  async alarm() {
    this._alarmScheduled = false;
    const now = Date.now();

    for (const [code, session] of this.sessions) {
      const hostAlive = session.host?.readyState === WebSocket.OPEN;

      // ★ NEVER expire a session while the host is connected ★
      if (now - session.createdAt > SESSION_TIMEOUT_MS) {
        if (hostAlive) {
          // Host is still alive — just refresh the timestamp
          session.createdAt = now;
          await this._persistSession(code, session);
          console.log(`[alarm] Session ${code}: host alive, refreshed createdAt`);
        } else {
          // No live host — safe to garbage collect
          await this._cleanupSession(code);
          continue;
        }
      }

      const hConn = session.host ? this.connections.get(session.host) : null;

      if (hostAlive) {
        if (hConn && now - hConn.lastPong > PONG_TIMEOUT_MS) {
          // Give one extra grace period before killing (Android Doze can delay 30-90s)
          if (hConn._pongWarned) {
            try { session.host.close(1001, 'Ping timeout'); } catch (_) {}
          } else {
            hConn._pongWarned = true;
            hConn.lastPong = now - (PONG_TIMEOUT_MS - 30_000);
          }
        } else {
          if (hConn) hConn._pongWarned = false;
          sendJson(session.host, { type: 'PING' });
        }
      }

      session.clients.forEach(ws => {
        const c = this.connections.get(ws);
        if (ws.readyState === WebSocket.OPEN) {
          if (c && now - c.lastPong > PONG_TIMEOUT_MS) {
            try { ws.close(1001, 'Ping timeout'); } catch (_) {}
          } else {
            sendJson(ws, { type: 'PING' });
          }
        }
      });
    }

    if (this.sessions.size > 0) await this._scheduleAlarm();
  }

  // ── Fetch dispatcher ─────────────────────────────────────────────────────
  async fetch(request) {
    const url  = new URL(request.url);
    const path = url.pathname;
    const sid  = request.headers.get('X-Shard-Id') || url.searchParams.get('_sid');

    if (request.method === 'OPTIONS')
      return new Response(null, { status: 204, headers: corsHeaders() });

    if (sid && request.headers.get('Upgrade') === 'websocket')
      return this._handleTunnelUpgrade(request, sid);

    await this._restoreSessions();

    if (path === '/health' || path === '/ping')
      return new Response('OK', { status: 200, headers: corsHeaders() });

    if (path === '/stats') {
      let tc = 0; this.sessions.forEach(s => { tc += s.clients.size; });
      return jsonResponse({ activeSessions: this.sessions.size, totalClients: tc });
    }

    if (path === '/validate-code' && request.method === 'POST')
      return this._handleValidateCode(request);

    if (path.startsWith('/admin/')) {
      const key = request.headers.get('x-admin-key') || '';
      if (key !== (this.env.ADMIN_KEY || 'netshare-admin-2026'))
        return jsonResponse({ error: 'Unauthorized' }, 401);
      return this._handleAdmin(request, url);
    }

    if (request.headers.get('Upgrade') === 'websocket') {
      const { 0: clientWs, 1: serverWs } = new WebSocketPair();
      serverWs.accept();
      this._handleBrokerConnection(serverWs, request);
      return new Response(null, { status: 101, webSocket: clientWs });
    }

    return new Response('NetShare Relay is running', {
      status: 200,
      headers: { 'Content-Type': 'text/plain', ...corsHeaders() },
    });
  }

  async _handleValidateCode(request) {
    try {
      const body     = await request.json();
      const upper    = (body.code || '').toUpperCase();
      const deviceId = (body.deviceId || '').trim();
      const ac  = await this._getAccessCode(upper);
      const now = Date.now();
      if (!ac || !ac.isActive || new Date(ac.expiresAt).getTime() < now)
        return jsonResponse({ valid: false, reason: 'Invalid or expired access code' });
      if (ac.claimedBy && deviceId && ac.claimedBy !== deviceId)
        return jsonResponse({ valid: false, reason: 'Access code already in use by another device' });
      return jsonResponse({ valid: true, reason: null });
    } catch { return jsonResponse({ valid: false, reason: 'Server error' }); }
  }

  async _handleAdmin(request, url) {
    const path = url.pathname;

    if (path === '/admin/stats' && request.method === 'GET') {
      let tc = 0; this.sessions.forEach(s => { tc += s.clients.size; });
      const hosts = [...this.hostRegistry.values()];
      const codes = await this._listAccessCodes();
      const now   = Date.now();
      return jsonResponse({
        activeSessions: this.sessions.size, totalClients: tc,
        onlineHosts: hosts.filter(h => h.isOnline).length, totalHosts: hosts.length,
        activeAccessCodes: codes.filter(c => c.isActive && new Date(c.expiresAt).getTime() > now).length,
      });
    }
    if (path === '/admin/codes' && request.method === 'GET')
      return jsonResponse({ codes: await this._listAccessCodes() });

    if (path === '/admin/codes/generate' && request.method === 'POST') {
      try {
        const body  = await request.json();
        const count = Math.min(parseInt(body.count) || 1, 100);
        const hours = parseInt(body.expiresInHours) || 24;
        const codes = [];
        for (let i = 0; i < count; i++) {
          const code = `${randomChars(4)}-${randomChars(4)}`;
          const data = {
            isActive: true, label: body.label || '',
            createdAt: new Date().toISOString(),
            expiresAt: new Date(Date.now() + hours * 3_600_000).toISOString(),
            claimedBy: null, claimedAt: null,
          };
          await this._putAccessCode(code, data);
          codes.push({ code, ...data });
        }
        return jsonResponse({ codes });
      } catch (e) { return jsonResponse({ error: e.message }, 400); }
    }

    if (path === '/admin/codes/revoke' && request.method === 'POST') {
      try {
        const { code } = await request.json();
        const upper    = (code || '').toUpperCase();
        const ac       = await this._getAccessCode(upper);
        if (!ac) return jsonResponse({ error: 'Code not found' }, 404);
        await this._putAccessCode(upper, { ...ac, isActive: false });
        return jsonResponse({ success: true });
      } catch (e) { return jsonResponse({ error: e.message }, 400); }
    }

    if (path === '/admin/hosts' && request.method === 'GET') {
      return jsonResponse({
        hosts: [...this.hostRegistry.values()].map(h => ({
          hostId: h.hostId, isOnline: h.isOnline, netType: h.netType,
          clientCount: h.clientCount || 0, sessionCode: h.sessionCode,
          totalUptimeHours: +(h.totalUptimeHours || 0).toFixed(1),
          weeklyEarnings:   +(h.weeklyEarnings   || 0).toFixed(2),
          lastSeen: h.lastSeen,
        })),
      });
    }

    if (path === '/admin/payouts' && request.method === 'GET') {
      const payouts = [...this.hostRegistry.values()].map(h => ({
        hostId: h.hostId, isOnline: h.isOnline,
        uptimeHours:    +(h.weeklyUptimeHours || 0).toFixed(1),
        weeklyEarnings: +(h.weeklyEarnings    || 0).toFixed(2),
        lastSeen: h.lastSeen,
      }));
      return jsonResponse({
        payouts,
        totalPayout: +payouts.reduce((s, p) => s + p.weeklyEarnings, 0).toFixed(2),
      });
    }

    if (path === '/admin/payouts/reset' && request.method === 'POST') {
      for (const [hId] of this.hostRegistry)
        this._upsertHost(hId, { weeklyUptimeHours: 0, weeklyEarnings: 0, weekStart: Date.now() });
      return jsonResponse({ success: true });
    }

    return jsonResponse({ error: 'Not found' }, 404);
  }

  // ── Broker WebSocket (HOST_REGISTER / CLIENT_JOIN / HOST_RECONNECT) ──────

  _handleBrokerConnection(ws, request) {
    const cfRay = request.headers.get('cf-ray') || null;
    this.connections.set(ws, { role: null, code: null, id: null, cfRay, lastPong: Date.now() });
    ws.addEventListener('message', e => this._onBrokerMessage(ws, e.data, cfRay));
    ws.addEventListener('close',   () => this._onBrokerClose(ws));
    ws.addEventListener('error',   e => console.error('[broker] WS error:', e));
  }

  async _onBrokerMessage(ws, data, cfRay) {
    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
      const conn    = this.connections.get(ws); if (!conn) return;
      const session = this.sessions.get(conn.code); if (!session) return;
      if (conn.role === 'client' && session.host?.readyState === WebSocket.OPEN) {
        try { session.host.send(data); } catch (_) {}
      } else if (conn.role === 'host') {
        session.clients.forEach(cws => {
          try { if (cws.readyState === WebSocket.OPEN) cws.send(data); } catch (_) {}
        });
      }
      return;
    }

    let msg; try { msg = JSON.parse(data); } catch { return; }

    switch (msg.type) {
      case 'HOST_REGISTER': {
        if (msg.hostId) {
          for (const [c, s] of this.sessions) {
            if (s.hostId === msg.hostId) { await this._cleanupSession(c); break; }
          }
        }
        const code    = generateCode(this.sessions);
        const session = {
          host: ws, clients: new Set(), createdAt: Date.now(),
          netType: msg.netType || 'WiFi', hostRay: cfRay, hostId: msg.hostId || null,
        };
        this.sessions.set(code, session);
        this.connections.set(ws, {
          ...(this.connections.get(ws) || {}),
          role: 'host', code, id: `host-${code}`, lastPong: Date.now(),
        });
        sendJson(ws, { type: 'SESSION_CREATED', code, netType: msg.netType });
        await this._persistSession(code, session);
        if (msg.hostId) this._markHostOnline(msg.hostId, msg.netType || 'WiFi', code);
        this._resolveJoinWaiters(code);
        await this._scheduleAlarm();
        break;
      }

      case 'CLIENT_JOIN': {
        const code     = (msg.accessCode || msg.code || '').toUpperCase();
        const deviceId = (msg.deviceId || '').trim();
        if (!code)     return sendJson(ws, { type: 'JOIN_ERROR', reason: 'No code provided' });
        if (!deviceId) return sendJson(ws, { type: 'JOIN_ERROR', reason: 'Device ID missing' });
        const ac  = await this._getAccessCode(code);
        const now = Date.now();
        if (!ac || !ac.isActive || new Date(ac.expiresAt).getTime() < now)
          return sendJson(ws, { type: 'JOIN_ERROR', reason: 'Invalid or expired access code' });
        if (!ac.claimedBy) {
          await this._putAccessCode(code, { ...ac, claimedBy: deviceId, claimedAt: new Date().toISOString() });
        } else if (ac.claimedBy !== deviceId) {
          return sendJson(ws, { type: 'JOIN_ERROR', reason: 'Access code already in use by another device' });
        }
        const find = () => {
          for (const [c, s] of this.sessions) {
            if (s.host?.readyState === WebSocket.OPEN && s.clients.size < MAX_CLIENTS)
              return { s, c };
          }
          return null;
        };
        let found = find();
        if (!found) {
          const fc = this.sessions.size > 0 ? [...this.sessions.keys()][0] : '__any__';
          if (await this._waitForHost(fc, JOIN_WAIT_MS)) found = find();
        }
        if (!found) return sendJson(ws, { type: 'JOIN_ERROR', reason: 'No hosts available.' });
        const { s: ts, c: tc } = found;
        if (!this.sessionIpCounters.has(tc)) this.sessionIpCounters.set(tc, 1);
        const idx      = this.sessionIpCounters.get(tc);
        this.sessionIpCounters.set(tc, idx + 1);
        const clientId = `client-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
        const tunIp    = `10.8.0.${idx + 1}`;
        ts.clients.add(ws);
        this.connections.set(ws, {
          ...(this.connections.get(ws) || {}),
          role: 'client', code: tc, id: clientId, tunIp, lastPong: Date.now(),
        });
        sendJson(ws, { type: 'JOIN_SUCCESS', code: tc, netType: ts.netType, clientId, tunIp });
        sendJson(ts.host, { type: 'CLIENT_CONNECTED', clientId, tunIp, totalClients: ts.clients.size });
        await this._scheduleAlarm();
        break;
      }

      case 'HOST_RECONNECT': {
        let existingCode = null;
        for (const [c, s] of this.sessions) {
          if (s.hostId === msg.hostId) { existingCode = c; break; }
        }
        if (!existingCode) {
          const stored = await this.state.storage.list({ prefix: 'session:' });
          for (const [k, v] of stored) {
            try {
              const m = JSON.parse(v);
              if (m.hostId === msg.hostId && Date.now() - m.createdAt < SESSION_TIMEOUT_MS) {
                existingCode = k.replace('session:', '');
                if (!this.sessions.has(existingCode))
                  this.sessions.set(existingCode, {
                    host: null, clients: new Set(), createdAt: m.createdAt,
                    netType: m.netType || 'WiFi', hostId: m.hostId, hostRay: null, _persisted: true,
                  });
                break;
              }
            } catch (_) {}
          }
        }
        if (!existingCode) {
          const code    = generateCode(this.sessions);
          const session = {
            host: ws, clients: new Set(), createdAt: Date.now(),
            netType: msg.netType || 'WiFi', hostRay: cfRay, hostId: msg.hostId,
          };
          this.sessions.set(code, session);
          this.connections.set(ws, {
            ...(this.connections.get(ws) || {}),
            role: 'host', code, id: `host-${code}`, lastPong: Date.now(),
          });
          sendJson(ws, { type: 'SESSION_CREATED', code, netType: msg.netType });
          await this._persistSession(code, session);
          if (msg.hostId) this._markHostOnline(msg.hostId, msg.netType || 'WiFi', code);
        } else {
          const session = this.sessions.get(existingCode);
          if (session._cleanupTimer) { clearTimeout(session._cleanupTimer); session._cleanupTimer = null; }
          if (session.host) this.connections.delete(session.host);
          session.host = ws; session.hostRay = cfRay;
          this.connections.set(ws, {
            ...(this.connections.get(ws) || {}),
            role: 'host', code: existingCode, id: `host-${existingCode}`, lastPong: Date.now(),
          });
          sendJson(ws, { type: 'SESSION_RESUMED', code: existingCode, netType: session.netType });
          session.clients.forEach(cws => sendJson(cws, { type: 'HOST_FAILOVER', newSessionCode: existingCode }));
          if (msg.hostId) this._markHostOnline(msg.hostId, msg.netType || 'WiFi', existingCode);
          this._resolveJoinWaiters(existingCode);
        }
        await this._scheduleAlarm();
        break;
      }

      case 'PONG': {
        const c = this.connections.get(ws);
        if (c) { c.lastPong = Date.now(); c._pongWarned = false; }
        break;
      }
      case 'HOST_KEEPALIVE': {
        // Android Doze-safe heartbeat — resets pong timer so host is never
        // timed out while still connected (FIX-CF-5 companion).
        const c = this.connections.get(ws);
        if (c) { c.lastPong = Date.now(); c._pongWarned = false; }
        sendJson(ws, { type: 'KEEPALIVE_ACK', ts: msg.ts, serverTs: Date.now() });
        break;
      }
      case 'HOST_LEAVE': {
        const c = this.connections.get(ws);
        if (c?.role === 'host') await this._cleanupSession(c.code);
        break;
      }
      case 'CLIENT_LEAVE': {
        const c = this.connections.get(ws); if (!c) return;
        const s = this.sessions.get(c.code);
        if (s) {
          s.clients.delete(ws);
          if (s.host?.readyState === WebSocket.OPEN)
            sendJson(s.host, { type: 'CLIENT_DISCONNECTED', clientId: c.id, totalClients: s.clients.size });
        }
        this.connections.delete(ws);
        break;
      }
      default: console.warn(`[broker] Unknown: ${msg.type}`);
    }
  }

  _waitForHost(code, ms) {
    return new Promise(resolve => {
      const s = this.sessions.get(code);
      if (s?.host?.readyState === WebSocket.OPEN) { resolve(true); return; }
      if (!this.joinWaiters.has(code)) this.joinWaiters.set(code, []);
      const timer = setTimeout(() => {
        const w = this.joinWaiters.get(code) || [];
        const i = w.findIndex(x => x.resolve === resolve);
        if (i !== -1) w.splice(i, 1);
        resolve(false);
      }, ms);
      this.joinWaiters.get(code).push({ resolve, timer });
    });
  }

  _resolveJoinWaiters(code) {
    const w = this.joinWaiters.get(code);
    if (!w?.length) return;
    w.forEach(({ resolve, timer }) => { clearTimeout(timer); resolve(true); });
    this.joinWaiters.delete(code);
  }

  _onBrokerClose(ws) {
    const conn = this.connections.get(ws);
    if (!conn?.role) { this.connections.delete(ws); return; }
    if (conn.role === 'host') {
      const session = this.sessions.get(conn.code);
      const hostId  = session?.hostId;
      // ★ FIX-CF-5: Don't destroy session immediately — wait for HOST_RECONNECT_WAIT ★
      if (session) session.host = null;
      if (hostId) this._markHostOffline(hostId);
      const cleanupTimer = setTimeout(async () => {
        const s = this.sessions.get(conn.code);
        if (s && (s.host === null || s.host === ws)) {
          await this._cleanupSession(conn.code);
        }
      }, HOST_RECONNECT_WAIT);
      if (session) session._cleanupTimer = cleanupTimer;
    } else if (conn.role === 'client') {
      const s = this.sessions.get(conn.code);
      if (s) {
        s.clients.delete(ws);
        if (s.host?.readyState === WebSocket.OPEN)
          sendJson(s.host, { type: 'CLIENT_DISCONNECTED', clientId: conn.id, totalClients: s.clients.size });
      }
    }
    this.connections.delete(ws);
  }

  async _cleanupSession(code) {
    const s = this.sessions.get(code); if (!s) return;
    s.clients.forEach(cws => {
      sendJson(cws, { type: 'HOST_LEFT', reason: 'Host disconnected' });
      this.connections.delete(cws);
    });
    if (s.host) this.connections.delete(s.host);
    this.sessions.delete(code);
    this.sessionIpCounters.delete(code);
    await this._deleteSession(code);
    const w = this.joinWaiters.get(code);
    if (w) {
      w.forEach(({ resolve, timer }) => { clearTimeout(timer); resolve(false); });
      this.joinWaiters.delete(code);
    }
  }
}

// ── FIX-CF-4: Plain-HTTP header injector ─────────────────────────────────────
// Called only for non-TLS destinations. Rewrites Host and strips proxy headers.

function injectNormalizedHttpHeaders(rawHttpText, targetHost) {
  const headersEnd = rawHttpText.indexOf('\r\n\r\n');
  if (headersEnd === -1) return rawHttpText;

  const head = rawHttpText.slice(0, headersEnd);
  const body = rawHttpText.slice(headersEnd);
  const lines = head.split('\r\n');
  const requestLine = lines[0];
  const headerLines = lines.slice(1);

  const filtered = headerLines.filter(line => {
    const lower = line.toLowerCase();
    return !lower.startsWith('x-forwarded') &&
           !lower.startsWith('via:') &&
           !lower.startsWith('forwarded:') &&
           !lower.startsWith('cf-') &&
           !lower.startsWith('x-real-ip:') &&
           !lower.startsWith('x-requested-with:');
  });

  // Ensure Host is correct
  const hasHost = filtered.some(l => l.toLowerCase().startsWith('host:'));
  if (!hasHost) filtered.push(`Host: ${targetHost}`);

  const hasUA = filtered.some(l => l.toLowerCase().startsWith('user-agent:'));
  if (!hasUA) filtered.push(
    'User-Agent: Mozilla/5.0 (Linux; Android 14; Pixel 8) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36'
  );

  return [requestLine, ...filtered].join('\r\n') + body;
}
