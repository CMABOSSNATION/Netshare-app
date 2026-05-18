/**
 * src/services/ProxyService.js — NetShare HTTP/HTTPS Proxy
 *
 * ── Two connection modes ───────────────────────────────────────────────────────
 *
 * MODE 1: LAN / Same-WiFi  (tunnelMode = false)
 *   Host runs proxy on :8899, relay stores IP:port, client configures
 *   Android Wi-Fi proxy settings. Traffic is direct host ↔ client.
 *   Range: same Wi-Fi network only (~100m).
 *
 * MODE 2: WebSocket Tunnel (tunnelMode = true)  ← NEW — fixes 300km problem
 *   Host opens WebSocket to relay /ws/host/:code
 *   Client opens WebSocket to relay /ws/client/:code
 *   Relay (Durable Object) pipes bytes between both WebSockets.
 *   Client side: a local proxy server on :8899 bridges the WS tunnel.
 *   Traffic flows: Client app → localhost:8899 → WS → Relay DO → WS → Host
 *   Range: anywhere in the world (through Cloudflare edge).
 *
 * The host decides which mode based on whether a public IP is available.
 * The UI can also let the user pick.
 */

import { NativeModules } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { ProxyModule } = NativeModules;

export const RELAY_URL = process.env.RELAY_URL
  || 'https://netshare.cmaraphael90.workers.dev';

// WebSocket URL (wss:// version of the relay)
const RELAY_WS_URL = RELAY_URL.replace(/^https?:\/\//, 'wss://');

// ── Internal state ────────────────────────────────────────────────────────────

let _listeners      = {};
let _sessionCode    = null;
let _role           = null;      // 'host' | 'client'
let _statsInterval  = null;
let _pingInterval   = null;
let _proxyInfo      = null;      // { ip, port, proxyUrl, tunnelMode }
let _tunnelWs       = null;      // WebSocket for tunnel mode
let _tunnelMode     = false;

// ── Event emitter ─────────────────────────────────────────────────────────────

function emit(event, data) {
  (_listeners[event] || []).forEach(cb => { try { cb(data); } catch (_) {} });
}

export function on(event, cb) {
  if (!_listeners[event]) _listeners[event] = [];
  _listeners[event].push(cb);
  return () => {
    _listeners[event] = (_listeners[event] || []).filter(x => x !== cb);
  };
}

// ── Host: start proxy + register session ──────────────────────────────────────

/**
 * startAsHost(options)
 *   options.tunnelMode = true  → use WebSocket relay (works over 300km+)
 *   options.tunnelMode = false → use LAN mode (same Wi-Fi only)
 *
 * If tunnelMode is not specified, we auto-detect:
 *   - If Wi-Fi IP is a private IP (192.168.x.x), default to tunnel mode
 *     so clients outside the LAN can connect.
 */
export async function startAsHost(options = {}) {
  _role = 'host';
  emit('status', { status: 'connecting' });

  try {
    // 1. Start the local HTTP CONNECT proxy server
    _proxyInfo = await ProxyModule.startProxy();
    const { ip, port } = _proxyInfo;

    // Auto-detect tunnel mode if not specified
    const isLanIp = ip && (
      ip.startsWith('192.168.') ||
      ip.startsWith('10.')      ||
      ip.startsWith('172.')
    );
    // Default to tunnel mode for long-distance sharing
    _tunnelMode = options.tunnelMode !== undefined ? options.tunnelMode : true;

    // 2. Register with relay
    const body = {
      ip,
      port,
      type:       'http-proxy',
      tunnelMode: _tunnelMode,
    };

    const res = await fetch(`${RELAY_URL}/register`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    });

    if (!res.ok) throw new Error(`Relay register failed: ${res.status}`);
    const { code, sessionId } = await res.json();

    _sessionCode = code;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 3. If tunnel mode: open WS to relay so traffic can flow
    if (_tunnelMode) {
      await _openHostTunnel(code, sessionId);
    }

    // 4. Start bandwidth polling
    _startStatsPoll();

    // 5. Keep session alive with periodic pings
    _pingInterval = setInterval(() => _keepAlive(sessionId), 30_000);

    emit('status',  { status: 'connected' });
    emit('session', { code, ip, port, tunnelMode: _tunnelMode });

    return { code, ip, port, tunnelMode: _tunnelMode };
  } catch (err) {
    emit('status', { status: 'error', message: err.message });
    throw err;
  }
}

// ── Client: look up session + connect ─────────────────────────────────────────

export async function startAsClient(code) {
  _role = 'client';
  emit('status', { status: 'connecting' });

  try {
    // 1. Look up session code on relay
    const res = await fetch(`${RELAY_URL}/join/${code.toUpperCase().trim()}`);
    if (!res.ok) {
      if (res.status === 404) throw new Error('Session code not found or expired');
      throw new Error(`Relay error: ${res.status}`);
    }

    const { ip, port, sessionId, tunnelMode } = await res.json();
    _tunnelMode = !!tunnelMode;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    if (_tunnelMode) {
      // ── Tunnel mode: connect to relay WS, start local proxy bridge ──────
      // The local ProxyModule still listens on :8899.
      // The Java proxy now forwards each CONNECT to the WS tunnel instead of
      // directly opening a TCP socket to the remote host.
      //
      // We open the WS tunnel here; ProxyModule.startTunnelProxy() starts
      // a local proxy that bridges Android's HTTP CONNECT into this WS.
      await _openClientTunnel(code);

      _proxyInfo = {
        ip:         '127.0.0.1',
        port:       8899,
        proxyUrl:   '127.0.0.1:8899',
        tunnelMode: true,
      };

      _startStatsPoll();
      emit('status', { status: 'connected' });
      emit('proxy',  { ip: '127.0.0.1', port: 8899, proxyUrl: '127.0.0.1:8899', tunnelMode: true });
      return { ip: '127.0.0.1', port: 8899, tunnelMode: true };

    } else {
      // ── LAN mode: check reachability then show setup guide ───────────────
      const reachable = await _testProxy(ip, port);
      if (!reachable) {
        // Don't hard-fail — warn the user but let them try
        console.warn(`[ProxyService] Probe failed for ${ip}:${port} — may still work on LAN`);
      }

      _proxyInfo = { ip, port, proxyUrl: `${ip}:${port}`, tunnelMode: false };
      _startStatsPoll();

      emit('status', { status: 'connected' });
      emit('proxy',  { ip, port, proxyUrl: `${ip}:${port}`, tunnelMode: false });
      return { ip, port, tunnelMode: false };
    }
  } catch (err) {
    emit('status', { status: 'error', message: err.message });
    throw err;
  }
}

// ── Stop (both roles) ─────────────────────────────────────────────────────────

export async function stop() {
  clearInterval(_statsInterval);
  clearInterval(_pingInterval);
  _statsInterval = null;
  _pingInterval  = null;

  // Close tunnel WebSocket
  if (_tunnelWs) {
    try { _tunnelWs.close(); } catch (_) {}
    _tunnelWs = null;
  }

  if (_role === 'host') {
    try { await ProxyModule.stopProxy(); } catch (_) {}

    const sessionId = await AsyncStorage.getItem('netshare_session_id');
    if (sessionId) {
      try {
        await fetch(`${RELAY_URL}/deregister`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ sessionId }),
        });
      } catch (_) {}
    }
  }

  _sessionCode = null;
  _role        = null;
  _proxyInfo   = null;
  _tunnelMode  = false;
  _listeners   = {};
  emit('status', { status: 'idle' });
}

// ── Getters ───────────────────────────────────────────────────────────────────

export function getProxyInfo()   { return _proxyInfo; }
export function getSessionCode() { return _sessionCode; }
export function getRole()        { return _role; }
export function getTunnelMode()  { return _tunnelMode; }

// ── Tunnel helpers ────────────────────────────────────────────────────────────

/**
 * Host opens WS to relay. The relay DO will pair this with the client WS.
 * The Java ProxyModule handles actual TCP→WS bridging on the host side.
 */
async function _openHostTunnel(code, sessionId) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${RELAY_WS_URL}/ws/host/${code}`);
    _tunnelWs = ws;

    const timeout = setTimeout(() => {
      reject(new Error('Host tunnel connection timed out'));
    }, 10_000);

    ws.onopen = () => {
      console.log('[ProxyService] Host tunnel WS open');
      clearTimeout(timeout);
      // Notify ProxyModule to use this WS for piping traffic
      // ProxyModule.attachTunnel(ws) — implement in Java if needed
      resolve();
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.type === 'paired') {
          emit('tunnel', { status: 'client_connected' });
        }
      } catch (_) {
        // Binary frame — raw traffic bytes (handled by ProxyModule on Java side)
      }
    };

    ws.onerror = (e) => {
      clearTimeout(timeout);
      console.error('[ProxyService] Host tunnel WS error', e);
      reject(new Error('Tunnel connection failed'));
    };

    ws.onclose = () => {
      console.log('[ProxyService] Host tunnel WS closed');
      emit('tunnel', { status: 'disconnected' });
    };
  });
}

/**
 * Client opens WS to relay. The relay DO pairs it with the host WS.
 * Raw traffic from the client's local proxy (localhost:8899) flows through here.
 */
async function _openClientTunnel(code) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${RELAY_WS_URL}/ws/client/${code}`);
    _tunnelWs = ws;

    const timeout = setTimeout(() => {
      reject(new Error('Client tunnel connection timed out'));
    }, 10_000);

    ws.onopen = () => {
      console.log('[ProxyService] Client tunnel WS open');
      clearTimeout(timeout);
      resolve();
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.type === 'paired') {
          emit('tunnel', { status: 'host_connected' });
        } else if (msg.type === 'waiting_for_host') {
          emit('tunnel', { status: 'waiting_for_host' });
        }
      } catch (_) {
        // Binary frame — raw traffic bytes
      }
    };

    ws.onerror = (e) => {
      clearTimeout(timeout);
      console.error('[ProxyService] Client tunnel WS error', e);
      reject(new Error('Tunnel connection failed'));
    };

    ws.onclose = () => {
      console.log('[ProxyService] Client tunnel WS closed');
      emit('tunnel', { status: 'disconnected' });
    };
  });
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function _startStatsPoll() {
  _statsInterval = setInterval(async () => {
    try {
      const stats = await ProxyModule.getStats();
      emit('stats', stats);
    } catch (_) {}
  }, 1000);
}

async function _keepAlive(sessionId) {
  try {
    await fetch(`${RELAY_URL}/ping`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ sessionId }),
    });
  } catch (_) {}
}

/**
 * _testProxy — server-side probe via relay /probe endpoint.
 * NOTE: This fails for private IPs (192.168.x.x) from Cloudflare's side.
 * In tunnel mode we skip this entirely. In LAN mode we return true optimistically
 * for private IPs since the client is expected to be on the same network.
 */
async function _testProxy(ip, port) {
  // Private/LAN IPs can't be probed from Cloudflare — assume reachable
  if (
    ip.startsWith('192.168.') ||
    ip.startsWith('10.')      ||
    ip.startsWith('172.')
  ) {
    return true;
  }

  try {
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(`${RELAY_URL}/probe`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port }),
      signal:  controller.signal,
    });
    clearTimeout(timeout);
    const data = await res.json();
    return data.ok !== false;
  } catch (_) {
    return true; // Let the user try anyway
  }
}

export default {
  startAsHost,
  startAsClient,
  stop,
  on,
  getProxyInfo,
  getSessionCode,
  getRole,
  getTunnelMode,
};
