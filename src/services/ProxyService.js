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
 * MODE 2: WebSocket Tunnel via Cloudflare Durable Object (tunnelMode = true)
 *   Host opens WebSocket to relay /ws/host/:code  (Durable Object)
 *   Client opens WebSocket to relay /ws/client/:code (same DO instance)
 *   DO pipes binary frames between both WebSockets.
 *
 *   Host side:
 *     ProxyModule (Java) starts local proxy on :8899.
 *     ProxyModule.startTunnelBridge(wsUrl) — Java opens its OWN WS to the DO,
 *     intercepts every CONNECT request, and pipes raw TCP bytes over WS frames.
 *
 *   Client side:
 *     ProxyModule.startProxy() starts local proxy on :8899.
 *     ProxyModule.startClientTunnel(wsUrl) — Java opens its OWN WS to the DO,
 *     bridges localhost:8899 HTTP CONNECT traffic through the WS tunnel.
 *
 *   Result: Client app → localhost:8899 → WS → DO → WS → Host → Internet
 *   Range: anywhere in the world (Cloudflare edge).
 */

import { NativeModules, NativeEventEmitter } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { ProxyModule } = NativeModules;
const proxyEmitter = new NativeEventEmitter(ProxyModule);

export const RELAY_URL = process.env.RELAY_URL
  || 'https://netshare.cmaraphael90.workers.dev';

// WebSocket base URL (wss://)
const RELAY_WS_URL = RELAY_URL.replace(/^https?:\/\//, 'wss://');

// ── Internal state ────────────────────────────────────────────────────────────

let _listeners     = {};
let _sessionCode   = null;
let _role          = null;      // 'host' | 'client'
let _statsInterval = null;
let _pingInterval  = null;
let _proxyInfo     = null;      // { ip, port, tunnelMode }
let _tunnelMode    = false;
let _nativeSubList = [];        // NativeEventEmitter unsub functions

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
 *   options.tunnelMode = true  → Durable Object WS relay (works anywhere)
 *   options.tunnelMode = false → LAN mode (same Wi-Fi only)
 *
 *   Defaults to tunnelMode = true so 300km+ sharing works out of the box.
 */
export async function startAsHost(options = {}) {
  _role = 'host';
  emit('status', { status: 'connecting' });

  try {
    // 1. Start local HTTP CONNECT proxy on :8899
    // In tunnel mode, WiFi is not required — bypass WiFi check errors
    let proxyResult;
    try {
      proxyResult = await ProxyModule.startProxy();
    } catch (proxyErr) {
      const msg = proxyErr.message || '';
      if (!msg.toLowerCase().includes('wifi')) throw proxyErr;
      // WiFi check failed in tunnel mode — use fallback IP
      console.warn('[ProxyService] Host startProxy WiFi bypass:', msg);
      proxyResult = { ip: '127.0.0.1', port: 8899 };
    }
    const { ip, port } = proxyResult;

    _tunnelMode = options.tunnelMode !== undefined ? options.tunnelMode : true;

    // 2. Register with relay → get session code
    const res = await fetch(`${RELAY_URL}/register`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port, type: 'http-proxy', tunnelMode: _tunnelMode }),
    });

    if (!res.ok) throw new Error(`Relay register failed: ${res.status}`);
    const { code, sessionId } = await res.json();

    _sessionCode = code;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 3. Tunnel mode: tell Java to open its own WS to the DO and bridge traffic
    if (_tunnelMode) {
      const wsUrl = `${RELAY_WS_URL}/ws/host/${code}`;
      // Java opens WS, accepts local CONNECT requests,
      // wraps each request as a binary WS frame, receives response frames back.
      await ProxyModule.startTunnelBridge(wsUrl);
    }

    // 4. Listen for native events (client count, tunnel status, errors)
    _subscribeNativeEvents();

    // 5. Stats poll
    _startStatsPoll();

    // 6. Keep-alive pings every 30s
    _pingInterval = setInterval(() => _keepAlive(sessionId), 30_000);

    _proxyInfo = { ip, port, tunnelMode: _tunnelMode };

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
    // 1. Look up session on relay
    const res = await fetch(`${RELAY_URL}/join/${code.toUpperCase().trim()}`);
    if (!res.ok) {
      if (res.status === 404) throw new Error('Session code not found or expired');
      throw new Error(`Relay error: ${res.status}`);
    }

    const { ip, port, sessionId, tunnelMode } = await res.json();
    _tunnelMode = !!tunnelMode;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    if (_tunnelMode) {
      // ── Tunnel mode ───────────────────────────────────────────────────────
      // Java starts a local proxy on :8899 that speaks HTTP CONNECT.
      // Java also opens a WS to the DO client endpoint.
      // Every CONNECT request on :8899 is wrapped in a binary WS frame,
      // forwarded to the DO, which forwards it to the host WS.
      // Responses come back the same way.

      // startProxy() may throw a WiFi check error on the client side —
      // tunnel mode only needs mobile data/any internet, so we ignore it.
      try {
        await ProxyModule.startProxy();                         // local :8899
      } catch (proxyErr) {
        if (!proxyErr.message || !proxyErr.message.toLowerCase().includes('wifi')) {
          throw proxyErr; // real error, re-throw
        }
        // WiFi check failed but we're in tunnel mode — safe to continue
        console.warn('[ProxyService] startProxy WiFi check bypassed for tunnel mode:', proxyErr.message);
      }
      const wsUrl = `${RELAY_WS_URL}/ws/client/${code.toUpperCase().trim()}`;
      await ProxyModule.startClientTunnel(wsUrl);              // bridge WS

      _proxyInfo = { ip: '127.0.0.1', port: 8899, tunnelMode: true };
      _subscribeNativeEvents();
      _startStatsPoll();

      emit('status', { status: 'connected' });
      emit('proxy',  { ip: '127.0.0.1', port: 8899, tunnelMode: true });
      return { ip: '127.0.0.1', port: 8899, tunnelMode: true };

    } else {
      // ── LAN mode ──────────────────────────────────────────────────────────
      // Just hand the host IP:port to the client for manual proxy config.
      const reachable = await _testProxy(ip, port);
      if (!reachable) {
        console.warn(`[ProxyService] LAN probe failed for ${ip}:${port} — proceeding anyway`);
      }

      _proxyInfo = { ip, port, tunnelMode: false };
      _subscribeNativeEvents();
      _startStatsPoll();

      emit('status', { status: 'connected' });
      emit('proxy',  { ip, port, tunnelMode: false });
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

  // Unsubscribe native events
  _nativeSubList.forEach(sub => { try { sub.remove(); } catch (_) {} });
  _nativeSubList = [];

  if (_role === 'host') {
    try { await ProxyModule.stopProxy(); } catch (_) {}
    try { await ProxyModule.stopTunnelBridge(); } catch (_) {}

    const sessionId = await AsyncStorage.getItem('netshare_session_id');
    if (sessionId) {
      try {
        await fetch(`${RELAY_URL}/deregister`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ sessionId }),
        });
      } catch (_) {}
      await AsyncStorage.removeItem('netshare_session_id');
    }
  }

  if (_role === 'client') {
    try { await ProxyModule.stopProxy(); } catch (_) {}
    try { await ProxyModule.stopClientTunnel(); } catch (_) {}
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

// ── Native event subscriptions ────────────────────────────────────────────────

/**
 * Subscribe to events emitted by ProxyModule (Java → JS).
 * Java calls ProxyModule.emitEvent(name, data) for:
 *   'ProxyClientConnected'   → a new client connected to the tunnel
 *   'ProxyClientDisconnected'→ a client disconnected
 *   'ProxyTunnelError'       → fatal tunnel error (string message)
 *   'ProxyTunnelReady'       → tunnel WS fully paired with peer
 */
function _subscribeNativeEvents() {
  _nativeSubList.forEach(s => { try { s.remove(); } catch (_) {} });
  _nativeSubList = [];

  _nativeSubList.push(
    proxyEmitter.addListener('ProxyClientConnected', () => {
      emit('client', { event: 'connected' });
    }),
    proxyEmitter.addListener('ProxyClientDisconnected', () => {
      emit('client', { event: 'disconnected' });
    }),
    proxyEmitter.addListener('ProxyTunnelReady', (data) => {
      emit('tunnel', { status: 'ready', ...data });
    }),
    proxyEmitter.addListener('ProxyTunnelError', (msg) => {
      emit('status', { status: 'error', message: msg });
    }),
  );
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function _startStatsPoll() {
  _statsInterval = setInterval(async () => {
    try {
      const stats = await ProxyModule.getStats();
      emit('stats', { bytesUp: stats.up, bytesDown: stats.down });
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
 * _testProxy — only meaningful for public IPs in LAN mode.
 * Private IPs (192.168.x.x) can't be probed from Cloudflare, so we skip.
 */
async function _testProxy(ip, port) {
  if (
    ip.startsWith('192.168.') ||
    ip.startsWith('10.')      ||
    ip.startsWith('172.')
  ) {
    return true; // assume reachable on LAN
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(`${RELAY_URL}/probe`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port }),
      signal:  controller.signal,
    });
    clearTimeout(timer);
    const data = await res.json();
    return data.ok !== false;
  } catch (_) {
    return true; // let the user try anyway
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
  RELAY_URL,
};
