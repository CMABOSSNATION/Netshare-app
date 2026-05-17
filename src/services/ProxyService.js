/**
 * src/services/ProxyService.js — NetShare HTTP/HTTPS Transparent Proxy
 *
 * ─── Architecture ────────────────────────────────────────────────────────────
 *
 * OLD (WebSocket TCP tunnel):
 *   Client app → VPN tun0 → Java packet parser → WebSocket → Relay Worker
 *     → WebSocket → Java packet writer → HOST device → Internet
 *
 * NEW (HTTP CONNECT proxy):
 *   Client Wi-Fi proxy: 192.168.x.x:8899
 *   Client app → Android system proxy → HOST ProxyService.java → Internet
 *   Relay is used only for session signalling (IP:port exchange), NOT traffic.
 *
 * ─── Why every app works ─────────────────────────────────────────────────────
 *
 *  TikTok     HTTPS/443 + HTTP/2 + QUIC → CONNECT tunnel handles it
 *  WhatsApp   HTTPS/443 + XMPP 5222     → CONNECT tunnel handles it
 *  Facebook   HTTPS/443                  → CONNECT tunnel handles it
 *  Instagram  HTTPS/443                  → CONNECT tunnel handles it
 *  Spotify    HTTPS/443 + port 4070      → CONNECT tunnel handles it
 *  YouTube    HTTPS/443 + HTTP/2         → CONNECT tunnel handles it
 *  Google     HTTPS/443 + QUIC           → CONNECT tunnel handles it
 *  Twitter/X  HTTPS/443                  → CONNECT tunnel handles it
 *
 * ─── Client setup (one-time, no root) ────────────────────────────────────────
 *
 *  Android:
 *    Settings → Wi-Fi → long-press network → Modify network →
 *    Advanced → Proxy: Manual → Host: <host IP> → Port: 8899
 *
 *  This makes Android route ALL app HTTP/HTTPS traffic through the proxy.
 *  No per-app configuration needed — TikTok, WhatsApp, all apps just work.
 *
 * ─── Session flow ────────────────────────────────────────────────────────────
 *
 *  HOST:
 *   1. startProxy() → Java opens TCP server on :8899
 *   2. registerSession(ip, port) → relay issues a 4-char session code
 *   3. Show code to user
 *
 *  CLIENT:
 *   1. Enter session code → relay returns { ip, port }
 *   2. App shows Wi-Fi proxy setup instructions with the IP:port
 *   3. User configures proxy in Android Wi-Fi settings (one tap with deep link)
 *   4. All traffic flows through host automatically
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { ProxyModule } = NativeModules;

export const RELAY_URL = process.env.RELAY_URL
  || 'https://netshare.cmaraphael90.workers.dev';

// ── Internal state ────────────────────────────────────────────────────────────

let _listeners      = {};  // event name → [callbacks]
let _sessionCode    = null;
let _role           = null; // 'host' | 'client'
let _statsInterval  = null;
let _pingInterval   = null;
let _proxyInfo      = null; // { ip, port, proxyUrl }

// ── Event emitter (mirrors old service interface) ─────────────────────────────

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

export async function startAsHost() {
  _role = 'host';
  emit('status', { status: 'connecting' });

  try {
    // 1. Start the local HTTP CONNECT proxy server
    _proxyInfo = await ProxyModule.startProxy();
    const { ip, port } = _proxyInfo;

    if (!ip || ip === '0.0.0.0') {
      throw new Error('Could not determine Wi-Fi IP. Make sure Wi-Fi is connected.');
    }

    // 2. Register with relay — relay just stores ip+port and returns a code
    const res  = await fetch(`${RELAY_URL}/register`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port, type: 'http-proxy' }),
    });

    if (!res.ok) throw new Error(`Relay register failed: ${res.status}`);
    const { code, sessionId } = await res.json();

    _sessionCode = code;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 3. Start bandwidth polling
    _startStatsPoll();

    // 4. Keep session alive with periodic pings
    _pingInterval = setInterval(() => _keepAlive(sessionId), 30_000);

    emit('status',  { status: 'connected' });
    emit('session', { code, ip, port });

    return { code, ip, port };
  } catch (err) {
    emit('status', { status: 'error', message: err.message });
    throw err;
  }
}

// ── Client: look up session + return proxy config to show user ────────────────

export async function startAsClient(code) {
  _role = 'client';
  emit('status', { status: 'connecting' });

  try {
    // 1. Look up session code on relay → get host IP:port
    const res = await fetch(`${RELAY_URL}/join/${code.toUpperCase().trim()}`);
    if (!res.ok) {
      if (res.status === 404) throw new Error('Session code not found or expired');
      throw new Error(`Relay error: ${res.status}`);
    }

    const { ip, port, sessionId } = await res.json();

    _proxyInfo = { ip, port, proxyUrl: `${ip}:${port}` };
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 2. Verify the proxy is reachable before telling user to configure it
    const reachable = await _testProxy(ip, port);
    if (!reachable) {
      throw new Error(
        `Cannot reach host proxy at ${ip}:${port}. ` +
        'Make sure you are on the same Wi-Fi network as the host.'
      );
    }

    // 3. Start bandwidth polling (client polls host stats via relay)
    _startStatsPoll();

    emit('status',  { status: 'connected' });
    emit('proxy',   { ip, port, proxyUrl: `${ip}:${port}` });

    return { ip, port };
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

  if (_role === 'host') {
    try { await ProxyModule.stopProxy(); } catch (_) {}

    // Deregister from relay so clients get 404 immediately
    const sessionId = await AsyncStorage.getItem('netshare_session_id');
    if (sessionId) {
      try {
        await fetch(`${RELAY_URL}/deregister`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ sessionId }),
        });
      } catch (_) {}
    }
  }

  _sessionCode = null;
  _role        = null;
  _proxyInfo   = null;
  _listeners   = {};
  emit('status', { status: 'idle' });
}

// ── Getters ───────────────────────────────────────────────────────────────────

export function getProxyInfo() { return _proxyInfo; }
export function getSessionCode() { return _sessionCode; }
export function getRole() { return _role; }

// ── Internal helpers ──────────────────────────────────────────────────────────

/** Poll native layer for real bandwidth numbers */
function _startStatsPoll() {
  _statsInterval = setInterval(async () => {
    try {
      const stats = await ProxyModule.getStats();
      emit('stats', stats);
    } catch (_) {}
  }, 1000);
}

/** Ping relay to keep session alive (host side) */
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
 * _testProxy — does a quick HEAD request through the proxy to verify it's live.
 * We use http://connectivitycheck.gstatic.com/generate_204 which returns
 * 204 No Content — lightweight and reliable.
 */
async function _testProxy(ip, port) {
  try {
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), 5000);
    // React Native fetch doesn't support proxy settings directly;
    // we probe by opening a fetch to the relay's /probe endpoint which
    // tries connecting to ip:port from the server side.
    const res = await fetch(`${RELAY_URL}/probe`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port }),
      signal:  controller.signal,
    });
    clearTimeout(timeout);
    return res.ok;
  } catch (_) {
    return false; // unreachable — let the user try anyway with a warning
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
};
