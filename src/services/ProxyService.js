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
 * ── VPN background-data blocking (CLIENT ONLY) ────────────────────────────────
 *
 *   When a client connects, we also start NetShareVpnService (via ProxyModule).
 *   This creates a TUN interface that captures ALL outbound packets and only
 *   allows packets destined for 127.0.0.1:8899 (our proxy) through.
 *   Everything else — background apps, ads, trackers, other network traffic —
 *   is silently dropped at the OS level.
 *
 *   Flow:
 *     1. prepareVpn()     — shows Android VPN permission dialog (once ever)
 *     2. startClientVpn() — activates TUN packet filter
 *     3. stopClientVpn()  — removes TUN, restores normal routing on disconnect
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
let _vpnActive     = false;     // true while VPN is blocking background data

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

export async function startAsHost(options = {}) {
  _role = 'host';
  emit('status', { status: 'connecting' });

  try {
    // 1. Start local HTTP CONNECT proxy on :8899
    let proxyResult;
    try {
      proxyResult = await ProxyModule.startProxy();
    } catch (proxyErr) {
      const msg = proxyErr.message || '';
      if (!msg.toLowerCase().includes('wifi')) throw proxyErr;
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

    // 3. Tunnel mode: open WS bridge to Cloudflare DO
    if (_tunnelMode) {
      const wsUrl = `${RELAY_WS_URL}/ws/host/${code}`;
      await ProxyModule.startTunnelBridge(wsUrl);
    }

    // 4. Listen for native events
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

// ── Client: look up session + connect + activate VPN ─────────────────────────

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

    // 2. Start local proxy
    try {
      await ProxyModule.startProxy();
    } catch (proxyErr) {
      if (!proxyErr.message || !proxyErr.message.toLowerCase().includes('wifi')) {
        throw proxyErr;
      }
      console.warn('[ProxyService] startProxy WiFi check bypassed for tunnel mode:', proxyErr.message);
    }

    if (_tunnelMode) {
      // 3a. Tunnel mode: open WS bridge to DO
      const wsUrl = `${RELAY_WS_URL}/ws/client/${code.toUpperCase().trim()}`;
      await ProxyModule.startClientTunnel(wsUrl);
      _proxyInfo = { ip: '127.0.0.1', port: 8899, tunnelMode: true };
    } else {
      // 3b. LAN mode: use host IP directly
      const reachable = await _testProxy(ip, port);
      if (!reachable) {
        console.warn(`[ProxyService] LAN probe failed for ${ip}:${port} — proceeding anyway`);
      }
      _proxyInfo = { ip, port, tunnelMode: false };
    }

    // 4. Start VPN to block all background data on this client device.
    //    prepareVpn() shows the Android permission dialog on first run only.
    //    If user denies, we continue without VPN (proxy still works, but
    //    background apps can still use client's own data).
    await _startVpn();

    _subscribeNativeEvents();
    _startStatsPoll();

    emit('status', { status: 'connected' });
    emit('proxy',  { ..._proxyInfo });

    return _proxyInfo;

  } catch (err) {
    // Clean up partial start
    try { await ProxyModule.stopProxy(); } catch (_) {}
    try { await ProxyModule.stopClientTunnel(); } catch (_) {}
    await _stopVpn();
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
    // Stop VPN FIRST — restores normal routing before we kill the proxy
    await _stopVpn();
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
export function isVpnActive()    { return _vpnActive; }

// ── VPN helpers ───────────────────────────────────────────────────────────────

/**
 * _startVpn — requests permission then starts the VPN service.
 * Emits 'vpn' events so the UI can show VPN status.
 * Non-fatal: if permission denied or error, we log and continue.
 */
async function _startVpn() {
  try {
    const { granted } = await ProxyModule.prepareVpn();
    if (!granted) {
      console.warn('[ProxyService] VPN permission denied — background data not blocked');
      emit('vpn', { status: 'denied' });
      return;
    }
    await ProxyModule.startClientVpn();
    _vpnActive = true;
    emit('vpn', { status: 'active' });
    console.log('[ProxyService] VPN active — background data blocked');
  } catch (err) {
    console.warn('[ProxyService] VPN start failed (non-fatal):', err.message);
    emit('vpn', { status: 'error', message: err.message });
    _vpnActive = false;
  }
}

async function _stopVpn() {
  if (!_vpnActive) return;
  try {
    await ProxyModule.stopClientVpn();
    console.log('[ProxyService] VPN stopped — normal routing restored');
  } catch (err) {
    console.warn('[ProxyService] VPN stop error:', err.message);
  } finally {
    _vpnActive = false;
    emit('vpn', { status: 'idle' });
  }
}

// ── Native event subscriptions ────────────────────────────────────────────────

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
    // VPN events from NetShareVpnService
    proxyEmitter.addListener('ProxyVpnStarted', () => {
      _vpnActive = true;
      emit('vpn', { status: 'active' });
    }),
    proxyEmitter.addListener('ProxyVpnRevoked', () => {
      _vpnActive = false;
      emit('vpn', { status: 'revoked' });
    }),
    proxyEmitter.addListener('ProxyVpnError', (msg) => {
      _vpnActive = false;
      emit('vpn', { status: 'error', message: msg });
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

async function _testProxy(ip, port) {
  if (
    ip.startsWith('192.168.') ||
    ip.startsWith('10.')      ||
    ip.startsWith('172.')
  ) {
    return true;
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
    return true;
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
  isVpnActive,
  RELAY_URL,
};
