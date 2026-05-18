/**
 * src/services/ProxyService.js — NetShare Global Tunnel Only
 *
 * LAN / same-WiFi mode removed entirely.
 * Manual proxy setup guide removed entirely.
 *
 * HOST flow:
 *   1. startProxy()        — local HTTP CONNECT proxy on :8899
 *   2. POST /register      — get session code from relay
 *   3. startTunnelBridge() — open WS to Cloudflare /ws/host/:code
 *
 * CLIENT flow:
 *   1. GET /join/:code      — look up session on relay
 *   2. startVpn(wsUrl)      — activates VPN TUN interface
 *                             ALL traffic (WiFi + mobile data) captured
 *                             routed via host through Cloudflare
 *   3. No manual proxy setup needed — works automatically
 */

import { NativeModules, NativeEventEmitter } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { ProxyModule } = NativeModules;
const proxyEmitter = new NativeEventEmitter(ProxyModule);

export const RELAY_URL = process.env.RELAY_URL
  || 'https://netshare.cmaraphael90.workers.dev';

const RELAY_WS_URL = RELAY_URL.replace(/^https?:\/\//, 'wss://');

// ── Internal state ────────────────────────────────────────────────────────────

let _listeners     = {};
let _sessionCode   = null;
let _role          = null;
let _statsInterval = null;
let _pingInterval  = null;
let _proxyInfo     = null;
let _nativeSubList = [];
let _vpnActive     = false;

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

// ── Host ──────────────────────────────────────────────────────────────────────

export async function startAsHost(options = {}) {
  _role = 'host';
  emit('status', { status: 'connecting' });

  try {
    // 1. Start local HTTP CONNECT proxy on :8899
    let proxyResult;
    try {
      proxyResult = await ProxyModule.startProxy();
    } catch (err) {
      const msg = err.message || '';
      if (!msg.toLowerCase().includes('wifi')) throw err;
      console.warn('[ProxyService] Host WiFi bypass:', msg);
      proxyResult = { ip: '127.0.0.1', port: 8899 };
    }
    const { ip, port } = proxyResult;

    // 2. Register with relay
    const res = await fetch(`${RELAY_URL}/register`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ip, port, type: 'http-proxy', tunnelMode: true }),
    });
    if (!res.ok) throw new Error(`Relay register failed: ${res.status}`);
    const { code, sessionId } = await res.json();

    _sessionCode = code;
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 3. Open WebSocket bridge to Cloudflare DO
    const wsUrl = `${RELAY_WS_URL}/ws/host/${code}`;
    await ProxyModule.startTunnelBridge(wsUrl);

    _subscribeNativeEvents();
    _startStatsPoll();
    _pingInterval = setInterval(() => _keepAlive(sessionId), 30_000);

    _proxyInfo = { ip, port };

    emit('status',  { status: 'connected' });
    emit('session', { code, ip, port, tunnelMode: true });

    return { code, ip, port, tunnelMode: true };

  } catch (err) {
    emit('status', { status: 'error', message: err.message });
    throw err;
  }
}

// ── Client ────────────────────────────────────────────────────────────────────

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
    const { sessionId } = await res.json();
    await AsyncStorage.setItem('netshare_session_id', sessionId);

    // 2. Start VPN — TUN interface captures ALL traffic and tunnels via host
    const wsUrl = `${RELAY_WS_URL}/ws/client/${code.toUpperCase().trim()}`;
    await _startVpn(wsUrl);

    _proxyInfo = { ip: '127.0.0.1', port: 8899 };

    _subscribeNativeEvents();
    _startStatsPoll();

    emit('status', { status: 'connected' });
    emit('proxy',  { ..._proxyInfo, tunnelMode: true });

    return { ..._proxyInfo, tunnelMode: true };

  } catch (err) {
    try { await ProxyModule.stopVpn(); }            catch (_) {}
    try { await ProxyModule.stopClientTunnel(); }   catch (_) {}
    emit('status', { status: 'error', message: err.message });
    throw err;
  }
}

// ── Stop ──────────────────────────────────────────────────────────────────────

export async function stop() {
  clearInterval(_statsInterval);
  clearInterval(_pingInterval);
  _statsInterval = null;
  _pingInterval  = null;

  _nativeSubList.forEach(sub => { try { sub.remove(); } catch (_) {} });
  _nativeSubList = [];

  if (_role === 'host') {
    try { await ProxyModule.stopProxy(); }        catch (_) {}
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
    await _stopVpn();
    try { await ProxyModule.stopClientTunnel(); } catch (_) {}
  }

  _sessionCode = null;
  _role        = null;
  _proxyInfo   = null;
  _vpnActive   = false;
  _listeners   = {};

  emit('status', { status: 'idle' });
}

// ── Getters ───────────────────────────────────────────────────────────────────

export function getProxyInfo()   { return _proxyInfo; }
export function getSessionCode() { return _sessionCode; }
export function getRole()        { return _role; }
export function getTunnelMode()  { return true; }
export function isVpnActive()    { return _vpnActive; }

// ── VPN ───────────────────────────────────────────────────────────────────────

async function _startVpn(wsUrl) {
  try {
    await ProxyModule.startVpn(wsUrl);
    _vpnActive = true;
    emit('vpn', { status: 'active' });
  } catch (err) {
    const msg = err.message || '';
    if (msg.toLowerCase().includes('denied')) {
      emit('vpn', { status: 'denied' });
      // Non-fatal fallback — tunnel without full VPN
      try { await ProxyModule.startClientTunnel(wsUrl); }
      catch (e) { throw new Error('Connection failed: ' + e.message); }
    } else {
      emit('vpn', { status: 'error', message: msg });
      throw err;
    }
  }
}

async function _stopVpn() {
  if (!_vpnActive) return;
  try { await ProxyModule.stopVpn(); } catch (_) {}
  _vpnActive = false;
  emit('vpn', { status: 'idle' });
}

// ── Native events ─────────────────────────────────────────────────────────────

function _subscribeNativeEvents() {
  _nativeSubList.forEach(s => { try { s.remove(); } catch (_) {} });
  _nativeSubList = [];
  _nativeSubList.push(
    proxyEmitter.addListener('ProxyClientConnected',    () => emit('client', { event: 'connected' })),
    proxyEmitter.addListener('ProxyClientDisconnected', () => emit('client', { event: 'disconnected' })),
    proxyEmitter.addListener('ProxyTunnelReady',        () => emit('tunnel', { status: 'ready' })),
    proxyEmitter.addListener('ProxyTunnelError',       (m) => emit('status', { status: 'error', message: m })),
    proxyEmitter.addListener('ProxyVpnStarted',         () => { _vpnActive = true;  emit('vpn', { status: 'active' }); }),
    proxyEmitter.addListener('ProxyVpnRevoked',         () => { _vpnActive = false; emit('vpn', { status: 'revoked' }); }),
    proxyEmitter.addListener('ProxyVpnError',          (m) => { _vpnActive = false; emit('vpn', { status: 'error', message: m }); }),
  );
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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

export default {
  startAsHost, startAsClient, stop, on,
  getProxyInfo, getSessionCode, getRole, getTunnelMode, isVpnActive,
  RELAY_URL,
};
