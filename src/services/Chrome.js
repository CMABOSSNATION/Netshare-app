/**
 * src/services/Chrome.js — NetShare
 *
 * Self-contained VPN service for Chrome + Google apps.
 * Drop this file in and import ChromeService anywhere you need it.
 * No shared state with other app services — fully independent.
 *
 * Packages tunneled:
 *   com.android.chrome              — Chrome stable
 *   com.chrome.beta                 — Chrome Beta
 *   com.chrome.dev                  — Chrome Dev
 *   com.chrome.canary               — Chrome Canary
 *   com.google.android.apps.chrome  — Chrome alternate package
 *   com.google.android.googlequicksearchbox — Google Search / Assistant
 *   com.google.android.gm           — Gmail
 *   com.google.android.gms          — Google Play Services
 *   com.google.android.gsf          — Google Services Framework
 *   com.google.android.webview      — Android System WebView
 *   com.android.webview             — Android WebView (AOSP)
 *
 * Special network handling:
 *   Port 443 UDP — QUIC/HTTP3 (Chrome's primary transport), 600s, 16 MB buffer
 *   Port 443 TCP — HTTPS fallback, 600s
 *   Port 80  TCP — HTTP, 600s
 *   Port 53       — DNS, 5s
 *   Port 853      — DNS-over-TLS, 5s
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

export const RELAY_URL = process.env.RELAY_URL
  || 'wss://nshare.cmaraphael90.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://nshare.cmaraphael90.workers.dev';

export const CHROME_PACKAGES = [
  'com.android.chrome',
  'com.chrome.beta',
  'com.chrome.dev',
  'com.chrome.canary',
  'com.google.android.apps.chrome',
  'com.google.android.googlequicksearchbox',
  'com.google.android.gm',
  // System support
  'com.google.android.gms',
  'com.google.android.gsf',
  'com.google.android.webview',
  'com.android.webview',
];

export const CHROME_PORTS = {
  443:  600_000,  // QUIC/HTTPS — Chrome's primary transport
  80:   600_000,  // HTTP
  53:   5_000,    // DNS
  853:  5_000,    // DNS-over-TLS
  123:  10_000,   // NTP
};

const LOCAL_EVENTS        = new Set(['hostFailover']);
const MAX_RECONNECT_TRIES = 8;
const RECONNECT_BASE_MS   = 1_000;
const APP_NAME            = 'Chrome';

class ChromeVpnService {
  constructor() {
    this.nativeSubs = []; this.localListeners = new Map();
    this.role = null; this.currentCode = null; this.accessCode = null;
    this.hostId = null; this.deviceId = null; this.netType = 'WiFi';
    this.reconnectTimer = null; this.reconnectTries = 0;
    this._failoverBackoffMs = 200; this._stopping = false;
  }

  async getDeviceId() {
    if (this.deviceId) return this.deviceId;
    try {
      let id = await AsyncStorage.getItem('netshare_device_id');
      if (!id) { id = `js-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`; await AsyncStorage.setItem('netshare_device_id', id); }
      this.deviceId = id; return id;
    } catch { const f = `js-${Date.now()}`; this.deviceId = f; return f; }
  }

  async validateAccessCode(code) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    const deviceId = await this.getDeviceId();
    try {
      const res = await fetch(`${API_URL}/validate-code`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-requested-with': 'NetShareApp' }, body: JSON.stringify({ code, deviceId }), signal: controller.signal });
      return await res.json();
    } catch (e) { return e.name === 'AbortError' ? { valid: false, reason: 'Server took too long. Try again.' } : { valid: false, reason: 'Cannot reach server' }; }
    finally { clearTimeout(timeout); }
  }

  async getHostId() {
    try {
      let id = await AsyncStorage.getItem('netshare_host_id');
      if (!id) { id = `host-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`; await AsyncStorage.setItem('netshare_host_id', id); }
      return id;
    } catch { return `host-${Date.now()}`; }
  }

  async prepare() {
    if (Platform.OS !== 'android') throw new Error('VPN sharing is only supported on Android');
    if (!VpnModule) throw new Error('VpnModule native module not found. Rebuild the app.');
    return await VpnModule.prepare();
  }

  async startAsHost(netType = 'WiFi') {
    const granted = await this.prepare(); if (!granted) throw new Error('VPN permission denied by user');
    this.hostId = await this.getHostId(); this.role = 'host'; this.netType = netType; this._stopping = false; this.reconnectTries = 0;
    await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, netType, '');
  }

  async startAsClient(accessCode) {
    if (!accessCode || accessCode.length < 8) throw new Error('Invalid access code (format: XXXX-XXXX)');
    const validation = await this.validateAccessCode(accessCode); if (!validation.valid) throw new Error(validation.reason || 'Invalid access code');
    const granted = await this.prepare(); if (!granted) throw new Error('VPN permission denied by user');
    const deviceId = await this.getDeviceId();
    this.role = 'client'; this.accessCode = accessCode.toUpperCase(); this.currentCode = null; this._stopping = false; this.reconnectTries = 0;
    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '', deviceId);
  }

  _scheduleReconnect() {
    if (this._stopping) return;
    if (this.reconnectTries >= MAX_RECONNECT_TRIES) { this._fireLocalEvent('reconnectFailed', 'Max reconnect attempts reached'); return; }
    const delay = RECONNECT_BASE_MS * Math.pow(2, this.reconnectTries++);
    this.reconnectTimer = setTimeout(async () => {
      try {
        if (this._stopping) return;
        const deviceId = await this.getDeviceId();
        if (this.role === 'host') await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, this.netType, '');
        else if (this.role === 'client' && this.accessCode) await VpnModule.startVpn(RELAY_URL, this.accessCode, 'client', '', '', deviceId);
      } catch (e) { console.warn(`[${APP_NAME}Service] Reconnect failed:`, e?.message); this._scheduleReconnect(); }
    }, delay);
  }

  _handleFailover(code) { this.currentCode = code; this._failoverBackoffMs = 200; this._fireLocalEvent('hostFailover', code); }
  _sendControl(obj) { if (VpnModule?.sendControlMessage) VpnModule.sendControlMessage(JSON.stringify(obj)); }

  async stop() {
    this._stopping = true; clearTimeout(this.reconnectTimer); this.reconnectTimer = null; this.reconnectTries = 0;
    const role = this.role; this.role = null; this.currentCode = null; this.accessCode = null;
    if (!VpnModule) return;
    if (role === 'host')   this._sendControl({ type: 'HOST_LEAVE' });
    if (role === 'client') this._sendControl({ type: 'CLIENT_LEAVE' });
    try { await VpnModule.stopVpn(); } catch (e) { console.warn(`${APP_NAME}: stopVpn rejected:`, e?.message); }
  }

  on(event, callback) {
    if (LOCAL_EVENTS.has(event) || event === 'reconnectFailed') {
      if (!this.localListeners.has(event)) this.localListeners.set(event, new Set());
      this.localListeners.get(event).add(callback);
      return () => { const cbs = this.localListeners.get(event); if (cbs) cbs.delete(callback); };
    }
    if (!vpnEmitter) return () => {};
    const sub = vpnEmitter.addListener(event, (payload) => {
      if (event === 'relayMessage') { try { const msg = typeof payload === 'string' ? JSON.parse(payload) : payload; if (msg?.type === 'HOST_FAILOVER') this._handleFailover(msg.newSessionCode); if (msg?.type === 'PING') this._sendControl({ type: 'PONG' }); } catch {} }
      if (event === 'vpnDisconnected' && !this._stopping && this.role) this._scheduleReconnect();
      callback(payload);
    });
    this.nativeSubs.push(sub);
    return () => { sub.remove(); this.nativeSubs = this.nativeSubs.filter(s => s !== sub); };
  }

  _fireLocalEvent(event, payload) { const cbs = this.localListeners.get(event); if (!cbs) return; cbs.forEach(cb => { try { cb(payload); } catch {} }); }
  removeAllListeners() { this.nativeSubs.forEach(s => { try { s.remove(); } catch {} }); this.nativeSubs = []; this.localListeners.clear(); }
}

export default new ChromeVpnService();
