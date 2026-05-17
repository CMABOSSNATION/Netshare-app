/**
 * src/services/Instagram.js — NetShare
 *
 * Self-contained VPN service for Instagram.
 * Drop this file in and import InstagramService anywhere you need it.
 * No shared state with other app services — fully independent.
 *
 * Packages tunneled:
 *   com.instagram.android  — Instagram main
 *   com.instagram.lite     — Instagram Lite
 *   com.burbn.instagram    — Instagram alternate package
 *
 * Required system support:
 *   com.google.android.gms / com.google.android.gsf
 *     Instagram uses Play Services for auth token refresh and push.
 *   com.google.android.webview / com.android.webview
 *     Instagram's in-app browser (e.g. profile links) uses Android WebView.
 *
 * Special network handling:
 *   Port 443  — HTTPS/QUIC (feed, Reels, Stories), 600s
 *   Port 80   — HTTP fallback, 600s
 *   Port 3478 — STUN for Instagram Live / video calls, 600s
 *   Port 5228 — FCM push (DM notifications), 900s
 *   Port 53   — DNS, 5s
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

export const RELAY_URL = process.env.RELAY_URL
  || 'wss://netshare.cmaraphael90.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://netshare.cmaraphael90.workers.dev';

export const INSTAGRAM_PACKAGES = [
  'com.instagram.android',
  'com.instagram.lite',
  'com.burbn.instagram',
  'com.instagram.barcelona',  // Threads — shares Instagram auth/session
  // System support
  'com.google.android.gms',
  'com.google.android.gsf',
  'com.google.android.webview',
  'com.android.webview',
];

export const INSTAGRAM_PORTS = {
  443:   600_000,  // HTTPS/QUIC — Reels, feed, Stories
  80:    600_000,  // HTTP fallback
  3478:  600_000,  // STUN — Instagram Live / video calls
  3479:  600_000,
  19302: 600_000,  // Google STUN — Instagram Live fallback
  5228:  900_000,  // FCM push — DM notifications
  53:    5_000,
  853:   5_000,
  123:   10_000,
};

const LOCAL_EVENTS        = new Set(['hostFailover']);
const MAX_RECONNECT_TRIES = 8;
const RECONNECT_BASE_MS   = 1_000;
const APP_NAME            = 'Instagram';

class InstagramVpnService {
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
    const delay = RECONNECT_BASE_MS * Math.pow(2, this.reconnectTries);
    this.reconnectTries++;
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

export default new InstagramVpnService();
