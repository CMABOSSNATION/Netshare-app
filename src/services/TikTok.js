/**
 * src/services/TikTok.js — NetShare
 *
 * Self-contained VPN service for TikTok.
 * Drop this file in and import TikTokService anywhere you need it.
 * No shared state with other app services — fully independent.
 *
 * Packages tunneled (all TikTok variants worldwide):
 *   com.zhiliaoapp.musically   — TikTok global
 *   com.ss.android.ugc.trill   — TikTok SEA region
 *   com.ss.android.ugc.trill.go— TikTok Lite
 *   com.ss.android.ugc.aweme   — Douyin / TikTok China
 *   com.bytedance.tiktok        — TikTok alternate package
 *   com.tiktok.android          — TikTok alternate package
 *
 * Required system support (also tunneled):
 *   com.google.android.gms     — Play Services (auth / push)
 *   com.google.android.gsf     — Google Services Framework
 *   com.android.vending         — Play Store (update checks)
 *
 * Special network handling:
 *   Port 443 UDP — QUIC/HTTP3 (TikTok video streams), 600s socket timeout,
 *                  16 MB receive buffer to handle 4K burst traffic
 *   Port 80  TCP — HTTP fallback, 600s
 *   Port 53       — DNS, 5s fast timeout
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

export const RELAY_URL = process.env.RELAY_URL
  || 'wss://netshare.cmaraphael90.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://netshare.cmaraphael90.workers.dev';

// Every Android package name that belongs to TikTok + required system support.
// Java's addAllowedApplication() silently skips packages not installed on the device,
// so listing all variants here is safe.
export const TIKTOK_PACKAGES = [
  'com.zhiliaoapp.musically',    // TikTok global
  'com.ss.android.ugc.trill',    // TikTok SEA
  'com.ss.android.ugc.trill.go', // TikTok Lite
  'com.ss.android.ugc.aweme',    // Douyin / TikTok China
  'com.bytedance.tiktok',        // TikTok alternate
  'com.tiktok.android',          // TikTok alternate
  // System support
  'com.google.android.gms',
  'com.google.android.gsf',
  'com.android.vending',
];

// Ports TikTok uses and their required socket timeouts (milliseconds).
// The Java layer reads TIKTOK_PORTS via the APP_PORTS intent extra and
// applies per-port timeouts in socketTimeoutForPort().
export const TIKTOK_PORTS = {
  443:  600_000,  // QUIC/HTTP3 — primary video delivery
  80:   600_000,  // HTTP fallback
  53:   5_000,    // DNS
  853:  5_000,    // DNS-over-TLS
  123:  10_000,   // NTP
};

const LOCAL_EVENTS        = new Set(['hostFailover']);
const MAX_RECONNECT_TRIES = 8;
const RECONNECT_BASE_MS   = 1_000;
const APP_NAME            = 'TikTok';

class TikTokVpnService {
  constructor() {
    this.nativeSubs         = [];
    this.localListeners     = new Map();
    this.role               = null;
    this.currentCode        = null;
    this.accessCode         = null;
    this.hostId             = null;
    this.deviceId           = null;
    this.netType            = 'WiFi';
    this.reconnectTimer     = null;
    this.reconnectTries     = 0;
    this._failoverBackoffMs = 200;
    this._stopping          = false;
  }

  // ── Device ID ────────────────────────────────────────────────────────
  async getDeviceId() {
    if (this.deviceId) return this.deviceId;
    try {
      let id = await AsyncStorage.getItem('netshare_device_id');
      if (!id) {
        id = `js-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        await AsyncStorage.setItem('netshare_device_id', id);
      }
      this.deviceId = id;
      return id;
    } catch {
      const fallback = `js-${Date.now()}`;
      this.deviceId = fallback;
      return fallback;
    }
  }

  // ── Validate access code ─────────────────────────────────────────────
  async validateAccessCode(code) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    const deviceId = await this.getDeviceId();
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-requested-with': 'NetShareApp' },
        body:   JSON.stringify({ code, deviceId }),
        signal: controller.signal,
      });
      return await res.json();
    } catch (e) {
      if (e.name === 'AbortError') return { valid: false, reason: 'Server took too long. Try again.' };
      return { valid: false, reason: 'Cannot reach server' };
    } finally {
      clearTimeout(timeout);
    }
  }

  // ── Host ID ──────────────────────────────────────────────────────────
  async getHostId() {
    try {
      let id = await AsyncStorage.getItem('netshare_host_id');
      if (!id) {
        id = `host-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        await AsyncStorage.setItem('netshare_host_id', id);
      }
      return id;
    } catch {
      return `host-${Date.now()}`;
    }
  }

  async prepare() {
    if (Platform.OS !== 'android') throw new Error('VPN sharing is only supported on Android');
    if (!VpnModule) throw new Error('VpnModule native module not found. Rebuild the app.');
    return await VpnModule.prepare();
  }

  // ── Start as HOST ────────────────────────────────────────────────────
  async startAsHost(netType = 'WiFi') {
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    this.hostId  = await this.getHostId();
    this.role    = 'host';
    this.netType = netType;
    this._stopping      = false;
    this.reconnectTries = 0;
    await VpnModule.startVpn(
      RELAY_URL, '', 'host', this.hostId, netType, '',
      JSON.stringify(TIKTOK_PACKAGES),
    );
  }

  // ── Start as CLIENT ──────────────────────────────────────────────────
  async startAsClient(accessCode) {
    if (!accessCode || accessCode.length < 8)
      throw new Error('Invalid access code (format: XXXX-XXXX)');
    const validation = await this.validateAccessCode(accessCode);
    if (!validation.valid)
      throw new Error(validation.reason || 'Invalid access code');
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    const deviceId = await this.getDeviceId();
    this.role        = 'client';
    this.accessCode  = accessCode.toUpperCase();
    this.currentCode = null;
    this._stopping      = false;
    this.reconnectTries = 0;
    await VpnModule.startVpn(
      RELAY_URL, accessCode.toUpperCase(), 'client', '', '', deviceId,
      JSON.stringify(TIKTOK_PACKAGES),
    );
  }

  // ── Reconnect ────────────────────────────────────────────────────────
  _scheduleReconnect() {
    if (this._stopping) return;
    if (this.reconnectTries >= MAX_RECONNECT_TRIES) {
      this._fireLocalEvent('reconnectFailed', 'Max reconnect attempts reached');
      return;
    }
    const delay = RECONNECT_BASE_MS * Math.pow(2, this.reconnectTries);
    this.reconnectTries++;
    this.reconnectTimer = setTimeout(async () => {
      try {
        if (this._stopping) return;
        const deviceId = await this.getDeviceId();
        if (this.role === 'host') {
          await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, this.netType, '', JSON.stringify(TIKTOK_PACKAGES));
        } else if (this.role === 'client' && this.accessCode) {
          await VpnModule.startVpn(RELAY_URL, this.accessCode, 'client', '', '', deviceId, JSON.stringify(TIKTOK_PACKAGES));
        }
      } catch (e) {
        console.warn(`[${APP_NAME}Service] Reconnect failed:`, e?.message);
        this._scheduleReconnect();
      }
    }, delay);
  }

  _handleFailover(newSessionCode) {
    this.currentCode = newSessionCode;
    this._failoverBackoffMs = 200;
    this._fireLocalEvent('hostFailover', newSessionCode);
  }

  _sendControl(obj) {
    if (VpnModule?.sendControlMessage)
      VpnModule.sendControlMessage(JSON.stringify(obj));
  }

  // ── Stop ─────────────────────────────────────────────────────────────
  async stop() {
    this._stopping = true;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.reconnectTries = 0;
    const currentRole = this.role;
    this.role = null; this.currentCode = null; this.accessCode = null;
    if (!VpnModule) return;
    if (currentRole === 'host')   this._sendControl({ type: 'HOST_LEAVE' });
    if (currentRole === 'client') this._sendControl({ type: 'CLIENT_LEAVE' });
    try { await VpnModule.stopVpn(); } catch (e) {
      console.warn(`${APP_NAME}: stopVpn rejected:`, e?.message);
    }
  }

  // ── Events ───────────────────────────────────────────────────────────
  on(event, callback) {
    if (LOCAL_EVENTS.has(event) || event === 'reconnectFailed') {
      if (!this.localListeners.has(event)) this.localListeners.set(event, new Set());
      this.localListeners.get(event).add(callback);
      return () => { const cbs = this.localListeners.get(event); if (cbs) cbs.delete(callback); };
    }
    if (!vpnEmitter) return () => {};
    const sub = vpnEmitter.addListener(event, (payload) => {
      if (event === 'relayMessage') {
        try {
          const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
          if (msg?.type === 'HOST_FAILOVER') this._handleFailover(msg.newSessionCode);
          if (msg?.type === 'PING') this._sendControl({ type: 'PONG' });
        } catch {}
      }
      if (event === 'vpnDisconnected' && !this._stopping && this.role) this._scheduleReconnect();
      callback(payload);
    });
    this.nativeSubs.push(sub);
    return () => { sub.remove(); this.nativeSubs = this.nativeSubs.filter(s => s !== sub); };
  }

  _fireLocalEvent(event, payload) {
    const cbs = this.localListeners.get(event);
    if (!cbs) return;
    cbs.forEach(cb => { try { cb(payload); } catch {} });
  }

  removeAllListeners() {
    this.nativeSubs.forEach(s => { try { s.remove(); } catch {} });
    this.nativeSubs = [];
    this.localListeners.clear();
  }
}

export default new TikTokVpnService();
