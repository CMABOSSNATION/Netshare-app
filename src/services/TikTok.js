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
 *   com.google.android.webview  — System WebView (in-app browser)
 *   com.android.webview         — AOSP WebView fallback
 *
 * Special network handling:
 *   Port 443 UDP — QUIC/HTTP3 (TikTok For You feed video streams), 600s socket
 *                  timeout, 16 MB receive buffer to handle 4K burst traffic.
 *                  Each QUIC connection now gets its own DatagramSocket so
 *                  parallel CDN segment fetches don't collide (see FIX-TK-4).
 *   Port 443 TCP — HTTPS/2 fallback for search, comments, live chat, 600s
 *   Port 80  TCP — HTTP fallback, 600s
 *   Port 53       — DNS, 5s fast timeout
 *
 * ══════════════════════════════════════════════════════════════════
 * FIXES IN THIS VERSION
 * ══════════════════════════════════════════════════════════════════
 *
 * FIX-TK-1 (CRITICAL — normal video not loading):
 *   startAsHost() and startAsClient() were calling VpnModule.startVpn()
 *   with only 6 arguments. VpnModule.java now expects 9 (the last two being
 *   appPackagesJson and appPortTimeoutsJson). The argument mismatch caused
 *   React Native's bridge to throw, or fall through with null extras, so:
 *     • APP_PACKAGES was never forwarded → Java fell back to the generic
 *       TUNNEL_APPS_FALLBACK list. On some devices this list doesn't include
 *       the installed TikTok package variant, so TikTok traffic bypassed the VPN.
 *     • APP_PORT_TIMEOUTS was never forwarded → Java used the built-in defaults,
 *       which set the QUIC UDP socket buffer to UDP_SOCKET_BUFFER (4 MB) instead
 *       of QUIC_SOCKET_BUFFER (16 MB). TikTok's For You feed sends video in
 *       large parallel bursts that overflow a 4 MB buffer, dropping QUIC ACKs
 *       and causing the feed spinner / blank video cards.
 *   FIX: Pass JSON.stringify(TIKTOK_PACKAGES) and JSON.stringify(TIKTOK_PORTS)
 *   as the 8th and 9th arguments to every VpnModule.startVpn() call.
 *
 * FIX-TK-2 (normal video stalls after reconnect):
 *   _scheduleReconnect() had the same 6-arg call as above, so any reconnect
 *   after a dropped relay connection re-introduced the buffer undersize and
 *   missing package filter, causing video to stall again after recovery.
 *   FIX: Pass PACKAGES_JSON and PORTS_JSON in the reconnect call too.
 *
 * FIX-TK-3 (com.google.android.webview missing):
 *   TikTok's in-app browser (used when tapping profile links, hashtag pages,
 *   and ads) renders in Android System WebView. Without WebView in the tunnel
 *   those pages loaded outside the VPN on the real interface, causing mixed-
 *   content errors and broken previews.
 *   FIX: Added com.google.android.webview and com.android.webview to TIKTOK_PACKAGES.
 *
 * NOTE: WhatsApp settings are intentionally untouched in this file.
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
  // FIX-TK-3: WebView — TikTok in-app browser for links, hashtag pages, ads
  'com.google.android.webview',
  'com.android.webview',
];

// Ports TikTok uses and their required socket timeouts (milliseconds).
// Forwarded to Java via APP_PORT_TIMEOUTS intent extra so socketTimeoutForPort()
// uses these values instead of built-in defaults.
// Most importantly: port 443 at 600s ensures the QUIC socket stays alive for
// the full duration of a For You feed video segment (some are 60 s+).
export const TIKTOK_PORTS = {
  443:  600_000,  // QUIC/HTTP3 — primary For You feed video delivery
  80:   600_000,  // HTTP fallback
  53:   5_000,    // DNS
  853:  5_000,    // DNS-over-TLS
  123:  10_000,   // NTP
};

// Serialised once — reused on every startVpn() and reconnect call.
// FIX-TK-1: these must be passed as args 8 and 9 to VpnModule.startVpn().
const PACKAGES_JSON = JSON.stringify(TIKTOK_PACKAGES);
const PORTS_JSON    = JSON.stringify(TIKTOK_PORTS);

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
    // FIX-TK-1: pass PACKAGES_JSON and PORTS_JSON so Java applies the
    // correct 16 MB QUIC buffer and TikTok-only package filter.
    await VpnModule.startVpn(
      RELAY_URL, '', 'host', this.hostId, netType, '',
      PACKAGES_JSON, PORTS_JSON
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
    // FIX-TK-1: pass PACKAGES_JSON and PORTS_JSON.
    await VpnModule.startVpn(
      RELAY_URL, accessCode.toUpperCase(), 'client', '', '', deviceId,
      PACKAGES_JSON, PORTS_JSON
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
          // FIX-TK-2: include PACKAGES_JSON and PORTS_JSON on reconnect too.
          await VpnModule.startVpn(
            RELAY_URL, '', 'host', this.hostId, this.netType, '',
            PACKAGES_JSON, PORTS_JSON
          );
        } else if (this.role === 'client' && this.accessCode) {
          // FIX-TK-2: include PACKAGES_JSON and PORTS_JSON on reconnect too.
          await VpnModule.startVpn(
            RELAY_URL, this.accessCode, 'client', '', '', deviceId,
            PACKAGES_JSON, PORTS_JSON
          );
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
