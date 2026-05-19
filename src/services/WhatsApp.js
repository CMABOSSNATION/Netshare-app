/**
 * src/services/WhatsApp.js — NetShare
 *
 * Self-contained VPN service for WhatsApp.
 * Drop this file in and import WhatsAppService anywhere you need it.
 * No shared state with other app services — fully independent.
 *
 * Packages tunneled (all WhatsApp variants):
 *   com.whatsapp            — WhatsApp standard
 *   com.whatsapp.w4b        — WhatsApp Business
 *   com.whatsapp.beta       — WhatsApp Beta
 *   com.whatsapp.messenger  — WhatsApp on some OEM ROMs
 *
 * Required system support (also tunneled):
 *   com.google.android.webview / com.android.webview
 *     WhatsApp renders link previews in Android WebView — must be tunneled
 *     or preview fetches leak outside the VPN.
 *   com.google.android.gms / com.google.android.gsf
 *     WhatsApp uses Google Play Services for push notifications (FCM).
 *
 * Special network handling:
 *   Port 443  TCP/UDP — HTTPS + QUIC, 600s
 *   Port 80   TCP     — HTTP fallback, 600s
 *   Port 5222 TCP     — XMPP messaging (WhatsApp chat protocol), 600s
 *   Port 5223 TCP     — XMPP over TLS, 600s
 *   Port 3478 UDP     — STUN/TURN (voice & video calls), 600s
 *   Port 3479 UDP     — TURN alternate, 600s
 *   Port 5349 UDP/TCP — TURN over TLS, 600s
 *   Ports 19302–19309 — Google STUN (video calls), 600s
 *   Port 5228 TCP     — FCM/GCM push (persistent connection), 900s
 *   Port 53           — DNS, 5s
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

export const RELAY_URL = process.env.RELAY_URL
  || 'wss://nshare.cmaraphael90.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://nshare.cmaraphael90.workers.dev';

export const WHATSAPP_PACKAGES = [
  'com.whatsapp',
  'com.whatsapp.w4b',
  'com.whatsapp.beta',
  'com.whatsapp.messenger',
  // System support — link previews and push notifications
  'com.google.android.webview',
  'com.android.webview',
  'com.google.android.gms',
  'com.google.android.gsf',
];

// WhatsApp-specific port timeout map.
// 5222/5223: XMPP — WhatsApp's chat protocol uses long-lived TCP connections.
// 3478/3479/5349/19302-19309: STUN/TURN for voice & video calls.
// 5228: FCM push — idle for hours between messages; needs 900s so the connection
//       isn't torn down between messages on quiet chats.
export const WHATSAPP_PORTS = {
  443:  600_000,  // HTTPS / QUIC
  80:   600_000,  // HTTP fallback
  5222: 600_000,  // XMPP messaging
  5223: 600_000,  // XMPP over TLS
  3478: 600_000,  // STUN / TURN
  3479: 600_000,  // TURN alternate
  5349: 600_000,  // TURN over TLS
  5228: 900_000,  // FCM push (long idle)
  53:   5_000,    // DNS
  853:  5_000,    // DNS-over-TLS
  123:  10_000,   // NTP
};

const LOCAL_EVENTS        = new Set(['hostFailover']);
const MAX_RECONNECT_TRIES = 8;
const RECONNECT_BASE_MS   = 1_000;
const APP_NAME            = 'WhatsApp';

class WhatsAppVpnService {
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

  async startAsHost(netType = 'WiFi') {
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    this.hostId  = await this.getHostId();
    this.role    = 'host';
    this.netType = netType;
    this._stopping      = false;
    this.reconnectTries = 0;
    // WhatsApp FIX: pass APP_PACKAGES and APP_PORT_TIMEOUTS so the Java
    // layer uses WhatsApp-specific package list and port timeouts (XMPP 5222,
    // FCM 5228, STUN/TURN 3478-5349) instead of the generic fallback list.
    const packagesJson  = JSON.stringify(WHATSAPP_PACKAGES);
    const portMapJson   = JSON.stringify(
      Object.fromEntries(Object.entries(WHATSAPP_PORTS).map(([k, v]) => [String(k), v]))
    );
    await VpnModule.startVpn(
      RELAY_URL, '', 'host', this.hostId, netType, '',
      packagesJson, portMapJson
    );
  }

  async startAsClient(accessCode) {
    if (!accessCode || accessCode.length < 8)
      throw new Error('Invalid access code (format: XXXX-XXXX)');
    const validation = await this.validateAccessCode(accessCode);
    if (!validation.valid) throw new Error(validation.reason || 'Invalid access code');
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    const deviceId = await this.getDeviceId();
    this.role        = 'client';
    this.accessCode  = accessCode.toUpperCase();
    this.currentCode = null;
    this._stopping      = false;
    this.reconnectTries = 0;
    // WhatsApp FIX: pass APP_PACKAGES and APP_PORT_TIMEOUTS
    const packagesJson = JSON.stringify(WHATSAPP_PACKAGES);
    const portMapJson  = JSON.stringify(
      Object.fromEntries(Object.entries(WHATSAPP_PORTS).map(([k, v]) => [String(k), v]))
    );
    await VpnModule.startVpn(
      RELAY_URL, accessCode.toUpperCase(), 'client', '', '', deviceId,
      packagesJson, portMapJson
    );
  }

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
          await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, this.netType, '');
        } else if (this.role === 'client' && this.accessCode) {
          await VpnModule.startVpn(RELAY_URL, this.accessCode, 'client', '', '', deviceId);
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
    if (VpnModule?.sendControlMessage) VpnModule.sendControlMessage(JSON.stringify(obj));
  }

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

export default new WhatsAppVpnService();
