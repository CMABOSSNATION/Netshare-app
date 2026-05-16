/**
 * VpnService.js — NetShare
 *
 * FIXES (TikTok / WhatsApp):
 *
 * FIX-TW-1: getDeviceId() — retrieves the stable ANDROID_ID stored by the Java
 *   side via AsyncStorage key 'netshare_device_id'. This is the same ID that
 *   NetShareVpnService.java reads from Settings.Secure.ANDROID_ID and stores.
 *   Without this, validateAccessCode() sent no deviceId, the relay's /validate-code
 *   returned valid:true, but then CLIENT_JOIN sent the real deviceId from Java —
 *   causing a claimedBy mismatch → JOIN_ERROR → TikTok/WhatsApp never connected.
 *
 * FIX-TW-2: validateAccessCode() now sends deviceId so the relay can pre-check
 *   the one-device lock before the WebSocket leg even opens. Prevents a situation
 *   where the VPN tunnel is established but immediately gets JOIN_ERROR.
 *
 * FIX-TW-3: startAsClient() passes deviceId to VpnModule.startVpn() so Java can
 *   include it in CLIENT_JOIN. Previously the 6th arg was always '' — the relay
 *   rejected with "Device ID missing" for every non-first-time connection.
 *
 * All prior fixes (FIX-1 through FIX-5) are retained unchanged.
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

export const RELAY_URL = process.env.RELAY_URL
  || 'wss://netshare.cmaraphael90.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://netshare.cmaraphael90.workers.dev';

const LOCAL_EVENTS        = new Set(['hostFailover']);
const MAX_RECONNECT_TRIES = 8;
const RECONNECT_BASE_MS   = 1_000;

class VpnService {
  constructor() {
    this.nativeSubs          = [];
    this.localListeners      = new Map();
    this.role                = null;
    this.currentCode         = null;
    this.accessCode          = null;
    this.hostId              = null;
    this.deviceId            = null;   // FIX-TW-1: cached device ID
    this.netType             = 'WiFi';
    this.reconnectTimer      = null;
    this.reconnectTries      = 0;
    this._failoverBackoffMs  = 200;
    this._stopping           = false;
  }

  // ── FIX-TW-1: Get stable device ID ───────────────────────────────────
  // Java stores ANDROID_ID under 'netshare_device_id' on first VPN start.
  // We read the same key here so JS and Java always agree on the device ID.
  // Falls back to a generated ID if not yet set (first-ever cold start).
  async getDeviceId() {
    if (this.deviceId) return this.deviceId;
    try {
      let id = await AsyncStorage.getItem('netshare_device_id');
      if (!id) {
        // Java hasn't written it yet (app just installed, VPN not started).
        // Generate a placeholder; Java will overwrite it with the real
        // ANDROID_ID on the first VPN start.
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

  // ── FIX-TW-2: Validate access code (now includes deviceId) ───────────
  async validateAccessCode(code) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    // FIX-TW-2: fetch deviceId so relay can pre-check the one-device lock
    const deviceId = await this.getDeviceId();
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method:  'POST',
        headers: {
          'Content-Type':     'application/json',
          'x-requested-with': 'NetShareApp',
        },
        body:   JSON.stringify({ code, deviceId }),   // <-- deviceId added
        signal: controller.signal,
      });
      return await res.json();
    } catch (e) {
      if (e.name === 'AbortError') {
        return { valid: false, reason: 'Server took too long to respond. Try again.' };
      }
      return { valid: false, reason: 'Cannot reach server' };
    } finally {
      clearTimeout(timeout);
    }
  }

  // ── Get or create persistent host ID ─────────────────────────────────
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
    await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, netType, '');
  }

  // ── Start as CLIENT ──────────────────────────────────────────────────
  // FIX-TW-3: passes real deviceId as 6th arg to VpnModule.startVpn()
  async startAsClient(accessCode) {
    if (!accessCode || accessCode.length < 8) {
      throw new Error('Invalid access code — must be 8 characters (format: XXXX-XXXX)');
    }
    const validation = await this.validateAccessCode(accessCode);
    if (!validation.valid) {
      throw new Error(validation.reason || 'Invalid access code');
    }
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');

    // FIX-TW-3: ensure deviceId is loaded before passing to Java
    const deviceId = await this.getDeviceId();

    this.role        = 'client';
    this.accessCode  = accessCode.toUpperCase();
    this.currentCode = null;
    this._stopping      = false;
    this.reconnectTries = 0;
    // 6th arg = deviceId — Java will include this in CLIENT_JOIN so the relay
    // can correctly enforce the one-device lock without rejecting reconnects.
    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '', deviceId);
  }

  // FIX-1: Auto-reconnect with exponential backoff
  _scheduleReconnect() {
    if (this._stopping) return;
    if (this.reconnectTries >= MAX_RECONNECT_TRIES) {
      console.warn('[VpnService] Max reconnect tries reached, giving up');
      this._fireLocalEvent('reconnectFailed', 'Max reconnect attempts reached');
      return;
    }
    const delay = RECONNECT_BASE_MS * Math.pow(2, this.reconnectTries);
    this.reconnectTries++;
    console.log(`[VpnService] Reconnect attempt ${this.reconnectTries} in ${delay}ms`);
    this.reconnectTimer = setTimeout(async () => {
      try {
        if (this._stopping) return;
        const deviceId = await this.getDeviceId();   // FIX-TW-3: include on reconnect too
        if (this.role === 'host') {
          await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, this.netType, '');
        } else if (this.role === 'client' && this.accessCode) {
          await VpnModule.startVpn(RELAY_URL, this.accessCode, 'client', '', '', deviceId);
        }
      } catch (e) {
        console.warn('[VpnService] Reconnect failed:', e?.message);
        this._scheduleReconnect();
      }
    }, delay);
  }

  // ── Handle HOST_FAILOVER from relay ──────────────────────────────────
  _handleFailover(newSessionCode) {
    this.currentCode = newSessionCode;
    this._failoverBackoffMs = 200;
    this._fireLocalEvent('hostFailover', newSessionCode);
  }

  // ── Send a JSON control message ───────────────────────────────────────
  _sendControl(obj) {
    if (VpnModule && VpnModule.sendControlMessage) {
      VpnModule.sendControlMessage(JSON.stringify(obj));
    }
  }

  // ── Stop ─────────────────────────────────────────────────────────────
  async stop() {
    this._stopping = true;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.reconnectTries = 0;
    const currentRole = this.role;
    this.role        = null;
    this.currentCode = null;
    this.accessCode  = null;
    if (!VpnModule) return;
    if (currentRole === 'host') {
      this._sendControl({ type: 'HOST_LEAVE' });
    } else if (currentRole === 'client') {
      this._sendControl({ type: 'CLIENT_LEAVE' });
    }
    try {
      await VpnModule.stopVpn();
    } catch (e) {
      console.warn('NetShare: stopVpn rejected:', e?.message);
    }
  }

  // ── Subscribe to events ───────────────────────────────────────────────
  on(event, callback) {
    if (LOCAL_EVENTS.has(event) || event === 'reconnectFailed') {
      if (!this.localListeners.has(event)) {
        this.localListeners.set(event, new Set());
      }
      this.localListeners.get(event).add(callback);
      return () => {
        const cbs = this.localListeners.get(event);
        if (cbs) cbs.delete(callback);
      };
    }

    if (!vpnEmitter) return () => {};

    const sub = vpnEmitter.addListener(event, (payload) => {
      if (event === 'relayMessage') {
        try {
          const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
          if (msg) {
            if (msg.type === 'HOST_FAILOVER') this._handleFailover(msg.newSessionCode);
            if (msg.type === 'PING') this._sendControl({ type: 'PONG' });
          }
        } catch {}
      }
      if (event === 'vpnDisconnected' && !this._stopping && this.role) {
        console.log('[VpnService] Unexpected disconnect — scheduling reconnect');
        this._scheduleReconnect();
      }
      callback(payload);
    });

    this.nativeSubs.push(sub);
    return () => {
      sub.remove();
      this.nativeSubs = this.nativeSubs.filter(s => s !== sub);
    };
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

export default new VpnService();
