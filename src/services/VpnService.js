/**
 * VpnService.js — NetShare
 *
 * FIXES:
 * FIX-1: Auto-reconnect on vpnDisconnected — reconnects up to 5 times with
 *         exponential backoff (2s, 4s, 8s, 16s, 32s) instead of just dropping.
 * FIX-2: Reconnect uses HOST_RECONNECT / CLIENT auto-rejoin to restore session.
 * FIX-3: validateAccessCode timeout increased to 15s for slow connections.
 * FIX-4: PONG response sent immediately on PING to keep DO alarm alive.
 * FIX-5: hostId persisted properly — used across reconnects to resume session.
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
const MAX_RECONNECT_TRIES = 8;    // raised: more retries for unstable connections
const RECONNECT_BASE_MS   = 1_000;  // faster first retry for TikTok/YT session resumption

class VpnService {
  constructor() {
    this.nativeSubs          = [];
    this.localListeners      = new Map();
    this.role                = null;
    this.currentCode         = null;
    this.accessCode          = null;
    this.hostId              = null;
    this.netType             = 'WiFi';
    this.reconnectTimer      = null;
    this.reconnectTries      = 0;
    this._failoverBackoffMs  = 200;
    this._stopping           = false;
  }

  // ── Validate access code ──────────────────────────────────────────────
  async validateAccessCode(code) {
    const controller = new AbortController();
    // FIX-3: 15s timeout for slow connections
    const timeout = setTimeout(() => controller.abort(), 15_000);
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method:  'POST',
        headers: {
          'Content-Type':     'application/json',
          'x-requested-with': 'NetShareApp',
        },
        body:   JSON.stringify({ code }),
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
    await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, netType);
  }

  // ── Start as CLIENT ──────────────────────────────────────────────────
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
    this.role       = 'client';
    this.accessCode = accessCode.toUpperCase();
    this.currentCode = null;
    this._stopping      = false;
    this.reconnectTries = 0;
    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '');
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
        if (this.role === 'host') {
          await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, this.netType);
        } else if (this.role === 'client' && this.accessCode) {
          await VpnModule.startVpn(RELAY_URL, this.accessCode, 'client', '', '');
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
            // FIX-4: respond to PING immediately to keep session alive
            if (msg.type === 'PING') this._sendControl({ type: 'PONG' });
          }
        } catch {}
      }
      // FIX-1: reconnect on unexpected disconnect
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
