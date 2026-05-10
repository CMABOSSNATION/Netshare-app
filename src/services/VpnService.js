/**
 * VpnService.js — NetShare
 *
 * BUGS FIXED:
 * 1. _fireLocalEvent() accessed internal RN subscription properties that don't
 *    exist — hostFailover events were silently dropped. Fixed with a separate
 *    localListeners Map for JS-synthesised events.
 * 2. validateAccessCode fetch had no timeout — cold Render backend hung the UI
 *    indefinitely. Fixed with AbortController + 10s timeout.
 * 3. stop() called VpnModule.stopVpn() without try/catch — if the activity was
 *    null (app backgrounded), the rejection propagated uncaught. Fixed.
 * 4. removeAllListeners() cleared nativeSubs but NOT localListeners — hostFailover
 *    listeners survived a full teardown. Fixed to clear both maps.
 * 5. on() unsub for local events did nothing (filtered wrong array). Fixed.
 * 6. startAsClient passed sessionCode (null) as the 6th arg to startVpn instead
 *    of the accessCode for the relay JOIN message. Fixed: pass accessCode for relay.
 * 7. [NEW] startAsHost passed only 5 args to startVpn — RN bridge requires all
 *    declared positional args before the auto-injected Promise. netType was the 5th
 *    arg but the Java method declares (relayUrl, sessionCode, role, hostId, netType,
 *    Promise) — so all 5 data args must be present. Was missing empty sessionCode
 *    in the correct position — caused the bridge to mis-map args, resulting in the
 *    Promise never resolving and the app hanging on CONNECTING forever.
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

// ── Server URL ────────────────────────────────────────────────────────
export const RELAY_URL = 'wss://netshare-app-backend.onrender.com/relay';
export const API_URL   = 'https://netshare-app-backend.onrender.com';

// Events that are synthesised in JS (not from NativeEventEmitter)
const LOCAL_EVENTS = new Set(['hostFailover']);

class VpnService {
  constructor() {
    this.nativeSubs     = [];           // NativeEventEmitter subscriptions
    this.localListeners = new Map();    // event → Set<callback> for JS-only events
    this.role           = null;
    this.currentCode    = null;
    this.accessCode     = null;
    this.hostId         = null;
    this.reconnectTimer = null;
  }

  // ── Validate access code with server ─────────────────────────────────
  // FIX 2: added AbortController timeout so a cold Render backend doesn't
  // hang the UI indefinitely.
  async validateAccessCode(code) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10_000);
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ code }),
        signal:  controller.signal,
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
  // FIX 7: Java method signature is startVpn(relayUrl, sessionCode, role, hostId, netType, Promise).
  // All 5 data args must be passed in correct order before the auto-injected Promise.
  // Previously '' was missing for sessionCode, causing arg mis-mapping and the Promise
  // to never resolve — app hung on CONNECTING indefinitely.
  async startAsHost(netType = 'WiFi') {
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    this.hostId = await this.getHostId();
    this.role = 'host';
    // Args: relayUrl, sessionCode='', role='host', hostId, netType
    await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, netType);
  }

  // ── Start as CLIENT ──────────────────────────────────────────────────
  // FIX 6 + 7: Pass accessCode as the sessionCode arg (2nd position) so the
  // Java CLIENT_JOIN message sends the correct code to the relay. netType is
  // empty for clients (they don't broadcast a network type).
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
    this.role = 'client';
    this.accessCode = accessCode.toUpperCase();
    this.currentCode = null;
    // Args: relayUrl, sessionCode=accessCode, role='client', hostId='', netType=''
    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '');
  }

  // ── Handle HOST_FAILOVER from relay ──────────────────────────────────
  _handleFailover(newSessionCode) {
    this.currentCode = newSessionCode;
    this._fireLocalEvent('hostFailover', newSessionCode);
  }

  // ── Send a JSON control message through the active WebSocket ─────────
  _sendControl(obj) {
    if (VpnModule && VpnModule.sendControlMessage) {
      VpnModule.sendControlMessage(JSON.stringify(obj));
    }
  }

  // ── Stop ─────────────────────────────────────────────────────────────
  // FIX 3: stopVpn is now wrapped in try/catch so a rejected promise (e.g.
  // no current activity when app is backgrounded) doesn't propagate uncaught.
  async stop() {
    clearTimeout(this.reconnectTimer);
    const currentRole = this.role;
    this.role = null;
    this.currentCode = null;
    this.accessCode = null;
    if (!VpnModule) return;
    if (currentRole === 'host') {
      this._sendControl({ type: 'HOST_LEAVE' });
    } else if (currentRole === 'client') {
      this._sendControl({ type: 'CLIENT_LEAVE' });
    }
    try {
      await VpnModule.stopVpn();
    } catch (e) {
      // stopVpn can reject if activity is null (app backgrounded) — ignore.
      console.warn('NetShare: stopVpn rejected (may be backgrounded):', e?.message);
    }
  }

  // ── Subscribe to a native OR local event ─────────────────────────────
  // FIX 1 + 5: local JS-synthesised events (hostFailover) use a separate
  // Map so callbacks can be fired and unsubscribed reliably.
  on(event, callback) {
    if (LOCAL_EVENTS.has(event)) {
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
      // relayMessage events carry JSON; parse only those.
      if (event === 'relayMessage') {
        try {
          const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
          if (msg) {
            if (msg.type === 'HOST_FAILOVER') this._handleFailover(msg.newSessionCode);
            if (msg.type === 'PING') this._sendControl({ type: 'PONG' });
          }
        } catch {
          // Non-JSON relay messages — ignore
        }
      }
      callback(payload);
    });

    this.nativeSubs.push(sub);

    return () => {
      sub.remove();
      this.nativeSubs = this.nativeSubs.filter(s => s !== sub);
    };
  }

  // ── Fire a locally-synthesised event ─────────────────────────────────
  // FIX 1: uses the localListeners Map — reliable, no internal RN property access.
  _fireLocalEvent(event, payload) {
    const cbs = this.localListeners.get(event);
    if (!cbs) return;
    cbs.forEach(cb => { try { cb(payload); } catch {} });
  }

  // FIX 4: clears BOTH nativeSubs AND localListeners
  removeAllListeners() {
    this.nativeSubs.forEach(s => { try { s.remove(); } catch {} });
    this.nativeSubs = [];
    this.localListeners.clear();
  }
}

export default new VpnService();
