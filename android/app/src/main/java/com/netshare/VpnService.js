/**
 * VpnService.js — NetShare Business Edition
 *
 * Changes from original:
 *  - CLIENT now sends accessCode (admin-generated password) to join
 *  - HOST sends hostId for persistent identity across reconnects
 *  - Handles HOST_FAILOVER — auto-reconnects client to new host
 *  - Handles PING/PONG heartbeat
 *  - Auto-reconnect logic on disconnect
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

// ── Server URL — replace with your deployed backend ──────────────────
export const RELAY_URL = 'wss://netshare-app-backend.onrender.com/relay';
export const API_URL   = 'https://netshare-app-backend.onrender.com';

class VpnService {
  constructor() {
    this.listeners      = [];
    this.ws             = null;        // direct WS for host control messages
    this.role           = null;        // 'host' | 'client'
    this.currentCode    = null;        // session code in use
    this.accessCode     = null;        // client access code
    this.hostId         = null;        // persistent host identity
    this.reconnectTimer = null;
    this.reconnecting   = false;
  }

  // ── Validate access code with server before connecting ──────────────
  async validateAccessCode(code) {
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code }),
      });
      return await res.json();
    } catch (e) {
      return { valid: false, reason: 'Cannot reach server' };
    }
  }

  // ── Get or create persistent host ID ────────────────────────────────
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

    this.hostId = await this.getHostId();
    this.role = 'host';

    // The native layer connects to relay; once connected we send HOST_REGISTER
    await VpnModule.startVpn(RELAY_URL, '', 'host', this.hostId, netType);

    // Listen for native VPN tunnel up, then register with relay
    this._once('vpnConnected', () => {
      this._sendControl({ type: 'HOST_REGISTER', hostId: this.hostId, netType });
    });
  }

  // ── Start as CLIENT ──────────────────────────────────────────────────
  async startAsClient(accessCode, sessionCode = null) {
    if (!accessCode || accessCode.length < 8) {
      throw new Error('Invalid access code — must be 8 characters (format: XXXX-XXXX)');
    }

    // Validate first
    const validation = await this.validateAccessCode(accessCode);
    if (!validation.valid) {
      throw new Error(validation.reason || 'Invalid access code');
    }

    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');

    this.role = 'client';
    this.accessCode = accessCode.toUpperCase();
    this.currentCode = sessionCode;

    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '');

    // After VPN tunnel up, join session via relay
    this._once('vpnConnected', () => {
      this._sendControl({
        type: 'CLIENT_JOIN',
        accessCode: this.accessCode,
        sessionCode: this.currentCode || undefined,
      });
    });
  }

  // ── Handle failover — server moves client to new host ────────────────
  _handleFailover(newSessionCode) {
    this.currentCode = newSessionCode;
    // Notify app layer
    this.emit('hostFailover', newSessionCode);
    // No need to restart VPN — relay handles packet routing to new host
  }

  // ── Send a control message through the relay WebSocket ───────────────
  // (The native VPN module uses the same WS connection for both
  //  VPN packets and JSON control messages)
  _sendControl(obj) {
    if (VpnModule && VpnModule.sendControlMessage) {
      VpnModule.sendControlMessage(JSON.stringify(obj));
    }
    // Fallback: some builds expose this through the emitter
  }

  _once(event, cb) {
    const unsub = this.on(event, (...args) => { cb(...args); unsub(); });
    return unsub;
  }

  // ── Stop ─────────────────────────────────────────────────────────────
  async stop() {
    clearTimeout(this.reconnectTimer);
    this.role = null;
    this.currentCode = null;
    if (!VpnModule) return;
    if (this.role === 'host') {
      this._sendControl({ type: 'HOST_LEAVE' });
    } else {
      this._sendControl({ type: 'CLIENT_LEAVE' });
    }
    await VpnModule.stopVpn();
  }

  // ── Event system ─────────────────────────────────────────────────────
  on(event, callback) {
    if (!vpnEmitter) return () => {};
    const sub = vpnEmitter.addListener(event, (payload) => {
      // Intercept failover events here
      if (event === 'relayMessage') {
        try {
          const msg = JSON.parse(payload);
          if (msg.type === 'HOST_FAILOVER') {
            this._handleFailover(msg.newSessionCode);
          }
          if (msg.type === 'PING') {
            this._sendControl({ type: 'PONG' });
          }
        } catch {}
      }
      callback(payload);
    });
    this.listeners.push(sub);
    return () => sub.remove();
  }

  emit(event, payload) {
    if (vpnEmitter) vpnEmitter.emit(event, payload);
  }

  removeAllListeners() {
    this.listeners.forEach(l => l.remove());
    this.listeners = [];
  }
}

export default new VpnService();
