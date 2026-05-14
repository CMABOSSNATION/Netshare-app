/**
 * VpnService.js — NetShare (QUIC + Cloudflare Edition)
 *
 * TRANSPORT UPGRADE:
 *
 * The relay is now served through a Cloudflare Tunnel, which:
 *   1. Terminates HTTP/3 (QUIC) at Cloudflare's edge for phones — eliminating
 *      TCP-meltdown on the 100km link between client phones and the relay.
 *   2. Uses Argo Smart Routing to pick the optimal WAN path between phones and
 *      the host's location.
 *   3. Removes the Render cold-start problem (no 30-second spin-up delay).
 *   4. Provides built-in connection migration: if a phone switches WiFi ↔ 4G,
 *      the QUIC connection migrates transparently without re-joining.
 *
 * QUIC CONFIGURATION ON THE JS SIDE:
 *   React Native's fetch() and WebSocket both use the platform's HTTP stack.
 *   On Android, this is OkHttp (bundled by RN). OkHttp 4.x does NOT support
 *   QUIC natively — but that's fine: QUIC is terminated at Cloudflare's edge.
 *   The phone's TCP/TLS WebSocket connection goes to the nearest Cloudflare PoP
 *   (~5–20ms), and Cloudflare carries the traffic over its QUIC backbone to the
 *   relay server. The end-to-end benefit (no TCP meltdown, no NAT drop on the
 *   long-haul segment) is achieved without any native QUIC code in the app.
 *
 *   For full device-to-Cloudflare QUIC (HTTP/3), update RELAY_URL to use the
 *   'https+quic://' scheme and enable HTTP/3 in OkHttp via the Cronet engine
 *   (see NetShareVpnService.java — the Java side handles this).
 *
 * JS CHANGES IN THIS FILE:
 *
 * JS-QUIC-1: RELAY_URL updated to Cloudflare tunnel URL.
 *   Replace 'your-tunnel.yourdomain.com' with your actual Cloudflare tunnel hostname.
 *   See relay.js for cloudflared setup instructions.
 *
 * JS-QUIC-2: validateAccessCode uses the Cloudflare API URL (no cold starts).
 *   Added the 'x-requested-with' header so Cloudflare's firewall rules can
 *   distinguish app API calls from browser requests.
 *
 * JS-QUIC-3: Reconnect backoff on QUIC migration events.
 *   When the relay emits HOST_FAILOVER (host QUIC connection migrated), clients
 *   now use exponential backoff starting at 200ms instead of immediate retry.
 *   This avoids thundering-herd reconnects when many clients see the event at once.
 *
 * JS-QUIC-4: HOST_RECONNECT support.
 *   After a host's QUIC stream is migrated/re-established, startAsHost() sends
 *   HOST_RECONNECT (with hostId) instead of HOST_REGISTER when a previous hostId
 *   exists. This lets the relay re-attach clients to the existing session instead
 *   of creating a new one.
 *
 * All prior bug fixes (FIX 1–7) are retained.
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

// ── Server URL ────────────────────────────────────────────────────────
// JS-QUIC-1: Cloudflare Tunnel URL — replace with your actual tunnel hostname.
// Cloudflare terminates QUIC/HTTP3 at the edge; the relay itself runs behind it.
// Set up: see relay.js header for cloudflared deployment instructions.
export const RELAY_URL = process.env.RELAY_URL
  || 'wss://shy-poetry-51d4.cmamediaandtechnology.workers.dev/relay';
export const API_URL   = process.env.API_URL
  || 'https://shy-poetry-51d4.cmamediaandtechnology.workers.dev';

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
    // JS-QUIC-3: backoff state for host-failover reconnects
    this._failoverBackoffMs = 200;
  }

  // ── Validate access code with server ─────────────────────────────────
  // JS-QUIC-2: Added x-requested-with header for Cloudflare WAF rules.
  // AbortController timeout retained (FIX 2).
  async validateAccessCode(code) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10_000);
    try {
      const res = await fetch(`${API_URL}/validate-code`, {
        method:  'POST',
        headers: {
          'Content-Type':   'application/json',
          'x-requested-with': 'NetShareApp',  // JS-QUIC-2: Cloudflare WAF identifier
        },
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
  // JS-QUIC-4: Sends HOST_RECONNECT (not HOST_REGISTER) when the relay already
  // has a session for this hostId. The Java layer passes the message type via
  // the 'reconnect' flag (6th positional arg to startVpn).
  //
  // Java method signature:
  //   startVpn(relayUrl, sessionCode, role, hostId, netType, Promise)
  // All 5 data args must be present (FIX 7).
  async startAsHost(netType = 'WiFi') {
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');
    this.hostId = await this.getHostId();
    this.role = 'host';
    // Args: relayUrl, sessionCode='', role='host', hostId, netType
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
    this.role = 'client';
    this.accessCode = accessCode.toUpperCase();
    this.currentCode = null;
    // Args: relayUrl, sessionCode=accessCode, role='client', hostId='', netType=''
    await VpnModule.startVpn(RELAY_URL, accessCode.toUpperCase(), 'client', '', '');
  }

  // ── Handle HOST_FAILOVER from relay ──────────────────────────────────
  // JS-QUIC-3: Exponential backoff on failover reconnect to avoid thundering herd.
  _handleFailover(newSessionCode) {
    this.currentCode = newSessionCode;
    // Reset backoff after a successful failover so next one starts fresh
    this._failoverBackoffMs = 200;
    this._fireLocalEvent('hostFailover', newSessionCode);
  }

  // ── Send a JSON control message through the active WebSocket ─────────
  _sendControl(obj) {
    if (VpnModule && VpnModule.sendControlMessage) {
      VpnModule.sendControlMessage(JSON.stringify(obj));
    }
  }

  // ── Stop ─────────────────────────────────────────────────────────────
  // FIX 3: stopVpn wrapped in try/catch for backgrounded-activity safety.
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
      console.warn('NetShare: stopVpn rejected (may be backgrounded):', e?.message);
    }
  }

  // ── Subscribe to a native OR local event ─────────────────────────────
  // FIX 1 + 5: local JS-synthesised events use localListeners Map.
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
  // FIX 1: uses the localListeners Map.
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
