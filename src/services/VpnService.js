/**
 * VpnService.js
 * JavaScript bridge to the native Android VpnModule.
 * Wraps all native calls and event listeners in a clean API.
 */

import { NativeModules, NativeEventEmitter, Platform } from 'react-native';

const { VpnModule } = NativeModules;
const vpnEmitter = VpnModule ? new NativeEventEmitter(VpnModule) : null;

// ── Replace with your actual Render backend URL ──────────────────
export const RELAY_URL = 'wss://netshare-backend.onrender.com/relay';

class VpnService {
  constructor() {
    this.listeners = [];
  }

  /**
   * Request VPN permission from Android OS.
   * Shows a system dialog — user must tap "OK".
   * Returns true if granted, false if denied.
   */
  async prepare() {
    if (Platform.OS !== 'android') {
      throw new Error('VPN sharing is only supported on Android');
    }
    if (!VpnModule) {
      throw new Error('VpnModule native module not found. Rebuild the app.');
    }
    return await VpnModule.prepare();
  }

  /**
   * Start as HOST — shares this device's internet with clients.
   * The relay server will generate a session code and emit 'sessionCreated'.
   *
   * FIX: Was passing 'HOST' as sessionCode — the native side expects a real
   * or empty string. Now passes empty string; relay assigns the code.
   */
  async startAsHost() {
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');

    // Pass empty string for sessionCode — relay will assign and return via sessionCreated event
    await VpnModule.startVpn(RELAY_URL, '', 'host');
  }

  /**
   * Start as CLIENT — joins a host session using a 6-char code.
   * All internet traffic on this device will route through the host.
   */
  async startAsClient(sessionCode) {
    if (!sessionCode || sessionCode.length !== 6) {
      throw new Error('Invalid session code — must be 6 characters');
    }
    const granted = await this.prepare();
    if (!granted) throw new Error('VPN permission denied by user');

    await VpnModule.startVpn(RELAY_URL, sessionCode.toUpperCase(), 'client');
  }

  /**
   * Stop the VPN tunnel and disconnect from relay.
   */
  async stop() {
    if (!VpnModule) return;
    await VpnModule.stopVpn();
  }

  /**
   * Listen for VPN events from the native layer.
   *
   * Events:
   *   vpnConnected      — tunnel is up (payload: sessionCode)
   *   vpnDisconnected   — tunnel closed (payload: reason)
   *   vpnError          — error occurred (payload: message)
   *   sessionCreated    — host got a session code (payload: code)
   *   joinSuccess       — client joined successfully (payload: code)
   *   joinError         — client join failed (payload: reason)
   *   clientConnected   — a client joined host session (payload: clientId)
   *   clientDisconnected— a client left host session
   *   hostLeft          — host ended session (client side)
   */
  on(event, callback) {
    if (!vpnEmitter) return () => {};
    const sub = vpnEmitter.addListener(event, callback);
    this.listeners.push(sub);
    return () => sub.remove();
  }

  /**
   * Remove all event listeners (call on component unmount).
   */
  removeAllListeners() {
    this.listeners.forEach(l => l.remove());
    this.listeners = [];
  }
}

export default new VpnService();
