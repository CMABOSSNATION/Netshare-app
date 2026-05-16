/**
 * WhatsAppOptimizer.js — NetShare
 *
 * PURPOSE
 * -------
 * Independent helper that improves WhatsApp reliability over the NetShare VPN
 * tunnel.  It does NOT modify any existing file.  Import it in HomeScreen.jsx
 * (or wherever you set up VPN listeners) and call:
 *
 *   import WhatsAppOptimizer from './src/services/WhatsAppOptimizer';
 *   WhatsAppOptimizer.start();   // call once after VPN listeners are registered
 *   WhatsAppOptimizer.stop();    // call in your cleanup / useEffect return
 *
 * PROBLEMS ADDRESSED
 * ------------------
 *
 * WA-1  STUN/TURN NAT keep-alive
 *   WhatsApp voice and video calls use STUN (UDP/3478) and TURN (UDP/3478,
 *   3479, 5349) for hole-punching.  The Java layer (FIX-TW-2) already raises
 *   socket timeouts for these ports to 600 s, but the NAT device between the
 *   client Android phone and the relay server can still drop idle UDP mappings
 *   in 30–60 s.  We send a tiny PING every 28 s to keep the relay WebSocket
 *   alive, which in turn keeps the forwarded UDP sockets alive on the host.
 *   28 s is deliberately below the 30 s floor observed on Airtel/MTN Uganda
 *   CGNAT gear and below the 30 s UDP idle timeout on Render.com's network.
 *
 * WA-2  Voice/video call stall detection
 *   WhatsApp calls produce a steady stream of small UDP packets (RTCP, STUN
 *   keep-alives, SRTP audio).  If the relay goes silent for more than
 *   CALL_STALL_MS we assume the call path is stalled and:
 *     a) emit 'whatsappCallStall' so the UI can show a warning banner
 *     b) send a PING to keep the relay alive while WhatsApp's own retry fires
 *   We do NOT force a reconnect during a stall because WhatsApp's call engine
 *   handles its own ICE restart; a reconnect would tear down the VPN tunnel
 *   and end the call entirely.
 *
 * WA-3  Media download retry nudge
 *   WhatsApp media downloads (photos, videos, documents) use HTTPS/TLS over
 *   the VPN tunnel.  If a download stalls (no relay traffic for
 *   MEDIA_STALL_MS) we emit 'whatsappMediaStall' so the UI can prompt the
 *   user to retry the download in WhatsApp.  We also send a PING to prevent
 *   the relay WS from timing out mid-download.
 *
 * WA-4  Join success → mark session as call-ready
 *   On joinSuccess we record the timestamp and emit 'whatsappReady' so the UI
 *   can show a "WhatsApp calls supported" badge only when the VPN tunnel is
 *   actually up (not while still connecting).
 *
 * WA-5  WebView leak detection (link-preview DNS leaks)
 *   WhatsApp renders link previews inside Android WebView.  The Java layer
 *   (FIX-TW-3) already adds com.google.android.webview and com.android.webview
 *   to TUNNEL_APPS.  This helper watches for any relay message with type
 *   'DNS_LEAK_WARN' (emitted by future relay versions that can detect leaked
 *   DNS queries) and surfaces it as 'whatsappLeakWarn'.
 */

import vpnService from './VpnService';

// ── Tunables ──────────────────────────────────────────────────────────────────

/** Keep-alive PING interval — kept just under 30 s CGNAT UDP floor. */
const KEEPALIVE_INTERVAL_MS = 28_000;

/**
 * Silence window that signals a WhatsApp call may have stalled.
 * WhatsApp RTCP packets arrive every ~5 s during an active call; 12 s is
 * generous enough to avoid false positives on brief congestion.
 */
const CALL_STALL_MS         = 12_000;

/**
 * Silence window for media (photo/video) downloads.
 * Downloads should produce relay traffic continuously; 15 s of silence usually
 * means the TCP connection hit the relay's idle timeout or hit a RST.
 */
const MEDIA_STALL_MS        = 15_000;

/** Relay message type for DNS leak warnings (future relay feature). */
const DNS_LEAK_WARN_TYPE    = 'DNS_LEAK_WARN';

// ── Internal state ────────────────────────────────────────────────────────────

let _started            = false;
let _lastActivity       = Date.now();
let _callActive         = false; // becomes true when we detect STUN traffic pattern
let _keepAliveTimer     = null;
let _stallCheckTimer    = null;
let _unsubRelayMsg      = null;
let _unsubJoinSuccess   = null;
let _unsubDisconnected  = null;

const _listeners = new Map();

// ── Public API ────────────────────────────────────────────────────────────────

const WhatsAppOptimizer = {

  /**
   * Start monitoring.  Safe to call multiple times — subsequent calls are
   * no-ops until stop() is called.
   */
  start() {
    if (_started) return;
    _started = true;

    // WA-1: Keep-alive PING
    _keepAliveTimer = setInterval(() => {
      vpnService._sendControl({ type: 'PING', src: 'WhatsAppOptimizer' });
    }, KEEPALIVE_INTERVAL_MS);

    // Watch relay messages
    _unsubRelayMsg = vpnService.on('relayMessage', (payload) => {
      _lastActivity = Date.now();

      try {
        const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
        if (!msg) return;

        // WA-5: DNS leak warning from relay
        if (msg.type === DNS_LEAK_WARN_TYPE) {
          _emit('whatsappLeakWarn', {
            domain:  msg.domain  || '(unknown)',
            message: 'A DNS query leaked outside the VPN tunnel. WhatsApp link previews may not load correctly.',
          });
        }

        // Heuristic: STUN binding requests are small JSON-framed relay messages
        // with type 'RELAY_DATA' and a small payload; we use message frequency
        // as a proxy.  We do not deep-inspect the packet — that stays in Java.
        // Simply reset the call-active heuristic on any relay traffic.
        if (msg.type === 'RELAY_DATA' || msg.type === 'PONG') {
          _callActive = true; // relay is live → calls can proceed
        }
      } catch (_) {}
    });

    // WA-4: Emit ready event on join
    _unsubJoinSuccess = vpnService.on('joinSuccess', () => {
      _lastActivity = Date.now();
      _callActive   = false; // reset; will become true when traffic flows
      _emit('whatsappReady', { timestamp: Date.now() });
      _startStallCheck();
    });

    // Cleanup on disconnect
    _unsubDisconnected = vpnService.on('vpnDisconnected', () => {
      _callActive = false;
      _clearStallCheck();
      // Clear any active warnings
      _emit('whatsappCallStall',  null);
      _emit('whatsappMediaStall', null);
    });

    _startStallCheck();
  },

  /**
   * Stop monitoring and clean up.
   */
  stop() {
    if (!_started) return;
    _started = false;

    clearInterval(_keepAliveTimer);
    _keepAliveTimer = null;

    _clearStallCheck();

    _unsubRelayMsg?.();
    _unsubRelayMsg = null;

    _unsubJoinSuccess?.();
    _unsubJoinSuccess = null;

    _unsubDisconnected?.();
    _unsubDisconnected = null;

    _listeners.clear();
  },

  /**
   * Subscribe to WhatsApp optimizer events.
   *
   * Events:
   *   'whatsappReady'       — payload: { timestamp }
   *                           VPN tunnel is up; WhatsApp calls are supported.
   *
   *   'whatsappCallStall'   — payload: { elapsed: ms } or null (stall cleared)
   *                           No relay traffic for CALL_STALL_MS — call may be
   *                           frozen; WhatsApp will auto-recover via ICE restart.
   *
   *   'whatsappMediaStall'  — payload: { elapsed: ms } or null (stall cleared)
   *                           No relay traffic for MEDIA_STALL_MS — prompt user
   *                           to retry the download in WhatsApp.
   *
   *   'whatsappLeakWarn'    — payload: { domain, message }
   *                           A DNS query leaked outside the tunnel (relay
   *                           future feature).
   *
   * Returns an unsubscribe function.
   */
  on(event, callback) {
    if (!_listeners.has(event)) _listeners.set(event, new Set());
    _listeners.get(event).add(callback);
    return () => _listeners.get(event)?.delete(callback);
  },

  /** True while we believe a WhatsApp call is in progress. */
  get isCallActive() {
    return _callActive;
  },
};

// ── Internal helpers ──────────────────────────────────────────────────────────

function _startStallCheck() {
  _clearStallCheck();

  // Check every 4 s (well below both stall thresholds)
  _stallCheckTimer = setInterval(() => {
    if (!_started) return;
    const elapsed = Date.now() - _lastActivity;

    // WA-2: Call stall
    if (elapsed >= CALL_STALL_MS && _callActive) {
      _emit('whatsappCallStall', { elapsed });
      // Keep relay alive during the stall
      vpnService._sendControl({ type: 'PING', src: 'WhatsAppOptimizer-callStall' });
    } else if (elapsed < CALL_STALL_MS) {
      _emit('whatsappCallStall', null); // clear
    }

    // WA-3: Media download stall (separate, longer threshold)
    if (elapsed >= MEDIA_STALL_MS) {
      _emit('whatsappMediaStall', { elapsed });
      vpnService._sendControl({ type: 'PING', src: 'WhatsAppOptimizer-mediaStall' });
    } else {
      _emit('whatsappMediaStall', null); // clear
    }
  }, 4_000);
}

function _clearStallCheck() {
  clearInterval(_stallCheckTimer);
  _stallCheckTimer = null;
}

function _emit(event, payload) {
  const cbs = _listeners.get(event);
  if (!cbs) return;
  cbs.forEach(cb => { try { cb(payload); } catch (_) {} });
}

export default WhatsAppOptimizer;
