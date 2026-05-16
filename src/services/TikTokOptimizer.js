/**
 * TikTokOptimizer.js — NetShare
 *
 * PURPOSE
 * -------
 * Independent helper that improves TikTok reliability over the NetShare VPN
 * tunnel.  It does NOT modify any existing file.  Import it in HomeScreen.jsx
 * (or wherever you set up VPN listeners) and call:
 *
 *   import TikTokOptimizer from './src/services/TikTokOptimizer';
 *   TikTokOptimizer.start();   // call once after VPN listeners are registered
 *   TikTokOptimizer.stop();    // call in your cleanup / useEffect return
 *
 * PROBLEMS ADDRESSED
 * ------------------
 *
 * TT-1  QUIC stall detection
 *   TikTok strongly prefers QUIC (UDP/443).  When the relay link is congested
 *   or the host Android firewall drops UDP/443, TikTok's QUIC path stalls.
 *   TikTok's own retry logic waits 10–30 s before falling back to TCP, causing
 *   the "spinner of death".  We detect the stall earlier (via a silence window
 *   on the relay WebSocket) and emit a 'tiktokStall' event so the UI can show
 *   a user-friendly nudge ("TikTok is loading — hold on…") instead of a blank
 *   spinner.  We also fire a PONG to keep the relay WebSocket alive during the
 *   stall so the connection is not torn down.
 *
 * TT-2  Keep-alive heartbeat
 *   TikTok CDN servers send low-frequency TCP keep-alives (every 75 s by
 *   default on Linux).  Carrier-grade NAT devices common in East Africa and
 *   South Asia drop idle UDP/TCP mappings in as little as 30 s.  We send a
 *   lightweight PING control message every 25 s so the NAT mapping on both
 *   sides of the relay stays open.  The relay already handles PING→PONG; this
 *   just ensures we do not rely solely on OkHttp's 15 s WS ping (which lives
 *   on the host side only and does not travel through the relay to the client).
 *
 * TT-3  Stall auto-recovery
 *   If the stall lasts longer than STALL_RECOVER_MS we attempt a soft relay
 *   reconnect via VpnService._scheduleReconnect() — the same mechanism already
 *   used for unexpected disconnects — rather than forcing the user to manually
 *   stop/start.
 *
 * TT-4  Bandwidth burst guard
 *   TikTok video (especially 1080p/4K) bursts at 8–12 Mbps.  If the relay
 *   WebSocket buffer fills up the relay drops QUIC ACK frames, which causes
 *   TikTok to halve its bitrate and show a lower-quality stream.  We monitor
 *   the relay's reported bufferedAmount (via BUFFER_WARN relay messages, if
 *   the relay sends them) and emit a 'tiktokBufferWarn' event so the UI can
 *   optionally inform the host that their uplink is the bottleneck.
 */

import vpnService from './VpnService';

// ── Tunables ──────────────────────────────────────────────────────────────────

/** Seconds of relay silence before we consider the QUIC path stalled. */
const QUIC_STALL_WINDOW_MS  = 8_000;

/** Seconds after which we give up waiting and trigger soft reconnect. */
const STALL_RECOVER_MS      = 20_000;

/** Interval for our client-side keep-alive PING. */
const KEEPALIVE_INTERVAL_MS = 25_000;

/** Relay message type emitted when the relay's write buffer is large. */
const RELAY_BUFFER_WARN_TYPE = 'BUFFER_WARN';

// ── Internal state ────────────────────────────────────────────────────────────

let _started          = false;
let _lastRelayActivity = Date.now();
let _stallTimer        = null;
let _recoverTimer      = null;
let _keepAliveTimer    = null;
let _unsubRelayMsg     = null;
let _unsubJoinSuccess  = null;
let _unsubDisconnected = null;

// Local event listeners registered by the host app
const _listeners = new Map();

// ── Public API ────────────────────────────────────────────────────────────────

const TikTokOptimizer = {

  /**
   * Start monitoring.  Safe to call multiple times — subsequent calls are
   * no-ops until stop() is called first.
   */
  start() {
    if (_started) return;
    _started = true;

    // TT-2: Keep-alive PING every 25 s
    _keepAliveTimer = setInterval(() => {
      vpnService._sendControl({ type: 'PING', src: 'TikTokOptimizer' });
    }, KEEPALIVE_INTERVAL_MS);

    // Listen for relay messages to (a) reset the stall clock, (b) detect
    // BUFFER_WARN messages from the relay.
    _unsubRelayMsg = vpnService.on('relayMessage', (payload) => {
      _lastRelayActivity = Date.now();
      _resetStallTimer();

      try {
        const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
        if (!msg) return;

        // TT-4: buffer warning from relay
        if (msg.type === RELAY_BUFFER_WARN_TYPE) {
          _emit('tiktokBufferWarn', {
            bufferedBytes: msg.bufferedBytes || 0,
            message: 'Host uplink is congested — TikTok video quality may drop.',
          });
        }
      } catch (_) {}
    });

    // Reset stall timer when we get a successful join (fresh session start)
    _unsubJoinSuccess = vpnService.on('joinSuccess', () => {
      _lastRelayActivity = Date.now();
      _resetStallTimer();
      _startStallTimer();
    });

    // Clean up timers on disconnect
    _unsubDisconnected = vpnService.on('vpnDisconnected', () => {
      _clearStallTimers();
      _emit('tiktokStall', null); // clear any pending stall UI
    });

    // TT-1: Start the initial stall timer
    _startStallTimer();
  },

  /**
   * Stop monitoring and clean up all timers and subscriptions.
   * Call this in your component's unmount / useEffect cleanup.
   */
  stop() {
    if (!_started) return;
    _started = false;

    clearInterval(_keepAliveTimer);
    _keepAliveTimer = null;

    _clearStallTimers();

    _unsubRelayMsg?.();
    _unsubRelayMsg = null;

    _unsubJoinSuccess?.();
    _unsubJoinSuccess = null;

    _unsubDisconnected?.();
    _unsubDisconnected = null;

    _listeners.clear();
  },

  /**
   * Subscribe to TikTok optimizer events.
   *
   * Events:
   *   'tiktokStall'       — payload: { elapsed: ms } or null (stall cleared)
   *   'tiktokBufferWarn'  — payload: { bufferedBytes, message }
   *   'tiktokRecovering'  — payload: { elapsed: ms }  (soft reconnect triggered)
   *
   * Returns an unsubscribe function.
   */
  on(event, callback) {
    if (!_listeners.has(event)) _listeners.set(event, new Set());
    _listeners.get(event).add(callback);
    return () => _listeners.get(event)?.delete(callback);
  },
};

// ── Internal helpers ──────────────────────────────────────────────────────────

function _startStallTimer() {
  _clearStallTimers();

  // TT-1: Check every QUIC_STALL_WINDOW_MS whether relay has been silent
  _stallTimer = setInterval(() => {
    const elapsed = Date.now() - _lastRelayActivity;

    if (elapsed >= QUIC_STALL_WINDOW_MS) {
      // Relay has been silent — likely a QUIC stall
      _emit('tiktokStall', { elapsed });

      // TT-2: Prod the relay with a PING to keep the WS alive during the stall
      vpnService._sendControl({ type: 'PING', src: 'TikTokOptimizer-stall' });

      // TT-3: If stall exceeds STALL_RECOVER_MS, trigger soft reconnect
      if (elapsed >= STALL_RECOVER_MS && !_recoverTimer) {
        _recoverTimer = setTimeout(() => {
          _recoverTimer = null;
          _emit('tiktokRecovering', { elapsed });
          // _scheduleReconnect is the same backoff-reconnect already used by
          // VpnService for unexpected disconnects — no new logic needed.
          vpnService._scheduleReconnect();
        }, 1_000); // 1 s grace before triggering
      }
    } else {
      // Relay is active — clear any stall notification
      _emit('tiktokStall', null);
      if (_recoverTimer) {
        clearTimeout(_recoverTimer);
        _recoverTimer = null;
      }
    }
  }, QUIC_STALL_WINDOW_MS);
}

function _resetStallTimer() {
  // Relay sent something — reset the activity clock
  _lastRelayActivity = Date.now();
  if (_recoverTimer) {
    clearTimeout(_recoverTimer);
    _recoverTimer = null;
  }
  // Clear stall notification
  _emit('tiktokStall', null);
}

function _clearStallTimers() {
  clearInterval(_stallTimer);
  _stallTimer = null;
  clearTimeout(_recoverTimer);
  _recoverTimer = null;
}

function _emit(event, payload) {
  const cbs = _listeners.get(event);
  if (!cbs) return;
  cbs.forEach(cb => { try { cb(payload); } catch (_) {} });
}

export default TikTokOptimizer;
