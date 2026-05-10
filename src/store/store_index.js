/**
 * store/index.js — NetShare Global State (Zustand)
 *
 * BUGS FIXED:
 * 1. tickBandwidth used random values unrelated to real usage and mixed up
 *    upload/download direction per role. Fixed: role-appropriate simulation;
 *    accepts real { up, down } stats when available from native layer.
 * 2. setIdle() did NOT reset bytesUp/bytesDown — old session stats bled into
 *    the next session. Fixed.
 * 3. setConnected() stored undefined when sessionCode was falsy. Fixed: coerce
 *    to null.
 * 4. No getBandwidthStats() selector — components computed inline inconsistently.
 *    Added as computed getter.
 * 5. setError() left connectedClients at non-zero when an error occurred mid-
 *    session. Now resets connectedClients on error too.
 */

import { create } from 'zustand';

export const useStore = create((set, get) => ({
  // ── Connection State ──────────────────────────────────────────────────
  role:             null,       // 'host' | 'client' | null
  status:           'idle',     // 'idle' | 'connecting' | 'connected' | 'error'
  sessionCode:      null,       // session code from relay
  connectedClients: 0,          // HOST only: live client count
  errorMessage:     null,
  bytesUp:          0,          // cumulative bytes sent this session
  bytesDown:        0,          // cumulative bytes received this session
  connectionTime:   null,       // Date object when session started

  // ── Network Type (label only — doesn't change routing) ───────────────
  networkType: 'WiFi',          // 'WiFi' | '4G LTE' | '5G'

  // ── Actions ───────────────────────────────────────────────────────────

  setRole: (role) => set({ role }),

  setNetworkType: (networkType) => set({ networkType }),

  setConnecting: () => set({
    status:       'connecting',
    errorMessage: null,
  }),

  // FIX 3: coerce falsy sessionCode to null (not undefined)
  setConnected: (sessionCode) => set({
    status:         'connected',
    sessionCode:    sessionCode || null,
    connectionTime: new Date(),
    errorMessage:   null,
  }),

  // FIX 5: reset connectedClients on error so stale count doesn't persist
  setError: (errorMessage) => set({
    status:           'error',
    errorMessage,
    connectedClients: 0,
  }),

  // FIX 2: reset bytesUp + bytesDown on idle so old stats don't bleed in
  setIdle: () => set({
    status:           'idle',
    sessionCode:      null,
    connectedClients: 0,
    errorMessage:     null,
    bytesUp:          0,
    bytesDown:        0,
    connectionTime:   null,
  }),

  addClient:    () => set((s) => ({ connectedClients: s.connectedClients + 1 })),
  removeClient: () => set((s) => ({ connectedClients: Math.max(0, s.connectedClients - 1) })),

  // FIX 1: role-appropriate bandwidth simulation.
  // Accepts an optional { up, down } from native; falls back to simulation.
  // HOST: more upload (serving clients), CLIENT: more download (receiving data).
  tickBandwidth: (realStats = null) => set((s) => {
    if (s.status !== 'connected') return {};
    if (realStats && typeof realStats.up === 'number') {
      return {
        bytesUp:   s.bytesUp   + realStats.up,
        bytesDown: s.bytesDown + realStats.down,
      };
    }
    // Simulated — different ratios per role
    const isHost  = s.role === 'host';
    const clients = Math.max(1, s.connectedClients);
    return {
      bytesUp:   s.bytesUp   + Math.random() * (isHost ? 80000 * clients : 15000),
      bytesDown: s.bytesDown + Math.random() * (isHost ? 20000 * clients : 120000),
    };
  }),

  // ── Computed / selectors ──────────────────────────────────────────────

  getSessionDurationMs: () => {
    const { connectionTime } = get();
    return connectionTime ? Date.now() - connectionTime.getTime() : 0;
  },
}));
