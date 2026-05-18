/**
 * store/index.js — NetShare Global State (Zustand)
 */

import { create } from 'zustand';

export const useStore = create((set, get) => ({
  // ── Connection State ──────────────────────────────────────────────────────
  role:             null,    // 'host' | 'client' | null
  status:           'idle',  // 'idle' | 'connecting' | 'connected' | 'error'
  sessionCode:      null,
  connectedClients: 0,
  errorMessage:     null,
  bytesUp:          0,
  bytesDown:        0,
  connectionTime:   null,

  // ── Actions ───────────────────────────────────────────────────────────────

  setRole: (role) => set({ role }),

  setConnecting: () => set({ status: 'connecting', errorMessage: null }),

  setConnected: (sessionCode) => set({
    status:         'connected',
    sessionCode:    sessionCode || null,
    connectionTime: new Date(),
    errorMessage:   null,
  }),

  setError: (errorMessage) => set({
    status:           'error',
    errorMessage,
    connectedClients: 0,
  }),

  setIdle: () => set({
    status:           'idle',
    sessionCode:      null,
    connectedClients: 0,
    errorMessage:     null,
    bytesUp:          0,
    bytesDown:        0,
    connectionTime:   null,
    role:             null,
  }),

  addClient:    () => set(s => ({ connectedClients: s.connectedClients + 1 })),
  removeClient: () => set(s => ({ connectedClients: Math.max(0, s.connectedClients - 1) })),

  tickBandwidth: (realStats = null) => set(s => {
    if (s.status !== 'connected') return {};
    if (realStats && typeof realStats.up === 'number') {
      return {
        bytesUp:   s.bytesUp   + realStats.up,
        bytesDown: s.bytesDown + realStats.down,
      };
    }
    return {};
  }),

  getSessionDurationMs: () => {
    const { connectionTime } = get();
    return connectionTime ? Date.now() - connectionTime.getTime() : 0;
  },
}));
