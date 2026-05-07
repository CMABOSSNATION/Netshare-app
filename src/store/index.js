/**
 * store/index.js
 * Global app state using Zustand.
 */

import { create } from 'zustand';

export const useStore = create((set, get) => ({
  // ── Connection State ─────────────────────────────────────────
  role: null,              // 'host' | 'client' | null
  status: 'idle',          // 'idle' | 'connecting' | 'connected' | 'error'
  sessionCode: null,       // 6-char code
  connectedClients: 0,     // host only: number of clients
  errorMessage: null,
  bytesUp: 0,
  bytesDown: 0,
  connectionTime: null,    // Date when connected

  // ── Network Type (for display only) ─────────────────────────
  networkType: 'WiFi',     // 'WiFi' | '4G LTE' | '5G'

  // ── Actions ──────────────────────────────────────────────────
  setRole: (role) => set({ role }),
  setNetworkType: (networkType) => set({ networkType }),

  setConnecting: () => set({
    status: 'connecting',
    errorMessage: null,
  }),

  setConnected: (sessionCode) => set({
    status: 'connected',
    sessionCode,
    connectionTime: new Date(),
    errorMessage: null,
  }),

  setError: (errorMessage) => set({
    status: 'error',
    errorMessage,
  }),

  setIdle: () => set({
    status: 'idle',
    sessionCode: null,
    connectedClients: 0,
    errorMessage: null,
    bytesUp: 0,
    bytesDown: 0,
    connectionTime: null,
  }),

  addClient: () => set((s) => ({ connectedClients: s.connectedClients + 1 })),
  removeClient: () => set((s) => ({
    connectedClients: Math.max(0, s.connectedClients - 1)
  })),

  // Simulate bandwidth stats (real implementation would come from native layer)
  tickBandwidth: () => set((s) => {
    if (s.status !== 'connected') return {};
    return {
      bytesUp: s.bytesUp + Math.random() * 50000,
      bytesDown: s.bytesDown + Math.random() * 150000,
    };
  }),
}));
