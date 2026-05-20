/**
 * HomeScreen.jsx — NetShare
 *
 * Updated to use per-app VPN services (TikTok.js, WhatsApp.js, etc.)
 * instead of the old VpnService.js / TikTokOptimizer / WhatsAppOptimizer.
 *
 * The selected app service is chosen by the user via the app selector tabs.
 * All services share the same interface (startAsHost, startAsClient, stop, on).
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import {
  View,
  Text,
  TouchableOpacity,
  TextInput,
  StyleSheet,
  ScrollView,
  ActivityIndicator,
  Alert,
  Platform,
  StatusBar,
  SafeAreaView,
} from 'react-native';
import { useStore } from '../store';

// ── VPN services — one per app ───────────────────────────────────────────────
import TikTokService   from '../services/TikTok';
import WhatsAppService from '../services/WhatsApp';
import FacebookService from '../services/Facebook';

const APP_SERVICES = {
  TikTok:   TikTokService,
  WhatsApp: WhatsAppService,
  Facebook: FacebookService,
};
const APP_LIST = Object.keys(APP_SERVICES);

// ─── helpers ────────────────────────────────────────────────────────────────

function formatBytes(bytes) {
  if (!bytes || bytes < 1024)    return `${bytes || 0} B`;
  if (bytes < 1048576)           return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1073741824)        return `${(bytes / 1048576).toFixed(1)} MB`;
  return `${(bytes / 1073741824).toFixed(2)} GB`;
}

function formatDuration(ms) {
  if (!ms || ms < 0) return '0s';
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m ${sec}s`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

// ─── Component ──────────────────────────────────────────────────────────────

export default function HomeScreen() {
  const {
    role, status, sessionCode, connectedClients,
    errorMessage, bytesUp, bytesDown, networkType,
    setRole, setNetworkType, setConnecting, setConnected,
    setError, setIdle, addClient, removeClient,
    getSessionDurationMs,
  } = useStore();

  const [accessCodeInput, setAccessCodeInput] = useState('');
  const [eventLog,        setEventLog]        = useState([]);
  const [sessionTimer,    setSessionTimer]    = useState('0s');
  const [selectedApp,     setSelectedApp]     = useState('TikTok');
  const timerRef     = useRef(null);
  const bandwidthRef = useRef(null);
  const unsubsRef    = useRef([]);   // holds all active event unsub functions

  // Derive active service from selected app
  const vpnService = APP_SERVICES[selectedApp];

  // ── Event log helper ────────────────────────────────────────────────
  const log = useCallback((msg) => {
    const ts = new Date().toLocaleTimeString();
    setEventLog(prev => [`[${ts}] ${msg}`, ...prev].slice(0, 50));
  }, []);

  // ── Clear timers ────────────────────────────────────────────────────
  const clearTimers = useCallback(() => {
    clearInterval(timerRef.current);
    clearInterval(bandwidthRef.current);
    timerRef.current     = null;
    bandwidthRef.current = null;
  }, []);

  // ── Start session timers ────────────────────────────────────────────
  const startTimers = useCallback(() => {
    clearTimers();
    timerRef.current = setInterval(() => {
      const ms = useStore.getState().getSessionDurationMs();
      setSessionTimer(formatDuration(ms));
    }, 1000);
    bandwidthRef.current = setInterval(() => {
      useStore.getState().tickBandwidth();
    }, 1500);
  }, [clearTimers]);

  // ── Unsubscribe all listeners ────────────────────────────────────────
  const clearListeners = useCallback(() => {
    unsubsRef.current.forEach(fn => { try { fn?.(); } catch {} });
    unsubsRef.current = [];
  }, []);

  // ── Attach listeners to the currently selected service ───────────────
  const attachListeners = useCallback((svc) => {
    clearListeners();
    const unsubs = [];

    unsubs.push(svc.on('vpnConnected', (data) => {
      log(`VPN tunnel up (${data || 'ok'})`);
    }));

    unsubs.push(svc.on('sessionCreated', (code) => {
      log(`Session ready: ${code}`);
      setConnected(code);
      startTimers();
    }));

    unsubs.push(svc.on('joinSuccess', (code) => {
      log(`Joined session: ${code}`);
      setConnected(code);
      startTimers();
    }));

    unsubs.push(svc.on('joinError', (reason) => {
      log(`Join failed: ${reason}`);
      clearTimers();
      setError(reason || 'Invalid access code');
      setTimeout(() => {
        if (useStore.getState().status === 'error') setIdle();
      }, 3000);
    }));

    unsubs.push(svc.on('vpnError', (msg) => {
      log(`Error: ${msg}`);
      clearTimers();
      setError(msg || 'Connection failed');
      setTimeout(() => {
        if (useStore.getState().status === 'error') setIdle();
      }, 4000);
    }));

    unsubs.push(svc.on('vpnDisconnected', (reason) => {
      log(`Disconnected: ${reason || 'connection closed'} — reconnecting...`);
      clearTimers();
      setConnecting();
    }));

    unsubs.push(svc.on('reconnectFailed', (msg) => {
      log(`Reconnect failed: ${msg}`);
      clearTimers();
      setError(msg || 'Connection lost');
      setTimeout(() => {
        if (useStore.getState().status === 'error') setIdle();
      }, 4000);
    }));

    unsubs.push(svc.on('clientConnected', (clientId) => {
      log(`Client joined: ${clientId || 'unknown'}`);
      addClient();
    }));

    unsubs.push(svc.on('clientDisconnected', () => {
      log('Client left');
      removeClient();
    }));

    unsubs.push(svc.on('hostLeft', (msg) => {
      log(`Host ended session: ${msg}`);
      clearTimers();
      Alert.alert('Session Ended', 'The host ended the sharing session.', [
        { text: 'OK', onPress: () => setIdle() },
      ]);
    }));

    unsubsRef.current = unsubs;
  }, [clearListeners, clearTimers, startTimers, log, setConnected, setConnecting, setError, setIdle, addClient, removeClient]);

  // ── Attach listeners whenever selected app changes ───────────────────
  useEffect(() => {
    attachListeners(vpnService);
    return () => clearListeners();
  }, [selectedApp]); // re-attach when user switches app

  // ── Cleanup on unmount ───────────────────────────────────────────────
  useEffect(() => {
    return () => {
      clearListeners();
      clearTimers();
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Disconnect ──────────────────────────────────────────────────────
  const handleDisconnect = useCallback(async () => {
    log('Disconnecting...');
    clearTimers();
    try {
      await vpnService.stop();
    } catch (e) {
      log(`Stop error: ${e?.message}`);
    }
    setIdle();
    log('Disconnected.');
  }, [log, clearTimers, setIdle, vpnService]);

  // ── Start HOST ──────────────────────────────────────────────────────
  const handleStartHost = async () => {
    try {
      setRole('host');
      setConnecting();
      log(`Starting host on ${networkType}...`);
      await vpnService.startAsHost(networkType);
    } catch (e) {
      log(`Host start failed: ${e.message}`);
      setError(e.message);
      setTimeout(() => {
        if (useStore.getState().status === 'error') setIdle();
      }, 4000);
    }
  };

  // ── Start CLIENT ────────────────────────────────────────────────────
  const handleStartClient = async () => {
    const code = accessCodeInput.trim().toUpperCase();
    if (!code || code.replace('-', '').length < 8) {
      Alert.alert('Invalid Code', 'Enter a valid 8-character access code (XXXX-XXXX).');
      return;
    }
    try {
      setRole('client');
      setConnecting();
      log(`Joining with code ${code}...`);
      await vpnService.startAsClient(code);
    } catch (e) {
      log(`Join failed: ${e.message}`);
      setError(e.message);
      setTimeout(() => {
        if (useStore.getState().status === 'error') setIdle();
      }, 4000);
    }
  };

  // ─── Render ──────────────────────────────────────────────────────────

  const isIdle       = status === 'idle';
  const isConnecting = status === 'connecting';
  const isConnected  = status === 'connected';
  const isError      = status === 'error';
  const isActive     = isConnecting || isConnected;

  return (
    <SafeAreaView style={s.safeArea}>
      <StatusBar barStyle="light-content" backgroundColor="#0a0e1a" />
      <ScrollView contentContainerStyle={s.scroll} keyboardShouldPersistTaps="handled">

        {/* ── Header ── */}
        <View style={s.header}>
          <Text style={s.title}>NETSHARE</Text>
          <Text style={s.subtitle}>PREMIUM NETWORK SHARING</Text>
          <View style={s.badge}>
            <Text style={s.badgeText}>BUSINESS EDITION · v2.0</Text>
          </View>
        </View>

        {/* ── Role tabs ── */}
        <View style={s.tabs}>
          <TouchableOpacity
            style={[s.tab, role !== 'client' && s.tabActive]}
            onPress={() => { if (!isActive) { setRole('host'); setIdle(); } }}
            disabled={isActive}>
            <Text style={[s.tabTitle, role !== 'client' && s.tabTitleActive]}>HOST</Text>
            <Text style={[s.tabSub,   role !== 'client' && s.tabSubActive]}>Share &amp; Earn</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[s.tab, role === 'client' && s.tabActive]}
            onPress={() => { if (!isActive) { setRole('client'); setIdle(); } }}
            disabled={isActive}>
            <Text style={[s.tabTitle, role === 'client' && s.tabTitleActive]}>CLIENT</Text>
            <Text style={[s.tabSub,   role === 'client' && s.tabSubActive]}>Access Network</Text>
          </TouchableOpacity>
        </View>

        {/* ── App selector ── */}
        <View style={s.appRow}>
          {APP_LIST.map((app) => (
            <TouchableOpacity
              key={app}
              style={[s.appBtn, selectedApp === app && s.appBtnActive]}
              onPress={() => { if (!isActive) setSelectedApp(app); }}
              disabled={isActive}>
              <Text style={[s.appBtnText, selectedApp === app && s.appBtnTextActive]}>
                {app.toUpperCase()}
              </Text>
            </TouchableOpacity>
          ))}
        </View>

        {/* ── Network type selector (HOST only) ── */}
        {role !== 'client' && (
          <View style={s.netRow}>
            {['WiFi', '4G LTE', '5G'].map((t) => (
              <TouchableOpacity
                key={t}
                style={[s.netBtn, networkType === t && s.netBtnActive]}
                onPress={() => { if (!isActive) setNetworkType(t); }}
                disabled={isActive}>
                <Text style={[s.netBtnText, networkType === t && s.netBtnTextActive]}>{t}</Text>
              </TouchableOpacity>
            ))}
          </View>
        )}

        {/* ── Status card ── */}
        <View style={s.card}>

          {/* Status row */}
          <View style={s.statusRow}>
            <View style={[
              s.dot,
              isConnected  && s.dotGreen,
              isConnecting && s.dotYellow,
              isError      && s.dotRed,
              isIdle       && s.dotGrey,
            ]} />
            <Text style={s.statusText}>
              {isIdle       ? 'IDLE'
             : isConnecting ? 'CONNECTING...'
             : isConnected  ? 'CONNECTED'
             :                'ERROR'}
            </Text>
          </View>

          {/* Error message */}
          {isError && !!errorMessage && (
            <Text style={s.errorMsg}>{errorMessage}</Text>
          )}

          {/* Stats (connected) */}
          {isConnected && (
            <View style={s.statsRow}>
              <View style={s.statItem}>
                <Text style={s.statLabel}>⏱ Duration</Text>
                <Text style={s.statValue}>{sessionTimer}</Text>
              </View>
              <View style={s.statItem}>
                <Text style={s.statLabel}>↑ Upload</Text>
                <Text style={s.statValue}>{formatBytes(bytesUp)}</Text>
              </View>
              <View style={s.statItem}>
                <Text style={s.statLabel}>↓ Download</Text>
                <Text style={s.statValue}>{formatBytes(bytesDown)}</Text>
              </View>
              {role !== 'client' && (
                <View style={s.statItem}>
                  <Text style={s.statLabel}>👥 Clients</Text>
                  <Text style={s.statValue}>{connectedClients}</Text>
                </View>
              )}
            </View>
          )}

          {/* Client: access code input */}
          {role === 'client' && (isIdle || isError) && (
            <TextInput
              style={s.input}
              placeholder="Enter access code (XXXX-XXXX)"
              placeholderTextColor="#4a5568"
              value={accessCodeInput}
              onChangeText={setAccessCodeInput}
              autoCapitalize="characters"
              maxLength={9}
            />
          )}

          {/* CONNECT button */}
          {(isIdle || isError) && (
            <TouchableOpacity
              style={[s.btnConnect, isError && s.btnConnectDisabled]}
              onPress={role === 'client' ? handleStartClient : handleStartHost}
              disabled={isError}>
              <Text style={s.btnConnectText}>
                {role === 'client' ? 'JOIN NETWORK' : 'START HOSTING'}
              </Text>
            </TouchableOpacity>
          )}

          {/* CONNECTING spinner + cancel */}
          {isConnecting && (
            <View style={s.connectingBox}>
              <ActivityIndicator color="#00e5ff" size="large" />
              <Text style={s.connectingLabel}>CONNECTING...</Text>
              <TouchableOpacity style={s.btnDisconnect} onPress={handleDisconnect}>
                <Text style={s.btnDisconnectText}>CANCEL</Text>
              </TouchableOpacity>
            </View>
          )}

          {/* DISCONNECT button */}
          {isConnected && (
            <TouchableOpacity style={s.btnDisconnect} onPress={handleDisconnect}>
              <Text style={s.btnDisconnectText}>
                {role === 'client' ? 'DISCONNECT' : 'STOP HOSTING'}
              </Text>
            </TouchableOpacity>
          )}
        </View>

        {/* ── Host info (host idle) ── */}
        {role !== 'client' && isIdle && (
          <View style={s.infoCard}>
            <Text style={s.infoTitle}>HOST &amp; EARN</Text>
            <Text style={s.infoItem}>• Share your WiFi / mobile data connection</Text>
            <Text style={s.infoItem}>• Earn 50% of platform revenue from your uptime</Text>
            <Text style={s.infoItem}>• Paid weekly by the platform owner</Text>
            <Text style={s.infoItem}>• Clients auto-failover if your connection drops</Text>
          </View>
        )}

        {/* ── Event log ── */}
        {eventLog.length > 0 && (
          <View style={s.logCard}>
            <Text style={s.logTitle}>EVENT LOG</Text>
            {eventLog.slice(0, 10).map((line, i) => (
              <Text key={i} style={s.logLine}>{line}</Text>
            ))}
          </View>
        )}

      </ScrollView>
    </SafeAreaView>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const CYAN   = '#00e5ff';
const GREEN  = '#00ff88';
const RED    = '#ff4d6a';
const YELLOW = '#ffd600';
const BG     = '#0a0e1a';
const CARD   = '#111827';
const BORDER = '#1e2a3a';

const s = StyleSheet.create({
  safeArea: { flex: 1, backgroundColor: BG },
  scroll:   { padding: 16, paddingBottom: 40 },

  header:    { alignItems: 'center', marginBottom: 24, marginTop: 8 },
  title:     { fontSize: 38, fontWeight: '900', color: CYAN, letterSpacing: 8 },
  subtitle:  { color: '#4a6080', fontSize: 11, letterSpacing: 4, marginTop: 2 },
  badge:     { borderWidth: 1, borderColor: '#2a3a5a', borderRadius: 4,
               paddingHorizontal: 12, paddingVertical: 4, marginTop: 10 },
  badgeText: { color: '#6080a0', fontSize: 11, letterSpacing: 2 },



  appRow:           { flexDirection: 'row', gap: 8, marginBottom: 12 },
  appBtn:           { flex: 1, paddingVertical: 10, borderRadius: 8, borderWidth: 1,
                      borderColor: BORDER, backgroundColor: CARD, alignItems: 'center' },
  appBtnActive:     { borderColor: CYAN, backgroundColor: '#0d2030' },
  appBtnText:       { color: '#4a6080', fontWeight: '700', fontSize: 11, letterSpacing: 1 },
  appBtnTextActive: { color: CYAN },

  tabs:          { flexDirection: 'row', gap: 10, marginBottom: 12 },
  tab:           { flex: 1, padding: 14, borderRadius: 10, borderWidth: 1,
                   borderColor: BORDER, backgroundColor: CARD, alignItems: 'center' },
  tabActive:     { borderColor: CYAN, backgroundColor: '#0d2030' },
  tabTitle:      { color: '#4a6080', fontWeight: '700', fontSize: 15, letterSpacing: 2 },
  tabTitleActive:{ color: CYAN },
  tabSub:        { color: '#2a3a5a', fontSize: 11, marginTop: 2 },
  tabSubActive:  { color: '#6090b0' },

  netRow:           { flexDirection: 'row', gap: 8, marginBottom: 14 },
  netBtn:           { flex: 1, paddingVertical: 10, borderRadius: 8, borderWidth: 1,
                      borderColor: BORDER, backgroundColor: CARD, alignItems: 'center' },
  netBtnActive:     { borderColor: GREEN, backgroundColor: '#0d2318' },
  netBtnText:       { color: '#4a6080', fontWeight: '600', fontSize: 13 },
  netBtnTextActive: { color: GREEN },

  card:       { backgroundColor: CARD, borderRadius: 12, borderWidth: 1,
                borderColor: BORDER, padding: 16, marginBottom: 14 },
  statusRow:  { flexDirection: 'row', alignItems: 'center', marginBottom: 10 },
  dot:        { width: 12, height: 12, borderRadius: 6, marginRight: 10 },
  dotGreen:   { backgroundColor: GREEN },
  dotYellow:  { backgroundColor: YELLOW },
  dotRed:     { backgroundColor: RED },
  dotGrey:    { backgroundColor: '#2a3a5a' },
  statusText: { color: CYAN, fontWeight: '700', fontSize: 15, letterSpacing: 3 },

  errorMsg: { color: RED, fontSize: 13, marginBottom: 12, lineHeight: 18 },

  statsRow:  { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginBottom: 14 },
  statItem:  { flex: 1, minWidth: '45%', backgroundColor: '#0d1a2a', borderRadius: 8, padding: 10 },
  statLabel: { color: '#4a6080', fontSize: 10, letterSpacing: 1, marginBottom: 3 },
  statValue: { color: '#c0d8f0', fontSize: 15, fontWeight: '700' },

  input: {
    backgroundColor: '#0d1a2a', borderWidth: 1, borderColor: BORDER,
    borderRadius: 8, color: '#c0d8f0', fontSize: 16,
    paddingHorizontal: 14, paddingVertical: 12, marginBottom: 14,
    letterSpacing: 2, textAlign: 'center',
  },

  btnConnect:         { backgroundColor: CYAN, borderRadius: 10, paddingVertical: 16, alignItems: 'center' },
  btnConnectDisabled: { opacity: 0.4 },
  btnConnectText:     { color: '#000', fontWeight: '900', fontSize: 15, letterSpacing: 3 },

  connectingBox:   { alignItems: 'center', paddingVertical: 8, gap: 10 },
  connectingLabel: { color: YELLOW, fontWeight: '700', letterSpacing: 3, fontSize: 13 },

  btnDisconnect:     { borderWidth: 1, borderColor: RED, borderRadius: 10,
                       paddingVertical: 14, alignItems: 'center', marginTop: 4 },
  btnDisconnectText: { color: RED, fontWeight: '700', fontSize: 14, letterSpacing: 2 },

  infoCard:  { backgroundColor: CARD, borderRadius: 12, borderWidth: 1,
               borderColor: BORDER, padding: 16, marginBottom: 14 },
  infoTitle: { color: '#4a6080', fontSize: 10, letterSpacing: 3, marginBottom: 10 },
  infoItem:  { color: '#6080a0', fontSize: 13, lineHeight: 22 },

  logCard:  { backgroundColor: CARD, borderRadius: 12, borderWidth: 1,
              borderColor: BORDER, padding: 14 },
  logTitle: { color: '#4a6080', fontSize: 10, letterSpacing: 3, marginBottom: 8 },
  logLine:  { color: '#2a4060', fontSize: 11,
              fontFamily: Platform.OS === 'ios' ? 'Courier' : 'monospace', lineHeight: 17 },
});
