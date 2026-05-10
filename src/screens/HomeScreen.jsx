/**
 * HomeScreen.jsx — NetShare
 *
 * FIXES APPLIED HERE:
 * 1. Disconnect button — shown whenever status is 'connecting' or 'connected'.
 *    Calls vpnService.stop() then store.setIdle() to fully reset state.
 * 2. vpnError listener — catches native errors (including cold-start timeouts)
 *    and surfaces them to the user with a Retry button instead of freezing.
 * 3. vpnDisconnected listener — properly resets state when the host drops,
 *    server closes the socket, or the user taps Stop in the notification.
 * 4. sessionCreated / joinSuccess / vpnConnected listeners — drive the store
 *    into 'connected' state so the UI updates correctly.
 * 5. clientConnected / clientDisconnected listeners — update connected client count.
 * 6. hostLeft listener — notifies client that the host ended the session.
 * 7. All listeners are cleaned up on unmount — no memory leaks.
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
import vpnService from '../services/VpnService';

// ─── helpers ────────────────────────────────────────────────────────────────

function formatBytes(bytes) {
  if (bytes < 1024)       return `${bytes} B`;
  if (bytes < 1048576)    return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
  return `${(bytes / 1073741824).toFixed(2)} GB`;
}

function formatDuration(ms) {
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
  const [eventLog, setEventLog]               = useState([]);
  const [sessionTimer, setSessionTimer]       = useState('0s');
  const timerRef = useRef(null);
  const bandwidthRef = useRef(null);

  // ── Event log helper ────────────────────────────────────────────────
  const log = useCallback((msg) => {
    const ts = new Date().toLocaleTimeString();
    setEventLog(prev => [`[${ts}] ${msg}`, ...prev].slice(0, 50));
  }, []);

  // ── Disconnect (shared by host + client) ────────────────────────────
  const handleDisconnect = useCallback(async () => {
    log('Disconnecting...');
    clearInterval(timerRef.current);
    clearInterval(bandwidthRef.current);
    await vpnService.stop();
    setIdle();
    log('Disconnected.');
  }, [log, setIdle]);

  // ── Native event listeners ──────────────────────────────────────────
  useEffect(() => {
    // vpnConnected — WS is open; for HOST we still wait for SESSION_CREATED
    const unVpnConnected = vpnService.on('vpnConnected', (data) => {
      log(`VPN connected (${data || 'ok'})`);
      // CLIENT gets their session code via joinSuccess; HOST via sessionCreated.
      // Don't call setConnected here for host — wait for SESSION_CREATED.
      if (useStore.getState().role === 'client') {
        // Will be confirmed by joinSuccess, but optimistically show connecting
      }
    });

    // sessionCreated — relay assigned a code to HOST
    const unSessionCreated = vpnService.on('sessionCreated', (code) => {
      log(`Session created: ${code}`);
      setConnected(code);
      startTimers();
    });

    // joinSuccess — relay confirmed CLIENT joined a session
    const unJoinSuccess = vpnService.on('joinSuccess', (code) => {
      log(`Joined session: ${code}`);
      setConnected(code);
      startTimers();
    });

    // joinError — relay rejected the access code
    const unJoinError = vpnService.on('joinError', (reason) => {
      log(`Join error: ${reason}`);
      setError(reason || 'Invalid access code');
    });

    // vpnError — any native-layer error (timeout, socket failure, etc.)
    const unVpnError = vpnService.on('vpnError', (msg) => {
      log(`Error: ${msg}`);
      clearInterval(timerRef.current);
      clearInterval(bandwidthRef.current);
      setError(msg || 'Connection failed');
    });

    // vpnDisconnected — clean shutdown from native side or notification Stop tap
    const unVpnDisconnected = vpnService.on('vpnDisconnected', (reason) => {
      log(`Disconnected: ${reason}`);
      clearInterval(timerRef.current);
      clearInterval(bandwidthRef.current);
      // Only reset if we were connected/connecting (not if user already tapped disconnect)
      const s = useStore.getState().status;
      if (s !== 'idle') setIdle();
    });

    // clientConnected — HOST sees a new client join
    const unClientConnected = vpnService.on('clientConnected', (clientId) => {
      log(`Client connected: ${clientId || 'unknown'}`);
      addClient();
    });

    // clientDisconnected — HOST sees a client leave
    const unClientDisconnected = vpnService.on('clientDisconnected', () => {
      log('Client disconnected');
      removeClient();
    });

    // hostLeft — CLIENT sees host end the session
    const unHostLeft = vpnService.on('hostLeft', (msg) => {
      log(`Host ended session: ${msg}`);
      clearInterval(timerRef.current);
      clearInterval(bandwidthRef.current);
      Alert.alert('Session Ended', 'The host ended the sharing session.', [
        { text: 'OK', onPress: () => setIdle() },
      ]);
    });

    return () => {
      unVpnConnected();
      unSessionCreated();
      unJoinSuccess();
      unJoinError();
      unVpnError();
      unVpnDisconnected();
      unClientConnected();
      unClientDisconnected();
      unHostLeft();
      clearInterval(timerRef.current);
      clearInterval(bandwidthRef.current);
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Session timer + bandwidth simulation ───────────────────────────
  function startTimers() {
    clearInterval(timerRef.current);
    clearInterval(bandwidthRef.current);

    timerRef.current = setInterval(() => {
      setSessionTimer(formatDuration(useStore.getState().getSessionDurationMs()));
    }, 1000);

    bandwidthRef.current = setInterval(() => {
      useStore.getState().tickBandwidth();
    }, 1500);
  }

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
    }
  };

  // ── Start CLIENT ────────────────────────────────────────────────────
  const handleStartClient = async () => {
    const code = accessCodeInput.trim().toUpperCase();
    if (!code || code.replace('-', '').length < 8) {
      Alert.alert('Invalid Code', 'Please enter a valid 8-character access code (XXXX-XXXX).');
      return;
    }
    try {
      setRole('client');
      setConnecting();
      log(`Joining session with code ${code}...`);
      await vpnService.startAsClient(code);
    } catch (e) {
      log(`Client join failed: ${e.message}`);
      setError(e.message);
    }
  };

  // ── Retry after error ───────────────────────────────────────────────
  const handleRetry = async () => {
    await vpnService.stop();
    setIdle();
    log('Ready to retry.');
  };

  // ─── Render ──────────────────────────────────────────────────────────

  const isIdle        = status === 'idle';
  const isConnecting  = status === 'connecting';
  const isConnected   = status === 'connected';
  const isError       = status === 'error';
  const isActive      = isConnecting || isConnected;

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
            onPress={() => { if (isIdle || isError) { setRole('host'); setIdle(); } }}
            disabled={isActive}
          >
            <Text style={[s.tabTitle, role !== 'client' && s.tabTitleActive]}>HOST</Text>
            <Text style={[s.tabSub,   role !== 'client' && s.tabSubActive]}>Share &amp; Earn</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[s.tab, role === 'client' && s.tabActive]}
            onPress={() => { if (isIdle || isError) { setRole('client'); setIdle(); } }}
            disabled={isActive}
          >
            <Text style={[s.tabTitle, role === 'client' && s.tabTitleActive]}>CLIENT</Text>
            <Text style={[s.tabSub,   role === 'client' && s.tabSubActive]}>Access Network</Text>
          </TouchableOpacity>
        </View>

        {/* ── Network type selector (HOST only) ── */}
        {role !== 'client' && (
          <View style={s.netRow}>
            {['WiFi', '4G LTE', '5G'].map((t) => (
              <TouchableOpacity
                key={t}
                style={[s.netBtn, networkType === t && s.netBtnActive]}
                onPress={() => { if (!isActive) setNetworkType(t); }}
                disabled={isActive}
              >
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
          {isError && (
            <Text style={s.errorMsg}>{errorMessage}</Text>
          )}

          {/* Session code (host connected) */}
          {isConnected && role !== 'client' && sessionCode && (
            <View style={s.codeBox}>
              <Text style={s.codeLabel}>YOUR SESSION CODE</Text>
              <Text style={s.codeValue}>{sessionCode}</Text>
              <Text style={s.codeHint}>Share this code with clients</Text>
            </View>
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
          {role === 'client' && isIdle && (
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

          {/* ── Action buttons ── */}

          {/* CONNECT button — only when idle */}
          {isIdle && (
            <TouchableOpacity
              style={s.btnConnect}
              onPress={role === 'client' ? handleStartClient : handleStartHost}
            >
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

          {/* DISCONNECT button — when connected */}
          {isConnected && (
            <TouchableOpacity style={s.btnDisconnect} onPress={handleDisconnect}>
              <Text style={s.btnDisconnectText}>
                {role === 'client' ? 'DISCONNECT' : 'STOP HOSTING'}
              </Text>
            </TouchableOpacity>
          )}

          {/* RETRY button — on error */}
          {isError && (
            <TouchableOpacity style={s.btnRetry} onPress={handleRetry}>
              <Text style={s.btnRetryText}>TRY AGAIN</Text>
            </TouchableOpacity>
          )}
        </View>

        {/* ── Host & Earn info (host idle) ── */}
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
            {eventLog.slice(0, 8).map((line, i) => (
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
  safeArea:  { flex: 1, backgroundColor: BG },
  scroll:    { padding: 16, paddingBottom: 40 },

  // Header
  header:    { alignItems: 'center', marginBottom: 24, marginTop: 8 },
  title:     { fontSize: 38, fontWeight: '900', color: CYAN, letterSpacing: 8 },
  subtitle:  { color: '#4a6080', fontSize: 11, letterSpacing: 4, marginTop: 2 },
  badge:     { borderWidth: 1, borderColor: '#2a3a5a', borderRadius: 4, paddingHorizontal: 12, paddingVertical: 4, marginTop: 10 },
  badgeText: { color: '#6080a0', fontSize: 11, letterSpacing: 2 },

  // Role tabs
  tabs:          { flexDirection: 'row', gap: 10, marginBottom: 12 },
  tab:           { flex: 1, padding: 14, borderRadius: 10, borderWidth: 1, borderColor: BORDER, backgroundColor: CARD, alignItems: 'center' },
  tabActive:     { borderColor: CYAN, backgroundColor: '#0d2030' },
  tabTitle:      { color: '#4a6080', fontWeight: '700', fontSize: 15, letterSpacing: 2 },
  tabTitleActive:{ color: CYAN },
  tabSub:        { color: '#2a3a5a', fontSize: 11, marginTop: 2 },
  tabSubActive:  { color: '#6090b0' },

  // Network type
  netRow:           { flexDirection: 'row', gap: 8, marginBottom: 14 },
  netBtn:           { flex: 1, paddingVertical: 10, borderRadius: 8, borderWidth: 1, borderColor: BORDER, backgroundColor: CARD, alignItems: 'center' },
  netBtnActive:     { borderColor: GREEN, backgroundColor: '#0d2318' },
  netBtnText:       { color: '#4a6080', fontWeight: '600', fontSize: 13 },
  netBtnTextActive: { color: GREEN },

  // Status card
  card:       { backgroundColor: CARD, borderRadius: 12, borderWidth: 1, borderColor: BORDER, padding: 16, marginBottom: 14 },
  statusRow:  { flexDirection: 'row', alignItems: 'center', marginBottom: 10 },
  dot:        { width: 12, height: 12, borderRadius: 6, marginRight: 10 },
  dotGreen:   { backgroundColor: GREEN },
  dotYellow:  { backgroundColor: YELLOW },
  dotRed:     { backgroundColor: RED },
  dotGrey:    { backgroundColor: '#2a3a5a' },
  statusText: { color: CYAN, fontWeight: '700', fontSize: 15, letterSpacing: 3 },

  errorMsg:   { color: RED, fontSize: 13, marginBottom: 12, lineHeight: 18 },

  // Session code
  codeBox:   { backgroundColor: '#0d1a2a', borderRadius: 8, padding: 14, alignItems: 'center', marginBottom: 14 },
  codeLabel: { color: '#4a6080', fontSize: 10, letterSpacing: 3, marginBottom: 6 },
  codeValue: { color: GREEN, fontSize: 28, fontWeight: '900', letterSpacing: 6 },
  codeHint:  { color: '#2a4060', fontSize: 11, marginTop: 4 },

  // Stats
  statsRow:   { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginBottom: 14 },
  statItem:   { flex: 1, minWidth: '45%', backgroundColor: '#0d1a2a', borderRadius: 8, padding: 10 },
  statLabel:  { color: '#4a6080', fontSize: 10, letterSpacing: 1, marginBottom: 3 },
  statValue:  { color: '#c0d8f0', fontSize: 15, fontWeight: '700' },

  // Input
  input: {
    backgroundColor: '#0d1a2a',
    borderWidth: 1,
    borderColor: BORDER,
    borderRadius: 8,
    color: '#c0d8f0',
    fontSize: 16,
    paddingHorizontal: 14,
    paddingVertical: 12,
    marginBottom: 14,
    letterSpacing: 2,
    textAlign: 'center',
  },

  // Buttons
  btnConnect: {
    backgroundColor: CYAN,
    borderRadius: 10,
    paddingVertical: 16,
    alignItems: 'center',
  },
  btnConnectText: { color: '#000', fontWeight: '900', fontSize: 15, letterSpacing: 3 },

  connectingBox:    { alignItems: 'center', paddingVertical: 8, gap: 10 },
  connectingLabel:  { color: YELLOW, fontWeight: '700', letterSpacing: 3, fontSize: 13 },

  btnDisconnect: {
    borderWidth: 1,
    borderColor: RED,
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: 'center',
    marginTop: 4,
  },
  btnDisconnectText: { color: RED, fontWeight: '700', fontSize: 14, letterSpacing: 2 },

  btnRetry: {
    backgroundColor: YELLOW,
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: 'center',
    marginTop: 4,
  },
  btnRetryText: { color: '#000', fontWeight: '900', fontSize: 14, letterSpacing: 2 },

  // Info card
  infoCard:  { backgroundColor: CARD, borderRadius: 12, borderWidth: 1, borderColor: BORDER, padding: 16, marginBottom: 14 },
  infoTitle: { color: '#4a6080', fontSize: 10, letterSpacing: 3, marginBottom: 10 },
  infoItem:  { color: '#6080a0', fontSize: 13, lineHeight: 22 },

  // Log card
  logCard:   { backgroundColor: CARD, borderRadius: 12, borderWidth: 1, borderColor: BORDER, padding: 14 },
  logTitle:  { color: '#4a6080', fontSize: 10, letterSpacing: 3, marginBottom: 8 },
  logLine:   { color: '#2a4060', fontSize: 11, fontFamily: Platform.OS === 'ios' ? 'Courier' : 'monospace', lineHeight: 17 },
});
