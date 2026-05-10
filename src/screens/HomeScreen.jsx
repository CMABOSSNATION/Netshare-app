/**
 * HomeScreen.jsx — NetShare
 *
 * BUGS FIXED:
 * 1. vpnConnected for HOST set sessionCode to "host" in the store. Fixed: HOST
 *    waits for sessionCreated; vpnConnected only logs for both roles.
 * 2. role captured stale inside vpnConnected effect. Fixed with roleRef.
 * 3. addLog captured stale in listener closures. Fixed with useCallback for stability.
 * 4. handleStop had no try/catch — stopVpn rejection left UI stuck in CONNECTED.
 *    Fixed with try/finally always calling setIdle().
 * 5. handleStartHost/Client left status as 'connecting' on error — no retry possible.
 *    Fixed: setIdle() called after brief delay in catch block.
 * 6. connectionTimeRef had race between effect and interval. Fixed by reading
 *    connectionTime directly from useStore.getState() inside the interval callback.
 * 7. Bandwidth timer not cleared properly on unmount. Fixed with explicit cleanup.
 * 8. accessCode input allowed more than 9 chars but the 9th was always a dash
 *    at position 4 — auto-format the code as XXXX-XXXX while typing.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  View, Text, TouchableOpacity, StyleSheet, Animated,
  Alert, Platform, ScrollView, StatusBar, TextInput,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import LinearGradient from 'react-native-linear-gradient';
import { useStore } from '../store';
import VpnService from '../services/VpnService';

const formatBytes = (bytes) => {
  if (bytes < 1024) return `${bytes.toFixed(0)} B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1048576).toFixed(2)} MB`;
};

const formatDuration = (startMs) => {
  if (!startMs) return '00:00';
  const secs = Math.floor((Date.now() - startMs) / 1000);
  const h = Math.floor(secs / 3600);
  const m = String(Math.floor((secs % 3600) / 60)).padStart(2, '0');
  const s = String(secs % 60).padStart(2, '0');
  return h > 0 ? `${h}:${m}:${s}` : `${m}:${s}`;
};

// FIX 8: format input as XXXX-XXXX while typing
const formatAccessCode = (raw) => {
  // Strip everything except alphanumeric
  const clean = raw.replace(/[^A-Za-z0-9]/g, '').toUpperCase().slice(0, 8);
  if (clean.length > 4) return clean.slice(0, 4) + '-' + clean.slice(4);
  return clean;
};

// Estimated host earnings — $0.50 per client-hour (50% of $1/hr per client)
const estimateHostEarnings = (startMs, clientCount = 0) => {
  if (!startMs || clientCount === 0) return '0.00';
  const hrs = (Date.now() - startMs) / 3_600_000;
  return (hrs * clientCount * 0.50).toFixed(2);
};

const T = {
  bg:      '#0A0E1A',
  card:    '#111827',
  border:  '#1E2D40',
  accent:  '#00D4FF',
  accent2: '#7B61FF',
  green:   '#00FF88',
  red:     '#FF4466',
  amber:   '#FFB400',
  text:    '#E2E8F0',
  muted:   '#4A5568',
};

export default function HomeScreen() {
  const {
    role, status, sessionCode, connectedClients,
    networkType, errorMessage, bytesUp, bytesDown,
    connectionTime, setRole, setNetworkType,
    setConnecting, setConnected, setError, setIdle,
    addClient, removeClient, tickBandwidth,
  } = useStore();

  const [accessCode, setAccessCodeState] = useState('');
  const [log, setLog]                    = useState([]);
  const [duration, setDuration]          = useState('00:00');
  const [failoverMsg, setFailoverMsg]    = useState('');
  const [validating, setValidating]      = useState(false);

  const pulseAnim      = useRef(new Animated.Value(1)).current;
  const bandwidthTimer = useRef(null);
  const durationTimer  = useRef(null);

  // FIX 2: roleRef always reflects the current role without stale closure issues
  const roleRef = useRef(role);
  useEffect(() => { roleRef.current = role; }, [role]);

  // FIX 3: stable addLog that doesn't change between renders
  const addLog = useCallback((msg) => {
    const time = new Date().toLocaleTimeString();
    setLog(prev => [`[${time}] ${msg}`, ...prev].slice(0, 30));
  }, []);

  // FIX 8: handle access code input with auto-formatting
  const handleAccessCodeChange = useCallback((text) => {
    setAccessCodeState(formatAccessCode(text));
  }, []);

  // ── Pulse animation + bandwidth/duration timers ──────────────────────
  useEffect(() => {
    if (status === 'connected') {
      pulseAnim.stopAnimation();
      Animated.loop(
        Animated.sequence([
          Animated.timing(pulseAnim, { toValue: 1.15, duration: 800, useNativeDriver: true }),
          Animated.timing(pulseAnim, { toValue: 1,    duration: 800, useNativeDriver: true }),
        ])
      ).start();

      bandwidthTimer.current = setInterval(tickBandwidth, 500);

      // FIX 6: read connectionTime directly from store state inside the interval
      // so the closure is never stale.
      durationTimer.current = setInterval(() => {
        const ct = useStore.getState().connectionTime;
        setDuration(formatDuration(ct ? ct.getTime() : null));
      }, 1000);
    } else {
      pulseAnim.stopAnimation();
      pulseAnim.setValue(1);
      clearInterval(bandwidthTimer.current);
      clearInterval(durationTimer.current);
      setDuration('00:00');
    }
    // FIX 7: always clean up on unmount or status change
    return () => {
      clearInterval(bandwidthTimer.current);
      clearInterval(durationTimer.current);
    };
  }, [status]);

  // ── VPN Events ───────────────────────────────────────────────────────
  useEffect(() => {
    const unsubs = [

      VpnService.on('sessionCreated', (code) => {
        // HOST: relay confirmed session is live with this code
        addLog('✓ Session active — sharing is live');
        setConnected(code || 'host-session');
      }),

      // FIX 1+2: vpnConnected only logs; setConnected is triggered by
      // sessionCreated (host) or joinSuccess (client). Using roleRef avoids stale closure.
      VpnService.on('vpnConnected', (_payload) => {
        addLog('VPN tunnel established');
        // Do NOT call setConnected here for either role — wait for
        // sessionCreated (host) or joinSuccess (client) which carry the real code.
      }),

      VpnService.on('joinSuccess', (code) => {
        // CLIENT: relay confirmed we are connected through a host
        addLog('✓ Connected — traffic is routing through host');
        setConnected(typeof code === 'string' && code.length > 0 ? code : 'connected');
        setFailoverMsg('');
      }),

      VpnService.on('joinError', (reason) => {
        addLog(`✕ Join failed: ${reason}`);
        setError(reason);
        setTimeout(() => setIdle(), 3000);
      }),

      VpnService.on('clientConnected', (clientId) => {
        addClient();
        addLog(`Client connected${clientId ? ` (${clientId})` : ''}`);
      }),

      VpnService.on('clientDisconnected', (_payload) => {
        removeClient();
        addLog('Client disconnected');
      }),

      VpnService.on('hostLeft', () => {
        addLog('Host ended the session');
        setIdle();
      }),

      // hostFailover is a LOCAL JS event — fired by VpnService._handleFailover()
      VpnService.on('hostFailover', (_newCode) => {
        addLog('⟳ Auto-switched to a new host');
        setFailoverMsg('Switched to a faster host ⚡');
        setTimeout(() => setFailoverMsg(''), 4000);
      }),

      VpnService.on('vpnDisconnected', (reason) => {
        addLog(`Disconnected: ${reason || 'connection closed'}`);
        setIdle();
      }),

      VpnService.on('vpnError', (msg) => {
        addLog(`Error: ${msg}`);
        setError(msg);
        setTimeout(() => setIdle(), 3000);
      }),
    ];

    return () => {
      unsubs.forEach(unsub => { if (typeof unsub === 'function') unsub(); });
    };
  }, [addLog, addClient, removeClient, setConnected, setError, setIdle]);

  // ── Start HOST ───────────────────────────────────────────────────────
  const handleStartHost = async () => {
    try {
      setConnecting();
      addLog('Starting host mode...');
      await VpnService.startAsHost(networkType);
      addLog('Connecting to relay...');
    } catch (e) {
      addLog(`✕ ${e.message}`);
      setError(e.message);
      Alert.alert('Error', e.message);
      // FIX 5: let user retry after a short delay
      setTimeout(() => setIdle(), 2000);
    }
  };

  // ── Start CLIENT ─────────────────────────────────────────────────────
  const handleStartClient = async () => {
    // Strip dash for length check
    const code = accessCode.replace('-', '').trim();
    if (code.length < 8) {
      Alert.alert('Invalid Code', 'Enter the access code from the platform owner (format: XXXX-XXXX)');
      return;
    }
    try {
      setValidating(true);
      addLog('Validating access code...');
      setConnecting();
      await VpnService.startAsClient(accessCode);
      addLog('Connecting to best available host...');
    } catch (e) {
      addLog(`✕ ${e.message}`);
      setError(e.message);
      Alert.alert('Access Denied', e.message);
      // FIX 5: reset so user can try again
      setTimeout(() => setIdle(), 2000);
    } finally {
      setValidating(false);
    }
  };

  // ── Stop ─────────────────────────────────────────────────────────────
  // FIX 4: try/finally — setIdle() always runs even if stopVpn rejects.
  const handleStop = async () => {
    try {
      await VpnService.stop();
    } catch (e) {
      console.warn('handleStop error:', e?.message);
    } finally {
      setIdle();
      addLog('Stopped');
    }
  };

  const isConnected  = status === 'connected';
  const isConnecting = status === 'connecting';
  const connectionTimeMs = connectionTime ? connectionTime.getTime() : null;

  return (
    <SafeAreaView style={s.safe}>
      <StatusBar barStyle="light-content" backgroundColor={T.bg}/>
      <ScrollView style={s.scroll} contentContainerStyle={s.content}>

        {/* Header */}
        <View style={s.header}>
          <Text style={s.title}>NETSHARE</Text>
          <Text style={s.subtitle}>PREMIUM NETWORK SHARING</Text>
          <View style={s.badge}>
            <Text style={s.badgeText}>BUSINESS EDITION · v2.0</Text>
          </View>
        </View>

        {/* Role Tabs */}
        <View style={s.tabs}>
          {[
            { id: 'host',   label: 'HOST',   sub: 'Share & Earn' },
            { id: 'client', label: 'CLIENT', sub: 'Access Network' },
          ].map(r => (
            <TouchableOpacity
              key={r.id}
              style={[s.tab, role === r.id && s.tabActive]}
              onPress={() => { if (!isConnected && !isConnecting) setRole(r.id); }}
              disabled={isConnected || isConnecting}>
              <Text style={[s.tabText, role === r.id && s.tabTextActive]}>{r.label}</Text>
              <Text style={[s.tabSub,  role === r.id && { color: T.accent }]}>{r.sub}</Text>
            </TouchableOpacity>
          ))}
        </View>

        {/* Network Type (host only) */}
        {role === 'host' && (
          <View style={s.networkRow}>
            {['WiFi', '4G LTE', '5G'].map(n => (
              <TouchableOpacity
                key={n}
                style={[s.netBtn, networkType === n && s.netBtnActive]}
                onPress={() => setNetworkType(n)}
                disabled={isConnected}>
                <Text style={[s.netBtnText, networkType === n && s.netBtnTextActive]}>{n}</Text>
              </TouchableOpacity>
            ))}
          </View>
        )}

        {/* Failover Banner */}
        {!!failoverMsg && (
          <View style={s.failoverBanner}>
            <Text style={s.failoverText}>⚡ {failoverMsg}</Text>
          </View>
        )}

        {/* Main Card */}
        <View style={s.card}>

          {/* Status indicator */}
          <View style={s.statusRow}>
            <Animated.View style={[
              s.statusDot,
              { backgroundColor: isConnected ? T.green : isConnecting ? T.accent : T.muted },
              isConnected && { transform: [{ scale: pulseAnim }] },
            ]}/>
            <Text style={[s.statusText, {
              color: isConnected ? T.green : isConnecting ? T.accent : T.muted
            }]}>
              {isConnected
                ? 'CONNECTED'
                : isConnecting
                  ? (validating ? 'VALIDATING...' : 'CONNECTING...')
                  : 'OFFLINE'}
            </Text>
          </View>

          {/* HOST connected: earnings */}
          {isConnected && role === 'host' && (
            <View style={s.earningsBox}>
              <Text style={s.earningsLabel}>ESTIMATED THIS SESSION</Text>
              <Text style={s.earningsValue}>
                ${estimateHostEarnings(connectionTimeMs, connectedClients)}
              </Text>
              <Text style={s.earningsSub}>Based on active clients · Paid weekly · 50% revenue share</Text>
            </View>
          )}

          {/* CLIENT idle: access code input */}
          {role === 'client' && !isConnected && !isConnecting && (
            <View style={s.accessCodeSection}>
              <Text style={s.inputLabel}>ACCESS CODE</Text>
              <Text style={s.inputHint}>Enter the code provided by the platform owner</Text>
              <TextInput
                style={s.accessInput}
                value={accessCode}
                onChangeText={handleAccessCodeChange}
                placeholder="XXXX-XXXX"
                placeholderTextColor={T.muted}
                autoCapitalize="characters"
                maxLength={9}
              />
            </View>
          )}

          {/* CLIENT connected: status box */}
          {isConnected && role === 'client' && (
            <View style={s.codeBox}>
              <Text style={s.codeLabel}>NETWORK STATUS</Text>
              <Text style={[s.codeText, { color: T.green, fontSize: 22, letterSpacing: 2 }]}>
                SECURED ✓
              </Text>
              <Text style={s.codeSub}>Traffic routed through host network</Text>
            </View>
          )}

          {/* Stats (both roles when connected) */}
          {isConnected && (
            <View style={s.statsRow}>
              <View style={s.stat}>
                <Text style={s.statLabel}>DURATION</Text>
                <Text style={s.statValue}>{duration}</Text>
              </View>
              {role === 'host' && (
                <View style={s.stat}>
                  <Text style={s.statLabel}>CLIENTS</Text>
                  <Text style={s.statValue}>{connectedClients}</Text>
                </View>
              )}
              <View style={s.stat}>
                <Text style={s.statLabel}>↑ UP</Text>
                <Text style={s.statValue}>{formatBytes(bytesUp)}</Text>
              </View>
              <View style={s.stat}>
                <Text style={s.statLabel}>↓ DOWN</Text>
                <Text style={s.statValue}>{formatBytes(bytesDown)}</Text>
              </View>
            </View>
          )}

          {/* Action button */}
          {!isConnected ? (
            <TouchableOpacity
              style={[s.btn, (isConnecting || validating) && s.btnDisabled]}
              onPress={role === 'host' ? handleStartHost : handleStartClient}
              disabled={isConnecting || validating || !role}>
              <LinearGradient
                colors={(isConnecting || validating) ? [T.muted, T.muted] : [T.accent, T.accent2]}
                start={{ x: 0, y: 0 }} end={{ x: 1, y: 0 }}
                style={s.btnGrad}>
                <Text style={s.btnText}>
                  {validating    ? 'VALIDATING...'  :
                   isConnecting  ? 'CONNECTING...'  :
                   role === 'host' ? 'START HOSTING' : 'CONNECT'}
                </Text>
              </LinearGradient>
            </TouchableOpacity>
          ) : (
            <TouchableOpacity style={s.btnStop} onPress={handleStop}>
              <Text style={s.btnStopText}>DISCONNECT</Text>
            </TouchableOpacity>
          )}

          {status === 'error' && !!errorMessage && (
            <Text style={s.error}>{errorMessage}</Text>
          )}
        </View>

        {/* Info cards */}
        {role === 'client' && !isConnected && (
          <View style={s.infoCard}>
            <Text style={s.infoTitle}>HOW TO ACCESS</Text>
            <Text style={s.infoLine}>1. Get an access code from the platform owner</Text>
            <Text style={s.infoLine}>2. Enter it above and tap CONNECT</Text>
            <Text style={s.infoLine}>3. Your internet is automatically routed</Text>
            <Text style={s.infoLine}>4. If a host goes down, you switch automatically</Text>
          </View>
        )}

        {role === 'host' && !isConnected && (
          <View style={s.infoCard}>
            <Text style={s.infoTitle}>HOST & EARN</Text>
            <Text style={s.infoLine}>• Share your WiFi / mobile data connection</Text>
            <Text style={s.infoLine}>• Earn 50% of platform revenue from your uptime</Text>
            <Text style={s.infoLine}>• Paid weekly by the platform owner</Text>
            <Text style={s.infoLine}>• Clients auto-failover if your connection drops</Text>
          </View>
        )}

        {/* Event Log */}
        <View style={s.logCard}>
          <Text style={s.logTitle}>EVENT LOG</Text>
          {log.length === 0
            ? <Text style={s.logEmpty}>Waiting for events...</Text>
            : log.map((l, i) => <Text key={i} style={s.logLine}>{l}</Text>)
          }
        </View>

      </ScrollView>
    </SafeAreaView>
  );
}

const s = StyleSheet.create({
  safe:    { flex: 1, backgroundColor: T.bg },
  scroll:  { flex: 1 },
  content: { padding: 16, paddingBottom: 40 },

  header:   { alignItems: 'center', marginBottom: 24, paddingTop: 8 },
  title:    { fontSize: 30, fontWeight: '900', color: T.accent, letterSpacing: 8 },
  subtitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginTop: 2 },
  badge:    { marginTop: 8, paddingHorizontal: 12, paddingVertical: 4,
    borderWidth: 1, borderColor: '#7B61FF44', borderRadius: 4 },
  badgeText: { fontSize: 10, color: T.accent2, letterSpacing: 2 },

  tabs:        { flexDirection: 'row', marginBottom: 16, gap: 8 },
  tab:         { flex: 1, paddingVertical: 14, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border,
    alignItems: 'center' },
  tabActive:   { backgroundColor: '#00D4FF16', borderColor: T.accent },
  tabText:     { fontSize: 14, color: T.muted, fontWeight: '900', letterSpacing: 2 },
  tabTextActive: { color: T.accent },
  tabSub:      { fontSize: 10, color: T.muted, marginTop: 2 },

  networkRow:       { flexDirection: 'row', gap: 8, marginBottom: 16 },
  netBtn:           { flex: 1, paddingVertical: 10, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border, alignItems: 'center' },
  netBtnActive:     { backgroundColor: '#00FF8816', borderColor: T.green },
  netBtnText:       { fontSize: 11, color: T.muted, fontWeight: '700' },
  netBtnTextActive: { color: T.green },

  failoverBanner: { backgroundColor: '#FFB40020', borderWidth: 1, borderColor: '#FFB40060',
    borderRadius: 8, padding: 10, marginBottom: 12, alignItems: 'center' },
  failoverText: { color: T.amber, fontSize: 13, fontWeight: '700' },

  card: { backgroundColor: T.card, borderRadius: 16, padding: 20,
    borderWidth: 1, borderColor: T.border, marginBottom: 16 },

  statusRow: { flexDirection: 'row', alignItems: 'center', marginBottom: 20 },
  statusDot: { width: 10, height: 10, borderRadius: 5, marginRight: 8 },
  statusText: { fontSize: 13, fontWeight: '800', letterSpacing: 3 },

  codeBox:   { backgroundColor: '#00D4FF0E', borderRadius: 12,
    borderWidth: 1, borderColor: '#00D4FF44', padding: 16,
    alignItems: 'center', marginBottom: 16 },
  codeLabel: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 4 },
  codeText:  { fontSize: 32, fontWeight: '900', color: T.accent, letterSpacing: 6 },
  codeSub:   { fontSize: 10, color: T.muted, marginTop: 4, textAlign: 'center' },

  earningsBox:   { backgroundColor: '#00FF8810', borderRadius: 10,
    borderWidth: 1, borderColor: '#00FF8840', padding: 14,
    alignItems: 'center', marginBottom: 16 },
  earningsLabel: { fontSize: 10, color: T.muted, letterSpacing: 2 },
  earningsValue: { fontSize: 28, fontWeight: '900', color: T.green, marginVertical: 4 },
  earningsSub:   { fontSize: 10, color: T.muted, textAlign: 'center' },

  accessCodeSection: { marginBottom: 20 },
  inputLabel:        { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 4 },
  inputHint:         { fontSize: 11, color: T.muted, marginBottom: 10 },
  accessInput:       { backgroundColor: '#1A2535', borderWidth: 1, borderColor: T.border,
    borderRadius: 10, padding: 14, color: T.accent, fontSize: 22,
    fontWeight: '900', letterSpacing: 6, textAlign: 'center' },

  statsRow:  { flexDirection: 'row', flexWrap: 'wrap', gap: 12, marginBottom: 20 },
  stat:      { flex: 1, minWidth: 70, alignItems: 'center' },
  statLabel: { fontSize: 9, color: T.muted, letterSpacing: 2, marginBottom: 2 },
  statValue: { fontSize: 15, fontWeight: '800', color: T.text },

  btn:        { borderRadius: 12, overflow: 'hidden' },
  btnDisabled:{ opacity: 0.5 },
  btnGrad:    { paddingVertical: 16, alignItems: 'center' },
  btnText:    { fontSize: 15, fontWeight: '900', color: '#000', letterSpacing: 2 },

  btnStop:     { backgroundColor: '#FF446616', borderWidth: 1, borderColor: T.red,
    borderRadius: 12, paddingVertical: 16, alignItems: 'center' },
  btnStopText: { fontSize: 15, fontWeight: '900', color: T.red, letterSpacing: 2 },

  error: { color: T.red, fontSize: 12, textAlign: 'center', marginTop: 12 },

  infoCard:  { backgroundColor: T.card, borderRadius: 12, padding: 16,
    borderWidth: 1, borderColor: T.border, marginBottom: 16 },
  infoTitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 12 },
  infoLine:  { fontSize: 12, color: T.muted, marginBottom: 6, lineHeight: 18 },

  logCard:  { backgroundColor: T.card, borderRadius: 12, padding: 16,
    borderWidth: 1, borderColor: T.border },
  logTitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 10 },
  logEmpty: { fontSize: 12, color: T.muted, fontStyle: 'italic' },
  logLine:  { fontSize: 11, color: '#64748B', marginBottom: 3, fontFamily: 'monospace' },
});
