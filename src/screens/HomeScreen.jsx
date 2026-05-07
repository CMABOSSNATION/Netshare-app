/**
 * HomeScreen.jsx
 * Main screen — matches your existing NetShare UI but now hooks into real VPN.
 */

import React, { useEffect, useRef, useState } from 'react';
import {
  View, Text, TouchableOpacity, StyleSheet, Animated,
  Alert, Platform, ScrollView, StatusBar,
} from 'react-native';
import Clipboard from '@react-native-clipboard/clipboard';
import { SafeAreaView } from 'react-native-safe-area-context';
import LinearGradient from 'react-native-linear-gradient';
import { useStore } from '../store';
import VpnService from '../services/VpnService';

// ── Helpers ──────────────────────────────────────────────────────
const formatBytes = (bytes) => {
  if (bytes < 1024) return `${bytes.toFixed(0)} B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1048576).toFixed(2)} MB`;
};

const formatDuration = (start) => {
  if (!start) return '00:00';
  const secs = Math.floor((Date.now() - start) / 1000);
  const m = String(Math.floor(secs / 60)).padStart(2, '0');
  const s = String(secs % 60).padStart(2, '0');
  return `${m}:${s}`;
};

// ── Colors ───────────────────────────────────────────────────────
const T = {
  bg: '#0A0E1A',
  card: '#111827',
  border: '#1E2D40',
  accent: '#00D4FF',
  accent2: '#7B61FF',
  green: '#00FF88',
  red: '#FF4466',
  text: '#E2E8F0',
  muted: '#4A5568',
};

export default function HomeScreen() {
  const { role, status, sessionCode, connectedClients,
          networkType, errorMessage, bytesUp, bytesDown,
          connectionTime, setRole, setNetworkType,
          setConnecting, setConnected, setError, setIdle,
          addClient, removeClient, tickBandwidth } = useStore();

  const [clientCode, setClientCode] = useState('');
  const [log, setLog] = useState([]);
  const [duration, setDuration] = useState('00:00');
  const pulseAnim = useRef(new Animated.Value(1)).current;
  const bandwidthTimer = useRef(null);
  const durationTimer = useRef(null);

  // ── Pulse animation when connected ──────────────────────────
  useEffect(() => {
    if (status === 'connected') {
      Animated.loop(
        Animated.sequence([
          Animated.timing(pulseAnim, { toValue: 1.15, duration: 800, useNativeDriver: true }),
          Animated.timing(pulseAnim, { toValue: 1, duration: 800, useNativeDriver: true }),
        ])
      ).start();

      bandwidthTimer.current = setInterval(tickBandwidth, 500);
      durationTimer.current = setInterval(
        () => setDuration(formatDuration(connectionTime)), 1000
      );
    } else {
      pulseAnim.stopAnimation();
      pulseAnim.setValue(1);
      clearInterval(bandwidthTimer.current);
      clearInterval(durationTimer.current);
    }
    return () => {
      clearInterval(bandwidthTimer.current);
      clearInterval(durationTimer.current);
    };
  }, [status]);

  // ── VPN event listeners ──────────────────────────────────────
  useEffect(() => {
    const unsubs = [
      VpnService.on('sessionCreated', (code) => {
        addLog(`Session created: ${code}`);
        setConnected(code);
      }),
      VpnService.on('vpnConnected', (code) => {
        addLog('VPN tunnel established');
        if (role === 'client') setConnected(code);
      }),
      VpnService.on('joinSuccess', (code) => {
        addLog(`Joined session ${code} — routing traffic through host`);
        setConnected(code);
      }),
      VpnService.on('joinError', (reason) => {
        addLog(`Join failed: ${reason}`);
        setError(reason);
      }),
      VpnService.on('clientConnected', (id) => {
        addLog(`Client connected: ${id}`);
        addClient();
      }),
      VpnService.on('clientDisconnected', () => {
        addLog('A client disconnected');
        removeClient();
      }),
      VpnService.on('hostLeft', (msg) => {
        addLog('Host ended the session');
        setIdle();
      }),
      VpnService.on('vpnDisconnected', (reason) => {
        addLog(`Disconnected: ${reason}`);
        setIdle();
      }),
      VpnService.on('vpnError', (msg) => {
        addLog(`Error: ${msg}`);
        setError(msg);
      }),
    ];
    return () => VpnService.removeAllListeners();
  }, [role]);

  const addLog = (msg) => {
    const time = new Date().toLocaleTimeString();
    setLog(prev => [`[${time}] ${msg}`, ...prev].slice(0, 30));
  };

  // ── Start HOST ───────────────────────────────────────────────
  const handleStartHost = async () => {
    try {
      setConnecting();
      addLog('Requesting VPN permission...');
      await VpnService.startAsHost();
      addLog('Connecting to relay server...');
    } catch (e) {
      setError(e.message);
      addLog(`Error: ${e.message}`);
      Alert.alert('Error', e.message);
    }
  };

  // ── Start CLIENT ─────────────────────────────────────────────
  const handleStartClient = async () => {
    if (clientCode.length !== 6) {
      Alert.alert('Invalid Code', 'Enter the 6-character session code from the host.');
      return;
    }
    try {
      setConnecting();
      addLog(`Joining session ${clientCode}...`);
      await VpnService.startAsClient(clientCode);
      addLog('Connecting to relay server...');
    } catch (e) {
      setError(e.message);
      addLog(`Error: ${e.message}`);
      Alert.alert('Error', e.message);
    }
  };

  // ── Stop ─────────────────────────────────────────────────────
  const handleStop = async () => {
    await VpnService.stop();
    setIdle();
    addLog('Stopped sharing');
  };

  const copyCode = () => {
    if (sessionCode) {
      Clipboard.setString(sessionCode);
      Alert.alert('Copied!', 'Session code copied to clipboard');
    }
  };

  const isConnected = status === 'connected';
  const isConnecting = status === 'connecting';

  return (
    <SafeAreaView style={s.safe}>
      <StatusBar barStyle="light-content" backgroundColor={T.bg} />
      <ScrollView style={s.scroll} contentContainerStyle={s.content}>

        {/* ── Header ── */}
        <View style={s.header}>
          <Text style={s.title}>NETSHARE</Text>
          <Text style={s.subtitle}>REAL-TIME RELAY PROTOCOL</Text>
          <View style={s.badge}>
            <Text style={s.badgeText}>v2.0.0 · NATIVE VPN</Text>
          </View>
        </View>

        {/* ── Role Tabs ── */}
        <View style={s.tabs}>
          {['host', 'client'].map(r => (
            <TouchableOpacity
              key={r}
              style={[s.tab, role === r && s.tabActive]}
              onPress={() => { if (!isConnected && !isConnecting) setRole(r); }}
              disabled={isConnected || isConnecting}>
              <Text style={[s.tabText, role === r && s.tabTextActive]}>
                {r === 'host' ? 'HOST · Share WiFi' : 'CLIENT · Join Network'}
              </Text>
            </TouchableOpacity>
          ))}
        </View>

        {/* ── Network Type (host only) ── */}
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

        {/* ── Main Card ── */}
        <View style={s.card}>

          {/* Status indicator */}
          <View style={s.statusRow}>
            <Animated.View style={[
              s.statusDot,
              { backgroundColor: isConnected ? T.green : isConnecting ? T.accent : T.muted },
              isConnected && { transform: [{ scale: pulseAnim }] }
            ]} />
            <Text style={[s.statusText, {
              color: isConnected ? T.green : isConnecting ? T.accent : T.muted
            }]}>
              {isConnected ? 'CONNECTED' : isConnecting ? 'CONNECTING...' : 'OFFLINE'}
            </Text>
          </View>

          {/* Session code display */}
          {isConnected && sessionCode && (
            <TouchableOpacity style={s.codeBox} onPress={copyCode}>
              <Text style={s.codeLabel}>SESSION CODE</Text>
              <Text style={s.codeText}>{sessionCode}</Text>
              <Text style={s.codeTap}>Tap to copy</Text>
            </TouchableOpacity>
          )}

          {/* Stats when connected */}
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

          {/* Client code input */}
          {role === 'client' && !isConnected && !isConnecting && (
            <View style={s.codeInput}>
              <Text style={s.inputLabel}>ENTER HOST CODE</Text>
              <View style={s.codeBoxes}>
                {Array(6).fill(0).map((_, i) => (
                  <View key={i} style={[s.codeCell, clientCode[i] && s.codeCellFilled]}>
                    <Text style={s.codeCellText}>{clientCode[i] || '·'}</Text>
                  </View>
                ))}
              </View>
              {/* Number pad */}
              <View style={s.numpad}>
                {'123456789 0⌫'.split('').filter(c => c !== ' ').concat(['']).map((c, i) => (
                  c !== '' ? (
                    <TouchableOpacity key={i} style={s.numKey} onPress={() => {
                      if (c === '⌫') setClientCode(prev => prev.slice(0, -1));
                      else if (clientCode.length < 6) setClientCode(prev => prev + c);
                    }}>
                      <Text style={s.numKeyText}>{c}</Text>
                    </TouchableOpacity>
                  ) : <View key={i} style={s.numKey} />
                ))}
              </View>
            </View>
          )}

          {/* Action button */}
          {!isConnected ? (
            <TouchableOpacity
              style={[s.btn, isConnecting && s.btnDisabled]}
              onPress={role === 'host' ? handleStartHost : handleStartClient}
              disabled={isConnecting || !role}>
              <LinearGradient
                colors={isConnecting ? [T.muted, T.muted] : [T.accent, T.accent2]}
                start={{ x: 0, y: 0 }} end={{ x: 1, y: 0 }}
                style={s.btnGrad}>
                <Text style={s.btnText}>
                  {isConnecting ? 'CONNECTING...' : role === 'host' ? 'START SHARING' : 'JOIN NETWORK'}
                </Text>
              </LinearGradient>
            </TouchableOpacity>
          ) : (
            <TouchableOpacity style={s.btnStop} onPress={handleStop}>
              <Text style={s.btnStopText}>STOP</Text>
            </TouchableOpacity>
          )}

          {/* Error message */}
          {status === 'error' && errorMessage && (
            <Text style={s.error}>{errorMessage}</Text>
          )}
        </View>

        {/* ── Event Log ── */}
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
  safe: { flex: 1, backgroundColor: T.bg },
  scroll: { flex: 1 },
  content: { padding: 16, paddingBottom: 40 },

  header: { alignItems: 'center', marginBottom: 24, paddingTop: 8 },
  title: { fontSize: 32, fontWeight: '900', color: T.accent, letterSpacing: 8 },
  subtitle: { fontSize: 11, color: T.muted, letterSpacing: 4, marginTop: 2 },
  badge: { marginTop: 8, paddingHorizontal: 12, paddingVertical: 4,
    borderWidth: 1, borderColor: '#7B61FF44', borderRadius: 4 },
  badgeText: { fontSize: 10, color: T.accent2, letterSpacing: 2 },

  tabs: { flexDirection: 'row', marginBottom: 16, gap: 8 },
  tab: { flex: 1, paddingVertical: 12, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border,
    alignItems: 'center' },
  tabActive: { backgroundColor: '#00D4FF22', borderColor: T.accent },
  tabText: { fontSize: 12, color: T.muted, fontWeight: '700', letterSpacing: 1 },
  tabTextActive: { color: T.accent },

  networkRow: { flexDirection: 'row', gap: 8, marginBottom: 16 },
  netBtn: { flex: 1, paddingVertical: 10, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border,
    alignItems: 'center' },
  netBtnActive: { backgroundColor: '#00FF8822', borderColor: T.green },
  netBtnText: { fontSize: 11, color: T.muted, fontWeight: '700' },
  netBtnTextActive: { color: T.green },

  card: { backgroundColor: T.card, borderRadius: 16, padding: 20,
    borderWidth: 1, borderColor: T.border, marginBottom: 16 },

  statusRow: { flexDirection: 'row', alignItems: 'center', marginBottom: 20 },
  statusDot: { width: 10, height: 10, borderRadius: 5, marginRight: 8 },
  statusText: { fontSize: 13, fontWeight: '800', letterSpacing: 3 },

  codeBox: { backgroundColor: '#00D4FF11', borderRadius: 12,
    borderWidth: 1, borderColor: '#00D4FF44', padding: 16,
    alignItems: 'center', marginBottom: 16 },
  codeLabel: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 4 },
  codeText: { fontSize: 36, fontWeight: '900', color: T.accent, letterSpacing: 8 },
  codeTap: { fontSize: 10, color: T.muted, marginTop: 4 },

  statsRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 12, marginBottom: 20 },
  stat: { flex: 1, minWidth: 70, alignItems: 'center' },
  statLabel: { fontSize: 9, color: T.muted, letterSpacing: 2, marginBottom: 2 },
  statValue: { fontSize: 15, fontWeight: '800', color: T.text },

  codeInput: { marginBottom: 20 },
  inputLabel: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 12, textAlign: 'center' },
  codeBoxes: { flexDirection: 'row', justifyContent: 'center', gap: 8, marginBottom: 16 },
  codeCell: { width: 44, height: 52, borderRadius: 8, backgroundColor: '#1A2535',
    borderWidth: 1, borderColor: T.border, alignItems: 'center', justifyContent: 'center' },
  codeCellFilled: { borderColor: T.accent, backgroundColor: '#00D4FF11' },
  codeCellText: { fontSize: 22, fontWeight: '900', color: T.text },
  numpad: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, justifyContent: 'center' },
  numKey: { width: 72, height: 48, borderRadius: 8, backgroundColor: '#1A2535',
    alignItems: 'center', justifyContent: 'center' },
  numKeyText: { fontSize: 20, fontWeight: '700', color: T.text },

  btn: { borderRadius: 12, overflow: 'hidden' },
  btnDisabled: { opacity: 0.5 },
  btnGrad: { paddingVertical: 16, alignItems: 'center' },
  btnText: { fontSize: 15, fontWeight: '900', color: '#000', letterSpacing: 2 },

  btnStop: { backgroundColor: '#FF446622', borderWidth: 1, borderColor: T.red,
    borderRadius: 12, paddingVertical: 16, alignItems: 'center' },
  btnStopText: { fontSize: 15, fontWeight: '900', color: T.red, letterSpacing: 2 },

  error: { color: T.red, fontSize: 12, textAlign: 'center', marginTop: 12 },

  logCard: { backgroundColor: T.card, borderRadius: 12, padding: 16,
    borderWidth: 1, borderColor: T.border },
  logTitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 10 },
  logEmpty: { fontSize: 12, color: T.muted, fontStyle: 'italic' },
  logLine: { fontSize: 11, color: '#64748B', marginBottom: 3, fontFamily: 'monospace' },
});
