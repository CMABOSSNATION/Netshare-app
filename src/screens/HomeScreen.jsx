/**
 * HomeScreen.jsx — NetShare Business Edition
 *
 * Changes:
 *  - Clients enter admin-generated ACCESS CODE (not session code)
 *  - Hosts see earnings estimate & uptime tracker
 *  - Auto-failover notification ("Reconnecting to new host...")
 *  - Role-appropriate UI: HOST = share mode, CLIENT = access mode
 */

import React, { useEffect, useRef, useState } from 'react';
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

const formatDuration = (start) => {
  if (!start) return '00:00';
  const secs = Math.floor((Date.now() - start) / 1000);
  const h = Math.floor(secs / 3600);
  const m = String(Math.floor((secs % 3600) / 60)).padStart(2, '0');
  const s = String(secs % 60).padStart(2, '0');
  return h > 0 ? `${h}:${m}:${s}` : `${m}:${s}`;
};

// Estimated host earnings — $0.50 per client-hour (50% of $1/hr per client)
const estimateHostEarnings = (startMs, clientCount = 0) => {
  if (!startMs) return '0.00';
  const hrs = (Date.now() - startMs) / 3_600_000;
  return (hrs * clientCount * 0.50).toFixed(2);
};

const T = {
  bg: '#0A0E1A',
  card: '#111827',
  border: '#1E2D40',
  accent: '#00D4FF',
  accent2: '#7B61FF',
  green: '#00FF88',
  red: '#FF4466',
  amber: '#FFB400',
  text: '#E2E8F0',
  muted: '#4A5568',
};

export default function HomeScreen() {
  const {
    role, status, sessionCode, connectedClients,
    networkType, errorMessage, bytesUp, bytesDown,
    connectionTime, setRole, setNetworkType,
    setConnecting, setConnected, setError, setIdle,
    addClient, removeClient, tickBandwidth,
  } = useStore();

  const [accessCode, setAccessCode] = useState('');
  const [log, setLog] = useState([]);
  const [duration, setDuration] = useState('00:00');
  const [failoverMsg, setFailoverMsg] = useState('');
  const [validating, setValidating] = useState(false);

  const pulseAnim = useRef(new Animated.Value(1)).current;
  const bandwidthTimer = useRef(null);
  const durationTimer = useRef(null);

  // ── Pulse on connected ──────────────────────────────────────────
  useEffect(() => {
    if (status === 'connected') {
      Animated.loop(
        Animated.sequence([
          Animated.timing(pulseAnim, { toValue: 1.15, duration: 800, useNativeDriver: true }),
          Animated.timing(pulseAnim, { toValue: 1, duration: 800, useNativeDriver: true }),
        ])
      ).start();
      bandwidthTimer.current = setInterval(tickBandwidth, 500);
      durationTimer.current = setInterval(() => setDuration(formatDuration(connectionTime)), 1000);
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

  // ── VPN Events ──────────────────────────────────────────────────
  useEffect(() => {
    const unsubs = [
      VpnService.on('sessionCreated', (code) => {
        addLog(`✓ Session ${code} active — clients can connect`);
        setConnected(code);
      }),
      VpnService.on('vpnConnected', () => {
        addLog('VPN tunnel established');
        if (role === 'client') setConnected(accessCode);
      }),
      VpnService.on('joinSuccess', ({ code, netType }) => {
        addLog(`✓ Connected via ${netType || 'WiFi'} network`);
        setConnected(code);
        setFailoverMsg('');
      }),
      VpnService.on('joinError', (reason) => {
        addLog(`✕ ${reason}`);
        setError(reason);
      }),
      VpnService.on('clientConnected', ({ totalClients }) => {
        addLog(`Client connected (${totalClients} total)`);
        addClient();
      }),
      VpnService.on('clientDisconnected', ({ totalClients }) => {
        addLog(`Client left (${totalClients} remain)`);
        removeClient();
      }),
      VpnService.on('hostLeft', () => {
        addLog('Host ended the session');
        setIdle();
      }),
      VpnService.on('hostFailover', (newCode) => {
        addLog('⟳ Reconnected to new host automatically');
        setFailoverMsg('Switched to a faster host ⚡');
        setTimeout(() => setFailoverMsg(''), 4000);
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

  // ── Start HOST ──────────────────────────────────────────────────
  const handleStartHost = async () => {
    try {
      setConnecting();
      addLog('Starting host mode...');
      await VpnService.startAsHost(networkType);
    } catch (e) {
      setError(e.message);
      addLog(`✕ ${e.message}`);
      Alert.alert('Error', e.message);
    }
  };

  // ── Start CLIENT ────────────────────────────────────────────────
  const handleStartClient = async () => {
    const code = accessCode.trim().toUpperCase();
    if (code.length < 8) {
      Alert.alert('Invalid Code', 'Enter the access code from the platform owner (format: XXXX-XXXX)');
      return;
    }
    try {
      setValidating(true);
      addLog(`Validating access code...`);
      setConnecting();
      await VpnService.startAsClient(code);
      addLog('Connecting to best available host...');
    } catch (e) {
      setError(e.message);
      addLog(`✕ ${e.message}`);
      Alert.alert('Access Denied', e.message);
    } finally {
      setValidating(false);
    }
  };

  const handleStop = async () => {
    await VpnService.stop();
    setIdle();
    addLog('Stopped');
  };

  const isConnected  = status === 'connected';
  const isConnecting = status === 'connecting';

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
            { id: 'host',   label: 'HOST', sub: 'Share & Earn' },
            { id: 'client', label: 'CLIENT', sub: 'Access Network' },
          ].map(r => (
            <TouchableOpacity
              key={r.id}
              style={[s.tab, role === r.id && s.tabActive]}
              onPress={() => { if (!isConnected && !isConnecting) setRole(r.id); }}
              disabled={isConnected || isConnecting}>
              <Text style={[s.tabText, role === r.id && s.tabTextActive]}>{r.label}</Text>
              <Text style={[s.tabSub, role === r.id && { color: T.accent }]}>{r.sub}</Text>
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
        {failoverMsg ? (
          <View style={s.failoverBanner}>
            <Text style={s.failoverText}>⚡ {failoverMsg}</Text>
          </View>
        ) : null}

        {/* Main Card */}
        <View style={s.card}>

          {/* Status */}
          <View style={s.statusRow}>
            <Animated.View style={[
              s.statusDot,
              { backgroundColor: isConnected ? T.green : isConnecting ? T.accent : T.muted },
              isConnected && { transform: [{ scale: pulseAnim }] },
            ]}/>
            <Text style={[s.statusText, {
              color: isConnected ? T.green : isConnecting ? T.accent : T.muted
            }]}>
              {isConnected ? 'CONNECTED' : isConnecting ? (validating ? 'VALIDATING...' : 'CONNECTING...') : 'OFFLINE'}
            </Text>
          </View>

          {/* Host: earnings only — no session code shown */}
          {isConnected && role === 'host' && (
            <View style={s.earningsBox}>
              <Text style={s.earningsLabel}>ESTIMATED THIS SESSION</Text>
              <Text style={s.earningsValue}>
                ${estimateHostEarnings(connectionTime?.getTime?.() || Date.now(), connectedClients)}
              </Text>
              <Text style={s.earningsSub}>Based on active clients · Paid weekly · 50% revenue share</Text>
            </View>
          )}

          {/* Client: access code input */}
          {role === 'client' && !isConnected && !isConnecting && (
            <View style={s.accessCodeSection}>
              <Text style={s.inputLabel}>ACCESS CODE</Text>
              <Text style={s.inputHint}>Enter the code provided by the platform owner</Text>
              <TextInput
                style={s.accessInput}
                value={accessCode}
                onChangeText={setAccessCode}
                placeholder="XXXX-XXXX"
                placeholderTextColor={T.muted}
                autoCapitalize="characters"
                maxLength={9}
              />
            </View>
          )}

          {/* Client: connected info */}
          {isConnected && role === 'client' && (
            <View style={s.codeBox}>
              <Text style={s.codeLabel}>NETWORK STATUS</Text>
              <Text style={[s.codeText, { color: T.green, fontSize: 22, letterSpacing: 2 }]}>
                SECURED ✓
              </Text>
              <Text style={s.codeSub}>Traffic routed through host network</Text>
            </View>
          )}

          {/* Stats */}
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

          {/* Action Button */}
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
                  {validating ? 'VALIDATING...' :
                    isConnecting ? 'CONNECTING...' :
                    role === 'host' ? 'START HOSTING' : 'CONNECT'}
                </Text>
              </LinearGradient>
            </TouchableOpacity>
          ) : (
            <TouchableOpacity style={s.btnStop} onPress={handleStop}>
              <Text style={s.btnStopText}>DISCONNECT</Text>
            </TouchableOpacity>
          )}

          {status === 'error' && errorMessage && (
            <Text style={s.error}>{errorMessage}</Text>
          )}
        </View>

        {/* How it works (client, idle) */}
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
  safe: { flex: 1, backgroundColor: T.bg },
  scroll: { flex: 1 },
  content: { padding: 16, paddingBottom: 40 },

  header: { alignItems: 'center', marginBottom: 24, paddingTop: 8 },
  title: { fontSize: 30, fontWeight: '900', color: T.accent, letterSpacing: 8 },
  subtitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginTop: 2 },
  badge: { marginTop: 8, paddingHorizontal: 12, paddingVertical: 4,
    borderWidth: 1, borderColor: '#7B61FF44', borderRadius: 4 },
  badgeText: { fontSize: 10, color: T.accent2, letterSpacing: 2 },

  tabs: { flexDirection: 'row', marginBottom: 16, gap: 8 },
  tab: { flex: 1, paddingVertical: 14, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border,
    alignItems: 'center' },
  tabActive: { backgroundColor: '#00D4FF16', borderColor: T.accent },
  tabText: { fontSize: 14, color: T.muted, fontWeight: '900', letterSpacing: 2 },
  tabTextActive: { color: T.accent },
  tabSub: { fontSize: 10, color: T.muted, marginTop: 2 },

  networkRow: { flexDirection: 'row', gap: 8, marginBottom: 16 },
  netBtn: { flex: 1, paddingVertical: 10, borderRadius: 8,
    backgroundColor: T.card, borderWidth: 1, borderColor: T.border, alignItems: 'center' },
  netBtnActive: { backgroundColor: '#00FF8816', borderColor: T.green },
  netBtnText: { fontSize: 11, color: T.muted, fontWeight: '700' },
  netBtnTextActive: { color: T.green },

  failoverBanner: { backgroundColor: '#FFB40020', borderWidth: 1, borderColor: '#FFB40060',
    borderRadius: 8, padding: 10, marginBottom: 12, alignItems: 'center' },
  failoverText: { color: T.amber, fontSize: 13, fontWeight: '700' },

  card: { backgroundColor: T.card, borderRadius: 16, padding: 20,
    borderWidth: 1, borderColor: T.border, marginBottom: 16 },

  statusRow: { flexDirection: 'row', alignItems: 'center', marginBottom: 20 },
  statusDot: { width: 10, height: 10, borderRadius: 5, marginRight: 8 },
  statusText: { fontSize: 13, fontWeight: '800', letterSpacing: 3 },

  codeBox: { backgroundColor: '#00D4FF0E', borderRadius: 12,
    borderWidth: 1, borderColor: '#00D4FF44', padding: 16,
    alignItems: 'center', marginBottom: 16 },
  codeLabel: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 4 },
  codeText: { fontSize: 32, fontWeight: '900', color: T.accent, letterSpacing: 6 },
  codeSub: { fontSize: 10, color: T.muted, marginTop: 4, textAlign: 'center' },

  earningsBox: { backgroundColor: '#00FF8810', borderRadius: 10,
    borderWidth: 1, borderColor: '#00FF8840', padding: 14,
    alignItems: 'center', marginBottom: 16 },
  earningsLabel: { fontSize: 10, color: T.muted, letterSpacing: 2 },
  earningsValue: { fontSize: 28, fontWeight: '900', color: T.green, marginVertical: 4 },
  earningsSub: { fontSize: 10, color: T.muted, textAlign: 'center' },

  accessCodeSection: { marginBottom: 20 },
  inputLabel: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 4 },
  inputHint: { fontSize: 11, color: T.muted, marginBottom: 10 },
  accessInput: { backgroundColor: '#1A2535', borderWidth: 1, borderColor: T.border,
    borderRadius: 10, padding: 14, color: T.accent, fontSize: 22,
    fontWeight: '900', letterSpacing: 6, textAlign: 'center' },

  statsRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 12, marginBottom: 20 },
  stat: { flex: 1, minWidth: 70, alignItems: 'center' },
  statLabel: { fontSize: 9, color: T.muted, letterSpacing: 2, marginBottom: 2 },
  statValue: { fontSize: 15, fontWeight: '800', color: T.text },

  btn: { borderRadius: 12, overflow: 'hidden' },
  btnDisabled: { opacity: 0.5 },
  btnGrad: { paddingVertical: 16, alignItems: 'center' },
  btnText: { fontSize: 15, fontWeight: '900', color: '#000', letterSpacing: 2 },

  btnStop: { backgroundColor: '#FF446616', borderWidth: 1, borderColor: T.red,
    borderRadius: 12, paddingVertical: 16, alignItems: 'center' },
  btnStopText: { fontSize: 15, fontWeight: '900', color: T.red, letterSpacing: 2 },

  error: { color: T.red, fontSize: 12, textAlign: 'center', marginTop: 12 },

  infoCard: { backgroundColor: T.card, borderRadius: 12, padding: 16,
    borderWidth: 1, borderColor: T.border, marginBottom: 16 },
  infoTitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 12 },
  infoLine: { fontSize: 12, color: T.muted, marginBottom: 6, lineHeight: 18 },

  logCard: { backgroundColor: T.card, borderRadius: 12, padding: 16,
    borderWidth: 1, borderColor: T.border },
  logTitle: { fontSize: 10, color: T.muted, letterSpacing: 3, marginBottom: 10 },
  logEmpty: { fontSize: 12, color: T.muted, fontStyle: 'italic' },
  logLine: { fontSize: 11, color: '#64748B', marginBottom: 3, fontFamily: 'monospace' },
});
