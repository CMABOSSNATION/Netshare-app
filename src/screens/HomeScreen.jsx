/**
 * HomeScreen.jsx — NetShare Global Tunnel Mode (Remote Only)
 *
 * HOST flow:
 *   1. Tap "Share My Internet"
 *   2. App starts proxy on :8899, opens WS tunnel to Cloudflare, gets a code
 *   3. Show session code — clients anywhere in the world can connect
 *
 * CLIENT flow:
 *   1. Tap "Connect to Host"
 *   2. Enter 4-char session code
 *   3. App activates VPN → ALL traffic (WiFi + mobile data) routes via host
 *   4. No manual setup needed — it just works
 *
 * LAN / same-WiFi mode has been removed entirely.
 * Manual proxy setup guide has been removed entirely.
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import {
  View, Text, TouchableOpacity, TextInput, StyleSheet,
  ScrollView, ActivityIndicator, Alert,
  StatusBar, SafeAreaView, Clipboard,
} from 'react-native';
import { useStore } from '../store';
import proxyService from '../services/ProxyService';

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatBytes(b) {
  if (!b || b < 1024)      return `${b || 0} B`;
  if (b < 1048576)         return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1073741824)      return `${(b / 1048576).toFixed(1)} MB`;
  return `${(b / 1073741824).toFixed(2)} GB`;
}

function formatDuration(ms) {
  if (!ms || ms < 0) return '0s';
  const s   = Math.floor(ms / 1000);
  const h   = Math.floor(s / 3600);
  const m   = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m ${sec}s`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function HomeScreen() {
  const {
    role, status, sessionCode, connectedClients,
    errorMessage, bytesUp, bytesDown,
    setRole, setConnecting, setConnected, setError, setIdle,
    tickBandwidth, addClient, removeClient, getSessionDurationMs,
  } = useStore();

  const [codeInput,   setCodeInput]   = useState('');
  const [proxyInfo,   setProxyInfo]   = useState(null);
  const [copied,      setCopied]      = useState(false);
  const [tunnelReady, setTunnelReady] = useState(false);
  const [vpnStatus,   setVpnStatus]   = useState('idle'); // 'idle'|'active'|'denied'|'error'|'revoked'

  const timerRef = useRef(null);
  const unsubs   = useRef([]);

  // ── Service event wiring ────────────────────────────────────────────────────

  useEffect(() => {
    const u1 = proxyService.on('status', ({ status: s, message }) => {
      if (s === 'connecting') setConnecting();
      else if (s === 'connected') setConnected(proxyService.getSessionCode());
      else if (s === 'error')    setError(message || 'Unknown error');
      else if (s === 'idle')     setIdle();
    });

    const u2 = proxyService.on('session', ({ code, ip, port }) => {
      setConnected(code);
      setProxyInfo({ ip, port });
    });

    const u3 = proxyService.on('proxy', ({ ip, port }) => {
      setProxyInfo({ ip, port });
    });

    const u4 = proxyService.on('stats', ({ bytesUp: up, bytesDown: down }) => {
      tickBandwidth({ up, down });
    });

    const u5 = proxyService.on('tunnel', ({ status: ts }) => {
      if (ts === 'ready')        setTunnelReady(true);
      if (ts === 'disconnected') setTunnelReady(false);
    });

    const u6 = proxyService.on('client', ({ event }) => {
      if (event === 'connected')    addClient();
      if (event === 'disconnected') removeClient();
    });

    const u7 = proxyService.on('vpn', ({ status: vs }) => {
      setVpnStatus(vs);
      if (vs === 'denied') {
        Alert.alert(
          'VPN Permission Denied',
          'Without VPN, your mobile data is NOT blocked. Other apps can still use your own data in the background.\n\nReconnect and allow VPN for full protection.',
          [{ text: 'OK' }]
        );
      }
      if (vs === 'revoked') {
        Alert.alert(
          'VPN Disconnected',
          'The VPN was turned off. Your own data can now be used by background apps.',
          [{ text: 'OK' }]
        );
      }
    });

    unsubs.current = [u1, u2, u3, u4, u5, u6, u7];
    return () => unsubs.current.forEach(u => u());
  }, []);

  // ── Duration ticker ─────────────────────────────────────────────────────────

  useEffect(() => {
    if (status === 'connected') {
      timerRef.current = setInterval(() => {}, 1000);
    } else {
      clearInterval(timerRef.current);
    }
    return () => clearInterval(timerRef.current);
  }, [status]);

  // ── Actions ─────────────────────────────────────────────────────────────────

  const handleHostStart = useCallback(async () => {
    setRole('host');
    try {
      await proxyService.startAsHost({ tunnelMode: true });
    } catch (err) {
      Alert.alert('Error', err.message);
    }
  }, []);

  const handleClientConnect = useCallback(async () => {
    const code = codeInput.trim().toUpperCase();
    if (code.length < 4) {
      Alert.alert('Enter session code', 'Ask the host for their 4-character code.');
      return;
    }
    setRole('client');
    try {
      const info = await proxyService.startAsClient(code);
      setProxyInfo(info);
    } catch (err) {
      Alert.alert('Connection failed', err.message);
    }
  }, [codeInput]);

  const handleStop = useCallback(async () => {
    await proxyService.stop();
    setProxyInfo(null);
    setTunnelReady(false);
    setVpnStatus('idle');
    setCodeInput('');
  }, []);

  const copyCode = useCallback(() => {
    const code = proxyService.getSessionCode();
    if (code) {
      Clipboard.setString(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, []);

  // ── Derived state ───────────────────────────────────────────────────────────

  const isConnected  = status === 'connected';
  const isConnecting = status === 'connecting';
  const isHost       = role === 'host';
  const isClient     = role === 'client';
  const hostCode     = proxyService.getSessionCode();
  const vpnBlocking  = vpnStatus === 'active';

  // ── VPN status banner ───────────────────────────────────────────────────────

  function renderVpnBanner() {
    if (vpnStatus === 'idle') return null;
    const configs = {
      active:  { bg: '#14532d', text: '🔒 VPN active — all traffic via host' },
      denied:  { bg: '#78350f', text: '⚠️ VPN denied — your data is NOT fully blocked' },
      error:   { bg: '#7f1d1d', text: '⚠️ VPN error — your data may still be used' },
      revoked: { bg: '#7f1d1d', text: '⚠️ VPN off — reconnect for full protection' },
    };
    const cfg = configs[vpnStatus] || configs.error;
    return (
      <View style={[s.vpnBanner, { backgroundColor: cfg.bg }]}>
        <Text style={s.vpnBannerText}>{cfg.text}</Text>
      </View>
    );
  }

  // ── Main render ─────────────────────────────────────────────────────────────

  return (
    <SafeAreaView style={s.safe}>
      <StatusBar barStyle="light-content" backgroundColor="#0f172a" />
      <ScrollView contentContainerStyle={s.scroll} keyboardShouldPersistTaps="handled">

        {/* Header */}
        <View style={s.header}>
          <Text style={s.headerTitle}>NetShare</Text>
          <Text style={s.headerSub}>🌐 Global Tunnel Mode</Text>
        </View>

        {/* Status badge */}
        {status !== 'idle' && (
          <View style={[s.badge,
            isConnecting && s.badgeConnecting,
            isConnected  && s.badgeConnected,
            status === 'error' && s.badgeError,
          ]}>
            {isConnecting && (
              <ActivityIndicator color="#fff" size="small" style={{ marginRight: 8 }} />
            )}
            <Text style={s.badgeText}>
              {isConnecting ? 'Connecting…'
              : isConnected  ? (isHost ? '🟢 Sharing internet' : '🟢 Connected to host')
              : `⚠ ${errorMessage || 'Error'}`}
            </Text>
          </View>
        )}

        {/* ── Idle screen ── */}
        {status === 'idle' && (
          <>
            {/* Host card */}
            <TouchableOpacity style={s.roleCard} onPress={handleHostStart}>
              <Text style={s.roleIcon}>📡</Text>
              <View style={s.roleText}>
                <Text style={s.roleTitle}>Share My Internet</Text>
                <Text style={s.roleDesc}>
                  Start a global tunnel. Anyone anywhere can use your internet with your session code.
                </Text>
              </View>
            </TouchableOpacity>

            <View style={s.divider}><Text style={s.dividerText}>or</Text></View>

            {/* Client box */}
            <View style={s.clientBox}>
              <Text style={s.sectionLabel}>Connect to a host</Text>
              <TextInput
                style={s.codeInput}
                placeholder="Enter session code (e.g. HQRQ)"
                placeholderTextColor="#64748b"
                value={codeInput}
                onChangeText={t => setCodeInput(t.toUpperCase())}
                autoCapitalize="characters"
                maxLength={8}
              />
              <TouchableOpacity style={s.connectBtn} onPress={handleClientConnect}>
                <Text style={s.connectBtnText}>Connect</Text>
              </TouchableOpacity>
              <Text style={s.vpnNote}>
                🔒 VPN activates automatically — all apps use host's internet, your data is blocked
              </Text>
            </View>
          </>
        )}

        {/* ── Connecting screen ── */}
        {isConnecting && (
          <View style={s.center}>
            <ActivityIndicator size="large" color="#38bdf8" />
            <Text style={s.centerText}>
              {isHost ? 'Starting tunnel…' : 'Connecting & activating VPN…'}
            </Text>
          </View>
        )}

        {/* ── Connected: HOST ── */}
        {isConnected && isHost && (
          <>
            {/* Session code */}
            <View style={s.codeBox}>
              <Text style={s.codeLabel}>Session Code</Text>
              <TouchableOpacity onPress={copyCode}>
                <Text style={s.code}>{hostCode}</Text>
                <Text style={s.codeSub}>{copied ? '✓ Copied!' : 'Tap to copy'}</Text>
              </TouchableOpacity>
            </View>

            {/* Tunnel status */}
            <View style={[s.tunnelStatus, tunnelReady ? s.tunnelOk : s.tunnelWaiting]}>
              <Text style={s.tunnelStatusText}>
                {tunnelReady
                  ? '🟢 Cloudflare tunnel active — global range'
                  : '⏳ Opening tunnel to Cloudflare…'}
              </Text>
            </View>

            {/* Tunnel proxy info */}
            {proxyInfo && (
              <View style={s.infoBox}>
                <Text style={s.infoLabel}>Tunnel Proxy</Text>
                <Text style={s.infoValue}>
                  Via Cloudflare DO →{'\n'}{proxyInfo.ip}:{proxyInfo.port}
                </Text>
                <Text style={s.infoDesc}>
                  Clients anywhere can connect using the session code above. Traffic is relayed through Cloudflare.
                </Text>
              </View>
            )}

            {/* Stats */}
            <View style={s.statsRow}>
              <View style={s.stat}>
                <Text style={s.statVal}>{formatBytes(bytesUp)}</Text>
                <Text style={s.statLabel}>↑ Served</Text>
              </View>
              <View style={s.stat}>
                <Text style={s.statVal}>{formatDuration(getSessionDurationMs())}</Text>
                <Text style={s.statLabel}>Duration</Text>
              </View>
              <View style={s.stat}>
                <Text style={s.statVal}>{connectedClients}</Text>
                <Text style={s.statLabel}>Clients</Text>
              </View>
            </View>

            <Text style={s.compatNote}>
              ✅ Works with TikTok · WhatsApp · Facebook · Instagram ·
              Spotify · YouTube · Google · Twitter
            </Text>

            <TouchableOpacity style={s.stopBtn} onPress={handleStop}>
              <Text style={s.stopBtnText}>Stop Sharing</Text>
            </TouchableOpacity>
          </>
        )}

        {/* ── Connected: CLIENT ── */}
        {isConnected && isClient && (
          <>
            {/* VPN banner — most important info for client */}
            {renderVpnBanner()}

            {/* Connection confirmed */}
            <View style={s.connectedBox}>
              <Text style={s.connectedTitle}>
                {tunnelReady ? '🟢 Connected to host' : '⏳ Waiting for host tunnel…'}
              </Text>
              <Text style={s.connectedDesc}>
                {vpnBlocking
                  ? 'All your apps are now using the host\'s internet. Your own mobile data is blocked.'
                  : 'Connected via tunnel. Enable VPN for full data blocking.'}
              </Text>
            </View>

            {/* Tunnel status */}
            <View style={[s.tunnelStatus, tunnelReady ? s.tunnelOk : s.tunnelWaiting]}>
              <Text style={s.tunnelStatusText}>
                {tunnelReady ? '🟢 Tunnel connected to host' : '⏳ Waiting for tunnel…'}
              </Text>
            </View>

            {/* Stats */}
            <View style={s.statsRow}>
              <View style={s.stat}>
                <Text style={s.statVal}>{formatBytes(bytesDown)}</Text>
                <Text style={s.statLabel}>↓ Received</Text>
              </View>
              <View style={s.stat}>
                <Text style={s.statVal}>{formatDuration(getSessionDurationMs())}</Text>
                <Text style={s.statLabel}>Duration</Text>
              </View>
            </View>

            <TouchableOpacity style={s.stopBtn} onPress={handleStop}>
              <Text style={s.stopBtnText}>Disconnect</Text>
            </TouchableOpacity>
          </>
        )}

        {/* ── Error screen ── */}
        {status === 'error' && (
          <View style={s.center}>
            <Text style={s.errorText}>{errorMessage}</Text>
            <TouchableOpacity style={s.retryBtn} onPress={handleStop}>
              <Text style={s.retryBtnText}>Try Again</Text>
            </TouchableOpacity>
          </View>
        )}

      </ScrollView>
    </SafeAreaView>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const s = StyleSheet.create({
  safe:             { flex: 1, backgroundColor: '#0f172a' },
  scroll:           { padding: 20, paddingBottom: 40 },

  header:           { alignItems: 'center', marginBottom: 28 },
  headerTitle:      { fontSize: 32, fontWeight: '800', color: '#f1f5f9', letterSpacing: 1 },
  headerSub:        { fontSize: 13, color: '#38bdf8', marginTop: 4 },

  badge:            { flexDirection: 'row', alignItems: 'center', justifyContent: 'center',
                      borderRadius: 24, paddingVertical: 10, paddingHorizontal: 18, marginBottom: 20 },
  badgeConnecting:  { backgroundColor: '#1e3a5f' },
  badgeConnected:   { backgroundColor: '#14532d' },
  badgeError:       { backgroundColor: '#7f1d1d' },
  badgeText:        { color: '#f1f5f9', fontWeight: '600', fontSize: 14 },

  sectionLabel:     { color: '#94a3b8', fontSize: 13, fontWeight: '600',
                      textTransform: 'uppercase', letterSpacing: 1, marginBottom: 12 },

  roleCard:         { flexDirection: 'row', backgroundColor: '#1e293b', borderRadius: 16,
                      padding: 18, marginBottom: 12, alignItems: 'center' },
  roleIcon:         { fontSize: 36, marginRight: 16 },
  roleText:         { flex: 1 },
  roleTitle:        { color: '#f1f5f9', fontSize: 17, fontWeight: '700', marginBottom: 4 },
  roleDesc:         { color: '#64748b', fontSize: 13, lineHeight: 19 },

  divider:          { alignItems: 'center', marginVertical: 16 },
  dividerText:      { color: '#334155', fontWeight: '600' },

  clientBox:        { backgroundColor: '#1e293b', borderRadius: 16, padding: 18, marginBottom: 12 },
  codeInput:        { backgroundColor: '#0f172a', borderRadius: 10, padding: 14,
                      color: '#f1f5f9', fontSize: 20, letterSpacing: 4, textAlign: 'center',
                      marginBottom: 12, borderWidth: 1, borderColor: '#334155' },
  connectBtn:       { backgroundColor: '#0ea5e9', borderRadius: 12, padding: 14, alignItems: 'center' },
  connectBtnText:   { color: '#fff', fontWeight: '700', fontSize: 16 },
  vpnNote:          { color: '#475569', fontSize: 11, textAlign: 'center', marginTop: 10 },

  center:           { alignItems: 'center', paddingVertical: 30 },
  centerText:       { color: '#94a3b8', marginTop: 12, fontSize: 15 },
  errorText:        { color: '#fca5a5', textAlign: 'center', marginBottom: 16, fontSize: 14 },
  retryBtn:         { backgroundColor: '#334155', borderRadius: 12, paddingVertical: 12, paddingHorizontal: 28 },
  retryBtnText:     { color: '#f1f5f9', fontWeight: '700' },

  codeBox:          { backgroundColor: '#1e293b', borderRadius: 16, padding: 20,
                      alignItems: 'center', marginBottom: 16 },
  codeLabel:        { color: '#64748b', fontSize: 12, fontWeight: '600',
                      textTransform: 'uppercase', letterSpacing: 1, marginBottom: 8 },
  code:             { fontSize: 44, fontWeight: '900', color: '#38bdf8', letterSpacing: 8 },
  codeSub:          { color: '#475569', fontSize: 12, marginTop: 6 },

  infoBox:          { backgroundColor: '#1e293b', borderRadius: 14, padding: 16, marginBottom: 16 },
  infoLabel:        { color: '#64748b', fontSize: 12, fontWeight: '600',
                      textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 },
  infoValue:        { color: '#38bdf8', fontSize: 15, fontWeight: '700', fontFamily: 'monospace' },
  infoDesc:         { color: '#64748b', fontSize: 12, marginTop: 8, lineHeight: 17 },

  statsRow:         { flexDirection: 'row', justifyContent: 'space-around',
                      backgroundColor: '#1e293b', borderRadius: 14, padding: 16, marginBottom: 16 },
  stat:             { alignItems: 'center' },
  statVal:          { color: '#f1f5f9', fontSize: 20, fontWeight: '700' },
  statLabel:        { color: '#64748b', fontSize: 11, marginTop: 2 },

  compatNote:       { color: '#475569', fontSize: 12, textAlign: 'center',
                      lineHeight: 18, marginBottom: 16 },

  stopBtn:          { backgroundColor: '#7f1d1d', borderRadius: 14, padding: 16,
                      alignItems: 'center', marginTop: 8 },
  stopBtnText:      { color: '#fca5a5', fontWeight: '700', fontSize: 16 },

  tunnelStatus:     { borderRadius: 10, padding: 10, alignItems: 'center', marginBottom: 12 },
  tunnelOk:         { backgroundColor: '#14532d' },
  tunnelWaiting:    { backgroundColor: '#1e3a5f' },
  tunnelStatusText: { color: '#f1f5f9', fontSize: 13, fontWeight: '600' },

  vpnBanner:        { borderRadius: 10, padding: 12, alignItems: 'center', marginBottom: 14 },
  vpnBannerText:    { color: '#f1f5f9', fontSize: 13, fontWeight: '600', textAlign: 'center' },

  connectedBox:     { backgroundColor: '#1e293b', borderRadius: 14, padding: 18, marginBottom: 14 },
  connectedTitle:   { color: '#f1f5f9', fontSize: 17, fontWeight: '700', marginBottom: 6 },
  connectedDesc:    { color: '#64748b', fontSize: 13, lineHeight: 19 },
});
