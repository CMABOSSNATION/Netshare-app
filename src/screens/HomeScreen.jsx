/**
 * HomeScreen.jsx — NetShare HTTP/HTTPS Transparent Proxy
 *
 * HOST flow:
 *   1. Tap "Share My Internet"
 *   2. App starts local proxy on :8899, registers with relay, gets a code
 *   3. Show session code + QR to client
 *
 * CLIENT flow:
 *   1. Tap "Connect to Host"
 *   2. Enter session code
 *   3. App looks up host IP:port from relay
 *   4. Shows step-by-step Wi-Fi proxy setup instructions
 *   5. "Test Connection" button verifies proxy works
 *   6. All apps (TikTok, WhatsApp, etc.) now use host's internet automatically
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import {
  View, Text, TouchableOpacity, TextInput, StyleSheet,
  ScrollView, ActivityIndicator, Alert, Platform,
  StatusBar, SafeAreaView, Linking, Clipboard,
} from 'react-native';
import { useStore } from '../store';
import proxyService from '../services/ProxyService';

// ── Helpers ──────────────────────────────────────────────────────────────────

function formatBytes(b) {
  if (!b || b < 1024)      return `${b || 0} B`;
  if (b < 1048576)         return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1073741824)      return `${(b / 1048576).toFixed(1)} MB`;
  return `${(b / 1073741824).toFixed(2)} GB`;
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

// ── Component ────────────────────────────────────────────────────────────────

export default function HomeScreen() {
  const {
    role, status, sessionCode, connectedClients,
    errorMessage, bytesUp, bytesDown,
    setRole, setConnecting, setConnected, setError, setIdle,
    tickBandwidth, getSessionDurationMs,
  } = useStore();

  const [codeInput,   setCodeInput]   = useState('');
  const [proxyInfo,   setProxyInfo]   = useState(null);  // { ip, port }
  const [testResult,  setTestResult]  = useState(null);  // 'ok' | 'fail' | null
  const [testing,     setTesting]     = useState(false);
  const [showSetup,   setShowSetup]   = useState(false); // client setup guide
  const [copied,      setCopied]      = useState(false);
  const timerRef = useRef(null);
  const unsubs   = useRef([]);

  // ── Service event wiring ──────────────────────────────────────────────────

  useEffect(() => {
    const u1 = proxyService.on('status', ({ status: s, message }) => {
      if (s === 'connecting') setConnecting();
      else if (s === 'connected') setConnected(sessionCode);
      else if (s === 'error')    setError(message || 'Unknown error');
      else if (s === 'idle')     setIdle();
    });

    const u2 = proxyService.on('session', ({ code, ip, port }) => {
      setConnected(code);
      setProxyInfo({ ip, port });
    });

    const u3 = proxyService.on('proxy', ({ ip, port }) => {
      setProxyInfo({ ip, port });
      setShowSetup(true);
    });

    const u4 = proxyService.on('stats', ({ bytesUp: up, bytesDown: down }) => {
      tickBandwidth({ up, down });
    });

    unsubs.current = [u1, u2, u3, u4];
    return () => unsubs.current.forEach(u => u());
  }, []);

  // ── Duration ticker ───────────────────────────────────────────────────────

  useEffect(() => {
    if (status === 'connected') {
      timerRef.current = setInterval(() => {
        /* forces re-render for duration display */
      }, 1000);
    } else {
      clearInterval(timerRef.current);
    }
    return () => clearInterval(timerRef.current);
  }, [status]);

  // ── Actions ───────────────────────────────────────────────────────────────

  const handleHostStart = useCallback(async () => {
    setRole('host');
    try {
      await proxyService.startAsHost();
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
      setShowSetup(true);
    } catch (err) {
      Alert.alert('Connection failed', err.message);
    }
  }, [codeInput]);

  const handleStop = useCallback(async () => {
    await proxyService.stop();
    setProxyInfo(null);
    setShowSetup(false);
    setTestResult(null);
    setCodeInput('');
  }, []);

  const handleTestConnection = useCallback(async () => {
    setTesting(true);
    setTestResult(null);
    // We do a real test via the relay's /probe endpoint
    const info = proxyService.getProxyInfo();
    if (!info) { setTesting(false); setTestResult('fail'); return; }

    try {
      const res = await fetch(
        `${proxyService.RELAY_URL || 'https://netshare.cmaraphael90.workers.dev'}/probe`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ ip: info.ip, port: info.port }),
        }
      );
      setTestResult(res.ok ? 'ok' : 'fail');
    } catch {
      setTestResult('fail');
    } finally {
      setTesting(false);
    }
  }, []);

  const copyCode = useCallback(() => {
    const code = proxyService.getSessionCode();
    if (code) { Clipboard.setString(code); setCopied(true); setTimeout(() => setCopied(false), 2000); }
  }, []);

  // ── Open Android Wi-Fi settings ───────────────────────────────────────────

  const openWifiSettings = () => {
    Linking.sendIntent('android.settings.WIFI_SETTINGS').catch(() =>
      Linking.openSettings()
    );
  };

  // ── Render helpers ────────────────────────────────────────────────────────

  const isConnected  = status === 'connected';
  const isConnecting = status === 'connecting';
  const isHost       = role === 'host';
  const isClient     = role === 'client';
  const hostCode     = proxyService.getSessionCode();

  // ── Client proxy setup guide ──────────────────────────────────────────────

  function renderSetupGuide() {
    if (!proxyInfo) return null;
    const { ip, port } = proxyInfo;

    return (
      <View style={s.setupBox}>
        <Text style={s.setupTitle}>📡 Configure Wi-Fi Proxy</Text>
        <Text style={s.setupDesc}>
          All your apps (TikTok, WhatsApp, Instagram, etc.) will automatically
          use the host's internet once you set this up.
        </Text>

        <View style={s.stepList}>
          {[
            'Open Android Settings',
            'Tap Wi-Fi',
            'Long-press your connected network',
            'Tap "Modify network"',
            'Expand "Advanced options"',
            'Set Proxy to "Manual"',
            `Set Proxy hostname to:  ${ip}`,
            `Set Proxy port to:  ${port}`,
            'Tap Save',
          ].map((step, i) => (
            <View key={i} style={s.step}>
              <View style={s.stepNum}><Text style={s.stepNumText}>{i + 1}</Text></View>
              <Text style={s.stepText}>{step}</Text>
            </View>
          ))}
        </View>

        <TouchableOpacity style={s.wifiBtn} onPress={openWifiSettings}>
          <Text style={s.wifiBtnText}>Open Wi-Fi Settings →</Text>
        </TouchableOpacity>

        <View style={s.proxyRow}>
          <Text style={s.proxyLabel}>Host IP:</Text>
          <Text style={s.proxyValue}>{ip}</Text>
        </View>
        <View style={s.proxyRow}>
          <Text style={s.proxyLabel}>Port:</Text>
          <Text style={s.proxyValue}>{port}</Text>
        </View>

        <TouchableOpacity
          style={[s.testBtn, testResult === 'ok' && s.testBtnOk, testResult === 'fail' && s.testBtnFail]}
          onPress={handleTestConnection}
          disabled={testing}
        >
          {testing
            ? <ActivityIndicator color="#fff" />
            : <Text style={s.testBtnText}>
                {testResult === 'ok'   ? '✓ Proxy is working!'
                : testResult === 'fail' ? '✗ Not reachable — check settings'
                : '🔍 Test Connection'}
              </Text>
          }
        </TouchableOpacity>
      </View>
    );
  }

  // ── Main render ───────────────────────────────────────────────────────────

  return (
    <SafeAreaView style={s.safe}>
      <StatusBar barStyle="light-content" backgroundColor="#0f172a" />
      <ScrollView contentContainerStyle={s.scroll} keyboardShouldPersistTaps="handled">

        {/* Header */}
        <View style={s.header}>
          <Text style={s.headerTitle}>NetShare</Text>
          <Text style={s.headerSub}>HTTP/HTTPS Transparent Proxy</Text>
        </View>

        {/* Status badge */}
        {status !== 'idle' && (
          <View style={[s.badge,
            isConnecting && s.badgeConnecting,
            isConnected  && s.badgeConnected,
            status === 'error' && s.badgeError,
          ]}>
            {isConnecting && <ActivityIndicator color="#fff" size="small" style={{ marginRight: 8 }} />}
            <Text style={s.badgeText}>
              {isConnecting ? 'Connecting…'
              : isConnected  ? (isHost ? '🟢 Sharing internet' : '🟢 Connected via proxy')
              : `⚠ ${errorMessage || 'Error'}`}
            </Text>
          </View>
        )}

        {/* ── Idle screen ── */}
        {status === 'idle' && (
          <>
            <Text style={s.sectionLabel}>I want to…</Text>

            <TouchableOpacity style={s.roleCard} onPress={handleHostStart}>
              <Text style={s.roleIcon}>📡</Text>
              <View style={s.roleText}>
                <Text style={s.roleTitle}>Share My Internet</Text>
                <Text style={s.roleDesc}>
                  Start a proxy server. Others on the same Wi-Fi network can
                  use your mobile data or broadband.
                </Text>
              </View>
            </TouchableOpacity>

            <View style={s.divider}><Text style={s.dividerText}>or</Text></View>

            <View style={s.clientBox}>
              <Text style={s.sectionLabel}>Connect to a host</Text>
              <TextInput
                style={s.codeInput}
                placeholder="Enter session code (e.g. AB3F)"
                placeholderTextColor="#64748b"
                value={codeInput}
                onChangeText={t => setCodeInput(t.toUpperCase())}
                autoCapitalize="characters"
                maxLength={8}
              />
              <TouchableOpacity style={s.connectBtn} onPress={handleClientConnect}>
                <Text style={s.connectBtnText}>Connect</Text>
              </TouchableOpacity>
            </View>
          </>
        )}

        {/* ── Connecting screen ── */}
        {isConnecting && (
          <View style={s.center}>
            <ActivityIndicator size="large" color="#38bdf8" />
            <Text style={s.centerText}>
              {isHost ? 'Starting proxy server…' : 'Looking up session…'}
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

            {/* Proxy info */}
            {proxyInfo && (
              <View style={s.infoBox}>
                <Text style={s.infoLabel}>Proxy Address</Text>
                <Text style={s.infoValue}>{proxyInfo.ip}:{proxyInfo.port}</Text>
                <Text style={s.infoDesc}>
                  Clients on the same Wi-Fi must set this as their proxy.
                  Share the session code and they'll get the address automatically.
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
            {showSetup ? renderSetupGuide() : (
              <View style={s.center}>
                <ActivityIndicator color="#38bdf8" />
                <Text style={s.centerText}>Getting host details…</Text>
              </View>
            )}

            {/* Stats */}
            {testResult === 'ok' && (
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
            )}

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
  headerSub:        { fontSize: 13, color: '#64748b', marginTop: 4 },

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
  infoValue:        { color: '#38bdf8', fontSize: 22, fontWeight: '700', fontFamily: 'monospace' },
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

  // Client setup guide
  setupBox:         { backgroundColor: '#1e293b', borderRadius: 16, padding: 18, marginBottom: 16 },
  setupTitle:       { color: '#f1f5f9', fontSize: 17, fontWeight: '700', marginBottom: 8 },
  setupDesc:        { color: '#64748b', fontSize: 13, lineHeight: 19, marginBottom: 16 },
  stepList:         { marginBottom: 16 },
  step:             { flexDirection: 'row', alignItems: 'flex-start', marginBottom: 10 },
  stepNum:          { width: 24, height: 24, borderRadius: 12, backgroundColor: '#0ea5e9',
                      alignItems: 'center', justifyContent: 'center', marginRight: 10, marginTop: 1 },
  stepNumText:      { color: '#fff', fontSize: 12, fontWeight: '700' },
  stepText:         { color: '#cbd5e1', fontSize: 13, flex: 1, lineHeight: 19 },
  wifiBtn:          { backgroundColor: '#0ea5e9', borderRadius: 10, padding: 12,
                      alignItems: 'center', marginBottom: 16 },
  wifiBtnText:      { color: '#fff', fontWeight: '700' },
  proxyRow:         { flexDirection: 'row', justifyContent: 'space-between',
                      paddingVertical: 6, borderBottomWidth: 1, borderBottomColor: '#334155' },
  proxyLabel:       { color: '#64748b', fontSize: 13 },
  proxyValue:       { color: '#38bdf8', fontSize: 13, fontFamily: 'monospace', fontWeight: '700' },
  testBtn:          { backgroundColor: '#334155', borderRadius: 10, padding: 14,
                      alignItems: 'center', marginTop: 14 },
  testBtnOk:        { backgroundColor: '#14532d' },
  testBtnFail:      { backgroundColor: '#7f1d1d' },
  testBtnText:      { color: '#f1f5f9', fontWeight: '600' },
});
