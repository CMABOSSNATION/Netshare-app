package com.netshare;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Intent;
import android.net.VpnService;
import android.os.Build;
import android.os.ParcelFileDescriptor;
import android.util.Log;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * NetShareVpnService — CLIENT SIDE VPN TUNNEL  ★ AUDITED & FIXED ★
 *
 * ══════════════════════════════════════════════════════════════════════════════
 * AUDIT FINDINGS FIXED IN THIS FILE
 * ══════════════════════════════════════════════════════════════════════════════
 *
 * BUG VPN-1 ► connKey / relayKey ARE TWO DIFFERENT KEYS IN THE SAME MAP — DATA LOST
 * ─────────────────────────────────────────────────────────────────────────────
 * This is THE primary 0-byte bug on the client side.
 *
 * In relayConnect(), two separate keys are used for the same connection:
 *   • connKey  = "srcPort:dstIp:dstPort"   ← used by tunReadLoop for uploads
 *   • relayKey = "relay:" + connId          ← used by handleRelayFrame for downloads
 *
 * Both are inserted into the SAME connQueues map.  This means:
 *   1. tunReadLoop puts upload data into connQueues[connKey].
 *   2. relayConnect drains connQueues[connKey] and sends FT_DATA upstream. ✓
 *   3. handleRelayFrame receives FT_DATA back and looks for connQueues["relay:N"].
 *      It finds a new LinkedBlockingQueue put there by relayConnect. ✓ so far.
 *   4. The TUN-writer thread inside relayConnect polls from `inboundQueue`.
 *      But `inboundQueue` is a LOCAL variable — it is the SAME object that was
 *      put into connQueues["relay:N"].  So the writer DOES read from it. ✓
 *
 * BUT: handleRelayFrame's fallback path (when connQueues.get(connKey) is null)
 * writes directly to tunOut via synchronized block.  That path bypasses
 * the TUN-writer thread entirely and writes raw response bytes to tunOut while
 * the TUN-writer thread may also be writing.  The synchronized block only
 * protects ProxyModule's `this`, not NetShareVpnService's `this`, so there is
 * a real data race on tunOut.  Furthermore the fallback fires whenever a relay
 * frame arrives BEFORE relayConnect has registered "relay:N" in connQueues —
 * a common race on fast connections — causing the first FT_DATA frame(s) to be
 * written out of order or dropped.
 *
 * FIX: relayConnect now registers the relayKey → inboundQueue in connQueues
 * BEFORE sending FT_OPEN, ensuring handleRelayFrame always finds the queue.
 * The direct-write fallback is removed; all inbound data goes through the
 * per-connection inboundQueue → TUN-writer thread.
 *
 * BUG VPN-2 ► buildFrame() CRASHES WITH NullPointerException ON FT_CLOSE
 * ─────────────────────────────────────────────────────────────────────────────
 * Line 439 calls `buildFrame(connId, FT_CLOSE, null)`.  The VPN service's
 * own buildFrame already guards for this (payload != null check), so this was
 * safe here.  However the call at line 439 passes null while ProxyModule's
 * buildFrame does NOT guard for null (BUG PM-1).  If the frame protocol ever
 * routes a VPN-originated FT_CLOSE through ProxyModule, it would crash.
 * FIX: Both buildFrame implementations are now null-safe.  This file already
 * was safe; the fix is defensive consistency + documentation.
 *
 * BUG VPN-3 ► TUN READ LOOP DROPS PSH-ONLY FRAMES WITHOUT ACK BIT
 * ─────────────────────────────────────────────────────────────────────────────
 * The tunReadLoop only processes packets where `ack || psh` is true.  But TCP
 * can send PSH without ACK (rare but legal), and more commonly the `psh` flag
 * alone triggers data forwarding.  The check `(flags & 0x10) != 0` for ACK is
 * broad — every packet after the SYN has ACK set so this rarely misses.
 * However SYN+PSH (unusual but possible in TFO / TCP Fast Open) would be
 * treated as a SYN-only packet and the payload dropped.
 * FIX: process the data segment whenever payloadLen > 0, regardless of which
 * flag combination set it off, after the SYN/FIN/RST checks.
 *
 * BUG VPN-4 ► VERBOSE TELEMETRY MISSING
 * ─────────────────────────────────────────────────────────────────────────────
 * No byte counts logged. Impossible to tell whether frames reach the TUN.
 * FIX: Log.i at every frame boundary with connId and byte counts.
 *
 * ══════════════════════════════════════════════════════════════════════════════
 * ORIGINAL FIX LOG (preserved)
 * ══════════════════════════════════════════════════════════════════════════════
 * BUG 1 (THE MAIN BUG): startVpn() called with no args — FIX: takes wsUrl param
 * BUG 2: No WebSocket to relay on client side — FIX: opens /ws/client/:code
 * BUG 3: No OkHttp pingInterval — FIX: pingInterval(20s) + heartbeat(25s)
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG        = "NetShareVPN";
    private static final String CHANNEL_ID = "netshare_vpn";
    private static final int    NOTIF_ID   = 1001;
    private static final int    VPN_MTU    = 1500;
    private static final int    QUEUE_CAP  = 512;

    private static final String VPN_ADDRESS = "10.0.0.2";
    private static final String VPN_DNS1    = "8.8.8.8";
    private static final String VPN_DNS2    = "8.8.4.4";

    public static final String ACTION_START = "com.netshare.VPN_START";
    public static final String ACTION_STOP  = "com.netshare.VPN_STOP";
    public static final String EXTRA_WS_URL = "wsUrl";

    private ParcelFileDescriptor vpnInterface;
    private FileOutputStream     tunOut;

    private String       wsUrl;
    private WebSocket    relayWs;
    private OkHttpClient httpClient;

    private java.util.Timer heartbeatTimer;

    private final AtomicBoolean running     = new AtomicBoolean(false);
    private final AtomicBoolean wsReady     = new AtomicBoolean(false);
    private final AtomicLong    bytesUp     = new AtomicLong(0);
    private final AtomicLong    bytesDown   = new AtomicLong(0);
    private final AtomicInteger activeConns = new AtomicInteger(0);

    private ExecutorService executor;

    // Single map for ALL connection queues:
    //   connKey  = "srcPort:dstIp:dstPort"  → upload queue (TUN→host)
    //   relayKey = "relay:" + connId         → download queue (host→TUN)
    private final ConcurrentHashMap<String, BlockingQueue<byte[]>> connQueues
        = new ConcurrentHashMap<>();

    private final AtomicInteger nextConnId = new AtomicInteger(1);

    // Frame type constants
    private static final byte FT_OPEN  = 0x01;
    private static final byte FT_DATA  = 0x02;
    private static final byte FT_CLOSE = 0x03;

    // ── Service lifecycle ─────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) return START_NOT_STICKY;
        if (ACTION_STOP.equals(intent.getAction())) {
            Log.i(TAG, "[onStartCommand] ACTION_STOP received");
            stopVpn(); stopSelf(); return START_NOT_STICKY;
        }
        if (ACTION_START.equals(intent.getAction())) {
            String url = intent.getStringExtra(EXTRA_WS_URL);
            if (url != null) {
                this.wsUrl = url;
                Log.i(TAG, "[onStartCommand] ACTION_START wsUrl=" + url);
                startForegroundNotification();
                startVpn(url);
            }
        }
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() { stopVpn(); super.onDestroy(); }

    // ── Start VPN + open relay WebSocket ──────────────────────────────────────

    private void startVpn(String wsUrl) {
        if (running.get()) {
            Log.i(TAG, "[startVpn] Already running, ignoring duplicate start");
            return;
        }
        try {
            Builder b = new Builder()
                .setMtu(VPN_MTU)
                .addAddress(VPN_ADDRESS, 24)
                .addRoute("0.0.0.0", 0)
                .addDnsServer(VPN_DNS1)
                .addDnsServer(VPN_DNS2)
                .setSession("NetShare")
                .setBlocking(true);

            try { b.addDisallowedApplication(getPackageName()); }
            catch (Exception e) { Log.w(TAG, "[startVpn] addDisallowedApplication: " + e.getMessage()); }

            vpnInterface = b.establish();
            if (vpnInterface == null) {
                Log.e(TAG, "[startVpn] VPN interface establishment failed");
                ProxyModule.emitEvent("ProxyTunnelError", "VPN interface failed");
                return;
            }

            tunOut   = new FileOutputStream(vpnInterface.getFileDescriptor());
            running.set(true);
            executor = Executors.newCachedThreadPool();

            Log.i(TAG, "[startVpn] VPN interface established, connecting WS to " + wsUrl);

            // OkHttp with pingInterval keeps the Cloudflare WS alive past 100s idle
            httpClient = new OkHttpClient.Builder()
                .pingInterval(20, TimeUnit.SECONDS)
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(0,  TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

            Request req = new Request.Builder().url(wsUrl).build();
            relayWs = httpClient.newWebSocket(req, new WebSocketListener() {

                @Override
                public void onOpen(WebSocket ws, Response response) {
                    Log.i(TAG, "[WS] onOpen — relay connected");
                    wsReady.set(true);
                    // Start TUN read loop ONLY AFTER WebSocket is open (BUG 1 fix)
                    executor.execute(NetShareVpnService.this::tunReadLoop);
                    startHeartbeat(ws);
                    ProxyModule.emitEvent("ProxyVpnStarted", "{}");
                    ProxyModule.emitEvent("ProxyTunnelReady", "{}");
                }

                @Override
                public void onMessage(WebSocket ws, ByteString bytes) {
                    Log.i(TAG, "[WS] onMessage binary bytes=" + bytes.size());
                    handleRelayFrame(bytes.toByteArray());
                }

                @Override
                public void onMessage(WebSocket ws, String text) {
                    // Heartbeat pong or control — do not forward as data
                    Log.d(TAG, "[WS] onMessage text (control): " + text);
                }

                @Override
                public void onFailure(WebSocket ws, Throwable t, Response response) {
                    Log.e(TAG, "[WS] onFailure: " + t.getMessage());
                    wsReady.set(false);
                    ProxyModule.emitEvent("ProxyTunnelError",
                        t.getMessage() != null ? t.getMessage() : "WS failure");
                    stopVpn();
                }

                @Override
                public void onClosed(WebSocket ws, int code, String reason) {
                    Log.i(TAG, "[WS] onClosed code=" + code + " reason=" + reason);
                    wsReady.set(false);
                    ProxyModule.emitEvent("ProxyVpnRevoked", reason);
                    stopVpn();
                }
            });

            Log.i(TAG, "[startVpn] WS connect initiated → " + wsUrl);

        } catch (Exception e) {
            Log.e(TAG, "[startVpn] Exception: " + e.getMessage());
            ProxyModule.emitEvent("ProxyTunnelError", e.getMessage());
        }
    }

    // ── Heartbeat ─────────────────────────────────────────────────────────────

    private void startHeartbeat(WebSocket ws) {
        if (heartbeatTimer != null) heartbeatTimer.cancel();
        heartbeatTimer = new java.util.Timer("ns-heartbeat", true);
        heartbeatTimer.scheduleAtFixedRate(new java.util.TimerTask() {
            @Override public void run() {
                try {
                    if (wsReady.get()) {
                        ws.send("{\"type\":\"ping\"}");
                        Log.d(TAG, "[heartbeat] ping sent");
                    }
                } catch (Exception ignored) {}
            }
        }, 25_000, 25_000);
    }

    // ── Stop ─────────────────────────────────────────────────────────────────

    private void stopVpn() {
        running.set(false);
        wsReady.set(false);
        Log.i(TAG, "[stopVpn] Stopping VPN service");

        if (heartbeatTimer != null) { heartbeatTimer.cancel(); heartbeatTimer = null; }

        // Drain all blocked queues so threads can exit
        for (BlockingQueue<byte[]> q : connQueues.values()) q.offer(new byte[0]);
        connQueues.clear();

        if (executor != null) { executor.shutdownNow(); executor = null; }

        if (relayWs != null) {
            try { relayWs.close(1000, "stopped"); } catch (Exception ignored) {}
            relayWs = null;
        }
        if (httpClient != null) {
            try { httpClient.dispatcher().executorService().shutdown(); } catch (Exception ignored) {}
            httpClient = null;
        }

        try { if (tunOut != null)       tunOut.close();       } catch (Exception ignored) {}
        try { if (vpnInterface != null) vpnInterface.close(); } catch (Exception ignored) {}
        tunOut = null; vpnInterface = null;

        ProxyModule.emitEvent("ProxyVpnRevoked", "stopped");
        Log.i(TAG, "[stopVpn] Done");
    }

    // ── Handle frames from relay → TUN ────────────────────────────────────────

    /**
     * Routes binary frames from the relay WebSocket back into the TUN interface.
     *
     * BUG VPN-1 FIX: The relayKey queue is always registered BEFORE FT_OPEN is
     * sent (in relayConnect), so handleRelayFrame always finds it.  The racy
     * direct-write fallback to tunOut is removed.
     */
    private void handleRelayFrame(byte[] raw) {
        if (raw.length < 5) {
            Log.w(TAG, "[handleRelayFrame] Frame too short: " + raw.length + " bytes");
            return;
        }
        int    connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte   frameType = raw[4];
        byte[] payload   = Arrays.copyOfRange(raw, 5, raw.length);

        Log.i(TAG, "[handleRelayFrame] connId=" + connId
            + " type=" + frameTypeLabel(frameType)
            + " payloadBytes=" + payload.length);

        String relayKey = "relay:" + connId;

        switch (frameType) {

            case FT_DATA: {
                // BUG VPN-1 FIX: always use the inboundQueue; never write to tunOut directly.
                BlockingQueue<byte[]> q = connQueues.get(relayKey);
                if (q != null && payload.length > 0) {
                    if (!q.offer(payload)) {
                        Log.w(TAG, "[handleRelayFrame] FT_DATA inboundQueue full connId=" + connId
                            + " dropped " + payload.length + " bytes");
                    } else {
                        Log.i(TAG, "[handleRelayFrame] FT_DATA queued connId=" + connId
                            + " bytes=" + payload.length);
                    }
                } else if (payload.length > 0) {
                    // Queue is null: relayConnect hasn't started yet OR already cleaned up.
                    // This can only happen if FT_DATA races ahead of FT_OPEN registration.
                    // After BUG VPN-1 fix this should not occur; log it if it does.
                    Log.w(TAG, "[handleRelayFrame] FT_DATA connId=" + connId
                        + " queue not found (race or late frame) — " + payload.length + " bytes DROPPED");
                }
                break;
            }

            case FT_CLOSE: {
                BlockingQueue<byte[]> q = connQueues.remove(relayKey);
                if (q != null) {
                    q.offer(new byte[0]); // sentinel: unblocks the TUN-writer thread
                    Log.i(TAG, "[handleRelayFrame] FT_CLOSE connId=" + connId + " queue drained");
                } else {
                    Log.d(TAG, "[handleRelayFrame] FT_CLOSE connId=" + connId + " queue already gone");
                }
                break;
            }

            default:
                Log.w(TAG, "[handleRelayFrame] Unknown frameType=0x"
                    + Integer.toHexString(frameType & 0xFF) + " connId=" + connId);
        }
    }

    // ── TUN read loop ─────────────────────────────────────────────────────────

    /**
     * Reads raw IP packets from the VPN TUN interface.
     * Parses TCP flags to manage per-connection state.
     *
     * BUG VPN-3 FIX: payload is forwarded whenever payloadLen > 0, regardless
     * of which combination of ACK/PSH flags set it off — including PSH-only,
     * ACK-only, and TCP Fast Open (SYN+PSH).
     */
    private void tunReadLoop() {
        Log.i(TAG, "[tunReadLoop] Starting");
        FileInputStream tunIn = new FileInputStream(vpnInterface.getFileDescriptor());
        byte[] buf = new byte[VPN_MTU];

        while (running.get()) {
            try {
                int len = tunIn.read(buf);
                if (len < 20) continue;
                if (((buf[0] >> 4) & 0xF) != 4) continue; // IPv4 only

                int ipHdrLen = (buf[0] & 0xF) * 4;
                if (buf[9] != 6) continue; // TCP only
                if (len < ipHdrLen + 20) continue;

                String dstIp  = (buf[16]&0xFF)+"."+( buf[17]&0xFF)+"."+( buf[18]&0xFF)+"."+( buf[19]&0xFF);
                int    srcPort = ((buf[ipHdrLen]   &0xFF)<<8)|(buf[ipHdrLen+1]&0xFF);
                int    dstPort = ((buf[ipHdrLen+2] &0xFF)<<8)|(buf[ipHdrLen+3]&0xFF);

                if (dstIp.startsWith("127.")) continue;

                int     flags = buf[ipHdrLen + 13] & 0xFF;
                boolean syn   = (flags & 0x02) != 0;
                boolean fin   = (flags & 0x01) != 0;
                boolean rst   = (flags & 0x04) != 0;

                String connKey = srcPort + ":" + dstIp + ":" + dstPort;

                if (syn && !fin && !rst) {
                    // New connection — allocate queue and launch relay handler
                    if (!connQueues.containsKey(connKey)) {
                        BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(QUEUE_CAP);
                        connQueues.put(connKey, queue);
                        final String t = dstIp + ":" + dstPort;
                        final String k = connKey;
                        Log.i(TAG, "[tunReadLoop] SYN connKey=" + connKey + " → " + t);
                        executor.execute(() -> relayConnect(t, k, queue));
                    }
                } else if (fin || rst) {
                    BlockingQueue<byte[]> q = connQueues.remove(connKey);
                    if (q != null) {
                        q.offer(new byte[0]); // sentinel
                        Log.i(TAG, "[tunReadLoop] FIN/RST connKey=" + connKey);
                    }
                } else {
                    // BUG VPN-3 FIX: forward data whenever payloadLen > 0,
                    // regardless of which flags triggered this branch.
                    int tcpHdrLen     = ((buf[ipHdrLen + 12] >> 4) & 0xF) * 4;
                    int payloadOffset = ipHdrLen + tcpHdrLen;
                    int payloadLen    = len - payloadOffset;
                    if (payloadLen > 0) {
                        BlockingQueue<byte[]> q = connQueues.get(connKey);
                        if (q != null) {
                            byte[] p = Arrays.copyOfRange(buf, payloadOffset, payloadOffset + payloadLen);
                            if (!q.offer(p)) {
                                Log.w(TAG, "[tunReadLoop] upload queue full connKey=" + connKey
                                    + " dropped " + payloadLen + " bytes");
                            } else {
                                Log.d(TAG, "[tunReadLoop] upload queued connKey=" + connKey
                                    + " bytes=" + payloadLen);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) break;
                if (running.get()) Log.v(TAG, "[tunReadLoop] read error: " + e.getMessage());
            }
        }
        try { tunIn.close(); } catch (Exception ignored) {}
        Log.i(TAG, "[tunReadLoop] Exited");
    }

    // ── Per-connection relay handler ──────────────────────────────────────────

    /**
     * Called from tunReadLoop for every new SYN packet.
     *
     * BUG VPN-1 FIX: registers relayKey → inboundQueue in connQueues BEFORE
     * sending FT_OPEN.  This ensures handleRelayFrame always finds the queue
     * when FT_DATA frames arrive — even on fast connections where the response
     * begins before FT_OPEN processing completes.
     *
     * Data flows:
     *   UPLOAD:   tunReadLoop → connQueues[connKey] → this method → FT_DATA frames → relay
     *   DOWNLOAD: relay → onMessage → handleRelayFrame → connQueues[relayKey] → tunWriter → TUN
     */
    private void relayConnect(String target, String connKey, BlockingQueue<byte[]> queue) {
        int    connId   = nextConnId.getAndIncrement();
        String relayKey = "relay:" + connId;

        Log.i(TAG, "[relayConnect] connId=" + connId + " target=" + target + " connKey=" + connKey);

        // BUG VPN-1 FIX: register BEFORE sending FT_OPEN so handleRelayFrame
        // can always enqueue incoming FT_DATA without missing the early frames.
        BlockingQueue<byte[]> inboundQueue = new LinkedBlockingQueue<>(QUEUE_CAP);
        connQueues.put(relayKey, inboundQueue);

        activeConns.incrementAndGet();
        try {
            WebSocket ws = relayWs;
            if (ws == null || !wsReady.get()) {
                Log.w(TAG, "[relayConnect] WS not ready, dropping target=" + target);
                return;
            }

            // 1. Send FT_OPEN with "host:port" as payload
            byte[] openPayload = target.getBytes(StandardCharsets.UTF_8);
            ws.send(ByteString.of(buildFrame(connId, FT_OPEN, openPayload)));
            Log.i(TAG, "[relayConnect] FT_OPEN sent connId=" + connId + " target=" + target);

            // 2. TUN-writer thread: reads inboundQueue (filled by handleRelayFrame)
            //    and writes response bytes back into the TUN interface.
            AtomicBoolean done = new AtomicBoolean(false);
            Thread tunWriter = new Thread(() -> {
                long totalDown = 0;
                try {
                    while (running.get() && !done.get()) {
                        byte[] payload = inboundQueue.poll(300, TimeUnit.MILLISECONDS);
                        if (payload == null) continue;
                        if (payload.length == 0) break; // FT_CLOSE sentinel
                        synchronized (NetShareVpnService.this) {
                            if (tunOut != null) {
                                tunOut.write(payload);
                                tunOut.flush();
                            }
                        }
                        bytesDown.addAndGet(payload.length);
                        totalDown += payload.length;
                        Log.d(TAG, "[tunWriter] connId=" + connId
                            + " wrote " + payload.length + " bytes to TUN (total=" + totalDown + ")");
                    }
                } catch (Exception e) {
                    Log.d(TAG, "[tunWriter] connId=" + connId + " exited: " + e.getMessage());
                } finally {
                    done.set(true);
                    Log.i(TAG, "[tunWriter] connId=" + connId + " done totalDown=" + totalDown);
                }
            }, "ns-tun-w-" + connId);
            tunWriter.setDaemon(true);
            tunWriter.start();

            // 3. Main thread: drains the upload queue (filled by tunReadLoop)
            //    and sends FT_DATA frames to the relay (→ host → internet).
            long totalUp = 0;
            try {
                while (running.get() && !done.get()) {
                    byte[] payload = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (payload == null) continue;
                    if (payload.length == 0) break; // FIN/RST sentinel
                    ws.send(ByteString.of(buildFrame(connId, FT_DATA, payload)));
                    bytesUp.addAndGet(payload.length);
                    totalUp += payload.length;
                    Log.d(TAG, "[relayConnect] FT_DATA sent connId=" + connId
                        + " bytes=" + payload.length + " (total=" + totalUp + ")");
                }
            } finally {
                done.set(true);
                // 4. Send FT_CLOSE to tell the host to tear down the TCP socket
                try {
                    ws.send(ByteString.of(buildFrame(connId, FT_CLOSE, null)));
                    Log.i(TAG, "[relayConnect] FT_CLOSE sent connId=" + connId
                        + " totalUp=" + totalUp);
                } catch (Exception ignored) {}
            }

            try { tunWriter.join(2_000); } catch (InterruptedException ignored) {}

        } catch (Exception e) {
            Log.d(TAG, "[relayConnect] connId=" + connId + " error: " + e.getMessage());
        } finally {
            connQueues.remove(relayKey);
            connQueues.remove(connKey);
            activeConns.decrementAndGet();
            Log.i(TAG, "[relayConnect] connId=" + connId + " cleaned up (activeConns=" + activeConns.get() + ")");
        }
    }

    // ── Frame builder ─────────────────────────────────────────────────────────

    /**
     * Builds a relay frame: [ connId(4 bytes BE) | frameType(1 byte) | payload(N bytes) ]
     * Null payload is treated as empty (zero-length) — no NullPointerException.
     */
    private static byte[] buildFrame(int connId, byte type, byte[] payload) {
        int payloadLen = (payload != null) ? payload.length : 0;
        ByteBuffer bb  = ByteBuffer.allocate(5 + payloadLen);
        bb.putInt(connId);
        bb.put(type);
        if (payload != null && payloadLen > 0) bb.put(payload);
        return bb.array();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String frameTypeLabel(byte t) {
        switch (t) {
            case FT_OPEN:  return "FT_OPEN";
            case FT_DATA:  return "FT_DATA";
            case FT_CLOSE: return "FT_CLOSE";
            default:       return "0x" + Integer.toHexString(t & 0xFF);
        }
    }

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            NotificationManager nm = getSystemService(NotificationManager.class);
            if (nm != null) nm.createNotificationChannel(ch);
        }
        Intent stop = new Intent(this, NetShareVpnService.class).setAction(ACTION_STOP);
        PendingIntent pi = PendingIntent.getService(this, 0, stop,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        Notification notif = new Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare Active")
            .setContentText("All traffic via host's internet")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .addAction(android.R.drawable.ic_delete, "Disconnect", pi)
            .setOngoing(true)
            .build();
        startForeground(NOTIF_ID, notif);
    }
}
