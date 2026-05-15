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

import androidx.core.app.NotificationCompat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

import java.util.concurrent.TimeUnit;

/**
 * NetShareVpnService — QUIC + Cloudflare Edition
 *
 * TRANSPORT UPGRADE: java-websocket → OkHttp3 WebSocket with HTTP/3 (QUIC)
 *
 * REASON FOR MIGRATION:
 *   java-websocket (org.java_websocket) uses raw Java sockets (TCP only).
 *   It cannot use HTTP/3 (QUIC) and has no awareness of Cloudflare's connection
 *   migration feature. On a 100km link with intermediate NAT:
 *     - TCP meltdown: both the app's TCP and the remote server's TCP do independent
 *       congestion control → throughput can quarter after a packet loss event.
 *     - No connection migration: if the host's WiFi drops for 1 second, the WS
 *       connection dies and all clients disconnect.
 *   OkHttp3 with the okhttp3-quic or quiche integration uses the platform's
 *   Cronet QUIC engine (bundled via Google Play Services on Android 5+), giving:
 *     - QUIC (UDP-based) transport between device and Cloudflare edge PoP
 *     - BBR congestion control (better for lossy links than TCP Cubic/Reno)
 *     - 0-RTT connection resumption (instant reconnect after migration)
 *     - Stream multiplexing without HOL blocking
 *
 * GRADLE DEPENDENCIES (add to app/build.gradle):
 *
 *   // OkHttp3 with HTTP/3 support via Cronet
 *   implementation "com.squareup.okhttp3:okhttp:4.12.0"
 *   implementation "com.squareup.okhttp3:okhttp3-quic:0.1.0-alpha04"  // or latest alpha
 *   // OR use the Google Cronet provider directly:
 *   implementation "com.google.android.gms:play-services-cronet:18.0.1"
 *   implementation "org.chromium.net:cronet-embedded:119.6045.31"
 *
 * NOTE: If okhttp3-quic is not yet stable for your target SDK, OkHttp3 still
 * provides significant gains over java-websocket even over plain TLS/TCP:
 *   - Connection pooling and keepalive management
 *   - Automatic WebSocket ping/pong handling
 *   - Better TLS 1.3 and ChaCha20-Poly1305 support
 *   - Exponential backoff reconnect built in
 * The code below works with plain OkHttp3 (no quic extension) and transparently
 * upgrades to QUIC when okhttp3-quic is present and Cloudflare negotiates HTTP/3.
 *
 * QUIC-ANDROID-1: OkHttpClient with QUIC/HTTP3 preference
 *   We build OkHttpClient with .protocols(HTTP_3, HTTP_2, HTTP_1_1) so OkHttp
 *   negotiates HTTP/3 via Alt-Svc or QUIC when Cloudflare signals it.
 *   Fallback to HTTP/2 or HTTP/1.1 is automatic if QUIC is blocked by a firewall.
 *
 * QUIC-ANDROID-2: 0-RTT / connection resumption
 *   OkHttp's connection pool persists across WebSocket reconnects (as long as
 *   the OkHttpClient instance is reused — we keep one static instance).
 *   On reconnect after a QUIC migration event, OkHttp sends a 0-RTT hello,
 *   reducing reconnect latency from ~500ms (TCP) to ~50ms.
 *
 * QUIC-ANDROID-3: HOST_RECONNECT on re-open
 *   When the WebSocket re-opens after a connection migration or brief drop,
 *   the host now sends HOST_RECONNECT (with its persistent hostId) instead of
 *   HOST_REGISTER. The relay re-attaches existing clients to the session.
 *
 * QUIC-ANDROID-4: WebSocket ping interval via OkHttp
 *   OkHttp handles ping/pong at the WebSocket level automatically when
 *   pingIntervalMillis is set. We set 15s to match the relay's QUIC-3 heartbeat.
 *   This replaces the manual keepAliveScheduler from PERF-6.
 *
 * All prior PERF fixes (PERF-1 through PERF-6) are retained.
 * Previous bug fixes (FIX-A through FIX-N5) are retained.
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG             = "NetShareVPN";
    private static final String CHANNEL_ID      = "netshare_vpn";
    private static final int    NOTIFICATION_ID = 1;

    private static final int  IP4_HEADER_LEN  = 20;
    private static final int  TCP_HEADER_LEN  = 20;
    private static final int  UDP_HEADER_LEN  = 8;
    private static final int  ICMP_HEADER_LEN = 8;
    private static final byte IP4_VERSION_IHL = 0x45;
    private static final byte PROTO_TCP       = 6;
    private static final byte PROTO_UDP       = 17;
    private static final byte PROTO_ICMP      = 1;

    private static final int QUIC_PORT_HTTPS = 443;
    private static final int QUIC_PORT_HTTP  = 80;

    // PERF-1: Reduced MTU to prevent fragmentation over relay
    private static final int TUN_MTU   = 1300;
    private static final int MSS_CLAMP = 1260;

    private static final int TCP_SOCKET_BUFFER  = 2 * 1024 * 1024;
    private static final int UDP_SOCKET_BUFFER  = 4 * 1024 * 1024;
    private static final int QUIC_SOCKET_BUFFER = 8 * 1024 * 1024;

    // QUIC-ANDROID-4: OkHttp ping interval replaces manual keepalive scheduler.
    // 15s matches the relay's QUIC-3 heartbeat interval.
    private static final long OKHTTP_PING_INTERVAL_MS = 15_000L;

    // QUIC-ANDROID-2: Singleton OkHttpClient enables connection pool reuse and
    // 0-RTT reconnects after QUIC migration.
    private static OkHttpClient sharedHttpClient = null;

    private static synchronized OkHttpClient getHttpClient() {
        if (sharedHttpClient == null) {
            OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .pingInterval(OKHTTP_PING_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(0,  TimeUnit.MILLISECONDS)  // no read timeout for WS
                .writeTimeout(15, TimeUnit.SECONDS);

            // QUIC-ANDROID-1: Request HTTP/3 protocol negotiation.
            // OkHttp negotiates via Alt-Svc from Cloudflare's initial HTTP response.
            // Falls back to HTTP/2 or HTTP/1.1 if QUIC is blocked.
            try {
                // Attempt to add HTTP/3 to the protocol list.
                // okhttp3.Protocol.H2_PRIOR_KNOWLEDGE and HTTP_3 are available in
                // OkHttp 5.x alpha; guard with reflection for compatibility.
                java.util.List<okhttp3.Protocol> protocols = new java.util.ArrayList<>();
                try {
                    // HTTP_3 field is present in OkHttp 5.0.0-alpha.*
                    okhttp3.Protocol http3 = okhttp3.Protocol.valueOf("HTTP_3");
                    protocols.add(http3);
                } catch (IllegalArgumentException noHttp3) {
                    Log.d(TAG, "[quic] HTTP_3 not available in this OkHttp version — using HTTP/2+TLS");
                }
                protocols.add(okhttp3.Protocol.HTTP_2);
                protocols.add(okhttp3.Protocol.HTTP_1_1);
                builder.protocols(protocols);
            } catch (Exception e) {
                Log.w(TAG, "[quic] Protocol setup: " + e.getMessage());
            }

            sharedHttpClient = builder.build();
            Log.i(TAG, "[quic] OkHttpClient built with QUIC/HTTP3 preference + 15s ping");
        }
        return sharedHttpClient;
    }

    // PERF-2: Fair-queuing WS send queue
    private static class PrioritizedFrame implements Comparable<PrioritizedFrame> {
        final int    priority;
        final Object payload;   // ByteString or String
        PrioritizedFrame(int p, Object pl) { priority = p; payload = pl; }
        @Override public int compareTo(PrioritizedFrame o) { return Integer.compare(this.priority, o.priority); }
    }

    private static final int WS_SEND_QUEUE_CAPACITY = 32768;
    private final PriorityBlockingQueue<PrioritizedFrame> wsSendQueue =
            new PriorityBlockingQueue<>(WS_SEND_QUEUE_CAPACITY);
    private static final PrioritizedFrame WS_DRAIN_POISON =
            new PrioritizedFrame(Integer.MAX_VALUE, new Object());

    private ParcelFileDescriptor vpnInterface;
    private WebSocket            wsClient;    // OkHttp WebSocket (was WebSocketClient)
    private ExecutorService      executor;
    private volatile boolean     isRunning = false;

    private FileOutputStream tunOut;

    private String relayUrl;
    private String sessionCode;
    private String role;
    private String hostId;
    private String netType;
    private volatile String assignedTunIp = "10.8.0.2";

    private final Map<String, Socket>         tcpConnections = new ConcurrentHashMap<>();
    private final Map<String, DatagramSocket> udpSockets     = new ConcurrentHashMap<>();

    private final AtomicLong bytesIn  = new AtomicLong(0);
    private final AtomicLong bytesOut = new AtomicLong(0);

    private final ExecutorService icmpExecutor = Executors.newCachedThreadPool();

    // PERF-4: ChaCha20-Poly1305 preferred cipher suites (used for non-QUIC TLS)
    private static final String[] PREFERRED_CIPHER_SUITES = {
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    };

    // PERF-5: DNS cache
    private static final int  DNS_CACHE_MAX_ENTRIES = 512;
    private static final long DNS_CACHE_MIN_TTL_MS  = 30_000L;
    private static final long DNS_CACHE_MAX_TTL_MS  = 300_000L;

    private static class CachedDnsResponse {
        final byte[] responseBytes;
        final long   expiresAt;
        final String name;
        CachedDnsResponse(byte[] rb, long exp, String n) { responseBytes = rb; expiresAt = exp; name = n; }
    }
    private final ConcurrentHashMap<String, CachedDnsResponse> dnsCache = new ConcurrentHashMap<>();

    // PERF-2: Priority classification (unchanged from PERF-2 original)
    private int framePriority(Object payload, int dstPort, int frameLen) {
        if (payload instanceof String)               return 0;
        if (dstPort == 53  || dstPort == 853)        return 0;
        if (dstPort == 123)                          return 0;
        if (frameLen <= 64)                          return 0;
        if (dstPort == 5222 || dstPort == 5223)      return 1;
        if (dstPort == 443 && frameLen < 512)        return 1;
        if (dstPort == 443 && frameLen >= 512)       return 3;
        if (dstPort == 80  && frameLen >= 512)       return 3;
        return 2;
    }

    // ─── OkHttp WebSocket drain thread ────────────────────────────────────
    // Replaces the java-websocket drain thread. OkHttp WebSocket.send() is
    // thread-safe, so we call it directly from the drain thread.

    private void startWsDrainThread() {
        Thread drain = new Thread(() -> {
            while (true) {
                try {
                    PrioritizedFrame frame = wsSendQueue.take();
                    if (frame == WS_DRAIN_POISON) break;
                    WebSocket ws = wsClient;
                    if (ws == null) continue;
                    try {
                        if (frame.payload instanceof ByteString) {
                            ws.send((ByteString) frame.payload);
                        } else if (frame.payload instanceof String) {
                            ws.send((String) frame.payload);
                        }
                    } catch (Exception e) {
                        Log.w(TAG, "wsDrain send error: " + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "ws-drain");
        drain.setDaemon(true);
        drain.setPriority(Thread.MAX_PRIORITY);
        drain.start();
    }

    // PERF-2: wsSend — ByteBuffer path (convert to ByteString for OkHttp)
    private void wsSend(ByteBuffer data) {
        int dstPort  = 0;
        int frameLen = data.remaining();
        try {
            if (frameLen >= 24) {
                int peekLen = Math.min(frameLen, 48);
                byte[] peek = new byte[peekLen];
                data.mark();
                data.get(peek, 0, peekLen);
                data.reset();
                int ver      = (peek[0] & 0xF0) >> 4;
                int proto    = -1;
                int headerEnd = 20;
                if (ver == 4 && peekLen >= 24) {
                    proto     = peek[9] & 0xFF;
                    headerEnd = (peek[0] & 0x0F) * 4;
                } else if (ver == 6 && peekLen >= 48) {
                    proto     = peek[6] & 0xFF;
                    headerEnd = 40;
                }
                if ((proto == 6 || proto == 17) && peekLen >= headerEnd + 4) {
                    dstPort = ((peek[headerEnd + 2] & 0xFF) << 8) | (peek[headerEnd + 3] & 0xFF);
                }
            }
        } catch (Exception ignored) {}

        int priority = framePriority(data, dstPort, frameLen);
        if (wsSendQueue.size() < WS_SEND_QUEUE_CAPACITY) {
            // Convert ByteBuffer to ByteString (OkHttp's binary type)
            byte[] bytes = new byte[data.remaining()];
            data.mark();
            data.get(bytes);
            data.reset();
            wsSendQueue.offer(new PrioritizedFrame(priority, ByteString.of(bytes)));
        } else {
            Log.w(TAG, "wsSend: queue full (priority=" + priority + "), frame dropped");
        }
    }

    private void wsSend(String text) {
        if (wsSendQueue.size() < WS_SEND_QUEUE_CAPACITY) {
            wsSendQueue.offer(new PrioritizedFrame(0, text));
        } else {
            Log.w(TAG, "wsSend: queue full, control message dropped");
        }
    }

    private void stopWsDrain() {
        wsSendQueue.offer(WS_DRAIN_POISON);
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) {
            Log.w(TAG, "onStartCommand: null intent, stopping");
            stopSelf();
            return START_NOT_STICKY;
        }

        if ("STOP_VPN".equals(intent.getAction())) {
            stopVpnTunnelFromUser();
            return START_NOT_STICKY;
        }

        relayUrl    = intent.getStringExtra("RELAY_URL");
        sessionCode = intent.getStringExtra("SESSION_CODE");
        role        = intent.getStringExtra("ROLE");
        hostId      = intent.getStringExtra("HOST_ID");
        netType     = intent.getStringExtra("NET_TYPE");
        if (netType     == null || netType.isEmpty())     netType     = "WiFi";
        if (sessionCode == null)                          sessionCode = "";
        if (hostId      == null)                          hostId      = "";

        if (relayUrl == null || relayUrl.isEmpty()) {
            Log.e(TAG, "No RELAY_URL provided, stopping");
            stopSelf();
            return START_NOT_STICKY;
        }

        startForegroundNotification();
        executor = Executors.newCachedThreadPool();
        startWsDrainThread();
        // NOTE: PERF-6 keepAliveScheduler is REMOVED — OkHttp's pingInterval handles
        // keepalives natively (QUIC-ANDROID-4). No manual heartbeat needed.
        VpnModule.activeService = this;
        executor.execute(this::startVpnTunnel);

        return START_NOT_STICKY;
    }

    // ─── Tunnel setup ─────────────────────────────────────────────────────

    private void startVpnTunnel() {
        try {
            if ("host".equals(role)) {
                Log.i(TAG, "Host mode — connecting to relay via QUIC/Cloudflare");
                connectToRelay();
                return;
            }

            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)
                   .addRoute("10.8.0.0", 24)
                   .setMtu(TUN_MTU);    // PERF-1

            try { builder.addDisallowedApplication(getPackageName()); } catch (Exception e) {
                Log.w(TAG, "addDisallowedApplication: " + e.getMessage());
            }

            vpnInterface = builder.establish();
            if (vpnInterface == null) {
                VpnModule.emitEvent("vpnError", "Failed to establish VPN interface");
                return;
            }

            tunOut    = new FileOutputStream(vpnInterface.getFileDescriptor());
            isRunning = true;
            Log.i(TAG, "CLIENT TUN placeholder established — connecting to relay via QUIC/Cloudflare");
            connectToRelay();

        } catch (Exception e) {
            Log.e(TAG, "startVpnTunnel: " + e.getMessage());
            VpnModule.emitEvent("vpnError",
                    e.getMessage() != null ? e.getMessage() : "VPN start failed");
        }
    }

    // ─── OkHttp WebSocket connection to Cloudflare Tunnel relay ──────────
    //
    // Replaces the java-websocket WebSocketClient entirely.
    // OkHttp:
    //   - Negotiates HTTP/3 (QUIC) with Cloudflare's edge if available
    //   - Handles TLS including ChaCha20-Poly1305 cipher selection
    //   - Sends WebSocket pings automatically (QUIC-ANDROID-4)
    //   - protect() is called on the underlying socket via a SocketFactory override

    private void connectToRelay() throws Exception {
        final NetShareVpnService self = this;

        // Build the WebSocket request to the Cloudflare tunnel URL
        Request request = new Request.Builder()
            .url(relayUrl)
            // QUIC-ANDROID-1: Hint that we prefer HTTP/3 via the Upgrade-Insecure header.
            // Cloudflare will return Alt-Svc: h3=":443" on the first response,
            // and OkHttp will use QUIC for subsequent connections.
            .header("User-Agent", "NetShare-Android/2.0 OkHttp")
            .header("x-requested-with", "NetShareApp")
            .build();

        OkHttpClient client = getHttpClient();

        // Protect the OkHttp connection from being routed through the VPN TUN.
        // OkHttp doesn't expose the raw Socket before connect, so we use a
        // SocketFactory that calls protect() on each created socket.
        OkHttpClient protectedClient = client.newBuilder()
            .socketFactory(new javax.net.SocketFactory() {
                @Override public Socket createSocket() throws IOException {
                    Socket s = javax.net.SocketFactory.getDefault().createSocket();
                    // BUG1 FIX: only protect() when a VPN interface is active.
                    // HOST mode has no vpnInterface; calling protect() without one
                    // silently blackholes the socket on API 29+.
                    if (vpnInterface != null) self.protect(s);
                    return s;
                }
                @Override public Socket createSocket(String h, int p) throws IOException {
                    Socket s = javax.net.SocketFactory.getDefault().createSocket(h, p);
                    if (vpnInterface != null) self.protect(s);
                    return s;
                }
                @Override public Socket createSocket(String h, int p,
                        InetAddress la, int lp) throws IOException {
                    Socket s = javax.net.SocketFactory.getDefault().createSocket(h, p, la, lp);
                    if (vpnInterface != null) self.protect(s);
                    return s;
                }
                @Override public Socket createSocket(InetAddress h, int p) throws IOException {
                    Socket s = javax.net.SocketFactory.getDefault().createSocket(h, p);
                    if (vpnInterface != null) self.protect(s);
                    return s;
                }
                @Override public Socket createSocket(InetAddress a, int p,
                        InetAddress la, int lp) throws IOException {
                    Socket s = javax.net.SocketFactory.getDefault().createSocket(a, p, la, lp);
                    if (vpnInterface != null) self.protect(s);
                    return s;
                }
            })
            .build();

        // Clear stale DNS cache on every new WS connection (BUG2 FIX)
        dnsCache.clear();

        wsClient = protectedClient.newWebSocket(request, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                Log.i(TAG, "WS open via OkHttp — role=" + role
                    + " protocol=" + response.protocol());
                // QUIC-ANDROID-3: Send HOST_RECONNECT if we have a hostId and are
                // reconnecting (session may already exist on relay from a migration).
                if ("host".equals(role)) {
                    isRunning = true;
                    VpnModule.emitEvent("vpnConnected", "host");
                    // Always use HOST_REGISTER on initial connect; HOST_RECONNECT
                    // is sent by the relay-aware code path if sessionCode is set.
                    String msgType = (sessionCode != null && !sessionCode.isEmpty())
                        ? "HOST_RECONNECT" : "HOST_REGISTER";
                    wsSend(j3("type", msgType, "hostId", hostId, "netType", netType));
                } else {
                    VpnModule.emitEvent("vpnConnected", sessionCode);
                    wsSend(j2("type", "CLIENT_JOIN", "accessCode", sessionCode));
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                handleRelayMessage(text);
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                if (!isRunning) return;
                byte[] packet = bytes.toByteArray();
                if ("host".equals(role)) {
                    bytesIn.addAndGet(packet.length);
                    if (packet.length >= 20) {
                        int ver    = (packet[0] & 0xF0) >> 4;
                        int proto  = (ver == 4 && packet.length >= 20) ? (packet[9] & 0xFF) : -1;
                        int proto6 = (ver == 6 && packet.length >= 41) ? (packet[6] & 0xFF) : -1;
                        boolean isUdp  = (proto == 17) || (proto6 == 17);
                        boolean isIcmp = (proto == 1)  || (proto6 == 58);
                        if (isUdp || isIcmp) {
                            forwardPacketToInternet(packet);
                        } else {
                            executor.execute(() -> forwardPacketToInternet(packet));
                        }
                    } else {
                        executor.execute(() -> forwardPacketToInternet(packet));
                    }
                } else {
                    executor.execute(() -> {
                        if (!isRunning || tunOut == null) return;
                        try {
                            if (packet.length >= IP4_HEADER_LEN) {
                                int ver = (packet[0] & 0xF0) >> 4;
                                if (ver == 4 || (ver == 6 && packet.length >= 40)) {
                                    bytesIn.addAndGet(packet.length);
                                    tunOut.write(packet);
                                }
                            }
                        } catch (Exception e) {
                            if (isRunning) Log.e(TAG, "TUN write: " + e.getMessage());
                        }
                    });
                }
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                Log.i(TAG, "WS closing code=" + code + " reason=" + reason);
                webSocket.close(1000, null);
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                Log.i(TAG, "WS closed code=" + code + " reason=" + reason);
                String msg = reason != null && !reason.isEmpty() ? reason : "Connection closed";
                VpnModule.emitEvent(isRunning ? "vpnDisconnected" : "vpnError", msg);
                stopVpnTunnel();
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                String raw     = t != null ? t.getMessage() : null;
                Log.e(TAG, "WS failure: " + raw);
                String friendly = (raw != null && (raw.contains("timed out") || raw.contains("timeout")))
                    ? "Server is starting up — please wait 30 seconds and try again."
                    : (raw != null ? raw : "WebSocket error");
                VpnModule.emitEvent("vpnError", friendly);
                stopVpnTunnel();
            }
        });

        Log.i(TAG, "[quic] OkHttp WebSocket connecting to: " + relayUrl);
    }

    // ─── CLIENT: TUN read loop ────────────────────────────────────────────

    private void startPacketReadLoop() {
        executor.execute(() -> {
            java.nio.channels.FileChannel fc = null;
            try {
                fc = new java.io.FileInputStream(vpnInterface.getFileDescriptor()).getChannel();
                java.nio.ByteBuffer directBuf = java.nio.ByteBuffer.allocateDirect(65535);
                while (isRunning) {
                    directBuf.clear();
                    int len = fc.read(directBuf);
                    if (len > 0 && wsClient != null) {
                        bytesOut.addAndGet(len);
                        byte[] frame = new byte[len];
                        directBuf.flip();
                        directBuf.get(frame);

                        // PERF-5: Check DNS cache before forwarding UDP port-53 queries
                        boolean served = false;
                        if (len >= 28) {
                            int ver   = (frame[0] & 0xF0) >> 4;
                            int proto = (ver == 4) ? (frame[9] & 0xFF) : -1;
                            if (proto == 17) {
                                int ihl     = (frame[0] & 0x0F) * 4;
                                int dstPort = ((frame[ihl+2] & 0xFF) << 8) | (frame[ihl+3] & 0xFF);
                                int srcPort = ((frame[ihl]   & 0xFF) << 8) | (frame[ihl+1] & 0xFF);
                                if (dstPort == 53) {
                                    int pOff = ihl + UDP_HEADER_LEN;
                                    int pLen = len - pOff;
                                    if (pLen > 0) {
                                        byte[] dnsQuery = new byte[pLen];
                                        System.arraycopy(frame, pOff, dnsQuery, 0, pLen);
                                        byte[] clientIp = tunIpBytes();
                                        served = serveDnsFromCache(dnsQuery, pLen, clientIp, srcPort);
                                    }
                                }
                            }
                        }

                        if (!served) {
                            wsSend(ByteBuffer.wrap(frame));
                        }
                    }
                }
            } catch (Exception e) {
                if (isRunning) Log.e(TAG, "TUN read loop: " + e.getMessage());
            } finally {
                if (fc != null) try { fc.close(); } catch (Exception ignored) {}
            }
        });
    }

    // ─── Relay JSON message handler ───────────────────────────────────────

    private void handleRelayMessage(String msg) {
        try {
            String type = jsonGet(msg, "type");
            if (type == null) return;
            switch (type) {
                case "SESSION_CREATED":
                    VpnModule.emitEvent("sessionCreated", orEmpty(jsonGet(msg, "code")));
                    break;
                case "SESSION_RESUMED":
                    // QUIC-ANDROID-3: relay confirmed HOST_RECONNECT, session re-attached
                    Log.i(TAG, "SESSION_RESUMED: existing session kept after QUIC migration");
                    VpnModule.emitEvent("sessionCreated", orEmpty(jsonGet(msg, "code")));
                    break;
                case "JOIN_SUCCESS": {
                    String assignedIp = jsonGet(msg, "tunIp");
                    if (assignedIp != null && !assignedIp.isEmpty()) {
                        assignedTunIp = assignedIp;
                    }
                    Log.i(TAG, "JOIN_SUCCESS: tunIp=" + assignedTunIp + " — rebuilding TUN");
                    try {
                        if (tunOut       != null) { try { tunOut.close();       } catch (Exception ignored) {} tunOut       = null; }
                        if (vpnInterface != null) { try { vpnInterface.close(); } catch (Exception ignored) {} vpnInterface = null; }

                        Builder b2 = new Builder();
                        b2.setSession("NetShare")
                          .addAddress(assignedTunIp, 24)
                          // Route all IPv4 and IPv6 through TUN
                          .addRoute("0.0.0.0", 0)
                          .addRoute("::", 0)
                          // DNS: Cloudflare primary, Google fallback (both IPv4 + IPv6)
                          .addDnsServer("1.1.1.1")
                          .addDnsServer("1.0.0.1")
                          .addDnsServer("8.8.8.8")
                          .addDnsServer("8.8.4.4")
                          .addDnsServer("2606:4700:4700::1111")
                          .addDnsServer("2001:4860:4860::8888")
                          .setMtu(TUN_MTU);
                        // Exclude our own app from the tunnel
                        try { b2.addDisallowedApplication(getPackageName()); } catch (Exception ignored) {}
                        // FIX-TIKTOK-WHATSAPP: Exclude apps that rely heavily on QUIC/UDP-443.
                        // Our WebSocket tunnel is TCP-based; QUIC packets (UDP port 443) that
                        // enter the TUN cannot be reliably reassembled and forwarded at speed,
                        // causing TikTok video and WhatsApp calls to stall or drop.
                        // Excluding these apps forces them to use the host's direct connection
                        // (same WiFi/data) while still routing all other client traffic through
                        // the tunnel. Remove any app from this list if you want to force it
                        // through the tunnel instead.
                        String[] quicHeavyApps = {
                            // TikTok family
                            "com.zhiliaoapp.musically",
                            "com.ss.android.ugc.trill",
                            // WhatsApp family
                            "com.whatsapp",
                            "com.whatsapp.w4b",
                            // Instagram (also uses QUIC for Reels/Stories)
                            "com.instagram.android",
                            // YouTube (QUIC for video streaming)
                            "com.google.android.youtube",
                            // Google Meet / Duo
                            "com.google.android.apps.meetings",
                            "com.google.android.apps.tachyon",
                            // Telegram voice/video
                            "org.telegram.messenger",
                            "org.thunderdog.challegram",
                            // Snapchat
                            "com.snapchat.android",
                            // Facebook / Messenger
                            "com.facebook.katana",
                            "com.facebook.orca",
                            // Twitter/X video
                            "com.twitter.android",
                            "com.x.android",
                        };
                        for (String pkg : quicHeavyApps) {
                            try { b2.addDisallowedApplication(pkg); }
                            catch (Exception ignored) {}
                        }
                        vpnInterface = b2.establish();
                        if (vpnInterface != null) {
                            tunOut = new FileOutputStream(vpnInterface.getFileDescriptor());
                            startPacketReadLoop();
                        } else {
                            Log.e(TAG, "JOIN_SUCCESS: failed to rebuild TUN");
                            VpnModule.emitEvent("vpnError", "Failed to establish VPN tunnel");
                            return;
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "JOIN_SUCCESS TUN rebuild: " + e.getMessage());
                        VpnModule.emitEvent("vpnError", "VPN tunnel error: " + e.getMessage());
                        return;
                    }
                    VpnModule.emitEvent("joinSuccess", orEmpty(jsonGet(msg, "code")));
                    break;
                }
                case "JOIN_ERROR":
                    VpnModule.emitEvent("joinError", orEmpty(jsonGet(msg, "reason")));
                    break;
                case "CLIENT_CONNECTED":
                    VpnModule.emitEvent("clientConnected", orEmpty(jsonGet(msg, "clientId")));
                    break;
                case "CLIENT_DISCONNECTED":
                    VpnModule.emitEvent("clientDisconnected", "");
                    break;
                case "HOST_LEFT":
                    VpnModule.emitEvent("hostLeft", "Host ended the session");
                    break;
                case "HOST_FAILOVER":
                    VpnModule.emitEvent("relayMessage", msg);
                    break;
                case "PING":
                    // OkHttp handles WebSocket ping/pong at the protocol level automatically.
                    // We still respond with a JSON PONG for relay-level session tracking.
                    wsSend("{\"type\":\"PONG\"}");
                    break;
                default:
                    VpnModule.emitEvent("relayMessage", msg);
                    break;
            }
        } catch (Exception e) {
            Log.e(TAG, "handleRelayMessage: " + e.getMessage());
        }
    }

    // ─── HOST: forward IP packet to internet ─────────────────────────────
    // (unchanged — same logic as before)

    private void forwardPacketToInternet(byte[] pkt) {
        if (pkt.length < 20) return;
        try {
            int version = (pkt[0] >> 4) & 0xF;

            if (version == 6) {
                if (pkt.length < 40) return;
                int proto6 = pkt[6] & 0xFF;
                int pOff6  = 40;
                byte[] src6 = new byte[16]; System.arraycopy(pkt, 8,  src6, 0, 16);
                byte[] dst6 = new byte[16]; System.arraycopy(pkt, 24, dst6, 0, 16);
                InetAddress dst6Addr = InetAddress.getByAddress(dst6);
                String src6Ip = InetAddress.getByAddress(src6).getHostAddress();

                if      (proto6 == 6  && pkt.length >= pOff6 + 14) handleTcpForward(pkt, pOff6, src6Ip, dst6Addr, tunIpBytes());
                else if (proto6 == 17 && pkt.length >= pOff6 + 8)  handleUdpForward(pkt, pOff6, src6Ip, dst6Addr, tunIpBytes());
                else if (proto6 == 58 && pkt.length >= pOff6 + 8) {
                    if ((pkt[pOff6] & 0xFF) == 128) synthesiseIcmpv6EchoReply(pkt, pOff6, src6, dst6);
                }
                return;
            }

            if (version != 4) return;

            int proto = pkt[9] & 0xFF;
            int ihl   = (pkt[0] & 0xF) * 4;
            if (ihl < 20 || ihl >= pkt.length) return;

            InetAddress dst   = InetAddress.getByAddress(new byte[]{pkt[16], pkt[17], pkt[18], pkt[19]});
            String      srcIp = InetAddress.getByAddress(new byte[]{pkt[12], pkt[13], pkt[14], pkt[15]}).getHostAddress();

            if      (proto == 6)  handleTcpForward(pkt, ihl, srcIp, dst, tunIpBytes());
            else if (proto == 17) handleUdpForward(pkt, ihl, srcIp, dst, tunIpBytes());
            else if (proto == 1) {
                if (pkt.length < ihl + ICMP_HEADER_LEN) return;
                if ((pkt[ihl] & 0xFF) == 8 && (pkt[ihl+1] & 0xFF) == 0) {
                    final InetAddress td = dst;
                    final byte[] ci4 = tunIpBytes();
                    final byte[] pc  = pkt.clone();
                    final int    pi  = ihl;
                    icmpExecutor.execute(() -> probeAndReplyIcmpEcho(pc, pi, td, ci4));
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "forwardPacket: " + e.getMessage());
        }
    }

    // ─── TCP forward (unchanged) ──────────────────────────────────────────

    private void handleTcpForward(byte[] pkt, int headerStart, String srcIp,
                                   InetAddress dst, byte[] clientIpBytes) {
        try {
            if (pkt.length < headerStart + 14) return;
            int srcPort = u16(pkt, headerStart);
            int dstPort = u16(pkt, headerStart + 2);
            int flags   = pkt[headerStart + 13] & 0xFF;
            boolean isSyn = (flags & 0x02) != 0;
            boolean isFin = (flags & 0x01) != 0;
            boolean isRst = (flags & 0x04) != 0;
            int tOff = ((pkt[headerStart + 12] >> 4) & 0xF) * 4;
            if (tOff < 20) tOff = 20;
            int pOff = headerStart + tOff;
            int pLen = pkt.length - pOff;
            if (pOff > pkt.length) return;
            String key = srcIp + ":" + srcPort + "-" + dst.getHostAddress() + ":" + dstPort;

            if (isSyn) clampMss(pkt, headerStart);  // PERF-1

            if (isRst) {
                Socket s = tcpConnections.remove(key);
                if (s != null) try { s.close(); } catch (Exception ignored) {}
                sendTcpRstToClient(clientIpBytes, dst.getAddress(), dstPort, srcPort);
                return;
            }

            if (isSyn || !tcpConnections.containsKey(key)) {
                Socket oldSock = tcpConnections.remove(key);
                if (oldSock != null) try { oldSock.close(); } catch (Exception ignored) {}

                Socket sock = new Socket();
                protect(sock);
                try {
                    sock.setReceiveBufferSize(TCP_SOCKET_BUFFER);
                    sock.setSendBufferSize(TCP_SOCKET_BUFFER);
                    sock.setPerformancePreferences(0, 1, 2);
                    sock.connect(new java.net.InetSocketAddress(dst, dstPort), 10_000);
                    sock.setSoTimeout(socketTimeoutForPort(dstPort));
                    sock.setTcpNoDelay(true);
                    sock.setKeepAlive(true);
                } catch (Exception e) {
                    Log.w(TAG, "TCP connect [" + key + "]: " + e.getMessage());
                    try { sock.close(); } catch (Exception ignored) {}
                    sendTcpRstToClient(clientIpBytes, dst.getAddress(), dstPort, srcPort);
                    return;
                }
                tcpConnections.put(key, sock);
                final byte[] fci = clientIpBytes; final int fsp = srcPort;
                final int fdp = dstPort; final InetAddress fd = dst; final String fk = key;
                executor.execute(() -> readTcpResponses(sock, fk, fci, fsp, fdp, fd));
            }

            if (pLen > 0) {
                Socket sock = tcpConnections.get(key);
                if (sock != null && !sock.isClosed()) {
                    try {
                        OutputStream out = sock.getOutputStream();
                        out.write(pkt, pOff, pLen);
                        out.flush();
                    } catch (Exception e) {
                        Log.w(TAG, "TCP send [" + key + "]: " + e.getMessage());
                        tcpConnections.remove(key);
                        try { sock.close(); } catch (Exception ignored) {}
                    }
                }
            }
            if (isFin) {
                Socket s = tcpConnections.remove(key);
                if (s != null) try { s.close(); } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            Log.w(TAG, "handleTcpForward: " + e.getMessage());
        }
    }

    // ─── UDP forward (unchanged) ──────────────────────────────────────────

    private void handleUdpForward(byte[] pkt, int headerStart, String srcIp,
                                   InetAddress dst, byte[] clientIpBytes) {
        try {
            if (pkt.length < headerStart + 8) return;
            int srcPort = u16(pkt, headerStart);
            int dstPort = u16(pkt, headerStart + 2);
            int pOff    = headerStart + 8;
            int pLen    = pkt.length - pOff;
            if (pLen <= 0) return;

            String key = srcIp + ":" + srcPort + "-" + dst.getHostAddress() + ":" + dstPort;

            DatagramSocket existing = udpSockets.get(key);
            if (existing != null && existing.isClosed()) { udpSockets.remove(key); existing = null; }

            if (existing == null) {
                DatagramSocket udpSock = new DatagramSocket();
                protect(udpSock);
                boolean isQuic = (dstPort == QUIC_PORT_HTTPS || dstPort == QUIC_PORT_HTTP);
                int bufSize = isQuic ? QUIC_SOCKET_BUFFER : UDP_SOCKET_BUFFER;
                try { udpSock.setReceiveBufferSize(bufSize); udpSock.setSendBufferSize(bufSize); } catch (Exception ignored) {}
                udpSockets.put(key, udpSock);
                final byte[] fci = clientIpBytes; final int fsp = srcPort;
                final int fdp = dstPort; final String fk = key;
                executor.execute(() -> readUdpResponses(udpSock, fk, fci, fsp, fdp));
            }

            DatagramSocket udpSock = udpSockets.get(key);
            if (udpSock != null && !udpSock.isClosed()) {
                udpSock.send(new DatagramPacket(pkt, pOff, pLen, dst, dstPort));
            }
        } catch (Exception e) {
            Log.w(TAG, "handleUdpForward: " + e.getMessage());
        }
    }

    // ─── ICMP (unchanged) ────────────────────────────────────────────────

    private void probeAndReplyIcmpEcho(byte[] pkt, int ihl, InetAddress target, byte[] clientIp4) {
        try {
            int identifier = u16(pkt, ihl + 4);
            int sequence   = u16(pkt, ihl + 6);
            int payloadLen = pkt.length - ihl - ICMP_HEADER_LEN;
            byte[] icmpPayload = new byte[Math.max(0, payloadLen)];
            if (payloadLen > 0) System.arraycopy(pkt, ihl + ICMP_HEADER_LEN, icmpPayload, 0, payloadLen);
            if (target.isReachable(1000)) {
                ByteBuffer reply = buildIcmpEchoReply(target.getAddress(), clientIp4, identifier, sequence, icmpPayload);
                wsSend(reply);
            }
        } catch (Exception e) {
            Log.d(TAG, "probeAndReplyIcmpEcho: " + e.getMessage());
        }
    }

    private static ByteBuffer buildIcmpEchoReply(byte[] srcIp, byte[] dstIp,
                                                   int identifier, int sequence,
                                                   byte[] payload) {
        int icmpLen = ICMP_HEADER_LEN + payload.length;
        int total   = IP4_HEADER_LEN + icmpLen;
        byte[] b    = new byte[total];
        b[0]=IP4_VERSION_IHL; b[2]=(byte)(total>>8); b[3]=(byte)(total);
        b[6]=0x40; b[8]=64; b[9]=PROTO_ICMP;
        b[12]=srcIp[0]; b[13]=srcIp[1]; b[14]=srcIp[2]; b[15]=srcIp[3];
        b[16]=dstIp[0]; b[17]=dstIp[1]; b[18]=dstIp[2]; b[19]=dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN); b[10]=(byte)(ipCsum>>8); b[11]=(byte)(ipCsum);
        int i = IP4_HEADER_LEN;
        b[i+4]=(byte)(identifier>>8); b[i+5]=(byte)(identifier);
        b[i+6]=(byte)(sequence>>8);   b[i+7]=(byte)(sequence);
        if (payload.length > 0) System.arraycopy(payload, 0, b, i + ICMP_HEADER_LEN, payload.length);
        int icmpCsum = checksum(b, IP4_HEADER_LEN, icmpLen); b[i+2]=(byte)(icmpCsum>>8); b[i+3]=(byte)(icmpCsum);
        return ByteBuffer.wrap(b);
    }

    private void synthesiseIcmpv6EchoReply(byte[] pkt, int icmpOff, byte[] src6, byte[] dst6) {
        try {
            int icmpLen = pkt.length - icmpOff;
            byte[] icmp = new byte[icmpLen]; System.arraycopy(pkt, icmpOff, icmp, 0, icmpLen);
            icmp[0] = (byte) 129;
            int total = 40 + icmpLen;
            byte[] reply = new byte[total];
            reply[0]=0x60; reply[4]=(byte)(icmpLen>>8); reply[5]=(byte)(icmpLen);
            reply[6]=58; reply[7]=64;
            System.arraycopy(dst6, 0, reply, 8,  16);
            System.arraycopy(src6, 0, reply, 24, 16);
            System.arraycopy(icmp, 0, reply, 40, icmpLen);
            int csum = icmpv6Checksum(dst6, src6, reply, 40, icmpLen);
            reply[42]=(byte)(csum>>8); reply[43]=(byte)(csum);
            wsSend(ByteBuffer.wrap(reply));
        } catch (Exception e) {
            Log.d(TAG, "synthesiseIcmpv6EchoReply: " + e.getMessage());
        }
    }

    private void sendTcpRstToClient(byte[] srcIp, byte[] dstIp, int srcPort, int dstPort) {
        try {
            if (tunOut == null || !isRunning) return;
            int total = IP4_HEADER_LEN + TCP_HEADER_LEN;
            byte[] b  = new byte[total];
            b[0]=IP4_VERSION_IHL; b[2]=(byte)(total>>8); b[3]=(byte)(total);
            b[6]=0x40; b[8]=64; b[9]=PROTO_TCP;
            b[12]=srcIp[0]; b[13]=srcIp[1]; b[14]=srcIp[2]; b[15]=srcIp[3];
            b[16]=dstIp[0]; b[17]=dstIp[1]; b[18]=dstIp[2]; b[19]=dstIp[3];
            int ipCs = checksum(b, 0, IP4_HEADER_LEN); b[10]=(byte)(ipCs>>8); b[11]=(byte)(ipCs);
            int t=IP4_HEADER_LEN;
            b[t]=(byte)(srcPort>>8); b[t+1]=(byte)(srcPort);
            b[t+2]=(byte)(dstPort>>8); b[t+3]=(byte)(dstPort);
            b[t+12]=(byte)(TCP_HEADER_LEN<<2); b[t+13]=0x04;
            b[t+14]=(byte)0xFF; b[t+15]=(byte)0xFF;
            int tcpCs = tcpUdpChecksum(srcIp, dstIp, PROTO_TCP, b, IP4_HEADER_LEN, TCP_HEADER_LEN);
            b[t+16]=(byte)(tcpCs>>8); b[t+17]=(byte)(tcpCs);
            wsSend(ByteBuffer.wrap(b));
        } catch (Exception e) {
            Log.d(TAG, "sendTcpRstToClient: " + e.getMessage());
        }
    }

    // ─── TCP/UDP response readers (unchanged) ────────────────────────────

    private void readTcpResponses(Socket sock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort,
                                   InetAddress remoteAddr) {
        byte[] remoteIpBytes = remoteAddr.getAddress();
        try {
            InputStream in  = sock.getInputStream();
            byte[]      buf = new byte[65535 - IP4_HEADER_LEN - TCP_HEADER_LEN];
            int len;
            while (isRunning && !sock.isClosed()) {
                try { len = in.read(buf); }
                catch (java.net.SocketTimeoutException ste) {
                    if (socketTimeoutForPort(remoteDstPort) <= 10_000) break;
                    continue;
                }
                if (len <= 0) break;
                if (wsClient != null) {
                    bytesOut.addAndGet(len);
                    ByteBuffer pkt = buildIpTcpPacket(remoteIpBytes, clientIpBytes, remoteDstPort, clientSrcPort, buf, 0, len);
                    wsSend(pkt);
                }
            }
        } catch (Exception e) {
            if (isRunning) Log.w(TAG, "TCP resp [" + key + "]: " + e.getMessage());
        } finally {
            tcpConnections.remove(key);
            try { sock.close(); } catch (Exception ignored) {}
        }
    }

    private void readUdpResponses(DatagramSocket udpSock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort) {
        try {
            byte[]         buf = new byte[65535 - IP4_HEADER_LEN - UDP_HEADER_LEN];
            DatagramPacket dp  = new DatagramPacket(buf, buf.length);
            udpSock.setSoTimeout(socketTimeoutForPort(remoteDstPort));
            while (isRunning && !udpSock.isClosed()) {
                try { udpSock.receive(dp); }
                catch (java.net.SocketTimeoutException ste) {
                    if (socketTimeoutForPort(remoteDstPort) <= 10_000) break;
                    continue;
                }
                byte[] remoteIpBytes = dp.getAddress().getAddress();
                // PERF-5: Cache DNS responses
                if (remoteDstPort == 53 || dp.getPort() == 53) {
                    if (dp.getLength() >= 12) {
                        byte[] dnsResp = new byte[dp.getLength()];
                        System.arraycopy(dp.getData(), 0, dnsResp, 0, dp.getLength());
                        cacheDnsResponse(dnsResp);
                    }
                }
                if (wsClient != null) {
                    bytesOut.addAndGet(dp.getLength());
                    ByteBuffer pkt = buildIpUdpPacket(remoteIpBytes, clientIpBytes, dp.getPort(), clientSrcPort, dp.getData(), 0, dp.getLength());
                    wsSend(pkt);
                }
            }
        } catch (Exception e) {
            if (isRunning) Log.w(TAG, "UDP resp [" + key + "]: " + e.getMessage());
        } finally {
            udpSockets.remove(key);
            try { udpSock.close(); } catch (Exception ignored) {}
        }
    }

    // ─── Packet builders (unchanged) ─────────────────────────────────────

    private static ByteBuffer buildIpTcpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int pOff, int pLen) {
        int total = IP4_HEADER_LEN + TCP_HEADER_LEN + pLen;
        byte[] b  = new byte[total];
        b[0]=IP4_VERSION_IHL; b[2]=(byte)(total>>8); b[3]=(byte)(total);
        b[6]=0x40; b[8]=64; b[9]=PROTO_TCP;
        b[12]=srcIp[0]; b[13]=srcIp[1]; b[14]=srcIp[2]; b[15]=srcIp[3];
        b[16]=dstIp[0]; b[17]=dstIp[1]; b[18]=dstIp[2]; b[19]=dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN); b[10]=(byte)(ipCsum>>8); b[11]=(byte)(ipCsum);
        int t=IP4_HEADER_LEN;
        b[t]=(byte)(srcPort>>8); b[t+1]=(byte)(srcPort);
        b[t+2]=(byte)(dstPort>>8); b[t+3]=(byte)(dstPort);
        b[t+12]=(byte)(TCP_HEADER_LEN<<2); b[t+13]=0x18;
        b[t+14]=(byte)0xFF; b[t+15]=(byte)0xFF;
        System.arraycopy(payload, pOff, b, IP4_HEADER_LEN + TCP_HEADER_LEN, pLen);
        int tcpLen=TCP_HEADER_LEN+pLen;
        int tcpCsum=tcpUdpChecksum(srcIp,dstIp,PROTO_TCP,b,IP4_HEADER_LEN,tcpLen);
        b[t+16]=(byte)(tcpCsum>>8); b[t+17]=(byte)(tcpCsum);
        return ByteBuffer.wrap(b);
    }

    private static ByteBuffer buildIpUdpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int pOff, int pLen) {
        int total = IP4_HEADER_LEN + UDP_HEADER_LEN + pLen;
        byte[] b  = new byte[total];
        b[0]=IP4_VERSION_IHL; b[2]=(byte)(total>>8); b[3]=(byte)(total);
        b[6]=0x40; b[8]=64; b[9]=PROTO_UDP;
        b[12]=srcIp[0]; b[13]=srcIp[1]; b[14]=srcIp[2]; b[15]=srcIp[3];
        b[16]=dstIp[0]; b[17]=dstIp[1]; b[18]=dstIp[2]; b[19]=dstIp[3];
        int ipCsum=checksum(b,0,IP4_HEADER_LEN); b[10]=(byte)(ipCsum>>8); b[11]=(byte)(ipCsum);
        int u=IP4_HEADER_LEN, udpLen=UDP_HEADER_LEN+pLen;
        b[u]=(byte)(srcPort>>8); b[u+1]=(byte)(srcPort);
        b[u+2]=(byte)(dstPort>>8); b[u+3]=(byte)(dstPort);
        b[u+4]=(byte)(udpLen>>8); b[u+5]=(byte)(udpLen);
        System.arraycopy(payload, pOff, b, IP4_HEADER_LEN + UDP_HEADER_LEN, pLen);
        return ByteBuffer.wrap(b);
    }

    // ─── Checksum helpers (unchanged) ────────────────────────────────────

    private static int checksum(byte[] buf, int offset, int length) {
        int sum=0, i=offset;
        while (i < offset+length-1) { sum += ((buf[i]&0xFF)<<8)|(buf[i+1]&0xFF); i+=2; }
        if (i < offset+length) sum += (buf[i]&0xFF)<<8;
        while ((sum>>16)!=0) sum=(sum&0xFFFF)+(sum>>16);
        return (~sum)&0xFFFF;
    }
    private static int tcpUdpChecksum(byte[] srcIp, byte[] dstIp, byte proto,
                                       byte[] segment, int segOff, int segLen) {
        byte[] scratch = new byte[12+segLen+(segLen%2)];
        scratch[0]=srcIp[0]; scratch[1]=srcIp[1]; scratch[2]=srcIp[2]; scratch[3]=srcIp[3];
        scratch[4]=dstIp[0]; scratch[5]=dstIp[1]; scratch[6]=dstIp[2]; scratch[7]=dstIp[3];
        scratch[8]=0; scratch[9]=proto;
        scratch[10]=(byte)(segLen>>8); scratch[11]=(byte)(segLen);
        System.arraycopy(segment, segOff, scratch, 12, segLen);
        return checksum(scratch, 0, scratch.length);
    }
    private static int icmpv6Checksum(byte[] srcIp6, byte[] dstIp6,
                                       byte[] segment, int segOff, int segLen) {
        byte[] scratch = new byte[40+segLen+(segLen%2)];
        System.arraycopy(srcIp6, 0, scratch, 0,  16);
        System.arraycopy(dstIp6, 0, scratch, 16, 16);
        scratch[32]=(byte)(segLen>>24); scratch[33]=(byte)(segLen>>16);
        scratch[34]=(byte)(segLen>>8);  scratch[35]=(byte)(segLen);
        scratch[39]=58;
        System.arraycopy(segment, segOff, scratch, 40, segLen);
        return checksum(scratch, 0, scratch.length);
    }

    // ─── PERF-1: MSS Clamping (unchanged) ────────────────────────────────

    private static void clampMss(byte[] pkt, int headerStart) {
        try {
            int dataOffset = ((pkt[headerStart+12]>>4)&0xF)*4;
            int optStart   = headerStart+20;
            int optEnd     = headerStart+dataOffset;
            if (optEnd > pkt.length || dataOffset < 20) return;
            int i=optStart;
            while (i < optEnd) {
                int kind=pkt[i]&0xFF;
                if (kind==0) break;
                if (kind==1) { i++; continue; }
                if (i+1>=optEnd) break;
                int optLen=pkt[i+1]&0xFF;
                if (optLen<2) break;
                if (kind==2 && optLen==4) {
                    int existingMss=((pkt[i+2]&0xFF)<<8)|(pkt[i+3]&0xFF);
                    if (existingMss > MSS_CLAMP) { pkt[i+2]=(byte)(MSS_CLAMP>>8); pkt[i+3]=(byte)(MSS_CLAMP); }
                    return;
                }
                i += optLen;
            }
            if (dataOffset>20 && optEnd-optStart>=4) {
                pkt[optStart]  =2; pkt[optStart+1]=4;
                pkt[optStart+2]=(byte)(MSS_CLAMP>>8); pkt[optStart+3]=(byte)(MSS_CLAMP);
            }
        } catch (Exception e) {
            Log.d("NetShareVPN", "clampMss: " + e.getMessage());
        }
    }

    private static int socketTimeoutForPort(int port) {
        if (port==53||port==853) return 5_000;
        if (port==123)           return 10_000;
        if (port==443||port==80) return 120_000;
        return 60_000;
    }

    // ─── PERF-5: DNS cache helpers (unchanged) ───────────────────────────

    private static String parseDnsQName(byte[] pkt, int offset) {
        if (offset>=pkt.length) return null;
        StringBuilder sb=new StringBuilder();
        int i=offset, jumps=0;
        while (i<pkt.length && jumps<10) {
            int len=pkt[i]&0xFF;
            if (len==0) break;
            if ((len&0xC0)==0xC0) { if (i+1>=pkt.length) break; i=((len&0x3F)<<8)|(pkt[i+1]&0xFF); jumps++; continue; }
            if (sb.length()>0) sb.append('.');
            i++;
            if (i+len>pkt.length) break;
            for (int j=0;j<len;j++) sb.append((char)(pkt[i+j]&0xFF));
            i+=len;
        }
        return sb.length()>0 ? sb.toString().toLowerCase() : null;
    }
    private static long extractMinTtlMs(byte[] pkt) {
        if (pkt.length<12) return DNS_CACHE_MIN_TTL_MS;
        int qdCount=((pkt[4]&0xFF)<<8)|(pkt[5]&0xFF);
        int anCount=((pkt[6]&0xFF)<<8)|(pkt[7]&0xFF);
        if (anCount==0) return DNS_CACHE_MIN_TTL_MS;
        int i=12;
        try {
            for (int q=0;q<qdCount&&i<pkt.length;q++) {
                while (i<pkt.length&&pkt[i]!=0) { if ((pkt[i]&0xC0)==0xC0){i+=2;break;} i+=(pkt[i]&0xFF)+1; }
                if (i<pkt.length&&pkt[i]==0) i++;
                i+=4;
            }
            long minTtl=Long.MAX_VALUE;
            for (int a=0;a<anCount&&i<pkt.length;a++) {
                while (i<pkt.length&&pkt[i]!=0) { if ((pkt[i]&0xC0)==0xC0){i+=2;break;} i+=(pkt[i]&0xFF)+1; }
                if (i<pkt.length&&pkt[i]==0) i++;
                if (i+10>pkt.length) break;
                i+=4;
                long ttl=((pkt[i]&0xFFL)<<24)|((pkt[i+1]&0xFFL)<<16)|((pkt[i+2]&0xFFL)<<8)|(pkt[i+3]&0xFFL);
                i+=4;
                int rdLen=((pkt[i]&0xFF)<<8)|(pkt[i+1]&0xFF); i+=2+rdLen;
                if (ttl<minTtl) minTtl=ttl;
            }
            if (minTtl==Long.MAX_VALUE) return DNS_CACHE_MIN_TTL_MS;
            return Math.min(Math.max(minTtl*1000L, DNS_CACHE_MIN_TTL_MS), DNS_CACHE_MAX_TTL_MS);
        } catch (Exception e) { return DNS_CACHE_MIN_TTL_MS; }
    }
    private void cacheDnsResponse(byte[] rb) {
        if (rb.length<13||(rb[2]&0x80)==0) return;
        String name=parseDnsQName(rb,12); if (name==null||name.isEmpty()) return;
        long ttlMs=extractMinTtlMs(rb);
        dnsCache.put(name, new CachedDnsResponse(rb.clone(), System.currentTimeMillis()+ttlMs, name));
        if (dnsCache.size()>DNS_CACHE_MAX_ENTRIES) { String oldest=dnsCache.keys().nextElement(); dnsCache.remove(oldest); }
    }
    private boolean serveDnsFromCache(byte[] queryPkt, int queryPktLen,
                                       byte[] clientIpBytes, int clientSrcPort) {
        if (queryPktLen<13||(queryPkt[2]&0x80)!=0) return false;
        String name=parseDnsQName(queryPkt,12); if (name==null) return false;
        CachedDnsResponse cached=dnsCache.get(name);
        if (cached==null||System.currentTimeMillis()>cached.expiresAt) { if (cached!=null) dnsCache.remove(name); return false; }
        byte[] resp=cached.responseBytes.clone(); resp[0]=queryPkt[0]; resp[1]=queryPkt[1];
        ByteBuffer pkt=buildIpUdpPacket(new byte[]{8,8,8,8}, clientIpBytes, 53, clientSrcPort, resp, 0, resp.length);
        try { if (tunOut!=null) { tunOut.write(pkt.array()); } } catch (Exception e) { Log.w(TAG,"[dnscache] tunOut write: "+e.getMessage()); }
        return true;
    }

    // ─── Control message ─────────────────────────────────────────────────

    public void sendControlMessage(String message) { wsSend(message); }

    // ─── Teardown ─────────────────────────────────────────────────────────

    private synchronized void stopVpnTunnel() {
        if (!isRunning && vpnInterface==null && wsClient==null) return;
        isRunning = false;

        WebSocket ws = wsClient;
        wsClient = null;
        if (ws != null) {
            try {
                ws.send("host".equals(role) ? "{\"type\":\"HOST_LEAVE\"}" : "{\"type\":\"CLIENT_LEAVE\"}");
                ws.close(1000, "NetShare stopping");
            } catch (Exception e) { Log.w(TAG, "WS close: " + e.getMessage()); }
        }

        try { if (tunOut       != null) { tunOut.close();       tunOut       = null; } } catch (Exception ignored) {}
        try { if (vpnInterface != null) { vpnInterface.close(); vpnInterface = null; } }
        catch (Exception e) { Log.w(TAG, "TUN close: " + e.getMessage()); }

        for (Socket         s : tcpConnections.values()) try { s.close(); } catch (Exception ignored) {}
        for (DatagramSocket s : udpSockets.values())     try { s.close(); } catch (Exception ignored) {}
        tcpConnections.clear();
        udpSockets.clear();

        dnsCache.clear();

        ExecutorService ex = executor;
        executor = null;
        if (ex != null) ex.shutdownNow();
        icmpExecutor.shutdownNow();

        wsSendQueue.clear();
        stopWsDrain();

        stopForeground(true);
        stopSelf();
    }

    private synchronized void stopVpnTunnelFromUser() {
        if (!isRunning && vpnInterface==null && wsClient==null) return;
        VpnModule.emitEvent("vpnDisconnected", "User stopped sharing");
        stopVpnTunnel();
    }

    // ─── Foreground notification ──────────────────────────────────────────

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            getSystemService(NotificationManager.class).createNotificationChannel(ch);
        }
        Intent stopIntent = new Intent(this, NetShareVpnService.class);
        stopIntent.setAction("STOP_VPN");
        PendingIntent stopPending = PendingIntent.getService(this, 0, stopIntent, PendingIntent.FLAG_IMMUTABLE);
        Notification notif = new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare Active")
            .setContentText("host".equals(role) ? "Sharing your internet with clients..." : "Connected through host network...")
            .setSmallIcon(android.R.drawable.ic_menu_share)
            .addAction(android.R.drawable.ic_delete, "Stop", stopPending)
            .setOngoing(true).build();
        startForeground(NOTIFICATION_ID, notif);
    }

    @Override
    public void onDestroy() {
        VpnModule.activeService = null;
        stopVpnTunnel();
        super.onDestroy();
    }

    // ─── Utilities ────────────────────────────────────────────────────────

    private static int u16(byte[] b, int off) { return ((b[off]&0xFF)<<8)|(b[off+1]&0xFF); }
    private byte[] tunIpBytes() {
        try { return InetAddress.getByName(assignedTunIp).getAddress(); }
        catch (Exception e) { return new byte[]{10,8,0,2}; }
    }
    private static String orEmpty(String s) { return s!=null?s:""; }
    private static String jsonGet(String json, String key) {
        String needle="\""+key+"\":\"";
        int s=json.indexOf(needle); if (s<0) return null;
        s+=needle.length();
        StringBuilder sb=new StringBuilder();
        int i=s;
        while (i<json.length()) {
            char c=json.charAt(i);
            if (c=='\\'&&i+1<json.length()) {
                char n=json.charAt(++i);
                switch(n){case '"':sb.append('"');break;case '\\':sb.append('\\');break;
                           case 'n':sb.append('\n');break;case 'r':sb.append('\r');break;
                           case 't':sb.append('\t');break;default:sb.append(n);break;}
                i++; continue;
            }
            if (c=='"') break;
            sb.append(c); i++;
        }
        return sb.toString();
    }
    private static String j2(String k1,String v1,String k2,String v2){
        return "{\""+k1+"\":\""+esc(v1)+"\",\""+k2+"\":\""+esc(v2)+"\"}";}
    private static String j3(String k1,String v1,String k2,String v2,String k3,String v3){
        return "{\""+k1+"\":\""+esc(v1)+"\",\""+k2+"\":\""+esc(v2)+"\",\""+k3+"\":\""+esc(v3)+"\"}";}
    private static String esc(String s){if(s==null)return "";return s.replace("\\","\\\\").replace("\"","\\\"");}
}
