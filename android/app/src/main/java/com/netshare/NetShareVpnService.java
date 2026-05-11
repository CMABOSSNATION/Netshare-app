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

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * NetShareVpnService — fully fixed
 *
 * FIXES IN THIS VERSION:
 *
 * 1. START_STICKY → START_NOT_STICKY
 *    START_STICKY causes Android to restart the service with a NULL intent after
 *    it's killed. onStartCommand then calls intent.getStringExtra() on null → NPE
 *    → service crashes → Android restarts → infinite crash/disconnect loop.
 *    FIX: Return START_NOT_STICKY so Android does NOT restart the service.
 *
 * 2. null-intent guard added anyway as a safety net.
 *    Even though START_NOT_STICKY is used, a defensive null-check on intent
 *    and relayUrl prevents any future crash if the service is accidentally
 *    triggered without extras.
 *
 * 3. connectToRelay() used connectBlocking() which blocks the executor thread
 *    for the entire duration of the connection (returns only when WS closes).
 *    For HOST: this permanently consumed a thread from the cached pool while
 *    connected. onOpen/onMessage/onClose already fire on the WS internal thread,
 *    so blocking the executor thread gains nothing and wastes resources.
 *    FIX: switched back to non-blocking connect() + the WS internal threads
 *    handle all callbacks. The executor is freed for packet-forwarding work.
 *    connectBlocking with 60s timeout was also causing the host to appear stuck
 *    for a full minute before reporting failure on cold-start timeouts.
 *
 * 4. onDestroy() called stopVpnTunnelFromUser() which emits vpnDisconnected.
 *    This caused JS to see a spurious "Disconnected" event on every normal stop,
 *    and also on service recreation. Since stopVpnTunnelFromUser already emits
 *    the event before stopVpnTunnel, and stopVpnTunnel is idempotent (synchronized),
 *    double-calling was safe but noisy.
 *    FIX: onDestroy calls stopVpnTunnel() directly (no user event) since the
 *    user already called stop() explicitly or the system is killing the service.
 *
 * 5. isRunning = true set for HOST in startVpnTunnel() BEFORE connectToRelay().
 *    If connectToRelay() threw (malformed URI, SSL init failure), isRunning was
 *    true but no WS existed. The onMessage handler checked isRunning before
 *    processing, so no crash, but the state was inconsistent.
 *    FIX: isRunning = true for HOST moved into onOpen() (after WS handshake).
 *
 * 6. buildIpTcpPacket() had a ByteBuffer position bug.
 *    After putting IP header fields sequentially, b.putShort(10, checksum) uses
 *    an absolute index — correct. But b.put(srcIp) after putShort(totalLen) etc.
 *    relies on sequential position tracking. After putShort(10, ...) the position
 *    is NOT changed (absolute put). Continued sequential puts after IP header
 *    checksum write started at wrong position.
 *    FIX: IP header is built completely first, checksum filled at absolute index 10,
 *    then TCP header written at absolute offsets from IP4_HEADER_LEN.
 *    All puts converted to explicit absolute-position writes to be unambiguous.
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG             = "NetShareVPN";
    private static final String CHANNEL_ID      = "netshare_vpn";
    private static final int    NOTIFICATION_ID = 1;

    private static final int  IP4_HEADER_LEN  = 20;
    private static final int  TCP_HEADER_LEN  = 20;
    private static final int  UDP_HEADER_LEN  = 8;
    private static final byte IP4_VERSION_IHL = 0x45;
    private static final byte PROTO_TCP       = 6;
    private static final byte PROTO_UDP       = 17;

    // QUIC runs over UDP port 443 (and sometimes 80).
    // TikTok, WhatsApp, YouTube all use QUIC for low-latency media.
    // We don't block it — it flows through the UDP path — but we give
    // QUIC flows larger socket buffers to avoid dropped datagrams.
    private static final int QUIC_PORT_HTTPS  = 443;
    private static final int QUIC_PORT_HTTP   = 80;

    // Socket buffer sizes tuned for streaming media
    private static final int TCP_SOCKET_BUFFER  = 512 * 1024;      // 512 KB  (was 256 KB)
    private static final int UDP_SOCKET_BUFFER  = 1024 * 1024;     // 1 MB    (was 512 KB)
    private static final int QUIC_SOCKET_BUFFER = 4 * 1024 * 1024; // 4 MB    (was 2 MB — TikTok CDN)

    // MTU 1420: maximises payload per WS frame while leaving room for WS+TLS framing
    // overhead (~60 bytes). Previously 1400 — the extra 20 bytes yield ~1.4% more
    // throughput on back-to-back frames (WhatsApp video calls send continuous 1400-byte
    // segments; raising MTU means fewer IP fragments and fewer WS send() calls).
    private static final int TUN_MTU = 1420;

    // WS send queue: replaces the global synchronized lock.
    // A single drain thread serialises all sends — no lock contention between
    // forwarding threads. Capacity 4096 frames ≈ 256 MB headroom before drop.
    private static final int WS_SEND_QUEUE_CAPACITY = 4096;
    private final LinkedBlockingQueue<Object> wsSendQueue =
            new LinkedBlockingQueue<>(WS_SEND_QUEUE_CAPACITY);
    // Sentinel that tells the drain thread to exit cleanly.
    private static final Object WS_DRAIN_POISON = new Object();

    private ParcelFileDescriptor vpnInterface;
    private WebSocketClient      wsClient;
    private ExecutorService      executor;
    private volatile boolean     isRunning = false;

    private FileOutputStream tunOut;

    private String relayUrl;
    private String sessionCode;
    private String role;
    private String hostId;
    private String netType;
    // Assigned TUN IP received from server in JOIN_SUCCESS (e.g. "10.8.0.3")
    private volatile String assignedTunIp = "10.8.0.2";

    private final Map<String, Socket>         tcpConnections = new ConcurrentHashMap<>();
    private final Map<String, DatagramSocket> udpSockets     = new ConcurrentHashMap<>();

    // Byte counters for debugging / admin UI
    private final AtomicLong bytesIn  = new AtomicLong(0);
    private final AtomicLong bytesOut = new AtomicLong(0);

    // ─── Lock-free WebSocket send via dedicated drain thread ────────────────
    // DESIGN: Previously all forwarding threads called synchronized wsSend(),
    // which serialised every send through one global lock. Under load (WhatsApp
    // video + background QUIC + DNS), threads piled up waiting for the lock.
    //
    // NEW DESIGN: Forwarding threads enqueue frames into a LinkedBlockingQueue
    // (non-blocking offer — drops frame if full rather than blocking the thread).
    // A single dedicated drain thread dequeues and sends, so java-websocket's
    // non-thread-safe send() is called from only one thread. No lock contention.
    //
    // Result: forwarding threads are never blocked by a slow WS send. WhatsApp
    // media frames and QUIC packets are queued instantly and drained as fast as
    // the WS connection allows.
    private void startWsDrainThread() {
        Thread drain = new Thread(() -> {
            while (true) {
                try {
                    Object item = wsSendQueue.take();   // blocks only when queue is empty
                    if (item == WS_DRAIN_POISON) break;
                    WebSocketClient ws = wsClient;
                    if (ws == null || !ws.isOpen()) continue;
                    try {
                        if (item instanceof ByteBuffer) ws.send((ByteBuffer) item);
                        else if (item instanceof String) ws.send((String) item);
                    } catch (Exception e) {
                        Log.w(TAG, "wsDrain send error: " + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "ws-drain");
        drain.setDaemon(true);
        drain.setPriority(Thread.MAX_PRIORITY);  // drain thread must not be starved
        drain.start();
    }

    private void wsSend(ByteBuffer data) {
        // offer() is non-blocking — drops if queue is full (backpressure).
        // Dropping is better than blocking a forwarding thread.
        if (!wsSendQueue.offer(data)) {
            Log.w(TAG, "wsSend: queue full, frame dropped");
        }
    }
    private void wsSend(String text) {
        if (!wsSendQueue.offer(text)) {
            Log.w(TAG, "wsSend: queue full, control message dropped");
        }
    }
    private void stopWsDrain() {
        wsSendQueue.offer(WS_DRAIN_POISON);   // wake drain thread to exit
    }

    // DNS flows need a short timeout (5s) — DNS server responds and closes
    // immediately. Waiting 300s blocks the response thread so DNS replies
    // never reach the client, making WhatsApp unable to resolve g.whatsapp.net.
    // WhatsApp XMPP (5222) and streaming (443) need 5 min to stay alive.
    private static int socketTimeoutForPort(int port) {
        if (port == 53 || port == 853) return 5_000;       // DNS / DNS-over-TLS
        if (port == 123)               return 10_000;      // NTP
        return 300_000;                                    // everything else: 5 min
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        // FIX 1+2: Handle null intent (safety net) and STOP action first
        if (intent == null) {
            // Service was restarted by Android with no intent — just stop cleanly.
            Log.w(TAG, "onStartCommand: null intent, stopping");
            stopSelf();
            return START_NOT_STICKY;  // FIX 1
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
        if (netType    == null || netType.isEmpty())    netType    = "WiFi";
        if (sessionCode == null)                        sessionCode = "";
        if (hostId      == null)                        hostId      = "";

        // FIX 2: Guard against missing relay URL (would cause URI exception later)
        if (relayUrl == null || relayUrl.isEmpty()) {
            Log.e(TAG, "No RELAY_URL provided, stopping");
            stopSelf();
            return START_NOT_STICKY;
        }

        startForegroundNotification();
        // Bounded thread pool: I/O-bound tasks (TCP/UDP forwarding) benefit from
        // more threads than CPU count, but unbounded (newCachedThreadPool) causes
        // thread explosion under load (100+ flows = 100+ threads = heavy context switching).
        // CPU_COUNT * 8 gives good throughput for I/O-bound network forwarding.
        int poolSize = Math.max(8, Runtime.getRuntime().availableProcessors() * 8);
        executor = Executors.newFixedThreadPool(poolSize);
        startWsDrainThread();   // start before connectToRelay so first send doesn't race
        VpnModule.activeService = this;
        executor.execute(this::startVpnTunnel);

        return START_NOT_STICKY;  // FIX 1: was START_STICKY → caused null-intent crash loop
    }

    // ─── Tunnel setup ─────────────────────────────────────────────────────

    private void startVpnTunnel() {
        try {
            if ("host".equals(role)) {
                Log.i(TAG, "Host mode — connecting to relay");
                // FIX 5: isRunning set in onOpen() after WS handshake succeeds
                connectToRelay();
                return;
            }

            // CLIENT: build a PLACEHOLDER TUN interface before connecting to relay.
            // We use a minimal route (only 10.8.0.0/24, no default route) so that
            // the WS relay connection itself is NOT routed through the VPN tunnel.
            // Full routing (0.0.0.0/0, ::/0) is set up in JOIN_SUCCESS after the
            // server assigns our tunIp. This prevents a routing loop where the WS
            // connection tries to go through the VPN that hasn't opened yet.
            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)   // placeholder — overwritten in JOIN_SUCCESS
                   // Only route the VPN subnet for now — no default route yet.
                   // The WS relay traffic uses the real network interface (protected socket).
                   .addRoute("10.8.0.0", 24)
                   .addDnsServer("8.8.8.8")
                   .addDnsServer("1.1.1.1")
                   .setMtu(TUN_MTU);

            try {
                // Exclude this app so the WS relay connection is not routed through the TUN.
                builder.addDisallowedApplication(getPackageName());
            } catch (Exception e) {
                Log.w(TAG, "addDisallowedApplication: " + e.getMessage());
            }

            vpnInterface = builder.establish();
            if (vpnInterface == null) {
                VpnModule.emitEvent("vpnError", "Failed to establish VPN interface");
                return;
            }

            tunOut    = new FileOutputStream(vpnInterface.getFileDescriptor());
            isRunning = true;
            Log.i(TAG, "CLIENT TUN placeholder established — connecting to relay for JOIN_SUCCESS");
            // Connect to relay. Full TUN (with 0.0.0.0/0 route) is set up in
            // JOIN_SUCCESS after the server assigns our tunIp.
            connectToRelay();

        } catch (Exception e) {
            Log.e(TAG, "startVpnTunnel: " + e.getMessage());
            VpnModule.emitEvent("vpnError",
                    e.getMessage() != null ? e.getMessage() : "VPN start failed");
        }
    }

    // ─── Relay WebSocket ──────────────────────────────────────────────────

    private void connectToRelay() throws Exception {
        URI uri = new URI(relayUrl);
        final NetShareVpnService self = this;

        wsClient = new WebSocketClient(uri) {

            @Override
            public void onOpen(ServerHandshake hs) {
                Log.i(TAG, "WS open — role=" + role);
                if ("host".equals(role)) {
                    isRunning = true;  // FIX 5: set here, not before connect
                    VpnModule.emitEvent("vpnConnected", "host");
                    wsSend(j3("type", "HOST_REGISTER", "hostId", hostId, "netType", netType));
                } else {
                    // CLIENT TUN already up; WS is now ready.
                    // CRITICAL FIX: Do NOT start the packet read loop here.
                    // We must wait for JOIN_SUCCESS (which assigns our tunIp).
                    // Sending packets before the server knows our tunIp means
                    // reply packets can never be routed back — this was why
                    // WhatsApp (and all apps) never received any responses.
                    // startPacketReadLoop() is called in JOIN_SUCCESS handler
                    // after the TUN is rebuilt with the correct assigned IP.
                    VpnModule.emitEvent("vpnConnected", sessionCode);
                    wsSend(j2("type", "CLIENT_JOIN", "accessCode", sessionCode));
                }
            }

            @Override
            public void onMessage(String message) {
                handleRelayMessage(message);
            }

            @Override
            public void onMessage(ByteBuffer bytes) {
                if (!isRunning) return;
                if ("host".equals(role)) {
                    byte[] packet = new byte[bytes.remaining()];
                    bytes.get(packet);
                    bytesIn.addAndGet(packet.length);
                    executor.execute(() -> forwardPacketToInternet(packet));
                } else {
                    // Copy the payload off the ByteBuffer immediately (ByteBuffer is
                    // reused by java-websocket after this callback returns).
                    byte[] data = new byte[bytes.remaining()];
                    bytes.get(data);
                    // PERF FIX: offload TUN write to executor so the WS reader thread
                    // is never blocked by a kernel TUN buffer that is temporarily full.
                    // Without this, a stalled tunOut.write() stops ALL incoming packets.
                    executor.execute(() -> {
                        if (!isRunning || tunOut == null) return;
                        try {
                            if (data.length >= IP4_HEADER_LEN) {
                                int ver = (data[0] & 0xF0) >> 4;
                                if (ver == 4 || (ver == 6 && data.length >= 40)) {
                                    bytesIn.addAndGet(data.length);
                                    tunOut.write(data);
                                } else {
                                    Log.w(TAG, "CLIENT: dropped unknown IP frame ver=" + ver + " len=" + data.length);
                                }
                            }
                        } catch (Exception e) {
                            if (isRunning) Log.e(TAG, "TUN write: " + e.getMessage());
                        }
                    });
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                Log.i(TAG, "WS closed code=" + code + " reason=" + reason);
                // Always emit — even if isRunning=false (WS failed before onOpen for HOST).
                // Without this, a pre-connection close leaves the app frozen on CONNECTING.
                String msg = reason != null && !reason.isEmpty() ? reason : "Connection closed";
                VpnModule.emitEvent(isRunning ? "vpnDisconnected" : "vpnError", msg);
                stopVpnTunnel();
            }

            @Override
            public void onError(Exception ex) {
                String raw = ex != null ? ex.getMessage() : null;
                Log.e(TAG, "WS error: " + raw);
                // Always emit — even if isRunning=false (WS failed before onOpen for HOST).
                // Without this, a connection error leaves the app frozen on CONNECTING forever.
                String friendly = (raw != null && (raw.contains("timed out") || raw.contains("timeout")))
                        ? "Server is starting up — please wait 30 seconds and try again."
                        : (raw != null ? raw : "WebSocket error");
                VpnModule.emitEvent("vpnError", friendly);
                stopVpnTunnel();
            }
        };

        // SSL socket factory that calls protect() so WS traffic bypasses the VPN tunnel
        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(null, null, null);
        final SSLSocketFactory baseSSL = sslCtx.getSocketFactory();

        // FIX: Previous factory created plain new Socket() objects without SSL wrapping.
        // java-websocket connects wss:// by calling createSocket(Socket,host,port,autoClose)
        // on a plain socket it opened itself. We protect() that socket first (so the
        // VPN tunnel doesn't route it back through itself), then delegate to baseSSL to
        // wrap it with TLS. All other overrides also delegate to baseSSL so SSL is applied.
        wsClient.setSocketFactory(new SSLSocketFactory() {
            @Override public Socket createSocket(Socket plain, String h, int p, boolean ac) throws IOException {
                self.protect(plain);
                return baseSSL.createSocket(plain, h, p, ac); }
            @Override public Socket createSocket() throws IOException {
                Socket s = baseSSL.createSocket(); self.protect(s); return s; }
            @Override public Socket createSocket(String h, int p) throws IOException {
                Socket s = baseSSL.createSocket(h, p); self.protect(s); return s; }
            @Override public Socket createSocket(String h, int p, InetAddress la, int lp) throws IOException {
                Socket s = baseSSL.createSocket(h, p, la, lp); self.protect(s); return s; }
            @Override public Socket createSocket(InetAddress h, int p) throws IOException {
                Socket s = baseSSL.createSocket(h, p); self.protect(s); return s; }
            @Override public Socket createSocket(InetAddress a, int p, InetAddress la, int lp) throws IOException {
                Socket s = baseSSL.createSocket(a, p, la, lp); self.protect(s); return s; }
            @Override public String[] getDefaultCipherSuites() { return baseSSL.getDefaultCipherSuites(); }
            @Override public String[] getSupportedCipherSuites() { return baseSSL.getSupportedCipherSuites(); }
        });

        // 20s keepalive ping/pong: detects dead connections faster than the previous 30s.
        // WhatsApp requires a live WS connection — a 30s dead-connection window meant
        // WhatsApp could stall for up to 30s before reconnecting.
        wsClient.setConnectionLostTimeout(20);

        // FIX 3: Use non-blocking connect(). Callbacks (onOpen/onMessage/onClose/onError)
        // run on the WS internal thread — no executor thread is permanently consumed.
        // connectBlocking() was blocking the thread for the entire session lifetime.
        wsClient.connect();
    }

    // ─── CLIENT: TUN read loop ────────────────────────────────────────────

    private void startPacketReadLoop() {
        executor.execute(() -> {
            // 65535 = max IP packet size.
            byte[] buf = new byte[65535];
            try (FileInputStream in = new FileInputStream(vpnInterface.getFileDescriptor())) {
                while (isRunning) {
                    int len = in.read(buf);
                    if (len > 0 && wsClient != null && wsClient.isOpen()) {
                        bytesOut.addAndGet(len);
                        // CRITICAL: must copy slice into its own array before sending.
                        // ByteBuffer.wrap(buf) shares the underlying array — not a copy.
                        // The next in.read(buf) would overwrite the same memory before
                        // java-websocket finishes framing the previous packet, corrupting it.
                        //
                        // SPEED: allocate exactly `len` bytes (not always 65535) to
                        // reduce GC pressure. WhatsApp ACKs are typically 40–60 bytes;
                        // allocating 65535 every packet wastes memory bandwidth and GC cycles.
                        byte[] frame = new byte[len];
                        System.arraycopy(buf, 0, frame, 0, len);
                        // Enqueue into drain thread (non-blocking offer — no lock contention)
                        wsSend(ByteBuffer.wrap(frame));
                    }
                }
            } catch (Exception e) {
                if (isRunning) Log.e(TAG, "TUN read loop: " + e.getMessage());
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
                case "JOIN_SUCCESS": {
                    String assignedIp = jsonGet(msg, "tunIp");

                    // ALWAYS rebuild TUN on JOIN_SUCCESS — even if the assigned IP
                    // matches our default. The packet read loop must only start here,
                    // after the server has registered our tunIp. Starting it earlier
                    // (in onOpen) meant packets were sent before the server could
                    // route replies back — the root cause of WhatsApp never working.
                    if (assignedIp != null && !assignedIp.isEmpty()) {
                        assignedTunIp = assignedIp;
                    }
                    Log.i(TAG, "JOIN_SUCCESS: tunIp=" + assignedTunIp + " — rebuilding TUN and starting read loop");
                    try {
                        // Close old interface before rebuilding
                        if (tunOut != null) { try { tunOut.close(); } catch (Exception ignored) {} tunOut = null; }
                        if (vpnInterface != null) { try { vpnInterface.close(); } catch (Exception ignored) {} vpnInterface = null; }

                        Builder b2 = new Builder();
                        b2.setSession("NetShare")
                          .addAddress(assignedTunIp, 24)
                          .addRoute("0.0.0.0", 0)
                          .addRoute("::", 0)
                          // DNS via tunnel — prevents WhatsApp DNS leaks outside VPN.
                          // WhatsApp validates connectivity with its own DNS lookups;
                          // if DNS bypasses the VPN, WhatsApp detects a broken network.
                          .addDnsServer("8.8.8.8")
                          .addDnsServer("8.8.4.4")
                          .addDnsServer("1.1.1.1")
                          .addDnsServer("2001:4860:4860::8888")
                          .addDnsServer("2606:4700:4700::1111")
                          .setMtu(TUN_MTU);
                        try { b2.addDisallowedApplication(getPackageName()); } catch (Exception ignored) {}
                        vpnInterface = b2.establish();
                        if (vpnInterface != null) {
                            tunOut = new FileOutputStream(vpnInterface.getFileDescriptor());
                            // START READ LOOP HERE — server now knows our tunIp,
                            // so it can route reply packets back to this client.
                            startPacketReadLoop();
                        } else {
                            Log.e(TAG, "JOIN_SUCCESS: failed to rebuild TUN — VPN interface null");
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
                    if (wsClient != null && wsClient.isOpen())
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

    // ─── HOST: forward IP packet to internet ──────────────────────────────

    private void forwardPacketToInternet(byte[] pkt) {
        if (pkt.length < 20) return;
        try {
            int version = (pkt[0] >> 4) & 0xF;

            // ── IPv6 ──────────────────────────────────────────────────────
            // IPv6 header is fixed 40 bytes. Forward TCP/UDP the same way as IPv4.
            if (version == 6) {
                if (pkt.length < 40) return;
                int proto6  = pkt[6] & 0xFF;  // Next Header
                int pOff6   = 40;
                // Extract src/dst addresses (bytes 8-23 = src, 24-39 = dst)
                byte[] src6 = new byte[16]; System.arraycopy(pkt, 8,  src6, 0, 16);
                byte[] dst6 = new byte[16]; System.arraycopy(pkt, 24, dst6, 0, 16);
                InetAddress dst6Addr = InetAddress.getByAddress(dst6);
                String src6Ip = InetAddress.getByAddress(src6).getHostAddress();

                if (proto6 == 6 && pkt.length >= pOff6 + 14) {    // TCP over IPv6
                    int srcPort = u16(pkt, pOff6);
                    int dstPort = u16(pkt, pOff6 + 2);
                    int flags   = pkt[pOff6 + 13] & 0xFF;
                    boolean isSyn = (flags & 0x02) != 0;
                    boolean isFin = (flags & 0x01) != 0;
                    boolean isRst = (flags & 0x04) != 0;
                    int tOff = ((pkt[pOff6 + 12] >> 4) & 0xF) * 4;
                    if (tOff < 20) tOff = 20;
                    int pOff = pOff6 + tOff;
                    int pLen = pkt.length - pOff;
                    String key = src6Ip + ":" + srcPort + "-" + dst6Addr.getHostAddress() + ":" + dstPort;
                    if (isRst) {
                        Socket s = tcpConnections.remove(key);
                        if (s != null) try { s.close(); } catch (Exception ignored) {}
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
                            sock.connect(new java.net.InetSocketAddress(dst6Addr, dstPort), 10_000);
                            sock.setSoTimeout(socketTimeoutForPort(dstPort));
                            sock.setTcpNoDelay(true);
                            sock.setKeepAlive(true);
                        } catch (Exception e) {
                            Log.w(TAG, "IPv6 TCP connect [" + key + "]: " + e.getMessage());
                            try { sock.close(); } catch (Exception ignored) {}
                            return;
                        }
                        tcpConnections.put(key, sock);
                        // FIX 2: Pass tunIpBytes() (4-byte IPv4) NOT src6 (16-byte IPv6).
                        // buildIpTcpPacket builds an IPv4 response packet for the client TUN.
                        // The client TUN only accepts IPv4 packets (our TUN is IPv4).
                        // src6 is the client's IPv6 TUN source — irrelevant for the response dst.
                        // The response must go to the client's assigned TUN IPv4 address.
                        final byte[] fClientIpv4 = tunIpBytes();
                        final int fSrcPort = srcPort; final int fDstPort = dstPort;
                        final InetAddress fDst = dst6Addr; final String fk = key;
                        executor.execute(() -> readTcpResponses(sock, fk, fClientIpv4, fSrcPort, fDstPort, fDst));
                    }
                    // FIX 4: Send payload BEFORE closing on FIN — FIN+data is valid TCP
                    if (pLen > 0) {
                        Socket sock = tcpConnections.get(key);
                        if (sock != null && !sock.isClosed()) {
                            try { OutputStream out = sock.getOutputStream(); out.write(pkt, pOff, pLen); out.flush(); }
                            catch (Exception e) { tcpConnections.remove(key); try { sock.close(); } catch (Exception ignored) {} }
                        }
                    }
                    if (isFin) { Socket s = tcpConnections.remove(key); if (s != null) try { s.close(); } catch (Exception ignored) {} }

                } else if (proto6 == 17 && pkt.length >= pOff6 + 8) {  // UDP over IPv6
                    int srcPort = u16(pkt, pOff6);
                    int dstPort = u16(pkt, pOff6 + 2);
                    int pOff = pOff6 + 8;
                    int pLen = pkt.length - pOff;
                    if (pLen <= 0) return;
                    String key = src6Ip + ":" + srcPort + "-" + dst6Addr.getHostAddress() + ":" + dstPort;
                    if (!udpSockets.containsKey(key)) {
                        DatagramSocket udpSock = new DatagramSocket();
                        protect(udpSock);
                        boolean isQuic = (dstPort == QUIC_PORT_HTTPS || dstPort == QUIC_PORT_HTTP);
                        int bufSize = isQuic ? QUIC_SOCKET_BUFFER : UDP_SOCKET_BUFFER;
                        try { udpSock.setReceiveBufferSize(bufSize); udpSock.setSendBufferSize(bufSize); } catch (Exception ignored) {}
                        udpSockets.put(key, udpSock);
                        // FIX 2: Same as TCP — use tunIpBytes() not src6
                        final byte[] fClientIpv4 = tunIpBytes();
                        final int fSrcPort = srcPort; final int fDstPort = dstPort; final String fk = key;
                        executor.execute(() -> readUdpResponses(udpSock, fk, fClientIpv4, fSrcPort, fDstPort));
                    }
                    DatagramSocket udpSock = udpSockets.get(key);
                    if (udpSock != null && !udpSock.isClosed()) {
                        udpSock.send(new DatagramPacket(pkt, pOff, pLen, dst6Addr, dstPort));
                    }
                }
                return;
            }

            // ── IPv4 ──────────────────────────────────────────────────────
            if (version != 4) return;

            int proto = pkt[9] & 0xFF;
            int ihl   = (pkt[0] & 0xF) * 4;
            if (ihl < 20 || ihl >= pkt.length) return;

            InetAddress dst   = InetAddress.getByAddress(
                    new byte[]{pkt[16], pkt[17], pkt[18], pkt[19]});
            String      srcIp = InetAddress.getByAddress(
                    new byte[]{pkt[12], pkt[13], pkt[14], pkt[15]}).getHostAddress();

            if (proto == 6) {   // TCP
                if (pkt.length < ihl + 14) return;
                int srcPort = u16(pkt, ihl);
                int dstPort = u16(pkt, ihl + 2);
                int flags   = pkt[ihl + 13] & 0xFF;
                boolean isSyn = (flags & 0x02) != 0;
                boolean isFin = (flags & 0x01) != 0;
                boolean isRst = (flags & 0x04) != 0;
                int tOff = ((pkt[ihl + 12] >> 4) & 0xF) * 4;
                if (tOff < 20) tOff = 20;
                int pOff = ihl + tOff;
                int pLen = pkt.length - pOff;
                if (pOff > pkt.length) return;
                String key = srcIp + ":" + srcPort + "-" + dst.getHostAddress() + ":" + dstPort;

                if (isRst) {
                    Socket s = tcpConnections.remove(key);
                    if (s != null) try { s.close(); } catch (Exception ignored) {}
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
                        sock.connect(new java.net.InetSocketAddress(dst, dstPort), 10_000);
                        // FIX 3: Per-port timeout — DNS needs 5s, WhatsApp XMPP needs 5min
                        sock.setSoTimeout(socketTimeoutForPort(dstPort));
                        sock.setTcpNoDelay(true);
                        sock.setKeepAlive(true);
                    } catch (Exception e) {
                        Log.w(TAG, "TCP connect [" + key + "]: " + e.getMessage());
                        try { sock.close(); } catch (Exception ignored) {}
                        return;
                    }
                    tcpConnections.put(key, sock);
                    // Use tunIpBytes() as the client IPv4 address for the response packet.
                    // This ensures the response dst IP matches what the client TUN expects.
                    final byte[]      fClientIp = tunIpBytes();
                    final int         fSrcPort  = srcPort;
                    final int         fDstPort  = dstPort;
                    final InetAddress fDst      = dst;
                    final String      fk        = key;
                    executor.execute(() -> readTcpResponses(sock, fk, fClientIp, fSrcPort, fDstPort, fDst));
                }

                // FIX 4: Write payload BEFORE processing FIN.
                // TCP allows FIN+data in the same segment. Previously, data after FIN
                // was written but the socket was already removed by the FIN check above.
                // Now we always write payload first, then handle FIN.
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

            } else if (proto == 17) {   // UDP
                if (pkt.length < ihl + 8) return;
                int srcPort = u16(pkt, ihl);
                int dstPort = u16(pkt, ihl + 2);
                int pOff    = ihl + 8;
                int pLen    = pkt.length - pOff;
                if (pLen <= 0) return;

                // Key by srcIp+srcPort+dst — each unique client flow gets its own socket.
                // This is proper UDP NAT: responses come back to the bound socket and are
                // forwarded back to the correct client srcPort. WhatsApp and TikTok open
                // many parallel UDP flows; each needs its own socket for replies to route
                // back correctly.
                String key = srcIp + ":" + srcPort + "-" + dst.getHostAddress() + ":" + dstPort;
                if (!udpSockets.containsKey(key)) {
                    DatagramSocket udpSock = new DatagramSocket();
                    protect(udpSock);
                    boolean isQuic = (dstPort == QUIC_PORT_HTTPS || dstPort == QUIC_PORT_HTTP);
                    int bufSize = isQuic ? QUIC_SOCKET_BUFFER : UDP_SOCKET_BUFFER;
                    try {
                        udpSock.setReceiveBufferSize(bufSize);
                        udpSock.setSendBufferSize(bufSize);
                    } catch (Exception ignored) {}
                    udpSockets.put(key, udpSock);
                    // FIX 3: Use tunIpBytes() as clientIp for response packet dst.
                    // FIX DNS timeout: socketTimeoutForPort gives 5s for DNS (port 53),
                    // 5min for everything else. Passing dstPort so the reader can set it.
                    final byte[] fClientIp = tunIpBytes();
                    final int    fSrcPort  = srcPort;
                    final int    fDstPort  = dstPort;
                    final String fk        = key;
                    executor.execute(() -> readUdpResponses(udpSock, fk, fClientIp, fSrcPort, fDstPort));
                }
                DatagramSocket udpSock = udpSockets.get(key);
                if (udpSock != null && !udpSock.isClosed()) {
                    udpSock.send(new DatagramPacket(pkt, pOff, pLen, dst, dstPort));
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "forwardPacket: " + e.getMessage());
        }
    }

    // ─── HOST: TCP response → build IP packet → relay ─────────────────────

    private void readTcpResponses(Socket sock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort,
                                   InetAddress remoteAddr) {
        byte[] remoteIpBytes = remoteAddr.getAddress();
        try {
            InputStream in  = sock.getInputStream();
            // 64 KB read buffer — HTTP/2 (used by WhatsApp, TikTok API) sends large frames
            byte[]      buf = new byte[65535 - IP4_HEADER_LEN - TCP_HEADER_LEN];
            int len;
            // len == -1 means clean EOF (server closed connection gracefully).
            // len == 0 should never happen with blocking sockets but guard anyway.
            // SocketTimeoutException is caught below and is non-fatal — it just
            // means the socket was idle for setSoTimeout() ms (5 min). WhatsApp
            // XMPP connections can be idle for minutes between messages; a timeout
            // does not mean the connection is dead, so we log and continue.
            while (isRunning && !sock.isClosed()) {
                try {
                    len = in.read(buf);
                } catch (java.net.SocketTimeoutException ste) {
                    // For long-lived ports (XMPP 5222, HTTPS 443): idle is normal.
                    // For DNS (53): timeout means the DNS exchange is complete — exit.
                    if (socketTimeoutForPort(remoteDstPort) <= 10_000) break;
                    Log.d(TAG, "TCP idle timeout [" + key + "] — keeping alive");
                    continue;
                }
                if (len <= 0) break; // EOF
                if (wsClient != null && wsClient.isOpen()) {
                    bytesOut.addAndGet(len);
                    // buildIpTcpPacket allocates a new byte[] for the full packet
                    // (IP header + TCP header + payload copy), so the returned
                    // ByteBuffer owns its data independently of buf. Safe to reuse buf.
                    ByteBuffer pkt = buildIpTcpPacket(
                            remoteIpBytes, clientIpBytes,
                            remoteDstPort, clientSrcPort,
                            buf, 0, len);
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

    // ─── HOST: UDP response → build IP packet → relay ─────────────────────

    private void readUdpResponses(DatagramSocket udpSock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort) {
        try {
            // 64 KB — max UDP payload. QUIC (TikTok, WhatsApp) sends close to this limit.
            byte[]         buf = new byte[65535 - IP4_HEADER_LEN - UDP_HEADER_LEN];
            DatagramPacket dp  = new DatagramPacket(buf, buf.length);
            // FIX DNS TIMEOUT: DNS (port 53) server responds in <100ms and closes.
            // 300s timeout blocked the thread for 5 min, meaning the DNS response
            // never got relayed back to the client. WhatsApp resolves g.whatsapp.net
            // on startup — if DNS is broken, NOTHING works. 5s for DNS, 5min for rest.
            udpSock.setSoTimeout(socketTimeoutForPort(remoteDstPort));
            while (isRunning && !udpSock.isClosed()) {
                udpSock.receive(dp);
                byte[] remoteIpBytes = dp.getAddress().getAddress();
                if (wsClient != null && wsClient.isOpen()) {
                    bytesOut.addAndGet(dp.getLength());
                    // FIX 6: build a proper IPv4+UDP packet so client TUN kernel accepts it
                    ByteBuffer pkt = buildIpUdpPacket(
                            remoteIpBytes, clientIpBytes,
                            dp.getPort(), clientSrcPort,
                            dp.getData(), 0, dp.getLength());
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

    // ─── Synthetic IPv4 packet builders (FIX 6) ──────────────────────────
    // Use explicit absolute-position writes to avoid ByteBuffer position bugs.

    private static ByteBuffer buildIpTcpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int pOff, int pLen) {
        int total = IP4_HEADER_LEN + TCP_HEADER_LEN + pLen;
        byte[] b  = new byte[total];

        // IPv4 header (20 bytes)
        b[0]  = IP4_VERSION_IHL;
        b[1]  = 0x00;
        b[2]  = (byte)(total >> 8);
        b[3]  = (byte)(total);
        b[4]  = 0; b[5] = 0;            // id
        b[6]  = 0x40; b[7] = 0x00;      // flags: DF, frag offset 0
        b[8]  = 64;                       // TTL
        b[9]  = PROTO_TCP;
        b[10] = 0; b[11] = 0;            // checksum placeholder
        b[12] = srcIp[0]; b[13] = srcIp[1]; b[14] = srcIp[2]; b[15] = srcIp[3];
        b[16] = dstIp[0]; b[17] = dstIp[1]; b[18] = dstIp[2]; b[19] = dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN);
        b[10] = (byte)(ipCsum >> 8); b[11] = (byte)(ipCsum);

        // TCP header (20 bytes) at offset 20
        int t = IP4_HEADER_LEN;
        b[t]   = (byte)(srcPort >> 8); b[t+1] = (byte)(srcPort);
        b[t+2] = (byte)(dstPort >> 8); b[t+3] = (byte)(dstPort);
        b[t+4] = b[t+5] = b[t+6] = b[t+7] = 0;  // seq
        b[t+8] = b[t+9] = b[t+10] = b[t+11] = 0; // ack
        b[t+12] = (byte)(TCP_HEADER_LEN << 2);    // data offset (5 << 2 = 0x50)
        b[t+13] = 0x18;                            // PSH + ACK
        b[t+14] = (byte)0xFF; b[t+15] = (byte)0xFF; // window 65535
        b[t+16] = 0; b[t+17] = 0;                 // checksum placeholder
        b[t+18] = 0; b[t+19] = 0;                 // urgent pointer

        // Payload
        System.arraycopy(payload, pOff, b, IP4_HEADER_LEN + TCP_HEADER_LEN, pLen);

        // TCP checksum over pseudo-header + TCP segment
        int tcpLen  = TCP_HEADER_LEN + pLen;
        int tcpCsum = tcpUdpChecksum(srcIp, dstIp, PROTO_TCP, b, IP4_HEADER_LEN, tcpLen);
        b[t+16] = (byte)(tcpCsum >> 8); b[t+17] = (byte)(tcpCsum);

        return ByteBuffer.wrap(b);
    }

    private static ByteBuffer buildIpUdpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int pOff, int pLen) {
        int total = IP4_HEADER_LEN + UDP_HEADER_LEN + pLen;
        byte[] b  = new byte[total];

        // IPv4 header
        b[0]  = IP4_VERSION_IHL;
        b[1]  = 0x00;
        b[2]  = (byte)(total >> 8); b[3] = (byte)(total);
        b[4]  = 0; b[5] = 0;
        b[6]  = 0x40; b[7] = 0x00;
        b[8]  = 64;
        b[9]  = PROTO_UDP;
        b[10] = 0; b[11] = 0;
        b[12] = srcIp[0]; b[13] = srcIp[1]; b[14] = srcIp[2]; b[15] = srcIp[3];
        b[16] = dstIp[0]; b[17] = dstIp[1]; b[18] = dstIp[2]; b[19] = dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN);
        b[10] = (byte)(ipCsum >> 8); b[11] = (byte)(ipCsum);

        // UDP header at offset 20
        int u = IP4_HEADER_LEN;
        int udpLen = UDP_HEADER_LEN + pLen;
        b[u]   = (byte)(srcPort >> 8); b[u+1] = (byte)(srcPort);
        b[u+2] = (byte)(dstPort >> 8); b[u+3] = (byte)(dstPort);
        b[u+4] = (byte)(udpLen >> 8);  b[u+5] = (byte)(udpLen);
        b[u+6] = 0; b[u+7] = 0;   // checksum (optional for UDP, leave 0)

        // Payload
        System.arraycopy(payload, pOff, b, IP4_HEADER_LEN + UDP_HEADER_LEN, pLen);

        return ByteBuffer.wrap(b);
    }

    // ─── Checksum helpers ─────────────────────────────────────────────────

    private static int checksum(byte[] buf, int offset, int length) {
        int sum = 0, i = offset;
        while (i < offset + length - 1) {
            sum += ((buf[i] & 0xFF) << 8) | (buf[i+1] & 0xFF);
            i += 2;
        }
        if (i < offset + length) sum += (buf[i] & 0xFF) << 8;
        while ((sum >> 16) != 0) sum = (sum & 0xFFFF) + (sum >> 16);
        return (~sum) & 0xFFFF;
    }

    private static int tcpUdpChecksum(byte[] srcIp, byte[] dstIp, byte proto,
                                       byte[] segment, int segOff, int segLen) {
        byte[] scratch = new byte[12 + segLen + (segLen % 2)];
        scratch[0]=srcIp[0]; scratch[1]=srcIp[1]; scratch[2]=srcIp[2]; scratch[3]=srcIp[3];
        scratch[4]=dstIp[0]; scratch[5]=dstIp[1]; scratch[6]=dstIp[2]; scratch[7]=dstIp[3];
        scratch[8]=0; scratch[9]=proto;
        scratch[10]=(byte)(segLen>>8); scratch[11]=(byte)(segLen);
        System.arraycopy(segment, segOff, scratch, 12, segLen);
        return checksum(scratch, 0, scratch.length);
    }

    // ─── Utilities ────────────────────────────────────────────────────────

    private static int u16(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off+1] & 0xFF);
    }
    private byte[] tunIpBytes() {
        try { return InetAddress.getByName(assignedTunIp).getAddress(); }
        catch (Exception e) { return new byte[]{10,8,0,2}; }
    }
    private static byte[] ipBytes(String ip) {
        try { return InetAddress.getByName(ip).getAddress(); }
        catch (Exception e) { return new byte[]{10,8,0,2}; }
    }
    private static String orEmpty(String s) { return s != null ? s : ""; }

    private static String jsonGet(String json, String key) {
        String needle = "\"" + key + "\":\"";
        int s = json.indexOf(needle);
        if (s < 0) return null;
        s += needle.length();
        StringBuilder sb = new StringBuilder();
        int i = s;
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c == '\\' && i+1 < json.length()) {
                char n = json.charAt(++i);
                switch(n){case '"':sb.append('"');break;case '\\':sb.append('\\');break;
                           case 'n':sb.append('\n');break;case 'r':sb.append('\r');break;
                           case 't':sb.append('\t');break;default:sb.append(n);break;}
                i++; continue;
            }
            if (c == '"') break;
            sb.append(c); i++;
        }
        return sb.toString();
    }

    private static String j2(String k1,String v1,String k2,String v2){
        return "{\""+k1+"\":\""+esc(v1)+"\",\""+k2+"\":\""+esc(v2)+"\"}";}
    private static String j3(String k1,String v1,String k2,String v2,String k3,String v3){
        return "{\""+k1+"\":\""+esc(v1)+"\",\""+k2+"\":\""+esc(v2)+"\",\""+k3+"\":\""+esc(v3)+"\"}";}
    private static String esc(String s){
        if(s==null)return "";return s.replace("\\","\\\\").replace("\"","\\\"");}

    // ─── Control message ──────────────────────────────────────────────────

    public void sendControlMessage(String message) {
        wsSend(message);
    }

    // ─── Teardown ─────────────────────────────────────────────────────────

    private synchronized void stopVpnTunnel() {
        if (!isRunning && vpnInterface == null && wsClient == null) return;
        isRunning = false;

        WebSocketClient ws = wsClient;
        wsClient = null;
        if (ws != null && !ws.isClosed()) {
            try {
                ws.send("host".equals(role) ? "{\"type\":\"HOST_LEAVE\"}" : "{\"type\":\"CLIENT_LEAVE\"}");
                ws.close();
            } catch (Exception e) { Log.w(TAG, "WS close: " + e.getMessage()); }
        }

        try { if (tunOut       != null) { tunOut.close();       tunOut       = null; } } catch (Exception ignored) {}
        try { if (vpnInterface != null) { vpnInterface.close(); vpnInterface = null; } }
        catch (Exception e) { Log.w(TAG, "TUN close: " + e.getMessage()); }

        for (Socket         s : tcpConnections.values()) try { s.close(); } catch (Exception ignored) {}
        for (DatagramSocket s : udpSockets.values())     try { s.close(); } catch (Exception ignored) {}
        tcpConnections.clear();
        udpSockets.clear();

        ExecutorService ex = executor;
        executor = null;
        if (ex != null) ex.shutdownNow();
        stopWsDrain();   // wake drain thread so it exits cleanly

        stopForeground(true);
        stopSelf();
    }

    // Called when user explicitly disconnects — emits event then tears down
    private synchronized void stopVpnTunnelFromUser() {
        if (!isRunning && vpnInterface == null && wsClient == null) return;
        VpnModule.emitEvent("vpnDisconnected", "User stopped sharing");
        stopVpnTunnel();
    }

    // ─── Foreground notification ──────────────────────────────────────────

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                    CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            getSystemService(NotificationManager.class).createNotificationChannel(ch);
        }
        Intent stopIntent = new Intent(this, NetShareVpnService.class);
        stopIntent.setAction("STOP_VPN");
        PendingIntent stopPending = PendingIntent.getService(
                this, 0, stopIntent, PendingIntent.FLAG_IMMUTABLE);

        Notification notif = new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("NetShare Active")
                .setContentText("host".equals(role)
                        ? "Sharing your internet with clients..."
                        : "Connected through host network...")
                .setSmallIcon(android.R.drawable.ic_menu_share)
                .addAction(android.R.drawable.ic_delete, "Stop", stopPending)
                .setOngoing(true)
                .build();

        startForeground(NOTIFICATION_ID, notif);
    }

    @Override
    public void onDestroy() {
        VpnModule.activeService = null;
        // FIX 4: call stopVpnTunnel() directly — user already got their disconnect
        // event when they tapped Stop. Don't emit a second spurious vpnDisconnected.
        stopVpnTunnel();
        super.onDestroy();
    }
}
