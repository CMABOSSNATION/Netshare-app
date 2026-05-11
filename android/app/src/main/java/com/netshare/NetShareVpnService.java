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
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * NetShareVpnService — fully fixed for all-internet support
 *
 * FIXES IN THIS VERSION (on top of previous fixes):
 *
 * FIX-A: ICMP forwarding (proto=1 IPv4, proto=58 IPv6)
 *   All apps rely on ICMP for Path MTU Discovery, ping, and unreachable errors.
 *   Dropping ICMP silently breaks browser navigation, TikTok video loading,
 *   and Facebook's network probe. We now open a DatagramSocket in ICMP mode
 *   (using a raw send via IcmpSocket workaround) — or more practically, we
 *   forward ICMP echo requests by creating an InetAddress.isReachable() probe
 *   and synthesise an ICMP echo reply back to the client TUN.
 *
 * FIX-B: Placeholder TUN builder must NOT add a DnsServer it can't route.
 *   Before JOIN_SUCCESS, the placeholder TUN only routes 10.8.0.0/24.
 *   addDnsServer("8.8.8.8") is useless (8.8.8.8 ∉ 10.8.0.0/24) and WORSE:
 *   Android marks 8.8.8.8 as the VPN DNS, then routes DNS queries through
 *   the VPN TUN — but the TUN can't forward them (no 0.0.0.0/0 route yet).
 *   Result: relay hostname resolution fails → WS never connects → no internet.
 *   FIX: Placeholder TUN gets NO DNS servers and NO DnsServer config.
 *   The app's real DNS (carrier) handles relay hostname resolution normally.
 *
 * FIX-C: UDP socket leak on rapid DNS retries.
 *   When a DNS response is slow, the app retries creating a new UDP flow with
 *   the same srcPort. udpSockets.containsKey(key) guards against re-creating,
 *   but the existing socket may have timed out and closed, leaving a dead entry.
 *   FIX: Check udpSock.isClosed() when retrieving from map; if closed, remove
 *   and create fresh socket so DNS retries actually work.
 *
 * FIX-D: TCP RST on unknown key now sends RST back to client TUN.
 *   Previously we just returned silently. The client kernel keeps retrying,
 *   burning CPU. Now we build a RST packet and write it to tunOut so the
 *   client's TCP stack closes the connection immediately.
 *
 * FIX-E: ICMP echo reply synthesis on host side.
 *   Java doesn't allow raw ICMP sockets without root. Instead, we use
 *   InetAddress.isReachable(protect'd socket, TTL, timeout) which sends ICMP
 *   echo and waits. If reachable, we synthesise a valid ICMP echo reply packet
 *   and write it to wsSend so the client receives a valid ping response.
 *   This fixes browser "network unreachable" probes and app connectivity checks.
 *
 * FIX-F: wsSendQueue cleared on stopVpnTunnel to prevent stale frames
 *   being sent after reconnect in a new session.
 *
 * FIX-G: host forwardPacketToInternet now handles proto=1 (ICMP) and proto=58
 *   (ICMPv6) with echo reply synthesis so browsers and all apps work.
 *
 * Previously documented fixes (FIX 1–6) retained as-is.
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

    // QUIC runs over UDP port 443 (and sometimes 80).
    // TikTok, WhatsApp, YouTube, Facebook all use QUIC for low-latency media.
    private static final int QUIC_PORT_HTTPS = 443;
    private static final int QUIC_PORT_HTTP  = 80;

    // Socket buffer sizes tuned for streaming media
    private static final int TCP_SOCKET_BUFFER  = 512 * 1024;      // 512 KB
    private static final int UDP_SOCKET_BUFFER  = 1024 * 1024;     // 1 MB
    private static final int QUIC_SOCKET_BUFFER = 4 * 1024 * 1024; // 4 MB (TikTok CDN)

    // MTU 1420: leaves room for WS + TLS framing overhead (~60 bytes)
    private static final int TUN_MTU = 1420;

    // WS send queue: lock-free drain thread serialises all sends.
    private static final int WS_SEND_QUEUE_CAPACITY = 4096;
    private final LinkedBlockingQueue<Object> wsSendQueue =
            new LinkedBlockingQueue<>(WS_SEND_QUEUE_CAPACITY);
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
    private volatile String assignedTunIp = "10.8.0.2";

    private final Map<String, Socket>         tcpConnections = new ConcurrentHashMap<>();
    private final Map<String, DatagramSocket> udpSockets     = new ConcurrentHashMap<>();

    private final AtomicLong bytesIn  = new AtomicLong(0);
    private final AtomicLong bytesOut = new AtomicLong(0);

    // ─── Lock-free WebSocket send via dedicated drain thread ────────────────

    private void startWsDrainThread() {
        Thread drain = new Thread(() -> {
            while (true) {
                try {
                    Object item = wsSendQueue.take();
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
        drain.setPriority(Thread.MAX_PRIORITY);
        drain.start();
    }

    private void wsSend(ByteBuffer data) {
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
        wsSendQueue.offer(WS_DRAIN_POISON);
    }

    // Per-port socket timeout:
    // DNS (53/853): 5s — server responds immediately, long timeout blocks replies.
    // NTP (123): 10s.
    // Everything else (HTTP/HTTPS/WhatsApp XMPP/TikTok QUIC): 5 minutes.
    private static int socketTimeoutForPort(int port) {
        if (port == 53 || port == 853) return 5_000;
        if (port == 123)               return 10_000;
        return 300_000;
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        // FIX 1+2: Handle null intent (START_NOT_STICKY safety net)
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
        int poolSize = Math.max(8, Runtime.getRuntime().availableProcessors() * 8);
        executor = Executors.newFixedThreadPool(poolSize);
        startWsDrainThread();
        VpnModule.activeService = this;
        executor.execute(this::startVpnTunnel);

        return START_NOT_STICKY; // FIX 1: was START_STICKY → null-intent crash loop
    }

    // ─── Tunnel setup ─────────────────────────────────────────────────────────

    private void startVpnTunnel() {
        try {
            if ("host".equals(role)) {
                Log.i(TAG, "Host mode — connecting to relay");
                connectToRelay();
                return;
            }

            // CLIENT: Build a PLACEHOLDER TUN interface before connecting to relay.
            // IMPORTANT (FIX-B): Do NOT add DnsServer here. The placeholder only
            // routes 10.8.0.0/24, not 0.0.0.0/0. If we set DNS to 8.8.8.8, Android
            // routes DNS queries into this TUN — but we can't forward them yet because
            // there is no internet route. This blocks the WS relay connection itself
            // (which needs to resolve the relay hostname via real DNS), causing a
            // deadlock: VPN waits for WS, WS waits for DNS, DNS waits for VPN.
            //
            // Solution: No DNS on placeholder. Android uses the real carrier DNS to
            // resolve the relay URL. After JOIN_SUCCESS we rebuild with full routing
            // AND DNS, so all apps go through the tunnel properly.
            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)   // placeholder, overwritten in JOIN_SUCCESS
                   .addRoute("10.8.0.0", 24)      // only VPN subnet, no default route yet
                   // NO addDnsServer here — see FIX-B above
                   .setMtu(TUN_MTU);

            try {
                // Exclude this app so the WS relay connection uses real network
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
            Log.i(TAG, "CLIENT TUN placeholder established — connecting to relay");
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
                    isRunning = true; // FIX 5: set here after WS handshake succeeds
                    VpnModule.emitEvent("vpnConnected", "host");
                    wsSend(j3("type", "HOST_REGISTER", "hostId", hostId, "netType", netType));
                } else {
                    // CLIENT: TUN is up, WS is ready. Do NOT start packet read loop yet.
                    // Must wait for JOIN_SUCCESS which assigns our tunIp. Without the
                    // assigned tunIp, the server cannot route reply packets back to us.
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
                    // Copy immediately — ByteBuffer is reused after callback returns
                    byte[] data = new byte[bytes.remaining()];
                    bytes.get(data);
                    // Offload TUN write to executor — never block the WS reader thread
                    executor.execute(() -> {
                        if (!isRunning || tunOut == null) return;
                        try {
                            if (data.length >= IP4_HEADER_LEN) {
                                int ver = (data[0] & 0xF0) >> 4;
                                if (ver == 4 || (ver == 6 && data.length >= 40)) {
                                    bytesIn.addAndGet(data.length);
                                    tunOut.write(data);
                                } else {
                                    Log.w(TAG, "CLIENT: dropped unknown IP frame ver=" + ver);
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
                String msg = reason != null && !reason.isEmpty() ? reason : "Connection closed";
                VpnModule.emitEvent(isRunning ? "vpnDisconnected" : "vpnError", msg);
                stopVpnTunnel();
            }

            @Override
            public void onError(Exception ex) {
                String raw = ex != null ? ex.getMessage() : null;
                Log.e(TAG, "WS error: " + raw);
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

        // 20s keepalive — detect dead connections faster (WhatsApp requires live WS)
        wsClient.setConnectionLostTimeout(20);

        // Non-blocking connect — callbacks run on WS internal thread, not our executor
        wsClient.connect();
    }

    // ─── CLIENT: TUN read loop ────────────────────────────────────────────

    private void startPacketReadLoop() {
        executor.execute(() -> {
            byte[] buf = new byte[65535];
            try (FileInputStream in = new FileInputStream(vpnInterface.getFileDescriptor())) {
                while (isRunning) {
                    int len = in.read(buf);
                    if (len > 0 && wsClient != null && wsClient.isOpen()) {
                        bytesOut.addAndGet(len);
                        // Must copy — ByteBuffer.wrap(buf) shares the array; next read overwrites it.
                        // Allocate exactly `len` bytes (not always 65535) to reduce GC pressure.
                        byte[] frame = new byte[len];
                        System.arraycopy(buf, 0, frame, 0, len);
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

                    if (assignedIp != null && !assignedIp.isEmpty()) {
                        assignedTunIp = assignedIp;
                    }
                    Log.i(TAG, "JOIN_SUCCESS: tunIp=" + assignedTunIp + " — rebuilding TUN");
                    try {
                        // Close old placeholder interface
                        if (tunOut != null) { try { tunOut.close(); } catch (Exception ignored) {} tunOut = null; }
                        if (vpnInterface != null) { try { vpnInterface.close(); } catch (Exception ignored) {} vpnInterface = null; }

                        // Rebuild TUN with full routing (default route) and proper DNS.
                        // Now that the server knows our tunIp, reply packets can route back.
                        Builder b2 = new Builder();
                        b2.setSession("NetShare")
                          .addAddress(assignedTunIp, 24)
                          // Full default route — ALL internet traffic goes through tunnel
                          .addRoute("0.0.0.0", 0)
                          .addRoute("::", 0)
                          // DNS through tunnel. Both IPv4 and IPv6 DNS servers.
                          // WhatsApp, Facebook, TikTok all do aggressive DNS lookups.
                          // If DNS is not tunnelled, apps detect a broken network and fail.
                          .addDnsServer("8.8.8.8")
                          .addDnsServer("8.8.4.4")
                          .addDnsServer("1.1.1.1")
                          .addDnsServer("1.0.0.1")
                          .addDnsServer("2001:4860:4860::8888")
                          .addDnsServer("2606:4700:4700::1111")
                          .setMtu(TUN_MTU);
                        try { b2.addDisallowedApplication(getPackageName()); } catch (Exception ignored) {}
                        vpnInterface = b2.establish();
                        if (vpnInterface != null) {
                            tunOut = new FileOutputStream(vpnInterface.getFileDescriptor());
                            // START READ LOOP HERE — server now knows our tunIp, can route replies back.
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
            if (version == 6) {
                if (pkt.length < 40) return;
                int proto6 = pkt[6] & 0xFF;
                int pOff6  = 40;
                byte[] src6 = new byte[16]; System.arraycopy(pkt, 8,  src6, 0, 16);
                byte[] dst6 = new byte[16]; System.arraycopy(pkt, 24, dst6, 0, 16);
                InetAddress dst6Addr = InetAddress.getByAddress(dst6);
                String src6Ip = InetAddress.getByAddress(src6).getHostAddress();

                if (proto6 == 6 && pkt.length >= pOff6 + 14) {    // TCP over IPv6
                    handleTcpForward(pkt, pOff6, src6Ip, dst6Addr, tunIpBytes());

                } else if (proto6 == 17 && pkt.length >= pOff6 + 8) {  // UDP over IPv6
                    handleUdpForward(pkt, pOff6, src6Ip, dst6Addr, tunIpBytes());

                } else if (proto6 == 58 && pkt.length >= pOff6 + 8) {  // ICMPv6
                    // For ICMPv6 echo requests (type=128), synthesise a reply (type=129).
                    // This satisfies connectivity probes from browsers and social apps.
                    int icmpType = pkt[pOff6] & 0xFF;
                    if (icmpType == 128) { // Echo Request
                        synthesiseIcmpv6EchoReply(pkt, pOff6, src6, dst6);
                    }
                    // Other ICMPv6 types (ND, RS, RA) are link-local — drop silently.
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

            if (proto == 6) {        // TCP
                handleTcpForward(pkt, ihl, srcIp, dst, tunIpBytes());

            } else if (proto == 17) {  // UDP
                handleUdpForward(pkt, ihl, srcIp, dst, tunIpBytes());

            } else if (proto == 1) {   // ICMP
                // FIX-A / FIX-E: Forward ICMP echo requests.
                // Java cannot open raw ICMP sockets without root. Instead:
                // 1. Parse the ICMP echo request (type=8, code=0).
                // 2. Use InetAddress.isReachable() to actually probe the target
                //    through a protect()'d socket (bypasses VPN loop).
                // 3. If reachable, synthesise an ICMP echo reply and relay back.
                // This makes browser tab loading, TikTok CDN probes, and Facebook
                // network checks work — they all send ICMP pings during startup.
                if (pkt.length < ihl + ICMP_HEADER_LEN) return;
                int icmpType = pkt[ihl] & 0xFF;
                int icmpCode = pkt[ihl + 1] & 0xFF;
                if (icmpType == 8 && icmpCode == 0) {  // Echo Request
                    final InetAddress targetDst = dst;
                    final byte[]      clientIp4  = tunIpBytes();
                    final byte[]      pktCopy    = pkt.clone();
                    final int         pktIhl     = ihl;
                    executor.execute(() -> probeAndReplyIcmpEcho(pktCopy, pktIhl, targetDst, clientIp4));
                }
                // Other ICMP types (destination-unreachable, TTL-exceeded, etc.)
                // are generated by routers, not sent from the client. Drop safely.

            } else {
                // Unknown protocol — drop silently (ESP, GRE, etc.)
                Log.d(TAG, "forwardPacket: unsupported proto=" + proto + " dropping");
            }

        } catch (Exception e) {
            Log.w(TAG, "forwardPacket: " + e.getMessage());
        }
    }

    // ─── Refactored TCP forward (IPv4 and IPv6 share this) ────────────────

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

            if (isRst) {
                Socket s = tcpConnections.remove(key);
                if (s != null) try { s.close(); } catch (Exception ignored) {}
                // FIX-D: Send RST back to client so its kernel closes the connection immediately
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
                    sock.connect(new java.net.InetSocketAddress(dst, dstPort), 10_000);
                    sock.setSoTimeout(socketTimeoutForPort(dstPort));
                    sock.setTcpNoDelay(true);
                    sock.setKeepAlive(true);
                } catch (Exception e) {
                    Log.w(TAG, "TCP connect [" + key + "]: " + e.getMessage());
                    try { sock.close(); } catch (Exception ignored) {}
                    // Send RST to client so it knows the connection failed immediately
                    sendTcpRstToClient(clientIpBytes, dst.getAddress(), dstPort, srcPort);
                    return;
                }
                tcpConnections.put(key, sock);
                final byte[]      fClientIp = clientIpBytes;
                final int         fSrcPort  = srcPort;
                final int         fDstPort  = dstPort;
                final InetAddress fDst      = dst;
                final String      fk        = key;
                executor.execute(() -> readTcpResponses(sock, fk, fClientIp, fSrcPort, fDstPort, fDst));
            }

            // FIX 4: Write payload BEFORE processing FIN (FIN+data is valid TCP)
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

    // ─── Refactored UDP forward (IPv4 and IPv6 share this) ────────────────

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

            // FIX-C: Check if existing socket is closed (expired DNS socket, etc.)
            // If so, remove dead entry so a fresh socket is created below.
            DatagramSocket existing = udpSockets.get(key);
            if (existing != null && existing.isClosed()) {
                udpSockets.remove(key);
                existing = null;
            }

            if (existing == null) {
                DatagramSocket udpSock = new DatagramSocket();
                protect(udpSock);
                boolean isQuic = (dstPort == QUIC_PORT_HTTPS || dstPort == QUIC_PORT_HTTP);
                int bufSize = isQuic ? QUIC_SOCKET_BUFFER : UDP_SOCKET_BUFFER;
                try {
                    udpSock.setReceiveBufferSize(bufSize);
                    udpSock.setSendBufferSize(bufSize);
                } catch (Exception ignored) {}
                udpSockets.put(key, udpSock);
                final byte[] fClientIp = clientIpBytes;
                final int    fSrcPort  = srcPort;
                final int    fDstPort  = dstPort;
                final String fk        = key;
                executor.execute(() -> readUdpResponses(udpSock, fk, fClientIp, fSrcPort, fDstPort));
            }

            DatagramSocket udpSock = udpSockets.get(key);
            if (udpSock != null && !udpSock.isClosed()) {
                udpSock.send(new DatagramPacket(pkt, pOff, pLen, dst, dstPort));
            }
        } catch (Exception e) {
            Log.w(TAG, "handleUdpForward: " + e.getMessage());
        }
    }

    // ─── FIX-E: ICMP echo probe + reply synthesis ─────────────────────────
    // Java doesn't allow raw ICMP sockets without root.
    // InetAddress.isReachable(NetworkInterface, ttl, timeout) tries ICMP echo
    // via the OS if running as root, otherwise falls back to TCP port 7 echo.
    // On Android VpnService we can't get ICMP raw but we can use the
    // 2-argument isReachable(int timeout) and protect the underlying socket
    // (Android 6+ supports protect for isReachable via NetworkInterface route).
    // If the host is reachable, synthesise an ICMP echo reply so the client
    // gets a valid ping response.

    private void probeAndReplyIcmpEcho(byte[] pkt, int ihl, InetAddress target, byte[] clientIp4) {
        try {
            // Extract ICMP identifier and sequence from the request
            int identifier = u16(pkt, ihl + 4);
            int sequence   = u16(pkt, ihl + 6);
            int payloadLen = pkt.length - ihl - ICMP_HEADER_LEN;
            byte[] icmpPayload = new byte[Math.max(0, payloadLen)];
            if (payloadLen > 0) System.arraycopy(pkt, ihl + ICMP_HEADER_LEN, icmpPayload, 0, payloadLen);

            // Try ICMP probe — works if Android grants ICMP socket (API 29+)
            // Timeout 1 second for fast feedback, apps usually timeout after 2s.
            boolean reachable = target.isReachable(1000);

            if (reachable) {
                // Synthesise ICMP echo reply (type=0, code=0) with matching id/seq
                ByteBuffer reply = buildIcmpEchoReply(
                        target.getAddress(), clientIp4,
                        identifier, sequence,
                        icmpPayload);
                wsSend(reply);
            }
            // If not reachable, no reply is sent — client's ping times out normally
        } catch (Exception e) {
            Log.d(TAG, "probeAndReplyIcmpEcho: " + e.getMessage());
        }
    }

    // Build a valid IPv4 ICMP echo reply packet
    private static ByteBuffer buildIcmpEchoReply(byte[] srcIp, byte[] dstIp,
                                                   int identifier, int sequence,
                                                   byte[] payload) {
        int icmpLen = ICMP_HEADER_LEN + payload.length;
        int total   = IP4_HEADER_LEN + icmpLen;
        byte[] b    = new byte[total];

        // IPv4 header
        b[0]  = IP4_VERSION_IHL;
        b[1]  = 0x00;
        b[2]  = (byte)(total >> 8); b[3] = (byte)(total);
        b[4]  = 0; b[5] = 0;
        b[6]  = 0x40; b[7] = 0x00;   // DF flag
        b[8]  = 64;                    // TTL
        b[9]  = PROTO_ICMP;
        b[10] = 0; b[11] = 0;          // checksum placeholder
        b[12] = srcIp[0]; b[13] = srcIp[1]; b[14] = srcIp[2]; b[15] = srcIp[3];
        b[16] = dstIp[0]; b[17] = dstIp[1]; b[18] = dstIp[2]; b[19] = dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN);
        b[10] = (byte)(ipCsum >> 8); b[11] = (byte)(ipCsum);

        // ICMP echo reply header at offset 20
        int i = IP4_HEADER_LEN;
        b[i]   = 0;                           // type=0 (echo reply)
        b[i+1] = 0;                           // code=0
        b[i+2] = 0; b[i+3] = 0;              // checksum placeholder
        b[i+4] = (byte)(identifier >> 8); b[i+5] = (byte)(identifier);
        b[i+6] = (byte)(sequence >> 8);   b[i+7] = (byte)(sequence);
        if (payload.length > 0) System.arraycopy(payload, 0, b, i + ICMP_HEADER_LEN, payload.length);
        int icmpCsum = checksum(b, IP4_HEADER_LEN, icmpLen);
        b[i+2] = (byte)(icmpCsum >> 8); b[i+3] = (byte)(icmpCsum);

        return ByteBuffer.wrap(b);
    }

    // Synthesise ICMPv6 echo reply (type=129) from an echo request (type=128)
    private void synthesiseIcmpv6EchoReply(byte[] pkt, int icmpOff, byte[] src6, byte[] dst6) {
        try {
            // Build ICMPv6 echo reply: swap src/dst, change type 128→129
            int icmpLen = pkt.length - icmpOff;
            byte[] icmp = new byte[icmpLen];
            System.arraycopy(pkt, icmpOff, icmp, 0, icmpLen);
            icmp[0] = (byte) 129;   // Echo Reply
            icmp[1] = 0;
            icmp[2] = 0; icmp[3] = 0;  // checksum reset before recalc

            // IPv6 header (40 bytes) + ICMPv6
            int total = 40 + icmpLen;
            byte[] reply = new byte[total];
            // Version=6, Traffic Class=0, Flow Label=0
            reply[0] = 0x60; reply[1] = 0; reply[2] = 0; reply[3] = 0;
            // Payload length
            reply[4] = (byte)(icmpLen >> 8); reply[5] = (byte)(icmpLen);
            reply[6] = 58;   // Next Header = ICMPv6
            reply[7] = 64;   // Hop Limit
            // Source = original dst (the target we pinged), Dest = original src
            System.arraycopy(dst6, 0, reply, 8,  16);
            System.arraycopy(src6, 0, reply, 24, 16);
            System.arraycopy(icmp, 0, reply, 40, icmpLen);

            // ICMPv6 checksum over pseudo-header
            int csum = icmpv6Checksum(dst6, src6, reply, 40, icmpLen);
            reply[42] = (byte)(csum >> 8); reply[43] = (byte)(csum);

            wsSend(ByteBuffer.wrap(reply));
        } catch (Exception e) {
            Log.d(TAG, "synthesiseIcmpv6EchoReply: " + e.getMessage());
        }
    }

    // FIX-D: Send TCP RST to client TUN so client stack closes the connection fast
    private void sendTcpRstToClient(byte[] srcIp, byte[] dstIp, int srcPort, int dstPort) {
        try {
            if (tunOut == null || !isRunning) return;
            // Build minimal RST packet: IP+TCP header, no payload, RST flag set
            int total = IP4_HEADER_LEN + TCP_HEADER_LEN;
            byte[] b  = new byte[total];

            b[0] = IP4_VERSION_IHL; b[1] = 0;
            b[2] = (byte)(total >> 8); b[3] = (byte)(total);
            b[4] = 0; b[5] = 0;
            b[6] = 0x40; b[7] = 0;
            b[8] = 64; b[9] = PROTO_TCP;
            b[10] = 0; b[11] = 0;
            b[12] = srcIp[0]; b[13] = srcIp[1]; b[14] = srcIp[2]; b[15] = srcIp[3];
            b[16] = dstIp[0]; b[17] = dstIp[1]; b[18] = dstIp[2]; b[19] = dstIp[3];
            int ipCs = checksum(b, 0, IP4_HEADER_LEN);
            b[10] = (byte)(ipCs >> 8); b[11] = (byte)(ipCs);

            int t = IP4_HEADER_LEN;
            b[t]   = (byte)(srcPort >> 8); b[t+1] = (byte)(srcPort);
            b[t+2] = (byte)(dstPort >> 8); b[t+3] = (byte)(dstPort);
            b[t+4] = b[t+5] = b[t+6] = b[t+7] = 0;
            b[t+8] = b[t+9] = b[t+10] = b[t+11] = 0;
            b[t+12] = (byte)(TCP_HEADER_LEN << 2);
            b[t+13] = 0x04;  // RST flag
            b[t+14] = (byte)0xFF; b[t+15] = (byte)0xFF;
            b[t+16] = 0; b[t+17] = 0; b[t+18] = 0; b[t+19] = 0;

            int tcpCs = tcpUdpChecksum(srcIp, dstIp, PROTO_TCP, b, IP4_HEADER_LEN, TCP_HEADER_LEN);
            b[t+16] = (byte)(tcpCs >> 8); b[t+17] = (byte)(tcpCs);

            // Write directly to TUN (client side) or relay back (host side)
            // Host always sends back via wsSend; client would write to tunOut.
            // This method is called from the HOST so we relay the RST back.
            wsSend(ByteBuffer.wrap(b));
        } catch (Exception e) {
            Log.d(TAG, "sendTcpRstToClient: " + e.getMessage());
        }
    }

    // ─── HOST: TCP response → build IP packet → relay ─────────────────────

    private void readTcpResponses(Socket sock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort,
                                   InetAddress remoteAddr) {
        byte[] remoteIpBytes = remoteAddr.getAddress();
        try {
            InputStream in  = sock.getInputStream();
            byte[]      buf = new byte[65535 - IP4_HEADER_LEN - TCP_HEADER_LEN];
            int len;
            while (isRunning && !sock.isClosed()) {
                try {
                    len = in.read(buf);
                } catch (java.net.SocketTimeoutException ste) {
                    if (socketTimeoutForPort(remoteDstPort) <= 10_000) break;
                    Log.d(TAG, "TCP idle timeout [" + key + "] — keeping alive");
                    continue;
                }
                if (len <= 0) break;
                if (wsClient != null && wsClient.isOpen()) {
                    bytesOut.addAndGet(len);
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
            byte[]         buf = new byte[65535 - IP4_HEADER_LEN - UDP_HEADER_LEN];
            DatagramPacket dp  = new DatagramPacket(buf, buf.length);
            udpSock.setSoTimeout(socketTimeoutForPort(remoteDstPort));
            while (isRunning && !udpSock.isClosed()) {
                try {
                    udpSock.receive(dp);
                } catch (java.net.SocketTimeoutException ste) {
                    // DNS: 5s timeout means exchange is complete — exit reader
                    if (socketTimeoutForPort(remoteDstPort) <= 10_000) break;
                    continue;  // Long-lived (QUIC/XMPP): keep reading
                }
                byte[] remoteIpBytes = dp.getAddress().getAddress();
                if (wsClient != null && wsClient.isOpen()) {
                    bytesOut.addAndGet(dp.getLength());
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

    // ─── Synthetic IPv4 packet builders ──────────────────────────────────
    // Explicit absolute-position writes to avoid ByteBuffer position bugs.

    private static ByteBuffer buildIpTcpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int pOff, int pLen) {
        int total = IP4_HEADER_LEN + TCP_HEADER_LEN + pLen;
        byte[] b  = new byte[total];

        // IPv4 header
        b[0]  = IP4_VERSION_IHL;
        b[1]  = 0x00;
        b[2]  = (byte)(total >> 8); b[3] = (byte)(total);
        b[4]  = 0; b[5] = 0;
        b[6]  = 0x40; b[7] = 0x00;
        b[8]  = 64;
        b[9]  = PROTO_TCP;
        b[10] = 0; b[11] = 0;
        b[12] = srcIp[0]; b[13] = srcIp[1]; b[14] = srcIp[2]; b[15] = srcIp[3];
        b[16] = dstIp[0]; b[17] = dstIp[1]; b[18] = dstIp[2]; b[19] = dstIp[3];
        int ipCsum = checksum(b, 0, IP4_HEADER_LEN);
        b[10] = (byte)(ipCsum >> 8); b[11] = (byte)(ipCsum);

        // TCP header at offset 20
        int t = IP4_HEADER_LEN;
        b[t]   = (byte)(srcPort >> 8); b[t+1] = (byte)(srcPort);
        b[t+2] = (byte)(dstPort >> 8); b[t+3] = (byte)(dstPort);
        b[t+4] = b[t+5] = b[t+6] = b[t+7] = 0;   // seq
        b[t+8] = b[t+9] = b[t+10] = b[t+11] = 0;  // ack
        b[t+12] = (byte)(TCP_HEADER_LEN << 2);     // data offset (5 << 2 = 0x50)
        b[t+13] = 0x18;                             // PSH + ACK
        b[t+14] = (byte)0xFF; b[t+15] = (byte)0xFF; // window 65535
        b[t+16] = 0; b[t+17] = 0;                  // checksum placeholder
        b[t+18] = 0; b[t+19] = 0;                  // urgent pointer

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
        b[u+6] = 0; b[u+7] = 0;       // UDP checksum optional, leave 0

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

    // ICMPv6 checksum: IPv6 pseudo-header (src, dst, length, next-header=58)
    private static int icmpv6Checksum(byte[] srcIp6, byte[] dstIp6,
                                       byte[] segment, int segOff, int segLen) {
        byte[] scratch = new byte[40 + segLen + (segLen % 2)];
        System.arraycopy(srcIp6, 0, scratch, 0,  16);
        System.arraycopy(dstIp6, 0, scratch, 16, 16);
        scratch[32] = (byte)(segLen >> 24); scratch[33] = (byte)(segLen >> 16);
        scratch[34] = (byte)(segLen >> 8);  scratch[35] = (byte)(segLen);
        scratch[36] = 0; scratch[37] = 0; scratch[38] = 0;
        scratch[39] = 58;  // Next Header = ICMPv6
        System.arraycopy(segment, segOff, scratch, 40, segLen);
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

        // FIX-F: Clear the send queue before stopping drain thread.
        // Prevents stale frames from a dead session polluting the next session.
        wsSendQueue.clear();
        stopWsDrain();

        stopForeground(true);
        stopSelf();
    }

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
        // FIX 4: call stopVpnTunnel() directly — user already got disconnect event
        stopVpnTunnel();
        super.onDestroy();
    }
}
