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
import java.util.concurrent.TimeUnit;

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

    private final Map<String, Socket>         tcpConnections = new ConcurrentHashMap<>();
    private final Map<String, DatagramSocket> udpSockets     = new ConcurrentHashMap<>();

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
        executor = Executors.newCachedThreadPool();
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

            // CLIENT: build TUN interface
            // MTU 1400: WS+TLS framing adds overhead; 1500 causes fragmentation
            // that drops UDP datagrams (TikTok video, WhatsApp media). 1400 is safe.
            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)
                   .addRoute("0.0.0.0", 0)
                   .addDnsServer("8.8.8.8")
                   .addDnsServer("8.8.4.4")
                   .addDnsServer("1.1.1.1")
                   .setMtu(1400);

            try {
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
            Log.i(TAG, "CLIENT TUN established");
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
                    send(j3("type", "HOST_REGISTER", "hostId", hostId, "netType", netType));
                } else {
                    // CLIENT TUN already up; WS is now ready
                    VpnModule.emitEvent("vpnConnected", sessionCode);
                    send(j2("type", "CLIENT_JOIN", "accessCode", sessionCode));
                    startPacketReadLoop();
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
                    executor.execute(() -> forwardPacketToInternet(packet));
                } else {
                    if (tunOut == null) return;
                    try {
                        byte[] data = new byte[bytes.remaining()];
                        bytes.get(data);
                        // Only write valid IPv4 packets to TUN (version nibble == 4)
                        if (data.length >= IP4_HEADER_LEN && (data[0] & 0xF0) == 0x40) {
                            tunOut.write(data);
                        } else {
                            Log.w(TAG, "CLIENT: dropped non-IPv4 frame len=" + data.length);
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "TUN write: " + e.getMessage());
                    }
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

        // 30s keepalive ping/pong to detect dead connections
        wsClient.setConnectionLostTimeout(30);

        // FIX 3: Use non-blocking connect(). Callbacks (onOpen/onMessage/onClose/onError)
        // run on the WS internal thread — no executor thread is permanently consumed.
        // connectBlocking() was blocking the thread for the entire session lifetime.
        wsClient.connect();
    }

    // ─── CLIENT: TUN read loop ────────────────────────────────────────────

    private void startPacketReadLoop() {
        executor.execute(() -> {
            byte[] buf = new byte[32767];
            try (FileInputStream in = new FileInputStream(vpnInterface.getFileDescriptor())) {
                while (isRunning) {
                    int len = in.read(buf);
                    if (len > 0 && wsClient != null && wsClient.isOpen()) {
                        wsClient.send(ByteBuffer.wrap(buf, 0, len));
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
                case "JOIN_SUCCESS":
                    VpnModule.emitEvent("joinSuccess", orEmpty(jsonGet(msg, "code")));
                    break;
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
                        wsClient.send("{\"type\":\"PONG\"}");
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
                    Socket old = tcpConnections.remove(key);
                    if (old != null) try { old.close(); } catch (Exception ignored) {}

                    Socket sock = new Socket();
                    protect(sock);
                    try {
                        sock.connect(new java.net.InetSocketAddress(dst, dstPort), 10_000);
                        // 5 min timeout — streaming apps (TikTok, YouTube) hold TCP
                        // connections open for minutes; 30s killed them mid-stream.
                        sock.setSoTimeout(300_000);
                        sock.setTcpNoDelay(true);
                    } catch (Exception e) {
                        Log.w(TAG, "TCP connect [" + key + "]: " + e.getMessage());
                        try { sock.close(); } catch (Exception ignored) {}
                        return;
                    }
                    tcpConnections.put(key, sock);
                    final byte[]      fClientIp = ipBytes(srcIp);
                    final int         fSrcPort  = srcPort;
                    final int         fDstPort  = dstPort;
                    final InetAddress fDst      = dst;
                    final String      fk        = key;
                    executor.execute(() -> readTcpResponses(sock, fk, fClientIp, fSrcPort, fDstPort, fDst));
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
                    udpSockets.put(key, udpSock);
                    final byte[] fClientIp = ipBytes(srcIp);
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
            byte[]      buf = new byte[32767 - IP4_HEADER_LEN - TCP_HEADER_LEN];
            int len;
            while (isRunning && !sock.isClosed() && (len = in.read(buf)) > 0) {
                if (wsClient != null && wsClient.isOpen()) {
                    // FIX 6: build a proper IPv4+TCP packet so client TUN kernel accepts it
                    ByteBuffer pkt = buildIpTcpPacket(
                            remoteIpBytes, clientIpBytes,
                            remoteDstPort, clientSrcPort,
                            buf, 0, len);
                    wsClient.send(pkt);
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
            byte[]         buf = new byte[32767 - IP4_HEADER_LEN - UDP_HEADER_LEN];
            DatagramPacket dp  = new DatagramPacket(buf, buf.length);
            // 5 min timeout — WhatsApp voice/video and TikTok keep UDP flows open
            // for minutes. 60s killed these sessions mid-call or mid-video.
            udpSock.setSoTimeout(300_000);
            while (isRunning && !udpSock.isClosed()) {
                udpSock.receive(dp);
                byte[] remoteIpBytes = dp.getAddress().getAddress();
                if (wsClient != null && wsClient.isOpen()) {
                    // FIX 6: build a proper IPv4+UDP packet so client TUN kernel accepts it
                    ByteBuffer pkt = buildIpUdpPacket(
                            remoteIpBytes, clientIpBytes,
                            dp.getPort(), clientSrcPort,
                            dp.getData(), 0, dp.getLength());
                    wsClient.send(pkt);
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
        if (wsClient != null && wsClient.isOpen()) wsClient.send(message);
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
