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

public class NetShareVpnService extends VpnService {

    private static final String TAG             = "NetShareVPN";
    private static final String CHANNEL_ID      = "netshare_vpn";
    private static final int    NOTIFICATION_ID = 1;

    // IP packet construction constants
    private static final int  IP4_HEADER_LEN  = 20;
    private static final int  TCP_HEADER_LEN  = 20;
    private static final int  UDP_HEADER_LEN  = 8;
    private static final byte IP4_VERSION_IHL = 0x45; // version=4, IHL=5
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
        if (intent != null && "STOP_VPN".equals(intent.getAction())) {
            stopVpnTunnelFromUser();
            return START_NOT_STICKY;
        }

        relayUrl    = intent.getStringExtra("RELAY_URL");
        sessionCode = intent.getStringExtra("SESSION_CODE");
        role        = intent.getStringExtra("ROLE");
        hostId      = intent.getStringExtra("HOST_ID");
        netType     = intent.getStringExtra("NET_TYPE");
        if (netType == null || netType.isEmpty()) netType = "WiFi";

        startForegroundNotification();

        executor = Executors.newCachedThreadPool();
        VpnModule.activeService = this;
        executor.execute(this::startVpnTunnel);
        return START_STICKY;
    }

    // ─── Tunnel setup ─────────────────────────────────────────────────────

    private void startVpnTunnel() {
        try {
            if ("host".equals(role)) {
                Log.i(TAG, "Host mode: connecting to relay (no TUN)");
                isRunning = true;
                connectToRelay();
                return;
            }

            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)
                   .addRoute("0.0.0.0", 0)
                   .addDnsServer("8.8.8.8")
                   .addDnsServer("8.8.4.4")
                   .setMtu(1500);

            try {
                builder.addDisallowedApplication(getPackageName());
            } catch (Exception e) {
                Log.w(TAG, "addDisallowedApplication failed: " + e.getMessage());
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
            VpnModule.emitEvent("vpnError", e.getMessage() != null ? e.getMessage() : "VPN start failed");
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
                    VpnModule.emitEvent("vpnConnected", "host");
                    send(j3("type", "HOST_REGISTER", "hostId", hostId, "netType", netType));
                } else {
                    VpnModule.emitEvent("vpnConnected", sessionCode != null ? sessionCode : "");
                    send(j2("type", "CLIENT_JOIN", "accessCode", sessionCode != null ? sessionCode : ""));
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
                        // Accept only valid IPv4 packets (version nibble must be 4)
                        if (data.length >= IP4_HEADER_LEN && (data[0] & 0xF0) == 0x40) {
                            tunOut.write(data);
                        } else {
                            Log.w(TAG, "CLIENT: dropped non-IPv4 relay frame, len=" + data.length);
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "TUN write: " + e.getMessage());
                    }
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                Log.i(TAG, "WS closed: " + reason);
                if (isRunning) {
                    VpnModule.emitEvent("vpnDisconnected", reason != null ? reason : "closed");
                }
                stopVpnTunnel();
            }

            @Override
            public void onError(Exception ex) {
                String msg = ex != null ? ex.getMessage() : "WebSocket error";
                Log.e(TAG, "WS error: " + msg);
                if (isRunning) {
                    // FIX: emit a user-friendly timeout message if it's a connection timeout
                    String userMsg = (msg != null && msg.contains("timed out"))
                            ? "Server is starting up, please try again in 30 seconds."
                            : (msg != null ? msg : "WebSocket error");
                    VpnModule.emitEvent("vpnError", userMsg);
                }
                stopVpnTunnel();
            }
        };

        // Protect plain socket BEFORE SSL wrapping so VPN tunnel bypass works correctly.
        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(null, null, null);
        final SSLSocketFactory baseSSL = sslCtx.getSocketFactory();

        wsClient.setSocketFactory(new SSLSocketFactory() {
            @Override public Socket createSocket() throws IOException {
                Socket s = new Socket();
                self.protect(s);
                return s;
            }
            @Override public Socket createSocket(String h, int p) throws IOException {
                Socket s = new Socket(); self.protect(s);
                s.connect(new java.net.InetSocketAddress(h, p)); return s; }
            @Override public Socket createSocket(String h, int p, InetAddress la, int lp) throws IOException {
                Socket s = new Socket(); self.protect(s);
                s.bind(new java.net.InetSocketAddress(la, lp));
                s.connect(new java.net.InetSocketAddress(h, p)); return s; }
            @Override public Socket createSocket(InetAddress h, int p) throws IOException {
                Socket s = new Socket(); self.protect(s);
                s.connect(new java.net.InetSocketAddress(h, p)); return s; }
            @Override public Socket createSocket(InetAddress a, int p, InetAddress la, int lp) throws IOException {
                Socket s = new Socket(); self.protect(s);
                s.bind(new java.net.InetSocketAddress(la, lp));
                s.connect(new java.net.InetSocketAddress(a, p)); return s; }
            @Override public Socket createSocket(Socket plain, String h, int p, boolean ac) throws IOException {
                self.protect(plain);                            // protect plain fd first
                return baseSSL.createSocket(plain, h, p, ac);  // then wrap in SSL
            }
            @Override public String[] getDefaultCipherSuites() { return baseSSL.getDefaultCipherSuites(); }
            @Override public String[] getSupportedCipherSuites() { return baseSSL.getSupportedCipherSuites(); }
        });

        wsClient.setConnectionLostTimeout(30);

        // FIX: Increased from 15s to 60s — Render free-tier cold starts take up to ~50s.
        // If it still times out, onError fires with a user-friendly "starting up" message.
        boolean connected = wsClient.connectBlocking(60, TimeUnit.SECONDS);
        if (!connected && isRunning) {
            VpnModule.emitEvent("vpnError", "Could not reach server. It may be starting up — please try again in 30 seconds.");
            stopVpnTunnel();
        }
    }

    // ─── CLIENT: TUN → relay packet loop ─────────────────────────────────

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

            InetAddress dst = InetAddress.getByAddress(
                    new byte[]{pkt[16], pkt[17], pkt[18], pkt[19]});
            String srcIp = InetAddress.getByAddress(
                    new byte[]{pkt[12], pkt[13], pkt[14], pkt[15]}).getHostAddress();

            if (proto == 6) {
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
                        sock.connect(new java.net.InetSocketAddress(dst, dstPort), 5000);
                        sock.setSoTimeout(30_000);
                        sock.setTcpNoDelay(true);
                    } catch (Exception e) {
                        Log.w(TAG, "TCP connect [" + key + "]: " + e.getMessage());
                        try { sock.close(); } catch (Exception ignored) {}
                        return;
                    }
                    tcpConnections.put(key, sock);
                    final String      fk        = key;
                    final byte[]      fClientIp = ipBytes(srcIp);
                    final int         fSrcPort  = srcPort;
                    final int         fDstPort  = dstPort;
                    final InetAddress fDst      = dst;
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

            } else if (proto == 17) {
                if (pkt.length < ihl + 8) return;
                int srcPort = u16(pkt, ihl);
                int dstPort = u16(pkt, ihl + 2);
                int pOff    = ihl + 8;
                int pLen    = pkt.length - pOff;
                if (pLen <= 0) return;

                String key = srcIp + ":" + srcPort + "-" + dst.getHostAddress() + ":" + dstPort;
                if (!udpSockets.containsKey(key)) {
                    DatagramSocket udpSock = new DatagramSocket();
                    protect(udpSock);
                    udpSockets.put(key, udpSock);
                    final String  fk        = key;
                    final byte[]  fClientIp = ipBytes(srcIp);
                    final int     fSrcPort  = srcPort;
                    final int     fDstPort  = dstPort;
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

    // ─── HOST: TCP response → relay ───────────────────────────────────────

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

    // ─── HOST: UDP response → relay ───────────────────────────────────────

    private void readUdpResponses(DatagramSocket udpSock, String key,
                                   byte[] clientIpBytes, int clientSrcPort, int remoteDstPort) {
        try {
            byte[]         buf = new byte[32767 - IP4_HEADER_LEN - UDP_HEADER_LEN];
            DatagramPacket dp  = new DatagramPacket(buf, buf.length);
            udpSock.setSoTimeout(60_000);
            while (isRunning && !udpSock.isClosed()) {
                udpSock.receive(dp);
                byte[] remoteIpBytes = dp.getAddress().getAddress();
                if (wsClient != null && wsClient.isOpen()) {
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

    // ─── Synthetic IPv4 packet builders ──────────────────────────────────

    private static ByteBuffer buildIpTcpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int off, int len) {
        int totalLen = IP4_HEADER_LEN + TCP_HEADER_LEN + len;
        ByteBuffer b = ByteBuffer.allocate(totalLen);

        b.put(IP4_VERSION_IHL);
        b.put((byte) 0x00);
        b.putShort((short) totalLen);
        b.putShort((short) 0);
        b.putShort((short) 0x4000);
        b.put((byte) 64);
        b.put(PROTO_TCP);
        b.putShort((short) 0);
        b.put(srcIp);
        b.put(dstIp);
        b.putShort(10, (short) checksum(b.array(), 0, IP4_HEADER_LEN));

        b.putShort((short) srcPort);
        b.putShort((short) dstPort);
        b.putInt(0);
        b.putInt(0);
        b.put((byte) (TCP_HEADER_LEN << 2));
        b.put((byte) 0x18);
        b.putShort((short) 65535);
        b.putShort((short) 0);
        b.putShort((short) 0);

        b.put(payload, off, len);

        int tcpLen  = TCP_HEADER_LEN + len;
        int tcpCsum = tcpUdpChecksum(srcIp, dstIp, PROTO_TCP, b.array(), IP4_HEADER_LEN, tcpLen);
        b.putShort(IP4_HEADER_LEN + 16, (short) tcpCsum);

        b.flip();
        return b;
    }

    private static ByteBuffer buildIpUdpPacket(byte[] srcIp, byte[] dstIp,
                                                int srcPort, int dstPort,
                                                byte[] payload, int off, int len) {
        int totalLen = IP4_HEADER_LEN + UDP_HEADER_LEN + len;
        ByteBuffer b = ByteBuffer.allocate(totalLen);

        b.put(IP4_VERSION_IHL);
        b.put((byte) 0x00);
        b.putShort((short) totalLen);
        b.putShort((short) 0);
        b.putShort((short) 0x4000);
        b.put((byte) 64);
        b.put(PROTO_UDP);
        b.putShort((short) 0);
        b.put(srcIp);
        b.put(dstIp);
        b.putShort(10, (short) checksum(b.array(), 0, IP4_HEADER_LEN));

        b.putShort((short) srcPort);
        b.putShort((short) dstPort);
        b.putShort((short) (UDP_HEADER_LEN + len));
        b.putShort((short) 0);

        b.put(payload, off, len);

        b.flip();
        return b;
    }

    // ─── Checksum helpers ─────────────────────────────────────────────────

    private static int checksum(byte[] buf, int offset, int length) {
        int sum = 0;
        int i   = offset;
        while (i < offset + length - 1) {
            sum += ((buf[i] & 0xFF) << 8) | (buf[i + 1] & 0xFF);
            i   += 2;
        }
        if (i < offset + length) {
            sum += (buf[i] & 0xFF) << 8;
        }
        while ((sum >> 16) != 0) {
            sum = (sum & 0xFFFF) + (sum >> 16);
        }
        return (~sum) & 0xFFFF;
    }

    private static int tcpUdpChecksum(byte[] srcIp, byte[] dstIp, byte proto,
                                       byte[] segment, int segOff, int segLen) {
        int pseudoLen = 12 + segLen;
        byte[] scratch = new byte[pseudoLen + (pseudoLen % 2)];
        scratch[0] = srcIp[0]; scratch[1] = srcIp[1];
        scratch[2] = srcIp[2]; scratch[3] = srcIp[3];
        scratch[4] = dstIp[0]; scratch[5] = dstIp[1];
        scratch[6] = dstIp[2]; scratch[7] = dstIp[3];
        scratch[8] = 0;
        scratch[9] = proto;
        scratch[10] = (byte)((segLen >> 8) & 0xFF);
        scratch[11] = (byte)(segLen & 0xFF);
        System.arraycopy(segment, segOff, scratch, 12, segLen);
        return checksum(scratch, 0, scratch.length);
    }

    // ─── Utilities ────────────────────────────────────────────────────────

    private static int u16(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off + 1] & 0xFF);
    }

    private static byte[] ipBytes(String ip) {
        try { return InetAddress.getByName(ip).getAddress(); }
        catch (Exception e) { return new byte[]{10, 8, 0, 2}; }
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
            if (c == '\\' && i + 1 < json.length()) {
                char n = json.charAt(++i);
                switch (n) {
                    case '"':  sb.append('"');  break;
                    case '\\': sb.append('\\'); break;
                    case 'n':  sb.append('\n'); break;
                    case 'r':  sb.append('\r'); break;
                    case 't':  sb.append('\t'); break;
                    default:   sb.append(n);    break;
                }
                i++; continue;
            }
            if (c == '"') break;
            sb.append(c); i++;
        }
        return sb.toString();
    }

    private static String j2(String k1, String v1, String k2, String v2) {
        return "{\"" + k1 + "\":\"" + esc(v1) + "\",\"" + k2 + "\":\"" + esc(v2) + "\"}";
    }

    private static String j3(String k1, String v1, String k2, String v2, String k3, String v3) {
        return "{\"" + k1 + "\":\"" + esc(v1) + "\","
             + "\"" + k2 + "\":\"" + esc(v2) + "\","
             + "\"" + k3 + "\":\"" + esc(v3) + "\"}";
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ─── Control message ──────────────────────────────────────────────────

    public void sendControlMessage(String message) {
        if (wsClient != null && wsClient.isOpen()) {
            wsClient.send(message);
        }
    }

    // ─── Teardown ─────────────────────────────────────────────────────────

    private synchronized void stopVpnTunnel() {
        if (!isRunning && vpnInterface == null && wsClient == null) return;
        isRunning = false;

        WebSocketClient ws = wsClient;
        wsClient = null;
        try {
            if (ws != null && ws.isOpen()) {
                ws.send("host".equals(role)
                        ? "{\"type\":\"HOST_LEAVE\"}"
                        : "{\"type\":\"CLIENT_LEAVE\"}");
                ws.close();
            }
        } catch (Exception e) {
            Log.w(TAG, "WS close: " + e.getMessage());
        }

        try { if (tunOut       != null) { tunOut.close();       tunOut      = null; } } catch (Exception ignored) {}
        try { if (vpnInterface != null) { vpnInterface.close(); vpnInterface = null; } }
        catch (Exception e) { Log.w(TAG, "TUN close: " + e.getMessage()); }

        for (Socket         s : tcpConnections.values()) try { s.close(); } catch (Exception ignored) {}
        for (DatagramSocket s : udpSockets.values())     try { s.close(); } catch (Exception ignored) {}
        tcpConnections.clear();
        udpSockets.clear();

        if (executor != null) { executor.shutdownNow(); executor = null; }

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
                .setContentText(role != null && role.equals("host")
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
        stopVpnTunnelFromUser();
        super.onDestroy();
    }
}
