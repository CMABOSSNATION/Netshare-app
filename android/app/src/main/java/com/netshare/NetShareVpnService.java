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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * NetShare VPN Service
 *
 * HOW IT WORKS:
 * ─────────────
 * CLIENT mode:
 *   1. Creates a TUN interface (virtual network adapter) on the device
 *   2. All IP packets from the device are captured via the TUN fd
 *   3. Packets are sent as binary DATA messages over WebSocket to the relay
 *   4. Relay forwards packets to the HOST
 *   5. HOST injects them into their real network stack and fetches responses
 *   6. Responses travel back: HOST → relay → client TUN → device apps
 *
 * HOST mode:
 *   1. Connects to relay as HOST_REGISTER
 *   2. Receives DATA packets from clients via WebSocket
 *   3. Forwards them to internet and sends responses back
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG = "NetShareVPN";
    private static final String CHANNEL_ID = "netshare_vpn";
    private static final int NOTIFICATION_ID = 1;

    private ParcelFileDescriptor vpnInterface;
    private WebSocketClient wsClient;
    private ExecutorService executor;
    private volatile boolean isRunning = false;

    private String relayUrl;
    private String sessionCode;
    private String role;
    private String hostId;
    private String netType;

    // Host mode: track open TCP connections per flow key "srcIP:srcPort-dstIP:dstPort"
    private final Map<String, Socket> tcpConnections = new ConcurrentHashMap<>();
    private final Map<String, DatagramSocket> udpSockets = new ConcurrentHashMap<>();

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && "STOP_VPN".equals(intent.getAction())) {
            stopVpnTunnel();
            return START_NOT_STICKY;
        }

        relayUrl    = intent.getStringExtra("RELAY_URL");
        sessionCode = intent.getStringExtra("SESSION_CODE");
        role        = intent.getStringExtra("ROLE");
        hostId      = intent.getStringExtra("HOST_ID");
        netType     = intent.getStringExtra("NET_TYPE");
        if (netType == null || netType.isEmpty()) netType = "WiFi";

        startForegroundNotification();
        executor = Executors.newFixedThreadPool("host".equals(role) ? 20 : 3);
        VpnModule.activeService = this;
        executor.execute(this::startVpnTunnel);

        return START_STICKY;
    }

    private void startVpnTunnel() {
        try {
            if ("host".equals(role)) {
                // ── HOST MODE: no TUN interface needed ───────────────
                // Host just connects to relay via WebSocket and forwards
                // client packets to the internet. Host's own internet
                // continues working normally — no VPN tunnel on host.
                isRunning = true;
                Log.i(TAG, "Host mode started — connecting to relay");
                VpnModule.emitEvent("vpnConnected", "host");
                connectToRelay();
                return;
            }

            // ── CLIENT MODE: create TUN interface ────────────────────
            Builder builder = new Builder();
            builder.setSession("NetShare")
                   .addAddress("10.8.0.2", 24)
                   .addRoute("0.0.0.0", 0)
                   .addDnsServer("8.8.8.8")
                   .addDnsServer("8.8.4.4")
                   .setMtu(1500);

            // Exclude our own app so WebSocket doesn't loop through VPN
            builder.addDisallowedApplication(getPackageName());

            vpnInterface = builder.establish();

            if (vpnInterface == null) {
                Log.e(TAG, "Failed to establish VPN interface");
                VpnModule.emitEvent("vpnError", "Failed to establish VPN interface");
                return;
            }

            isRunning = true;
            Log.i(TAG, "VPN tunnel established");
            VpnModule.emitEvent("vpnConnected", sessionCode);

            // ── Connect to relay WebSocket ────────────────────────────
            connectToRelay();

            // ── Read packets from TUN and send to relay ───────────────
            executor.execute(this::readTunAndForwardToRelay);

        } catch (Exception e) {
            Log.e(TAG, "VPN start error: " + e.getMessage());
            VpnModule.emitEvent("vpnError", e.getMessage());
        }
    }

    // ── CLIENT: read IP packets from TUN and forward to relay ────────────
    private void readTunAndForwardToRelay() {
        FileInputStream tunIn = new FileInputStream(vpnInterface.getFileDescriptor());
        byte[] packet = new byte[32767];
        while (isRunning) {
            try {
                int len = tunIn.read(packet);
                if (len > 0 && wsClient != null && wsClient.isOpen()) {
                    wsClient.send(ByteBuffer.wrap(packet, 0, len));
                }
            } catch (Exception e) {
                if (isRunning) Log.w(TAG, "TUN read error: " + e.getMessage());
                break;
            }
        }
    }

    private void connectToRelay() throws Exception {
        URI uri = new URI(relayUrl);
        final NetShareVpnService self = this;

        wsClient = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                Log.i(TAG, "WebSocket connected to relay");
                if ("host".equals(role)) {
                    send("{\"type\":\"HOST_REGISTER\",\"hostId\":\"" + hostId + "\",\"netType\":\"" + netType + "\"}");
                } else {
                    send("{\"type\":\"CLIENT_JOIN\",\"accessCode\":\"" + sessionCode + "\"}");
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
                    // HOST: forward received client packet to internet
                    byte[] packet = new byte[bytes.remaining()];
                    bytes.get(packet);
                    executor.execute(() -> forwardPacketToInternet(packet));
                } else {
                    // CLIENT: inject received internet response into TUN
                    if (vpnInterface != null) {
                        try {
                            FileOutputStream out = new FileOutputStream(vpnInterface.getFileDescriptor());
                            byte[] data = new byte[bytes.remaining()];
                            bytes.get(data);
                            out.write(data);
                        } catch (Exception e) {
                            Log.e(TAG, "TUN write error: " + e.getMessage());
                        }
                    }
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                Log.i(TAG, "WebSocket closed: " + reason);
                VpnModule.emitEvent("vpnDisconnected", reason != null ? reason : "closed");
                isRunning = false;
            }

            @Override
            public void onError(Exception ex) {
                Log.e(TAG, "WebSocket error: " + (ex != null ? ex.getMessage() : "unknown"));
                VpnModule.emitEvent("vpnError", ex != null ? ex.getMessage() : "unknown error");
            }
        };

        // Use an SSLSocketFactory that calls protect() on every socket it creates.
        // This ensures the WebSocket connection bypasses the VPN tunnel (no loop)
        // AND handles wss:// TLS correctly using Android's default TLS context.
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        final SSLSocketFactory baseSSL = sslContext.getSocketFactory();

        wsClient.setSocketFactory(new SSLSocketFactory() {
            @Override
            public Socket createSocket() throws IOException {
                Socket s = baseSSL.createSocket();
                self.protect(s);
                return s;
            }

            @Override
            public Socket createSocket(String host, int port) throws IOException {
                Socket s = baseSSL.createSocket(host, port);
                self.protect(s);
                return s;
            }

            @Override
            public Socket createSocket(String host, int port,
                                       InetAddress localAddr, int localPort) throws IOException {
                Socket s = baseSSL.createSocket(host, port, localAddr, localPort);
                self.protect(s);
                return s;
            }

            @Override
            public Socket createSocket(InetAddress host, int port) throws IOException {
                Socket s = baseSSL.createSocket(host, port);
                self.protect(s);
                return s;
            }

            @Override
            public Socket createSocket(InetAddress address, int port,
                                       InetAddress localAddress, int localPort) throws IOException {
                Socket s = baseSSL.createSocket(address, port, localAddress, localPort);
                self.protect(s);
                return s;
            }

            @Override
            public Socket createSocket(Socket plain, String host,
                                       int port, boolean autoClose) throws IOException {
                Socket s = baseSSL.createSocket(plain, host, port, autoClose);
                self.protect(s);
                return s;
            }

            @Override
            public String[] getDefaultCipherSuites() {
                return baseSSL.getDefaultCipherSuites();
            }

            @Override
            public String[] getSupportedCipherSuites() {
                return baseSSL.getSupportedCipherSuites();
            }
        });

        wsClient.connectBlocking();

        // ── 3. Read loop: capture packets from TUN → send to relay ──
        if ("client".equals(role)) {
            startPacketReadLoop();
        }
    }

    private void startPacketReadLoop() {
        executor.execute(() -> {
            byte[] packet = new byte[32767];
            try (FileInputStream in = new FileInputStream(vpnInterface.getFileDescriptor())) {
                while (isRunning) {
                    int length = in.read(packet);
                    if (length > 0 && wsClient != null && wsClient.isOpen()) {
                        wsClient.send(ByteBuffer.wrap(packet, 0, length));
                    }
                }
            } catch (Exception e) {
                if (isRunning) Log.e(TAG, "Packet read error: " + e.getMessage());
            }
        });
    }

    private void handleRelayMessage(String message) {
        try {
            if (message.contains("\"SESSION_CREATED\"")) {
                String code = message.split("\"code\":\"")[1].split("\"")[0];
                VpnModule.emitEvent("sessionCreated", code);
            } else if (message.contains("\"JOIN_SUCCESS\"")) {
                VpnModule.emitEvent("joinSuccess", sessionCode);
            } else if (message.contains("\"JOIN_ERROR\"")) {
                String reason = message.split("\"reason\":\"")[1].split("\"")[0];
                VpnModule.emitEvent("joinError", reason);
            } else if (message.contains("\"CLIENT_CONNECTED\"")) {
                String clientId = message.split("\"clientId\":\"")[1].split("\"")[0];
                VpnModule.emitEvent("clientConnected", clientId);
            } else if (message.contains("\"CLIENT_DISCONNECTED\"")) {
                VpnModule.emitEvent("clientDisconnected", "");
            } else if (message.contains("\"HOST_LEFT\"")) {
                VpnModule.emitEvent("hostLeft", "Host ended the session");
            } else if (message.contains("\"PING\"")) {
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send("{\"type\":\"PONG\"}");
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "Message parse error: " + e.getMessage());
        }
    }

    /**
     * Called from VpnModule.sendControlMessage (JS layer).
     * Sends a JSON control message over the active WebSocket.
     */
    public void sendControlMessage(String message) {
        if (wsClient != null && wsClient.isOpen()) {
            wsClient.send(message);
        }
    }

    // ── Host: parse IP packet and forward to internet ─────────────────
    private void forwardPacketToInternet(byte[] packet) {
        if (packet.length < 20) return;
        try {
            int version = (packet[0] >> 4) & 0xF;
            if (version != 4) return; // IPv4 only

            int protocol = packet[9] & 0xFF;
            int ihl = (packet[0] & 0xF) * 4;

            // Destination IP
            byte[] dstIpBytes = new byte[]{packet[16], packet[17], packet[18], packet[19]};
            InetAddress dstAddr = InetAddress.getByAddress(dstIpBytes);

            // Source IP (client's virtual IP — used as flow key)
            byte[] srcIpBytes = new byte[]{packet[12], packet[13], packet[14], packet[15]};
            String srcIp = InetAddress.getByAddress(srcIpBytes).getHostAddress();

            if (protocol == 6) {
                // TCP
                int srcPort = ((packet[ihl] & 0xFF) << 8) | (packet[ihl + 1] & 0xFF);
                int dstPort = ((packet[ihl + 2] & 0xFF) << 8) | (packet[ihl + 3] & 0xFF);
                int tcpFlags = packet[ihl + 13] & 0xFF;
                boolean isSyn = (tcpFlags & 0x02) != 0;
                boolean isFin = (tcpFlags & 0x01) != 0 || (tcpFlags & 0x04) != 0;
                int dataOffset = ((packet[ihl + 12] >> 4) & 0xF) * 4;
                int payloadStart = ihl + dataOffset;
                int payloadLen = packet.length - payloadStart;

                String flowKey = srcIp + ":" + srcPort + "-" + dstAddr.getHostAddress() + ":" + dstPort;

                if (isSyn || !tcpConnections.containsKey(flowKey)) {
                    // New TCP connection
                    Socket sock = new Socket();
                    protect(sock);
                    sock.connect(new java.net.InetSocketAddress(dstAddr, dstPort), 5000);
                    sock.setSoTimeout(30000);
                    tcpConnections.put(flowKey, sock);
                    // Start response reader for this connection
                    final String fk = flowKey;
                    executor.execute(() -> readTcpResponses(sock, fk));
                }

                Socket sock = tcpConnections.get(flowKey);
                if (sock != null && !sock.isClosed() && payloadLen > 0) {
                    OutputStream out = sock.getOutputStream();
                    out.write(packet, payloadStart, payloadLen);
                    out.flush();
                }

                if (isFin) {
                    Socket s = tcpConnections.remove(flowKey);
                    if (s != null) try { s.close(); } catch (Exception ignored) {}
                }

            } else if (protocol == 17) {
                // UDP
                int srcPort = ((packet[ihl] & 0xFF) << 8) | (packet[ihl + 1] & 0xFF);
                int dstPort = ((packet[ihl + 2] & 0xFF) << 8) | (packet[ihl + 3] & 0xFF);
                int payloadStart = ihl + 8;
                int payloadLen = packet.length - payloadStart;
                if (payloadLen <= 0) return;

                String flowKey = srcIp + ":" + srcPort + "-" + dstAddr.getHostAddress() + ":" + dstPort;

                if (!udpSockets.containsKey(flowKey)) {
                    DatagramSocket udpSock = new DatagramSocket();
                    protect(udpSock);
                    udpSockets.put(flowKey, udpSock);
                    final String fk = flowKey;
                    executor.execute(() -> readUdpResponses(udpSock, fk));
                }

                DatagramSocket udpSock = udpSockets.get(flowKey);
                if (udpSock != null && !udpSock.isClosed()) {
                    DatagramPacket dp = new DatagramPacket(packet, payloadStart, payloadLen, dstAddr, dstPort);
                    udpSock.send(dp);
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "forwardPacket error: " + e.getMessage());
        }
    }

    private void readTcpResponses(Socket sock, String flowKey) {
        try {
            InputStream in = sock.getInputStream();
            byte[] buf = new byte[32767];
            int len;
            while (isRunning && !sock.isClosed() && (len = in.read(buf)) > 0) {
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send(ByteBuffer.wrap(buf, 0, len));
                }
            }
        } catch (Exception e) {
            if (isRunning) Log.w(TAG, "TCP read error [" + flowKey + "]: " + e.getMessage());
        } finally {
            tcpConnections.remove(flowKey);
            try { sock.close(); } catch (Exception ignored) {}
        }
    }

    private void readUdpResponses(DatagramSocket udpSock, String flowKey) {
        try {
            byte[] buf = new byte[32767];
            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            udpSock.setSoTimeout(30000);
            while (isRunning && !udpSock.isClosed()) {
                udpSock.receive(dp);
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send(ByteBuffer.wrap(dp.getData(), 0, dp.getLength()));
                }
            }
        } catch (Exception e) {
            if (isRunning) Log.w(TAG, "UDP read error [" + flowKey + "]: " + e.getMessage());
        } finally {
            udpSockets.remove(flowKey);
            try { udpSock.close(); } catch (Exception ignored) {}
        }
    }

    private void stopVpnTunnel() {
        isRunning = false;

        try {
            if (wsClient != null) {
                wsClient.send("host".equals(role)
                    ? "{\"type\":\"HOST_LEAVE\"}"
                    : "{\"type\":\"CLIENT_LEAVE\"}");
                wsClient.close();
            }
        } catch (Exception e) {
            Log.w(TAG, "WS close error: " + e.getMessage());
        }

        try {
            if (vpnInterface != null) {
                vpnInterface.close();
                vpnInterface = null;
            }
        } catch (Exception e) {
            Log.w(TAG, "TUN close error: " + e.getMessage());
        }

        if (executor != null) executor.shutdownNow();

        // Close all host forwarding connections
        for (Socket s : tcpConnections.values()) try { s.close(); } catch (Exception ignored) {}
        for (DatagramSocket s : udpSockets.values()) try { s.close(); } catch (Exception ignored) {}
        tcpConnections.clear();
        udpSockets.clear();

        VpnModule.emitEvent("vpnDisconnected", "User stopped sharing");
        stopForeground(true);
        stopSelf();
    }

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW
            );
            getSystemService(NotificationManager.class).createNotificationChannel(channel);
        }

        Intent stopIntent = new Intent(this, NetShareVpnService.class);
        stopIntent.setAction("STOP_VPN");
        PendingIntent stopPending = PendingIntent.getService(
            this, 0, stopIntent, PendingIntent.FLAG_IMMUTABLE
        );

        Notification notification = new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare Active")
            .setContentText("Sharing internet via relay...")
            .setSmallIcon(android.R.drawable.ic_menu_share)
            .addAction(android.R.drawable.ic_delete, "Stop", stopPending)
            .setOngoing(true)
            .build();

        startForeground(NOTIFICATION_ID, notification);
    }

    @Override
    public void onDestroy() {
        VpnModule.activeService = null;
        stopVpnTunnel();
        super.onDestroy();
    }
}
