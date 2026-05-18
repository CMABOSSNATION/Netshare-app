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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * NetShareVpnService.java — Client-side VPN for NetShare
 *
 * Captures ALL traffic from the device (WiFi + mobile data) via a TUN
 * interface and tunnels it through a WebSocket to the Cloudflare relay,
 * which forwards it to the host device.
 *
 * Flow:
 *   Any app on device
 *       ↓ IP packets via TUN interface
 *   NetShareVpnService
 *       ↓ HTTP CONNECT frames over WebSocket
 *   Cloudflare Worker (relay)
 *       ↓ forwards frames
 *   Host ProxyModule
 *       ↓ real TCP to internet
 *   Internet
 *
 * Frame protocol (same as existing ProxyModule):
 *   [4 bytes: connId][1 byte: frameType][N bytes: payload]
 *   0x01 OPEN  — open connection to host:port
 *   0x02 DATA  — raw bytes
 *   0x03 CLOSE — close connection
 *   0x04 READY — tunnel paired signal
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG               = "NetShareVPN";
    private static final String CHANNEL_ID        = "netshare_vpn";
    private static final int    NOTIF_ID          = 1001;
    private static final int    VPN_MTU           = 1500;

    // Frame types (must match ProxyModule.java)
    private static final byte FT_OPEN  = 0x01;
    private static final byte FT_DATA  = 0x02;
    private static final byte FT_CLOSE = 0x03;
    private static final byte FT_READY = 0x04;

    // DNS server to use inside the VPN
    private static final String VPN_DNS     = "8.8.8.8";
    private static final String VPN_ADDRESS = "10.0.0.2";
    private static final String VPN_GATEWAY = "10.0.0.1";
    private static final String VPN_ROUTE   = "0.0.0.0";

    private ParcelFileDescriptor vpnInterface;
    private FileInputStream      tunIn;
    private FileOutputStream     tunOut;

    private OkHttpClient   httpClient;
    private WebSocket      tunnelWs;
    private AtomicBoolean  running       = new AtomicBoolean(false);
    private AtomicBoolean  tunnelReady   = new AtomicBoolean(false);
    private ExecutorService executor;

    private final ConcurrentHashMap<Integer, ConnectionState> connections = new ConcurrentHashMap<>();
    private final AtomicInteger connIdCounter = new AtomicInteger(1);
    private final AtomicLong    bytesUp       = new AtomicLong(0);
    private final AtomicLong    bytesDown     = new AtomicLong(0);

    // Passed in via Intent from ProxyModule
    private String wsUrl;

    // ── Connection state per connId ────────────────────────────────────────────

    static class ConnectionState {
        byte[] pendingData; // data buffered before tunnel READY
        boolean closed = false;

        ConnectionState() {}
    }

    // ── Intent actions ────────────────────────────────────────────────────────

    public static final String ACTION_START = "com.netshare.VPN_START";
    public static final String ACTION_STOP  = "com.netshare.VPN_STOP";
    public static final String EXTRA_WS_URL = "wsUrl";

    // ── Service lifecycle ─────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) return START_NOT_STICKY;

        if (ACTION_STOP.equals(intent.getAction())) {
            stopVpn();
            stopSelf();
            return START_NOT_STICKY;
        }

        if (ACTION_START.equals(intent.getAction())) {
            wsUrl = intent.getStringExtra(EXTRA_WS_URL);
            if (wsUrl != null) {
                startForegroundNotification();
                startVpn();
            }
        }

        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() {
        stopVpn();
        super.onDestroy();
    }

    // ── Start VPN ─────────────────────────────────────────────────────────────

    private void startVpn() {
        try {
            // Build TUN interface — captures all device traffic
            Builder builder = new Builder()
                .setMtu(VPN_MTU)
                .addAddress(VPN_ADDRESS, 24)
                .addRoute(VPN_ROUTE, 0)          // capture ALL traffic
                .addDnsServer(VPN_DNS)
                .setSession("NetShare")
                .setBlocking(true);

            // Exclude our own app from the VPN to avoid loopback
            try { builder.addDisallowedApplication(getPackageName()); }
            catch (Exception e) { Log.w(TAG, "addDisallowedApplication: " + e.getMessage()); }

            vpnInterface = builder.establish();
            if (vpnInterface == null) {
                Log.e(TAG, "Failed to establish VPN interface");
                ProxyModule.emitEvent("ProxyTunnelError", "Failed to establish VPN interface");
                return;
            }

            tunIn  = new FileInputStream(vpnInterface.getFileDescriptor());
            tunOut = new FileOutputStream(vpnInterface.getFileDescriptor());

            running.set(true);
            executor = Executors.newCachedThreadPool();

            // Connect WebSocket tunnel to Cloudflare relay
            connectTunnel();

            // Start reading IP packets from TUN
            executor.execute(this::tunReadLoop);

            Log.i(TAG, "VPN started, tunnel: " + wsUrl);
            ProxyModule.emitEvent("ProxyTunnelReady", "{}");

        } catch (Exception e) {
            Log.e(TAG, "startVpn: " + e.getMessage());
            ProxyModule.emitEvent("ProxyTunnelError", e.getMessage());
        }
    }

    // ── Stop VPN ──────────────────────────────────────────────────────────────

    private void stopVpn() {
        running.set(false);
        tunnelReady.set(false);

        if (tunnelWs != null) {
            try { tunnelWs.close(1000, "VPN stopped"); } catch (Exception ignored) {}
            tunnelWs = null;
        }

        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        try { if (tunIn  != null) tunIn.close();  } catch (Exception ignored) {}
        try { if (tunOut != null) tunOut.close(); } catch (Exception ignored) {}
        try { if (vpnInterface != null) vpnInterface.close(); } catch (Exception ignored) {}

        tunIn  = null;
        tunOut = null;
        vpnInterface = null;

        connections.clear();
        Log.i(TAG, "VPN stopped");
        ProxyModule.emitEvent("ProxyTunnelError", "VPN stopped");
    }

    // ── WebSocket tunnel connection ───────────────────────────────────────────

    private void connectTunnel() {
        if (httpClient == null) {
            httpClient = new OkHttpClient.Builder()
                .pingInterval(20, java.util.concurrent.TimeUnit.SECONDS)
                .build();
        }

        Request req = new Request.Builder().url(wsUrl).build();
        tunnelWs = httpClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                Log.i(TAG, "Tunnel WS open");
                tunnelReady.set(true);
            }

            @Override
            public void onMessage(WebSocket ws, ByteString bytes) {
                handleTunnelFrame(bytes.toByteArray());
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                Log.d(TAG, "Text frame: " + text);
                // Handle READY signal if sent as JSON
                if (text.contains("paired") || text.contains("ready")) {
                    tunnelReady.set(true);
                }
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response response) {
                Log.e(TAG, "Tunnel WS failure: " + t.getMessage());
                tunnelReady.set(false);
                ProxyModule.emitEvent("ProxyTunnelError", t.getMessage());
                if (running.get()) {
                    // Reconnect after 3 seconds
                    try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
                    if (running.get()) connectTunnel();
                }
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                Log.i(TAG, "Tunnel WS closed: " + reason);
                tunnelReady.set(false);
            }
        });
    }

    // ── TUN read loop — reads IP packets, extracts TCP connections ────────────

    private void tunReadLoop() {
        ByteBuffer packet = ByteBuffer.allocate(VPN_MTU);
        byte[] buf = new byte[VPN_MTU];

        while (running.get()) {
            try {
                int len = tunIn.read(buf);
                if (len <= 0) continue;

                packet.clear();
                packet.put(buf, 0, len);
                packet.flip();

                processIpPacket(buf, len);

            } catch (IOException e) {
                if (running.get()) Log.w(TAG, "tunRead: " + e.getMessage());
            }
        }
    }

    /**
     * Parse IP packet and extract TCP CONNECT target (host:port).
     * For simplicity we intercept at the TCP level — extract destination
     * IP and port from the IP/TCP headers, then open a tunnel connection.
     *
     * IP header: version(4b)+IHL(4b)+... src(4B) dst(4B)
     * TCP header: srcPort(2B) dstPort(2B) ...
     */
    private void processIpPacket(byte[] buf, int len) {
        if (len < 20) return; // too short for IP header

        int ipVersion = (buf[0] >> 4) & 0xF;
        if (ipVersion != 4) return; // IPv6 not supported yet

        int ipHeaderLen = (buf[0] & 0xF) * 4;
        int protocol    = buf[9] & 0xFF; // 6=TCP, 17=UDP

        if (protocol != 6) return; // only TCP for now

        if (len < ipHeaderLen + 20) return; // too short for TCP header

        // Destination IP
        int dstIp = ((buf[16] & 0xFF) << 24) | ((buf[17] & 0xFF) << 16)
                  | ((buf[18] & 0xFF) << 8)  |  (buf[19] & 0xFF);
        String dstIpStr = ((dstIp >> 24) & 0xFF) + "." + ((dstIp >> 16) & 0xFF)
                        + "." + ((dstIp >> 8) & 0xFF) + "." + (dstIp & 0xFF);

        // Destination port
        int dstPort = ((buf[ipHeaderLen + 2] & 0xFF) << 8)
                    | (buf[ipHeaderLen + 3] & 0xFF);

        // TCP flags
        int tcpFlagsOffset = ipHeaderLen + 13;
        if (tcpFlagsOffset >= len) return;
        int tcpFlags = buf[tcpFlagsOffset] & 0xFF;
        boolean isSyn    = (tcpFlags & 0x02) != 0;
        boolean isFin    = (tcpFlags & 0x01) != 0;
        boolean isRst    = (tcpFlags & 0x04) != 0;

        // TCP data offset
        int tcpHeaderLen = ((buf[ipHeaderLen + 12] & 0xFF) >> 4) * 4;
        int dataOffset   = ipHeaderLen + tcpHeaderLen;
        int dataLen      = len - dataOffset;

        // Use src port as a pseudo connection key within this packet
        int srcPort = ((buf[ipHeaderLen] & 0xFF) << 8) | (buf[ipHeaderLen + 1] & 0xFF);
        int connKey = srcPort; // unique per TCP connection on client side

        String target = dstIpStr + ":" + dstPort;

        if (isSyn && !connections.containsKey(connKey)) {
            // New TCP connection — send OPEN frame
            openTunnelConnection(connKey, target);

        } else if ((isFin || isRst) && connections.containsKey(connKey)) {
            // Connection closing
            closeTunnelConnection(connKey);

        } else if (dataLen > 0 && connections.containsKey(connKey)) {
            // Data frame
            byte[] payload = Arrays.copyOfRange(buf, dataOffset, dataOffset + dataLen);
            sendDataFrame(connKey, payload);
        }
    }

    private void openTunnelConnection(int connId, String target) {
        if (!tunnelReady.get() || tunnelWs == null) return;

        connections.put(connId, new ConnectionState());
        byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
        byte[] frame       = buildFrame(connId, FT_OPEN, targetBytes);
        tunnelWs.send(ByteString.of(frame));
        ProxyModule.emitEvent("ProxyClientConnected", String.valueOf(connId));
        Log.d(TAG, "OPEN connId=" + connId + " target=" + target);
    }

    private void closeTunnelConnection(int connId) {
        ConnectionState state = connections.remove(connId);
        if (state == null) return;
        state.closed = true;
        if (tunnelWs != null) {
            tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
        }
        ProxyModule.emitEvent("ProxyClientDisconnected", String.valueOf(connId));
    }

    private void sendDataFrame(int connId, byte[] payload) {
        if (!tunnelReady.get() || tunnelWs == null) return;
        tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA, payload)));
        bytesUp.addAndGet(payload.length);
    }

    // ── Handle incoming frames from host via Cloudflare ───────────────────────

    private void handleTunnelFrame(byte[] raw) {
        if (raw.length < 5) return;

        int  connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte frameType = raw[4];
        byte[] payload = Arrays.copyOfRange(raw, 5, raw.length);

        switch (frameType) {

            case FT_READY: {
                tunnelReady.set(true);
                Log.i(TAG, "Tunnel paired with host");
                break;
            }

            case FT_DATA: {
                // Write response data back into the TUN interface
                // so the OS delivers it to the requesting app
                try {
                    if (tunOut != null && payload.length > 0) {
                        tunOut.write(payload);
                        bytesDown.addAndGet(payload.length);
                    }
                } catch (IOException e) {
                    Log.d(TAG, "tunOut write: " + e.getMessage());
                }
                break;
            }

            case FT_CLOSE: {
                connections.remove(connId);
                break;
            }
        }
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    public long getBytesUp()   { return bytesUp.get(); }
    public long getBytesDown() { return bytesDown.get(); }

    // ── Frame builder ─────────────────────────────────────────────────────────

    private static byte[] buildFrame(int connId, byte type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(5 + payload.length);
        buf.putInt(connId);
        buf.put(type);
        buf.put(payload);
        return buf.array();
    }

    // ── Foreground notification (required for VpnService) ─────────────────────

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            channel.setDescription("NetShare tunnel is active");
            NotificationManager nm = getSystemService(NotificationManager.class);
            if (nm != null) nm.createNotificationChannel(channel);
        }

        Intent stopIntent = new Intent(this, NetShareVpnService.class);
        stopIntent.setAction(ACTION_STOP);
        PendingIntent stopPi = PendingIntent.getService(
            this, 0, stopIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        Notification notif = new Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare Active")
            .setContentText("Tunneling through host's internet")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .addAction(android.R.drawable.ic_delete, "Disconnect", stopPi)
            .setOngoing(true)
            .build();

        startForeground(NOTIF_ID, notif);
    }
}
