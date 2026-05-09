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
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
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
        executor = Executors.newFixedThreadPool(3);
        executor.execute(this::startVpnTunnel);

        return START_STICKY;
    }

    private void startVpnTunnel() {
        try {
            // ── 1. Build the TUN interface ────────────────────────────
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

            // ── 2. Connect to relay WebSocket ────────────────────────
            connectToRelay();

        } catch (Exception e) {
            Log.e(TAG, "VPN start error: " + e.getMessage());
            VpnModule.emitEvent("vpnError", e.getMessage());
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
                // Binary packet received from relay — write to TUN interface
                if (vpnInterface != null && isRunning) {
                    try {
                        FileOutputStream out = new FileOutputStream(vpnInterface.getFileDescriptor());
                        out.write(bytes.array(), bytes.position(), bytes.remaining());
                    } catch (Exception e) {
                        Log.e(TAG, "TUN write error: " + e.getMessage());
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
        stopVpnTunnel();
        super.onDestroy();
    }
}
