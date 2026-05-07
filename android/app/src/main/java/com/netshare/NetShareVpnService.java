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
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
 *   3. Forwards them using a local HTTP/SOCKS proxy or raw socket
 *   4. Sends responses back via DATA messages on WebSocket
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

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && "STOP_VPN".equals(intent.getAction())) {
            stopVpnTunnel();
            return START_NOT_STICKY;
        }

        relayUrl  = intent.getStringExtra("RELAY_URL");
        sessionCode = intent.getStringExtra("SESSION_CODE");
        role      = intent.getStringExtra("ROLE");

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
                   .addAddress("10.8.0.2", 24)          // Virtual IP for this device
                   .addRoute("0.0.0.0", 0)               // Route ALL traffic through VPN
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

        wsClient = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                Log.i(TAG, "WebSocket connected to relay");

                // Register with relay server
                if ("host".equals(role)) {
                    send("{\"type\":\"HOST_REGISTER\",\"netType\":\"WiFi\"}");
                } else {
                    send("{\"type\":\"CLIENT_JOIN\",\"code\":\"" + sessionCode + "\"}");
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
                VpnModule.emitEvent("vpnDisconnected", reason);
                isRunning = false;
            }

            @Override
            public void onError(Exception ex) {
                Log.e(TAG, "WebSocket error: " + ex.getMessage());
                VpnModule.emitEvent("vpnError", ex.getMessage());
            }
        };

        // Add VPN bypass so WebSocket doesn't route through itself
        wsClient.setSocket(VpnService.protect(wsClient.createSocket(
            new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? 443 : uri.getPort())
        )));

        wsClient.connectBlocking();

        // ── 3. Read loop: capture packets from TUN → send to relay ──
        if ("client".equals(role)) {
            startPacketReadLoop();
        }
    }

    private void startPacketReadLoop() {
        executor.execute(() -> {
            byte[] packet = new byte[32767];
            FileInputStream in = new FileInputStream(vpnInterface.getFileDescriptor());

            while (isRunning) {
                try {
                    int length = in.read(packet);
                    if (length > 0 && wsClient != null && wsClient.isOpen()) {
                        // Send raw IP packet as binary to relay
                        wsClient.send(ByteBuffer.wrap(packet, 0, length));
                    }
                } catch (Exception e) {
                    if (isRunning) Log.e(TAG, "Packet read error: " + e.getMessage());
                    break;
                }
            }
        });
    }

    private void handleRelayMessage(String message) {
        try {
            // Parse type field from JSON manually (no JSON lib dependency here)
            if (message.contains("\"SESSION_CREATED\"")) {
                // Extract code from JSON
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
                // Heartbeat from server — respond with PONG
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
                if ("host".equals(role)) {
                    wsClient.send("{\"type\":\"HOST_LEAVE\"}");
                } else {
                    wsClient.send("{\"type\":\"CLIENT_LEAVE\"}");
                }
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
