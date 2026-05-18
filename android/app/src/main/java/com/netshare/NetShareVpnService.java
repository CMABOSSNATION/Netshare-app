package com.netshare;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.net.VpnService;
import android.os.Build;
import android.os.ParcelFileDescriptor;
import android.util.Log;

import androidx.core.app.NotificationCompat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NetShareVpnService — CLIENT-SIDE traffic blocker
 *
 * ── What this does ────────────────────────────────────────────────────────────
 *
 * Creates a TUN virtual network interface on the client device.
 * Android routes ALL outbound packets through this TUN interface.
 *
 * We inspect every IPv4 packet:
 *   • If destination is 127.0.0.1:8899  → allow (our proxy tunnel)
 *   • Everything else                   → DROP silently
 *
 * Result: when a client is connected to NetShare, ONLY traffic going through
 * the NetShare proxy (and therefore through the host's internet) is allowed.
 * All background app data (ads, trackers, OS updates, other apps) is blocked.
 *
 * ── Why this solves the problem ───────────────────────────────────────────────
 *
 * Before: Client apps could bypass the proxy and use the client's own mobile
 * data in the background. This used client data without going through the host.
 *
 * After: The VPN TUN intercepts 100% of traffic at the OS level. Apps cannot
 * bypass it. Only packets we explicitly allow (to our proxy) pass through.
 *
 * ── Lifecycle ─────────────────────────────────────────────────────────────────
 *
 * Started by ProxyModule.startClientVpn()  when client connects.
 * Stopped by ProxyModule.stopClientVpn()   when client disconnects.
 *
 * ── Packet filtering logic ────────────────────────────────────────────────────
 *
 * IPv4 header layout (bytes 0-19):
 *   [0]    version + IHL
 *   [1]    DSCP + ECN
 *   [2-3]  total length
 *   [4-5]  identification
 *   [6-7]  flags + fragment offset
 *   [8]    TTL
 *   [9]    protocol  (6=TCP, 17=UDP, 1=ICMP)
 *   [10-11] header checksum
 *   [12-15] source IP
 *   [16-19] destination IP
 *   [20-21] source port  (TCP/UDP)
 *   [22-23] destination port (TCP/UDP)
 *
 * We allow a packet when:
 *   protocol == TCP (6)  AND
 *   dest IP  == 127.0.0.1 (0x7F000001)  AND
 *   dest port == 8899
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG        = "NetShareVPN";
    private static final String CHANNEL_ID = "netshare_vpn";
    private static final int    NOTIF_ID   = 3;

    // The local proxy port — only packets going here are allowed through
    private static final int    PROXY_PORT = 8899;

    // TUN MTU — 1500 is standard Ethernet MTU
    private static final int    MTU        = 1500;

    // Singleton reference so ProxyModule can start/stop us easily
    static volatile NetShareVpnService instance;

    private ParcelFileDescriptor vpnInterface;
    private ExecutorService      executor;
    private final AtomicBoolean  running = new AtomicBoolean(false);

    // ── Service lifecycle ─────────────────────────────────────────────────────

    @Override
    public int onStartCommand(android.content.Intent intent, int flags, int startId) {
        if (running.compareAndSet(false, true)) {
            instance = this;
            startForeground(NOTIF_ID, buildNotification());
            executor = Executors.newSingleThreadExecutor();
            executor.execute(this::runVpn);
            Log.i(TAG, "VPN service started");
        }
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        stopVpn();
        super.onDestroy();
    }

    @Override
    public void onRevoke() {
        // Called by system when user manually disables VPN
        stopVpn();
        ProxyModule.emitEvent("ProxyVpnRevoked", "VPN permission revoked by user");
    }

    // ── Public stop method callable from ProxyModule ──────────────────────────

    public void stopVpn() {
        running.set(false);
        instance = null;
        try { if (vpnInterface != null) vpnInterface.close(); } catch (IOException ignored) {}
        vpnInterface = null;
        if (executor != null) executor.shutdownNow();
        stopForeground(true);
        stopSelf();
        Log.i(TAG, "VPN service stopped");
    }

    // ── VPN setup + packet loop ───────────────────────────────────────────────

    private void runVpn() {
        try {
            // Build the TUN interface
            vpnInterface = new Builder()
                .setSession("NetShare")
                // Assign a fake IP to ourselves — required by Android VPN API
                .addAddress("10.99.0.1", 32)
                // Route ALL IPv4 traffic through the TUN
                .addRoute("0.0.0.0", 0)
                // DNS — use Google DNS; requests go through our filter too
                .addDnsServer("8.8.8.8")
                .setMtu(MTU)
                // Do NOT block traffic by default — we handle it in the loop
                .establish();

            if (vpnInterface == null) {
                Log.e(TAG, "VPN interface could not be established (permission denied?)");
                ProxyModule.emitEvent("ProxyVpnError", "VPN permission denied");
                return;
            }

            ProxyModule.emitEvent("ProxyVpnStarted", "{}");
            Log.i(TAG, "TUN interface established — filtering packets");

            FileInputStream  reader = new FileInputStream(vpnInterface.getFileDescriptor());
            FileOutputStream writer = new FileOutputStream(vpnInterface.getFileDescriptor());

            ByteBuffer packet = ByteBuffer.allocate(MTU);

            while (running.get()) {
                packet.clear();
                byte[] buf = packet.array();

                // Read one packet from TUN (blocks until a packet arrives)
                int len = reader.read(buf);
                if (len <= 0) continue;

                // Only handle IPv4 (version nibble == 4)
                if ((buf[0] >> 4 & 0xF) != 4) continue;
                // Need at least 24 bytes for IP header + ports
                if (len < 24) continue;

                int  protocol = buf[9] & 0xFF;
                long destIp   = ((buf[16] & 0xFFL) << 24)
                              | ((buf[17] & 0xFFL) << 16)
                              | ((buf[18] & 0xFFL) <<  8)
                              |  (buf[19] & 0xFFL);
                int  destPort = ((buf[22] & 0xFF) << 8) | (buf[23] & 0xFF);

                // ALLOW: TCP packet going to 127.0.0.1:8899 (our proxy)
                boolean isToProxy = protocol == 6               // TCP
                                 && destIp   == 0x7F000001L     // 127.0.0.1
                                 && destPort == PROXY_PORT;     // 8899

                if (isToProxy) {
                    // Pass the packet through — write back to TUN so the OS
                    // can deliver it to localhost:8899 normally
                    writer.write(buf, 0, len);
                }
                // All other packets are silently dropped — no write = no delivery
            }

        } catch (IOException e) {
            if (running.get()) {
                Log.e(TAG, "VPN packet loop error: " + e.getMessage());
                ProxyModule.emitEvent("ProxyVpnError", e.getMessage());
            }
        } finally {
            running.set(false);
        }
    }

    // ── Notification ──────────────────────────────────────────────────────────

    private Notification buildNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            ((NotificationManager) getSystemService(NOTIFICATION_SERVICE))
                .createNotificationChannel(ch);
        }
        return new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare VPN Active")
            .setContentText("Blocking background data — only proxy traffic allowed")
            .setSmallIcon(android.R.drawable.ic_lock_lock)
            .setOngoing(true)
            .build();
    }
}
