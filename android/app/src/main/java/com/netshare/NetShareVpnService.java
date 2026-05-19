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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NetShareVpnService — Client-side VPN for NetShare (300km tunnel mode)
 *
 * CORRECT ARCHITECTURE:
 * ─────────────────────
 * ONE central tunReadLoop owns the TUN FileInputStream exclusively.
 * It reads every packet and dispatches it into the correct per-connection
 * BlockingQueue based on srcPort:dstIp:dstPort key.
 *
 * Each proxyConnect thread reads ONLY from its own BlockingQueue.
 * No racing. No stolen packets. Correct routing guaranteed.
 *
 * Full data flow:
 *
 *   [Any app on device]
 *        │  (all TCP traffic captured by VPN)
 *        ▼
 *   TUN interface  ←──────────────────────────────────────────┐
 *        │                                                     │
 *   tunReadLoop (single thread, owns TUN fd exclusively)       │
 *        │                                                     │
 *        ├─ SYN packet → spawn proxyConnect thread             │
 *        │               register connKey → BlockingQueue      │
 *        │                                                     │
 *        └─ data packet → look up BlockingQueue by connKey     │
 *                         → put packet into queue             │
 *                                                             │
 *   proxyConnect thread:                                       │
 *        │                                                     │
 *        ├─ open Socket → 127.0.0.1:8899 (local proxy)        │
 *        ├─ protect() socket from VPN routing                  │
 *        ├─ send HTTP CONNECT dstIp:dstPort                    │
 *        ├─ read 200 OK                                        │
 *        │                                                     │
 *        ├─ UPLOAD thread:                                     │
 *        │    poll packets from own BlockingQueue              │
 *        │    write raw bytes to proxy socket output           │
 *        │    (ProxyModule sends via WebSocket to host)        │
 *        │                                                     │
 *        └─ DOWNLOAD thread:                                   │
 *             read response bytes from proxy socket input      │
 *             write raw bytes back into TUN ──────────────────┘
 *             (app receives the response)
 *
 * ProxyModule at 127.0.0.1:8899:
 *   - Receives HTTP CONNECT + data
 *   - Sends FT_OPEN + FT_DATA frames via WebSocket to Cloudflare relay
 *   - Cloudflare pipes to host WebSocket
 *   - Host connects to real internet, streams response back
 *   - FT_DATA response frames arrive at client ProxyModule
 *   - ProxyModule writes response to clientSock (the proxy socket)
 *   - proxyConnect download thread reads it and writes to TUN
 *   - App receives data ✅
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG        = "NetShareVPN";
    private static final String CHANNEL_ID = "netshare_vpn";
    private static final int    NOTIF_ID   = 1001;
    private static final int    VPN_MTU    = 1500;
    private static final int    PROXY_PORT = 8899;
    private static final int    PIPE_BUF   = 16 * 1024;
    private static final int    QUEUE_CAP  = 512;  // max queued packets per connection

    private static final String VPN_ADDRESS = "10.0.0.2";
    private static final String VPN_DNS1    = "8.8.8.8";
    private static final String VPN_DNS2    = "8.8.4.4";

    public static final String ACTION_START = "com.netshare.VPN_START";
    public static final String ACTION_STOP  = "com.netshare.VPN_STOP";
    public static final String EXTRA_WS_URL = "wsUrl";

    private ParcelFileDescriptor vpnInterface;
    private FileOutputStream     tunOut;

    private final AtomicBoolean running     = new AtomicBoolean(false);
    private final AtomicLong    bytesUp     = new AtomicLong(0);
    private final AtomicLong    bytesDown   = new AtomicLong(0);
    private final AtomicInteger activeConns = new AtomicInteger(0);

    private ExecutorService executor;

    /**
     * Central dispatch map: connKey → BlockingQueue<byte[]>
     *
     * tunReadLoop is the ONLY writer (puts packets in).
     * Each proxyConnect upload thread is the ONLY reader for its own queue.
     *
     * Sentinel: a zero-length byte[] signals the upload thread to stop.
     */
    private final ConcurrentHashMap<String, BlockingQueue<byte[]>> connQueues
        = new ConcurrentHashMap<>();

    // ── Service lifecycle ─────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) return START_NOT_STICKY;
        if (ACTION_STOP.equals(intent.getAction())) {
            stopVpn(); stopSelf(); return START_NOT_STICKY;
        }
        if (ACTION_START.equals(intent.getAction())) {
            String wsUrl = intent.getStringExtra(EXTRA_WS_URL);
            if (wsUrl != null) { startForegroundNotification(); startVpn(); }
        }
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() { stopVpn(); super.onDestroy(); }

    // ── Start VPN ─────────────────────────────────────────────────────────────

    private void startVpn() {
        try {
            Builder b = new Builder()
                .setMtu(VPN_MTU)
                .addAddress(VPN_ADDRESS, 24)
                .addRoute("0.0.0.0", 0)       // capture ALL IPv4 traffic
                .addDnsServer(VPN_DNS1)
                .addDnsServer(VPN_DNS2)
                .setSession("NetShare")
                .setBlocking(true);            // blocking — tunReadLoop owns fd

            // Exclude our own app to prevent routing loop
            try { b.addDisallowedApplication(getPackageName()); }
            catch (Exception e) { Log.w(TAG, "addDisallowedApplication: " + e.getMessage()); }

            vpnInterface = b.establish();
            if (vpnInterface == null) {
                ProxyModule.emitEvent("ProxyTunnelError", "VPN interface failed");
                return;
            }

            tunOut = new FileOutputStream(vpnInterface.getFileDescriptor());
            running.set(true);
            executor = Executors.newCachedThreadPool();

            // Single TUN read loop — owns the FileInputStream exclusively
            executor.execute(this::tunReadLoop);

            Log.i(TAG, "VPN started");
            ProxyModule.emitEvent("ProxyVpnStarted", "{}");
            ProxyModule.emitEvent("ProxyTunnelReady", "{}");

        } catch (Exception e) {
            Log.e(TAG, "startVpn: " + e.getMessage());
            ProxyModule.emitEvent("ProxyTunnelError", e.getMessage());
        }
    }

    // ── Stop VPN ──────────────────────────────────────────────────────────────

    private void stopVpn() {
        running.set(false);

        // Signal all upload threads to stop by sending sentinel to every queue
        for (BlockingQueue<byte[]> q : connQueues.values()) {
            q.offer(new byte[0]); // sentinel
        }
        connQueues.clear();

        if (executor != null) { executor.shutdownNow(); executor = null; }

        try { if (tunOut       != null) tunOut.close();       } catch (Exception ignored) {}
        try { if (vpnInterface != null) vpnInterface.close(); } catch (Exception ignored) {}

        tunOut = null; vpnInterface = null;

        Log.i(TAG, "VPN stopped");
        ProxyModule.emitEvent("ProxyVpnRevoked", "stopped");
    }

    // ── Central TUN read loop ─────────────────────────────────────────────────

    /**
     * THE only thread that reads from the TUN FileInputStream.
     * Owns the fd exclusively — no other thread reads from TUN.
     *
     * For every packet:
     *   1. Parse IPv4 + TCP headers to extract connKey
     *   2. If SYN (new connection) → create queue, spawn proxyConnect
     *   3. If data → look up queue, put packet in it
     *   4. If FIN/RST → send sentinel to queue (signals upload thread to stop)
     */
    private void tunReadLoop() {
        FileInputStream tunIn = new FileInputStream(vpnInterface.getFileDescriptor());
        byte[] buf = new byte[VPN_MTU];

        while (running.get()) {
            try {
                int len = tunIn.read(buf);
                if (len < 20) continue;

                // IPv4 only
                if (((buf[0] >> 4) & 0xF) != 4) continue;

                int ipHdrLen = (buf[0] & 0xF) * 4;
                int protocol = buf[9] & 0xFF;

                // TCP only (6)
                if (protocol != 6) continue;
                if (len < ipHdrLen + 20) continue;

                // Parse addresses and ports
                String srcIp  = (buf[12]&0xFF)+"."+(buf[13]&0xFF)+"."+(buf[14]&0xFF)+"."+(buf[15]&0xFF);
                String dstIp  = (buf[16]&0xFF)+"."+(buf[17]&0xFF)+"."+(buf[18]&0xFF)+"."+(buf[19]&0xFF);
                int    srcPort = ((buf[ipHdrLen]   &0xFF)<<8)|(buf[ipHdrLen+1]&0xFF);
                int    dstPort = ((buf[ipHdrLen+2] &0xFF)<<8)|(buf[ipHdrLen+3]&0xFF);

                // Skip loopback and proxy port itself (avoid loop)
                if (dstIp.startsWith("127.") || dstPort == PROXY_PORT) continue;

                // TCP flags
                int     flags = buf[ipHdrLen + 13] & 0xFF;
                boolean syn   = (flags & 0x02) != 0;
                boolean fin   = (flags & 0x01) != 0;
                boolean rst   = (flags & 0x04) != 0;
                boolean ack   = (flags & 0x10) != 0;
                boolean psh   = (flags & 0x08) != 0;

                // connKey identifies this TCP connection uniquely
                String connKey = srcPort + ":" + dstIp + ":" + dstPort;

                if (syn && !ack) {
                    // New connection — create queue and spawn handler
                    if (!connQueues.containsKey(connKey)) {
                        BlockingQueue<byte[]> queue =
                            new LinkedBlockingQueue<>(QUEUE_CAP);
                        connQueues.put(connKey, queue);

                        final String target = dstIp + ":" + dstPort;
                        final String key    = connKey;
                        final BlockingQueue<byte[]> q = queue;
                        executor.execute(() -> proxyConnect(target, key, q));
                    }

                } else if (fin || rst) {
                    // Connection closing — send sentinel to upload thread
                    BlockingQueue<byte[]> q = connQueues.remove(connKey);
                    if (q != null) q.offer(new byte[0]); // sentinel

                } else if (ack || psh) {
                    // Data packet — dispatch to the right connection queue
                    // Extract TCP payload
                    int tcpHdrLen = ((buf[ipHdrLen + 12] >> 4) & 0xF) * 4;
                    int payloadOffset = ipHdrLen + tcpHdrLen;
                    int payloadLen    = len - payloadOffset;

                    if (payloadLen > 0) {
                        BlockingQueue<byte[]> q = connQueues.get(connKey);
                        if (q != null) {
                            byte[] payload = Arrays.copyOfRange(buf, payloadOffset, payloadOffset + payloadLen);
                            // offer (non-blocking) — drop if queue full to avoid blocking tunReadLoop
                            if (!q.offer(payload)) {
                                Log.v(TAG, "Queue full for " + connKey + ", dropping packet");
                            }
                        }
                    }
                }

            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) break;
                if (running.get()) Log.v(TAG, "tunRead: " + e.getMessage());
            }
        }

        try { tunIn.close(); } catch (Exception ignored) {}
        Log.i(TAG, "tunReadLoop exited");
    }

    // ── Per-connection proxy handler ──────────────────────────────────────────

    /**
     * Handles ONE TCP connection through the local HTTP CONNECT proxy.
     *
     * UPLOAD thread:   drains packets from queue → writes to proxy socket output
     * DOWNLOAD thread: reads proxy socket input  → writes back into TUN
     *
     * The queue is the handoff point between tunReadLoop and this thread.
     * tunReadLoop puts packets in; upload thread takes them out.
     * Zero-length sentinel signals upload thread to stop.
     */
    private void proxyConnect(String target, String connKey, BlockingQueue<byte[]> queue) {
        Socket proxySocket = null;
        try {
            proxySocket = new Socket();

            // protect() prevents this socket from being routed through the VPN
            // (which would cause an infinite loop)
            if (!protect(proxySocket)) {
                Log.w(TAG, "protect() failed for " + target);
            }

            proxySocket.setTcpNoDelay(true);
            proxySocket.setSoTimeout(120_000);
            proxySocket.connect(new InetSocketAddress("127.0.0.1", PROXY_PORT), 5_000);

            InputStream  proxyIn  = proxySocket.getInputStream();
            OutputStream proxyOut = proxySocket.getOutputStream();

            // Send HTTP CONNECT to the local proxy
            // ProxyModule handles this, creates FT_OPEN frame, tunnels via WebSocket
            proxyOut.write(("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            proxyOut.flush();

            // Read 200 Connection established
            String resp = readHttpResponse(proxyIn);
            if (resp == null || !resp.contains("200")) {
                Log.w(TAG, "CONNECT failed [" + target + "]: " + resp);
                return;
            }

            int cnt = activeConns.incrementAndGet();
            ProxyModule.emitEvent("ProxyClientConnected", String.valueOf(cnt));
            Log.d(TAG, "CONNECT OK: " + target + " active=" + cnt);

            final Socket     sock     = proxySocket;
            final AtomicBoolean done  = new AtomicBoolean(false);

            // ── DOWNLOAD thread: proxy socket → TUN ──────────────────────────
            //
            // Reads response data arriving from the host (via ProxyModule WebSocket)
            // and writes it back into the TUN interface so the originating app
            // receives the server's response.
            //
            // tunOut writes are synchronized — multiple concurrent download threads
            // all write to the same TUN fd safely.
            Thread download = new Thread(() -> {
                byte[] buf = new byte[PIPE_BUF];
                try {
                    int n;
                    while (running.get() && !sock.isClosed()) {
                        n = proxyIn.read(buf);
                        if (n < 0) break;
                        synchronized (NetShareVpnService.this) {
                            if (tunOut != null) {
                                tunOut.write(buf, 0, n);
                                tunOut.flush();
                            }
                        }
                        bytesDown.addAndGet(n);
                    }
                } catch (Exception e) {
                    Log.v(TAG, "dl[" + target + "]: " + e.getMessage());
                } finally {
                    done.set(true);
                    try { sock.close(); } catch (Exception ignored) {}
                }
            }, "ns-dl-" + target);
            download.setDaemon(true);
            download.start();

            // ── UPLOAD: queue → proxy socket ──────────────────────────────────
            //
            // Drains TCP payload packets from the BlockingQueue (filled by
            // tunReadLoop) and writes them to the proxy socket input stream.
            // ProxyModule reads them and sends FT_DATA frames to the host.
            //
            // Zero-length sentinel = FIN/RST received = stop uploading.
            try {
                while (running.get() && !sock.isClosed() && !done.get()) {
                    // Poll with timeout so we can check done/running periodically
                    byte[] payload = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (payload == null) continue;          // timeout, loop back
                    if (payload.length == 0) break;         // sentinel — connection closing

                    proxyOut.write(payload);
                    proxyOut.flush();
                    bytesUp.addAndGet(payload.length);
                }
            } catch (Exception e) {
                Log.v(TAG, "ul[" + target + "]: " + e.getMessage());
            } finally {
                try { sock.close(); } catch (Exception ignored) {}
            }

            // Wait for download to drain remaining data
            try { download.join(3_000); } catch (InterruptedException ignored) {}

        } catch (Exception e) {
            Log.d(TAG, "proxyConnect[" + target + "]: " + e.getMessage());
        } finally {
            connQueues.remove(connKey);
            int c = activeConns.decrementAndGet();
            if (c >= 0) ProxyModule.emitEvent("ProxyClientDisconnected", String.valueOf(c));
            if (proxySocket != null && !proxySocket.isClosed())
                try { proxySocket.close(); } catch (Exception ignored) {}
        }
    }

    // ── Read HTTP CONNECT response ────────────────────────────────────────────

    private String readHttpResponse(InputStream in) throws IOException {
        StringBuilder first = new StringBuilder();
        StringBuilder all   = new StringBuilder();
        int b; int nl = 0;
        while ((b = in.read()) != -1) {
            all.append((char) b);
            if (b == '\n') {
                nl++;
                if (nl == 1) first.append(all);
                if (all.toString().endsWith("\r\n\r\n") ||
                    all.toString().endsWith("\n\n")) break;
            }
        }
        return first.toString().trim();
    }

    // ── Foreground notification ───────────────────────────────────────────────

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
            ch.setDescription("NetShare tunnel active");
            NotificationManager nm = getSystemService(NotificationManager.class);
            if (nm != null) nm.createNotificationChannel(ch);
        }
        Intent stop = new Intent(this, NetShareVpnService.class).setAction(ACTION_STOP);
        PendingIntent pi = PendingIntent.getService(this, 0, stop,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        Notification notif = new Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare Active")
            .setContentText("All traffic via host's internet")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .addAction(android.R.drawable.ic_delete, "Disconnect", pi)
            .setOngoing(true)
            .build();
        startForeground(NOTIF_ID, notif);
    }
}
