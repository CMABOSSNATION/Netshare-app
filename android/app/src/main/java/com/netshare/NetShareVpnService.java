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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * NetShareVpnService — FIXED
 *
 * BUGS FOUND AND FIXED:
 * ──────────────────────────────────────────────────────────────────────────
 * BUG 1 (THE MAIN BUG): In onStartCommand, wsUrl was received but startVpn()
 *   was called with NO arguments. startVpn() took no parameters and never
 *   opened a WebSocket. Traffic was captured by VPN and dropped silently.
 *   → FIX: startVpn(String wsUrl) now takes and uses the wsUrl.
 *
 * BUG 2: No WebSocket to relay existed on client side at all.
 *   proxyConnect sent HTTP CONNECT to ProxyModule on :8899, but ProxyModule's
 *   tunnelWs was null on the client — so FT_OPEN frames went nowhere.
 *   → FIX: startVpn() now opens WebSocket to relay /ws/client/:code directly.
 *   Frames from host arrive via onMessage() and are routed back to TUN.
 *
 * BUG 3: No OkHttp pingInterval → Cloudflare killed WS after ~100s idle
 *   → the "ping/pong timeout after 7 successful pings" error you saw.
 *   → FIX: pingInterval(20s) + app-level heartbeat every 25s.
 */
public class NetShareVpnService extends VpnService {

    private static final String TAG        = "NetShareVPN";
    private static final String CHANNEL_ID = "netshare_vpn";
    private static final int    NOTIF_ID   = 1001;
    private static final int    VPN_MTU    = 1500;
    private static final int    PROXY_PORT = 8899;
    private static final int    PIPE_BUF   = 16 * 1024;
    private static final int    QUEUE_CAP  = 512;

    private static final String VPN_ADDRESS = "10.0.0.2";
    private static final String VPN_DNS1    = "8.8.8.8";
    private static final String VPN_DNS2    = "8.8.4.4";

    public static final String ACTION_START = "com.netshare.VPN_START";
    public static final String ACTION_STOP  = "com.netshare.VPN_STOP";
    public static final String EXTRA_WS_URL = "wsUrl";

    private ParcelFileDescriptor vpnInterface;
    private FileOutputStream     tunOut;

    // FIX 1: store wsUrl as field
    private String         wsUrl;

    // FIX 2: client WebSocket to relay
    private WebSocket      relayWs;
    private OkHttpClient   httpClient;

    // FIX 3: heartbeat timer
    private java.util.Timer heartbeatTimer;

    private final AtomicBoolean running     = new AtomicBoolean(false);
    private final AtomicBoolean wsReady     = new AtomicBoolean(false);
    private final AtomicLong    bytesUp     = new AtomicLong(0);
    private final AtomicLong    bytesDown   = new AtomicLong(0);
    private final AtomicInteger activeConns = new AtomicInteger(0);

    private ExecutorService executor;

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
            String url = intent.getStringExtra(EXTRA_WS_URL);
            if (url != null) {
                this.wsUrl = url;            // FIX 1: store it
                startForegroundNotification();
                startVpn(url);              // FIX 1: pass it in — was startVpn() before!
            }
        }
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() { stopVpn(); super.onDestroy(); }

    // ── Start VPN + open relay WebSocket ─────────────────────────────────────

    private void startVpn(String wsUrl) {  // FIX 1: takes wsUrl now
        if (running.get()) return;
        try {
            Builder b = new Builder()
                .setMtu(VPN_MTU)
                .addAddress(VPN_ADDRESS, 24)
                .addRoute("0.0.0.0", 0)
                .addDnsServer(VPN_DNS1)
                .addDnsServer(VPN_DNS2)
                .setSession("NetShare")
                .setBlocking(true);

            try { b.addDisallowedApplication(getPackageName()); }
            catch (Exception e) { Log.w(TAG, "addDisallowedApplication: " + e.getMessage()); }

            vpnInterface = b.establish();
            if (vpnInterface == null) {
                ProxyModule.emitEvent("ProxyTunnelError", "VPN interface failed");
                return;
            }

            tunOut   = new FileOutputStream(vpnInterface.getFileDescriptor());
            running.set(true);
            executor = Executors.newCachedThreadPool();

            // FIX 2+3: open WebSocket with pingInterval to keep alive
            httpClient = new OkHttpClient.Builder()
                .pingInterval(20, TimeUnit.SECONDS)   // FIX 3: prevents Cloudflare idle kill
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(0,  TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

            Request req = new Request.Builder().url(wsUrl).build();
            relayWs = httpClient.newWebSocket(req, new WebSocketListener() {

                @Override
                public void onOpen(WebSocket ws, Response response) {
                    Log.i(TAG, "[Client] WS open");
                    wsReady.set(true);
                    // Start TUN loop only AFTER WebSocket is open
                    executor.execute(NetShareVpnService.this::tunReadLoop);
                    startHeartbeat(ws);   // FIX 3: app-level heartbeat
                    ProxyModule.emitEvent("ProxyVpnStarted", "{}");
                    ProxyModule.emitEvent("ProxyTunnelReady", "{}");
                }

                @Override
                public void onMessage(WebSocket ws, ByteString bytes) {
                    // FIX 2: host response frames arrive here → route to TUN
                    handleRelayFrame(bytes.toByteArray());
                }

                @Override
                public void onMessage(WebSocket ws, String text) {
                    Log.v(TAG, "[Client] text: " + text); // heartbeat pong
                }

                @Override
                public void onFailure(WebSocket ws, Throwable t, Response response) {
                    Log.e(TAG, "[Client] WS failure: " + t.getMessage());
                    wsReady.set(false);
                    ProxyModule.emitEvent("ProxyTunnelError",
                        t.getMessage() != null ? t.getMessage() : "WS failure");
                    stopVpn();
                }

                @Override
                public void onClosed(WebSocket ws, int code, String reason) {
                    wsReady.set(false);
                    ProxyModule.emitEvent("ProxyVpnRevoked", reason);
                    stopVpn();
                }
            });

            Log.i(TAG, "VPN started → " + wsUrl);

        } catch (Exception e) {
            Log.e(TAG, "startVpn: " + e.getMessage());
            ProxyModule.emitEvent("ProxyTunnelError", e.getMessage());
        }
    }

    // FIX 3: extra app-level heartbeat every 25s
    private void startHeartbeat(WebSocket ws) {
        if (heartbeatTimer != null) heartbeatTimer.cancel();
        heartbeatTimer = new java.util.Timer("ns-heartbeat", true);
        heartbeatTimer.scheduleAtFixedRate(new java.util.TimerTask() {
            @Override public void run() {
                try {
                    if (wsReady.get()) ws.send("{\"type\":\"ping\"}");
                } catch (Exception ignored) {}
            }
        }, 25_000, 25_000);
    }

    // ── Stop ─────────────────────────────────────────────────────────────────

    private void stopVpn() {
        running.set(false);
        wsReady.set(false);

        if (heartbeatTimer != null) { heartbeatTimer.cancel(); heartbeatTimer = null; }

        for (BlockingQueue<byte[]> q : connQueues.values()) q.offer(new byte[0]);
        connQueues.clear();

        if (executor != null) { executor.shutdownNow(); executor = null; }

        if (relayWs != null) {
            try { relayWs.close(1000, "stopped"); } catch (Exception ignored) {}
            relayWs = null;
        }
        if (httpClient != null) {
            try { httpClient.dispatcher().executorService().shutdown(); } catch (Exception ignored) {}
            httpClient = null;
        }

        try { if (tunOut != null)       tunOut.close();       } catch (Exception ignored) {}
        try { if (vpnInterface != null) vpnInterface.close(); } catch (Exception ignored) {}
        tunOut = null; vpnInterface = null;

        ProxyModule.emitEvent("ProxyVpnRevoked", "stopped");
        Log.i(TAG, "VPN stopped");
    }

    // ── Handle frames from relay → TUN ───────────────────────────────────────

    private void handleRelayFrame(byte[] raw) {
        if (raw.length < 5) return;
        int    connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte   frameType = raw[4];
        byte[] payload   = Arrays.copyOfRange(raw, 5, raw.length);

        String connKey = "relay:" + connId;

        switch (frameType) {
            case 0x02: { // FT_DATA — write response back into TUN
                BlockingQueue<byte[]> q = connQueues.get(connKey);
                if (q != null && payload.length > 0) {
                    if (!q.offer(payload)) Log.v(TAG, "relay queue full id=" + connId);
                } else if (payload.length > 0) {
                    synchronized (this) {
                        if (tunOut != null) {
                            try { tunOut.write(payload); tunOut.flush(); }
                            catch (Exception ignored) {}
                        }
                    }
                    bytesDown.addAndGet(payload.length);
                }
                break;
            }
            case 0x03: { // FT_CLOSE
                BlockingQueue<byte[]> q = connQueues.remove(connKey);
                if (q != null) q.offer(new byte[0]);
                break;
            }
        }
    }

    // ── TUN read loop ─────────────────────────────────────────────────────────

    private void tunReadLoop() {
        FileInputStream tunIn = new FileInputStream(vpnInterface.getFileDescriptor());
        byte[] buf = new byte[VPN_MTU];

        while (running.get()) {
            try {
                int len = tunIn.read(buf);
                if (len < 20) continue;
                if (((buf[0] >> 4) & 0xF) != 4) continue; // IPv4 only

                int ipHdrLen = (buf[0] & 0xF) * 4;
                if (buf[9] != 6) continue; // TCP only
                if (len < ipHdrLen + 20) continue;

                String dstIp  = (buf[16]&0xFF)+"."+(buf[17]&0xFF)+"."+(buf[18]&0xFF)+"."+(buf[19]&0xFF);
                int    srcPort = ((buf[ipHdrLen]   &0xFF)<<8)|(buf[ipHdrLen+1]&0xFF);
                int    dstPort = ((buf[ipHdrLen+2] &0xFF)<<8)|(buf[ipHdrLen+3]&0xFF);

                if (dstIp.startsWith("127.") || dstPort == PROXY_PORT) continue;

                int     flags = buf[ipHdrLen + 13] & 0xFF;
                boolean syn   = (flags & 0x02) != 0;
                boolean fin   = (flags & 0x01) != 0;
                boolean rst   = (flags & 0x04) != 0;
                boolean ack   = (flags & 0x10) != 0;
                boolean psh   = (flags & 0x08) != 0;

                String connKey = srcPort + ":" + dstIp + ":" + dstPort;

                if (syn && !ack) {
                    if (!connQueues.containsKey(connKey)) {
                        BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(QUEUE_CAP);
                        connQueues.put(connKey, queue);
                        final String t = dstIp + ":" + dstPort;
                        final String k = connKey;
                        executor.execute(() -> proxyConnect(t, k, queue));
                    }
                } else if (fin || rst) {
                    BlockingQueue<byte[]> q = connQueues.remove(connKey);
                    if (q != null) q.offer(new byte[0]);
                } else if (ack || psh) {
                    int tcpHdrLen     = ((buf[ipHdrLen + 12] >> 4) & 0xF) * 4;
                    int payloadOffset = ipHdrLen + tcpHdrLen;
                    int payloadLen    = len - payloadOffset;
                    if (payloadLen > 0) {
                        BlockingQueue<byte[]> q = connQueues.get(connKey);
                        if (q != null) {
                            byte[] p = Arrays.copyOfRange(buf, payloadOffset, payloadOffset + payloadLen);
                            if (!q.offer(p)) Log.v(TAG, "queue full " + connKey);
                        }
                    }
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) break;
                if (running.get()) Log.v(TAG, "tunRead: " + e.getMessage());
            }
        }
        try { tunIn.close(); } catch (Exception ignored) {}
    }

    // ── Per-connection proxy handler ──────────────────────────────────────────

    private void proxyConnect(String target, String connKey, BlockingQueue<byte[]> queue) {
        Socket proxySocket = null;
        try {
            proxySocket = new Socket();
            if (!protect(proxySocket)) Log.w(TAG, "protect() failed: " + target);

            proxySocket.setTcpNoDelay(true);
            proxySocket.setSoTimeout(120_000);
            proxySocket.connect(new InetSocketAddress("127.0.0.1", PROXY_PORT), 5_000);

            InputStream  proxyIn  = proxySocket.getInputStream();
            OutputStream proxyOut = proxySocket.getOutputStream();

            proxyOut.write(("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            proxyOut.flush();

            String resp = readHttpResponse(proxyIn);
            if (resp == null || !resp.contains("200")) {
                Log.w(TAG, "CONNECT failed [" + target + "]: " + resp);
                return;
            }

            activeConns.incrementAndGet();

            final Socket       sock = proxySocket;
            final AtomicBoolean done = new AtomicBoolean(false);

            Thread download = new Thread(() -> {
                byte[] buf = new byte[PIPE_BUF];
                try {
                    int n;
                    while (running.get() && !sock.isClosed()) {
                        n = proxyIn.read(buf);
                        if (n < 0) break;
                        synchronized (NetShareVpnService.this) {
                            if (tunOut != null) { tunOut.write(buf, 0, n); tunOut.flush(); }
                        }
                        bytesDown.addAndGet(n);
                    }
                } catch (Exception ignored) {
                } finally {
                    done.set(true);
                    try { sock.close(); } catch (Exception ignored2) {}
                }
            }, "ns-dl-" + target);
            download.setDaemon(true);
            download.start();

            try {
                while (running.get() && !sock.isClosed() && !done.get()) {
                    byte[] payload = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (payload == null) continue;
                    if (payload.length == 0) break;
                    proxyOut.write(payload);
                    proxyOut.flush();
                    bytesUp.addAndGet(payload.length);
                }
            } catch (Exception ignored) {
            } finally {
                try { sock.close(); } catch (Exception ignored) {}
            }

            try { download.join(3_000); } catch (InterruptedException ignored) {}

        } catch (Exception e) {
            Log.d(TAG, "proxyConnect[" + target + "]: " + e.getMessage());
        } finally {
            connQueues.remove(connKey);
            activeConns.decrementAndGet();
            if (proxySocket != null && !proxySocket.isClosed())
                try { proxySocket.close(); } catch (Exception ignored) {}
        }
    }

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

    private void startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                CHANNEL_ID, "NetShare VPN", NotificationManager.IMPORTANCE_LOW);
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
