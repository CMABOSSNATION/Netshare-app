package com.netshare;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.IBinder;
import android.util.Log;

import androidx.core.app.NotificationCompat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLSocketFactory;

/**
 * ProxyService — NetShare HTTP/HTTPS Transparent Proxy (Host Side)
 *
 * Architecture overview:
 * ──────────────────────────────────────────────────────────────────
 * HOST device runs this service, which opens a local TCP server on
 * PORT 8899 that speaks the HTTP CONNECT protocol.
 *
 * CLIENT devices configure their Wi-Fi proxy settings to point at
 * the host's IP:8899. All apps (TikTok, WhatsApp, Instagram, etc.)
 * automatically use the system proxy — no VPN, no root required.
 *
 * For HTTPS: client sends  CONNECT host:443 HTTP/1.1
 *            proxy replies  HTTP/1.1 200 Connection established
 *            then raw TCP tunnel is piped through to the target
 *
 * For HTTP:  proxy forwards the full request to the target and
 *            streams the response back.
 *
 * Why this works for every major app:
 * ──────────────────────────────────────────────────────────────────
 *  TikTok    — uses HTTPS (443) + HTTP/2, works through CONNECT
 *  WhatsApp  — HTTPS + XMPP (5222/5223), both go through CONNECT
 *  Facebook  — HTTPS, works through CONNECT
 *  Instagram — HTTPS, works through CONNECT
 *  Spotify   — HTTPS + port 4070, works through CONNECT
 *  YouTube   — HTTPS, works through CONNECT
 *  Google    — HTTPS, works through CONNECT
 *  Twitter/X — HTTPS, works through CONNECT
 *
 * The relay backend (Cloudflare Worker) is used only for:
 *   1. Host registration (advertise IP + proxy port to the relay)
 *   2. Client discovery  (get host IP + port from relay using session code)
 *
 * All actual traffic flows HOST → INTERNET directly, not through the relay.
 * This means zero relay bandwidth cost and no relay-side TCP complexity.
 */
public class ProxyService extends Service {

    private static final String TAG          = "NetShareProxy";
    private static final String CHANNEL_ID   = "netshare_proxy";
    private static final int    NOTIF_ID     = 2;
    public  static final int    PROXY_PORT   = 8899;

    // Thread pool: one thread per client connection
    // 50 threads handles 5 clients × ~10 concurrent connections each
    private static final int THREAD_POOL_SIZE = 50;

    // Pipe buffer — 64 KB balances memory vs throughput for streaming video
    private static final int PIPE_BUF = 64 * 1024;

    // Timeouts
    private static final int CONNECT_TIMEOUT_MS = 15_000;
    private static final int SO_TIMEOUT_MS      = 120_000; // 2 min idle

    private ServerSocket      serverSocket;
    private ExecutorService   threadPool;
    private final AtomicBoolean running      = new AtomicBoolean(false);
    public  static final AtomicLong bytesUp   = new AtomicLong(0);
    public  static final AtomicLong bytesDown = new AtomicLong(0);

    // ── Service lifecycle ────────────────────────────────────────────────────

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (running.get()) {
            Log.d(TAG, "Already running");
            return START_STICKY;
        }

        startForeground(NOTIF_ID, buildNotification("Proxy active on port " + PROXY_PORT));
        running.set(true);
        threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        threadPool.execute(this::acceptLoop);
        Log.i(TAG, "Proxy started on :" + PROXY_PORT);
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        running.set(false);
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
        if (threadPool != null) threadPool.shutdownNow();
        Log.i(TAG, "Proxy stopped");
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) { return null; }

    // ── Accept loop ──────────────────────────────────────────────────────────

    private void acceptLoop() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(PROXY_PORT));
            Log.i(TAG, "Listening on :" + PROXY_PORT);

            while (running.get()) {
                try {
                    Socket client = serverSocket.accept();
                    threadPool.execute(() -> handleClient(client));
                } catch (IOException e) {
                    if (running.get()) Log.w(TAG, "Accept error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            Log.e(TAG, "Server socket error: " + e.getMessage());
        }
    }

    // ── Per-connection handler ───────────────────────────────────────────────

    private void handleClient(Socket client) {
        try {
            client.setSoTimeout(SO_TIMEOUT_MS);
            client.setTcpNoDelay(true);

            InputStream  in  = client.getInputStream();
            OutputStream out = client.getOutputStream();

            // Read the first line: e.g.
            //   CONNECT api.tiktok.com:443 HTTP/1.1
            //   GET http://example.com/path HTTP/1.1
            String requestLine = readLine(in);
            if (requestLine == null || requestLine.isEmpty()) return;

            Log.d(TAG, "Request: " + requestLine);

            String[] parts = requestLine.split(" ");
            if (parts.length < 2) return;

            String method = parts[0].toUpperCase();

            if (method.equals("CONNECT")) {
                handleConnect(client, in, out, parts[1]);
            } else {
                handleHttp(client, in, out, requestLine, method);
            }
        } catch (IOException e) {
            Log.d(TAG, "Client error: " + e.getMessage());
        } finally {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    // ── HTTPS CONNECT tunnel ─────────────────────────────────────────────────

    /**
     * handleConnect — handles HTTPS (and any CONNECT-tunneled protocol).
     *
     * Flow:
     *  1. Parse "host:port" from the CONNECT request
     *  2. Read + discard remaining request headers
     *  3. Open raw TCP socket to target
     *  4. Reply "200 Connection established"
     *  5. Pipe bytes bidirectionally until one side closes
     *
     * The client handles TLS itself — we never see decrypted content.
     * This is why certificate pinning in apps (TikTok, WhatsApp) still works.
     */
    private void handleConnect(Socket client, InputStream in, OutputStream out,
                               String hostPort) throws IOException {
        // Drain remaining headers
        readHeaders(in);

        // Parse host:port
        String host;
        int    port;
        int    colon = hostPort.lastIndexOf(':');
        if (colon > 0) {
            host = hostPort.substring(0, colon);
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } else {
            host = hostPort;
            port = 443;
        }

        // Connect to target
        Socket target = new Socket();
        try {
            target.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            target.setSoTimeout(SO_TIMEOUT_MS);
            target.setTcpNoDelay(true);
        } catch (IOException e) {
            out.write("HTTP/1.1 502 Bad Gateway\r\n\r\n".getBytes());
            out.flush();
            Log.w(TAG, "CONNECT failed " + host + ":" + port + " — " + e.getMessage());
            return;
        }

        // Tell client the tunnel is ready
        out.write("HTTP/1.1 200 Connection established\r\n\r\n".getBytes());
        out.flush();

        // Pipe bidirectionally
        pipe(client, target, in, target.getInputStream(),
                out, target.getOutputStream());
    }

    // ── Plain HTTP proxy ─────────────────────────────────────────────────────

    /**
     * handleHttp — forwards plain HTTP requests to the origin.
     *
     * Most modern apps use HTTPS exclusively, so this path handles
     * legacy HTTP traffic and app stores' update checks.
     */
    private void handleHttp(Socket client, InputStream in, OutputStream out,
                            String requestLine, String method) throws IOException {
        // Parse URL from request line: GET http://host/path HTTP/1.1
        String[] parts = requestLine.split(" ");
        if (parts.length < 2) return;

        String url = parts[1];
        String host;
        int    port = 80;
        String path;

        if (url.startsWith("http://")) {
            url = url.substring(7);
        }
        int slash = url.indexOf('/');
        if (slash < 0) {
            host = url; path = "/";
        } else {
            host = url.substring(0, slash);
            path = url.substring(slash);
        }
        int colon = host.lastIndexOf(':');
        if (colon > 0) {
            port = Integer.parseInt(host.substring(colon + 1));
            host = host.substring(0, colon);
        }

        // Read remaining client headers
        StringBuilder headers = new StringBuilder();
        String line;
        while (!(line = readLine(in)).isEmpty()) {
            // Strip Proxy-Connection header; rewrite Host if needed
            if (!line.toLowerCase().startsWith("proxy-connection:")) {
                headers.append(line).append("\r\n");
            }
        }

        // Open connection to origin
        Socket target = new Socket();
        try {
            target.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            target.setSoTimeout(SO_TIMEOUT_MS);
        } catch (IOException e) {
            out.write("HTTP/1.1 502 Bad Gateway\r\n\r\n".getBytes());
            out.flush();
            return;
        }

        // Forward request
        OutputStream tOut = target.getOutputStream();
        String fwdRequest = method + " " + path + " HTTP/1.1\r\n"
                          + headers + "\r\n";
        tOut.write(fwdRequest.getBytes());
        tOut.flush();

        // Pipe response back
        pipe(client, target, target.getInputStream(), in,
                out, tOut);
    }

    // ── Bidirectional pipe ───────────────────────────────────────────────────

    /**
     * pipe — copies bytes in both directions until either socket closes.
     *
     * Uses two threads (one per direction) and waits for the first to finish,
     * then closes both sockets to unblock the other.
     */
    private void pipe(Socket clientSock, Socket targetSock,
                      InputStream clientIn, InputStream targetIn,
                      OutputStream clientOut, OutputStream targetOut) {
        AtomicBoolean done = new AtomicBoolean(false);

        Thread upload = new Thread(() -> {
            byte[] buf = new byte[PIPE_BUF];
            try {
                int n;
                while (!done.get() && (n = clientIn.read(buf)) != -1) {
                    targetOut.write(buf, 0, n);
                    targetOut.flush();
                    bytesUp.addAndGet(n);
                }
            } catch (IOException ignored) {}
            finally { done.set(true); closeQuietly(targetSock); }
        });

        Thread download = new Thread(() -> {
            byte[] buf = new byte[PIPE_BUF];
            try {
                int n;
                while (!done.get() && (n = targetIn.read(buf)) != -1) {
                    clientOut.write(buf, 0, n);
                    clientOut.flush();
                    bytesDown.addAndGet(n);
                }
            } catch (IOException ignored) {}
            finally { done.set(true); closeQuietly(clientSock); }
        });

        upload.setDaemon(true);
        download.setDaemon(true);
        upload.start();
        download.start();

        // Wait for both to finish
        try { upload.join(); download.join(); } catch (InterruptedException ignored) {}
    }

    // ── HTTP parsing helpers ─────────────────────────────────────────────────

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') break;
            if (c != '\r') sb.append((char) c);
        }
        return sb.toString();
    }

    private void readHeaders(InputStream in) throws IOException {
        // Read until blank line (end of HTTP headers)
        while (!readLine(in).isEmpty()) { /* discard */ }
    }

    private void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) {}
    }

    // ── Notification ─────────────────────────────────────────────────────────

    private Notification buildNotification(String text) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel ch = new NotificationChannel(
                CHANNEL_ID, "NetShare Proxy", NotificationManager.IMPORTANCE_LOW);
            ((NotificationManager) getSystemService(NOTIFICATION_SERVICE))
                .createNotificationChannel(ch);
        }
        return new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("NetShare")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.ic_menu_share)
            .setOngoing(true)
            .build();
    }
}
