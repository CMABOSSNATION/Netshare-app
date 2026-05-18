package com.netshare;

import android.util.Log;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.WritableNativeMap;
import com.facebook.react.modules.core.DeviceEventManagerModule;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
 * ProxyModule.java — NetShare Native Module
 *
 * Registered as "ProxyModule" in JS.
 *
 * Methods exposed to JS:
 *   startProxy()                → starts local HTTP CONNECT proxy on :8899
 *   stopProxy()                 → stops proxy server
 *   getStats()                  → { up, down } bytes since last call
 *   startTunnelBridge(wsUrl)    → HOST: open WS to DO, pipe proxy traffic through it
 *   stopTunnelBridge()          → HOST: close tunnel WS
 *   startClientTunnel(wsUrl)    → CLIENT: open WS to DO, bridge local :8899 through it
 *   stopClientTunnel()          → CLIENT: close tunnel WS
 *
 * Tunnel protocol (binary frames over WebSocket):
 *
 *   Each logical TCP connection is given a 4-byte connection ID (big-endian int).
 *
 *   Frame layout:
 *     [4 bytes: connId][1 byte: frameType][N bytes: payload]
 *
 *   Frame types:
 *     0x01  OPEN    payload = "host:port" UTF-8   (client→host: open TCP to target)
 *     0x02  DATA    payload = raw bytes            (bidirectional)
 *     0x03  CLOSE   payload = empty                (either side closes connection)
 *     0x04  READY   payload = empty                (DO signals tunnel paired)
 *
 * How tunnel mode works end-to-end:
 *
 *   CLIENT SIDE:
 *     1. Client app sets Android Wi-Fi proxy to 127.0.0.1:8899.
 *     2. App traffic hits local ProxyModule proxy.
 *     3. ProxyModule reads the CONNECT request (e.g. "CONNECT api.tiktok.com:443").
 *     4. Instead of opening a real TCP socket, it assigns a connId and sends
 *        a WS OPEN frame to the Durable Object.
 *     5. DO forwards the OPEN frame to the host WS.
 *     6. Subsequent DATA frames carry raw bytes both ways.
 *     7. CLOSE frame ends the connection.
 *
 *   HOST SIDE:
 *     1. Host WS is connected to the same DO instance.
 *     2. On receiving OPEN frame: opens a real TCP socket to the target host:port.
 *     3. On receiving DATA frame: writes bytes to that TCP socket.
 *     4. Reads bytes from TCP socket, sends DATA frames back to DO → client.
 *     5. On CLOSE frame or TCP EOF: sends CLOSE frame back, closes socket.
 */
public class ProxyModule extends ReactContextBaseJavaModule {

    private static final String TAG        = "NetShareProxy";
    private static final int    PROXY_PORT = 8899;
    private static final int    PIPE_BUF   = 32 * 1024;

    // Frame type constants
    private static final byte FT_OPEN  = 0x01;
    private static final byte FT_DATA  = 0x02;
    private static final byte FT_CLOSE = 0x03;
    private static final byte FT_READY = 0x04;

    private static ReactApplicationContext reactCtx;

    // ── Local proxy server ────────────────────────────────────────────────────
    private ServerSocket    serverSocket;
    private ExecutorService executor;
    private AtomicBoolean   running   = new AtomicBoolean(false);
    private AtomicLong      bytesUp   = new AtomicLong(0);
    private AtomicLong      bytesDown = new AtomicLong(0);
    private AtomicLong      lastUp    = new AtomicLong(0);
    private AtomicLong      lastDown  = new AtomicLong(0);

    // ── Tunnel WS (host or client) ────────────────────────────────────────────
    private OkHttpClient    httpClient;
    private WebSocket       tunnelWs;
    private AtomicBoolean   tunnelRunning = new AtomicBoolean(false);

    // Host side: connId → open TCP Socket to remote target
    private final ConcurrentHashMap<Integer, Socket> hostConns = new ConcurrentHashMap<>();
    // Client side: connId → pending/active client socket + output stream
    private final ConcurrentHashMap<Integer, Socket> clientConns = new ConcurrentHashMap<>();

    private final AtomicInteger connIdCounter = new AtomicInteger(1);

    // ── Constructor ───────────────────────────────────────────────────────────

    public ProxyModule(ReactApplicationContext context) {
        super(context);
        reactCtx = context;
        httpClient = new OkHttpClient.Builder()
            .pingInterval(20, java.util.concurrent.TimeUnit.SECONDS)
            .build();
    }

    @NonNull
    @Override
    public String getName() { return "ProxyModule"; }

    @ReactMethod public void addListener(String eventName) {}
    @ReactMethod public void removeListeners(int count) {}

    // ── startProxy ────────────────────────────────────────────────────────────

    @ReactMethod
    public void startProxy(Promise promise) {
        if (running.get()) {
            WritableMap m = new WritableNativeMap();
            m.putString("ip",   getWifiIp());
            m.putInt("port",    PROXY_PORT);
            promise.resolve(m);
            return;
        }

        try {
            // Get best available IP — WiFi preferred, falls back to mobile data IP
            // or 127.0.0.1. We never reject just because WiFi is off; tunnel mode
            // works over any internet connection.
            String ip = getWifiIp();
            if (ip == null || ip.equals("0.0.0.0")) {
                ip = "127.0.0.1"; // tunnel mode uses localhost anyway
            }

            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("0.0.0.0", PROXY_PORT));
            running.set(true);
            bytesUp.set(0); bytesDown.set(0);
            lastUp.set(0);  lastDown.set(0);

            executor = Executors.newCachedThreadPool();
            executor.execute(this::acceptLoop);

            Log.i(TAG, "Proxy started on " + ip + ":" + PROXY_PORT);

            WritableMap result = new WritableNativeMap();
            result.putString("ip",   ip);
            result.putInt("port",    PROXY_PORT);
            promise.resolve(result);

        } catch (Exception e) {
            Log.e(TAG, "startProxy: " + e.getMessage());
            promise.reject("PROXY_START_ERROR", e.getMessage());
        }
    }

    // ── stopProxy ─────────────────────────────────────────────────────────────

    @ReactMethod
    public void stopProxy(Promise promise) {
        stopProxyInternal();
        promise.resolve(true);
    }

    // ── getStats ──────────────────────────────────────────────────────────────

    @ReactMethod
    public void getStats(Promise promise) {
        long up   = bytesUp.get()   - lastUp.get();
        long down = bytesDown.get() - lastDown.get();
        lastUp.set(bytesUp.get());
        lastDown.set(bytesDown.get());

        WritableMap m = new WritableNativeMap();
        m.putDouble("up",   (double) Math.max(0, up));
        m.putDouble("down", (double) Math.max(0, down));
        promise.resolve(m);
    }

    // ── startTunnelBridge (HOST) ──────────────────────────────────────────────

    /**
     * Host calls this after startProxy().
     * Opens a WebSocket to the Durable Object at wsUrl.
     * From then on, every OPEN frame received from the DO causes us to open
     * a real TCP socket to the target and pipe DATA frames both ways.
     */
    @ReactMethod
    public void startTunnelBridge(String wsUrl, Promise promise) {
        if (tunnelRunning.get()) {
            promise.resolve(true);
            return;
        }
        tunnelRunning.set(true);

        Request req = new Request.Builder().url(wsUrl).build();
        tunnelWs = httpClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                Log.i(TAG, "[Host] Tunnel WS open → " + wsUrl);
                emitEvent("ProxyTunnelReady", "{}");
                promise.resolve(true);
            }

            @Override
            public void onMessage(WebSocket ws, ByteString bytes) {
                handleHostFrame(bytes.toByteArray());
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                // JSON control frames (e.g. {"type":"paired"}) — ignore or log
                Log.d(TAG, "[Host] text frame: " + text);
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response response) {
                Log.e(TAG, "[Host] Tunnel WS failure: " + t.getMessage());
                tunnelRunning.set(false);
                emitEvent("ProxyTunnelError", t.getMessage());
                promise.reject("TUNNEL_ERROR", t.getMessage());
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                Log.i(TAG, "[Host] Tunnel WS closed: " + reason);
                tunnelRunning.set(false);
            }
        });
    }

    // ── stopTunnelBridge (HOST) ───────────────────────────────────────────────

    @ReactMethod
    public void stopTunnelBridge(Promise promise) {
        if (tunnelWs != null) {
            tunnelWs.close(1000, "Host stopped");
            tunnelWs = null;
        }
        tunnelRunning.set(false);
        // Close all open host connections
        for (Socket s : hostConns.values()) close(s);
        hostConns.clear();
        promise.resolve(true);
    }

    // ── startClientTunnel (CLIENT) ────────────────────────────────────────────

    /**
     * Client calls this after startProxy().
     * Opens a WebSocket to the Durable Object at wsUrl.
     * The local proxy (acceptLoop) now intercepts CONNECT requests and sends
     * OPEN frames over this WS instead of opening direct TCP sockets.
     */
    @ReactMethod
    public void startClientTunnel(String wsUrl, Promise promise) {
        if (tunnelRunning.get()) {
            promise.resolve(true);
            return;
        }
        tunnelRunning.set(true);

        Request req = new Request.Builder().url(wsUrl).build();
        tunnelWs = httpClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                Log.i(TAG, "[Client] Tunnel WS open → " + wsUrl);
                emitEvent("ProxyTunnelReady", "{}");
                promise.resolve(true);
            }

            @Override
            public void onMessage(WebSocket ws, ByteString bytes) {
                handleClientFrame(bytes.toByteArray());
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                Log.d(TAG, "[Client] text frame: " + text);
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response response) {
                Log.e(TAG, "[Client] Tunnel WS failure: " + t.getMessage());
                tunnelRunning.set(false);
                emitEvent("ProxyTunnelError", t.getMessage());
                promise.reject("TUNNEL_ERROR", t.getMessage());
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                Log.i(TAG, "[Client] Tunnel WS closed: " + reason);
                tunnelRunning.set(false);
            }
        });
    }

    // ── stopClientTunnel (CLIENT) ─────────────────────────────────────────────

    @ReactMethod
    public void stopClientTunnel(Promise promise) {
        if (tunnelWs != null) {
            tunnelWs.close(1000, "Client stopped");
            tunnelWs = null;
        }
        tunnelRunning.set(false);
        for (Socket s : clientConns.values()) close(s);
        clientConns.clear();
        promise.resolve(true);
    }

    // ── Local proxy accept loop ───────────────────────────────────────────────

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(120_000);
                executor.execute(() -> {
                    if (tunnelRunning.get() && tunnelWs != null) {
                        handleConnectViaTunnel(client);   // tunnel mode
                    } else {
                        handleConnectDirect(client);       // LAN mode
                    }
                });
            } catch (Exception e) {
                if (running.get()) Log.w(TAG, "accept: " + e.getMessage());
            }
        }
    }

    // ── Tunnel mode: client-side CONNECT handler ──────────────────────────────

    /**
     * Reads the HTTP CONNECT request from the client app, assigns a connId,
     * sends an OPEN frame to the DO, then bridges DATA frames both ways.
     */
    private void handleConnectViaTunnel(Socket clientSock) {
        try {
            InputStream  cIn  = clientSock.getInputStream();
            OutputStream cOut = clientSock.getOutputStream();

            String firstLine = readLine(cIn);
            if (firstLine == null) { close(clientSock); return; }

            String target;
            if (firstLine.toUpperCase().startsWith("CONNECT ")) {
                String[] parts = firstLine.split(" ");
                target = parts.length >= 2 ? parts[1] : null;
                drainHeaders(cIn);
            } else {
                // Plain HTTP — extract host:80 from URL
                String[] parts = firstLine.split(" ");
                if (parts.length < 2) { close(clientSock); return; }
                try {
                    java.net.URL url = new java.net.URL(parts[1]);
                    int p = url.getPort() > 0 ? url.getPort() : 80;
                    target = url.getHost() + ":" + p;
                } catch (Exception e) { close(clientSock); return; }
                drainHeaders(cIn);
            }

            if (target == null) { close(clientSock); return; }

            int connId = connIdCounter.getAndIncrement();
            clientConns.put(connId, clientSock);

            // Send OPEN frame: [connId(4)][FT_OPEN(1)][target UTF-8]
            byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
            byte[] openFrame   = buildFrame(connId, FT_OPEN, targetBytes);
            tunnelWs.send(ByteString.of(openFrame));

            // Tell client the tunnel is open (for CONNECT)
            if (firstLine.toUpperCase().startsWith("CONNECT ")) {
                cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
                cOut.flush();
            }

            // Read from client app → send DATA frames to DO
            byte[] buf = new byte[PIPE_BUF];
            int len;
            while ((len = cIn.read(buf)) != -1) {
                if (!tunnelRunning.get() || tunnelWs == null) break;
                byte[] payload = java.util.Arrays.copyOf(buf, len);
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA, payload)));
                bytesUp.addAndGet(len);
            }

            // Send CLOSE frame
            if (tunnelWs != null) {
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
            }

        } catch (Exception e) {
            Log.d(TAG, "handleConnectViaTunnel: " + e.getMessage());
        } finally {
            close(clientSock);
        }
    }

    // ── Tunnel mode: host-side incoming frame handler ─────────────────────────

    /**
     * Called on host when a binary WS frame arrives from the DO.
     * Dispatches by frame type.
     */
    private void handleHostFrame(byte[] raw) {
        if (raw.length < 5) return;
        int  connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte frameType = raw[4];
        byte[] payload = new byte[raw.length - 5];
        System.arraycopy(raw, 5, payload, 0, payload.length);

        switch (frameType) {

            case FT_OPEN: {
                // payload = "host:port"
                String target = new String(payload, StandardCharsets.UTF_8);
                String[] parts = target.split(":");
                String host = parts[0];
                int port;
                try { port = Integer.parseInt(parts[parts.length - 1]); }
                catch (Exception e) { port = 443; }

                final String fHost = host;
                final int    fPort = port;
                executor.execute(() -> {
                    try {
                        Socket remote = new Socket();
                        remote.setTcpNoDelay(true);
                        remote.setSoTimeout(120_000);
                        remote.connect(new InetSocketAddress(fHost, fPort), 15_000);
                        hostConns.put(connId, remote);
                        emitEvent("ProxyClientConnected", String.valueOf(connId));

                        // Read from remote → send DATA frames back to DO
                        InputStream rIn = remote.getInputStream();
                        byte[] buf = new byte[PIPE_BUF];
                        int len;
                        while ((len = rIn.read(buf)) != -1) {
                            if (!tunnelRunning.get() || tunnelWs == null) break;
                            byte[] data = java.util.Arrays.copyOf(buf, len);
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA, data)));
                            bytesDown.addAndGet(len);
                        }

                        // Remote closed → send CLOSE frame
                        if (tunnelWs != null) {
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
                        }
                    } catch (Exception e) {
                        Log.d(TAG, "[Host] OPEN connId=" + connId + " err: " + e.getMessage());
                        if (tunnelWs != null) {
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
                        }
                    } finally {
                        Socket s = hostConns.remove(connId);
                        if (s != null) close(s);
                        emitEvent("ProxyClientDisconnected", String.valueOf(connId));
                    }
                });
                break;
            }

            case FT_DATA: {
                Socket remote = hostConns.get(connId);
                if (remote == null || remote.isClosed()) return;
                try {
                    remote.getOutputStream().write(payload);
                    remote.getOutputStream().flush();
                    bytesUp.addAndGet(payload.length);
                } catch (Exception e) {
                    Log.d(TAG, "[Host] DATA write err: " + e.getMessage());
                }
                break;
            }

            case FT_CLOSE: {
                Socket remote = hostConns.remove(connId);
                if (remote != null) close(remote);
                break;
            }
        }
    }

    // ── Tunnel mode: client-side incoming frame handler ───────────────────────

    /**
     * Called on client when a binary WS frame arrives from the DO.
     * DATA frames are written back to the client app socket.
     * CLOSE frames close the client app socket.
     */
    private void handleClientFrame(byte[] raw) {
        if (raw.length < 5) return;
        int  connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte frameType = raw[4];
        byte[] payload = new byte[raw.length - 5];
        System.arraycopy(raw, 5, payload, 0, payload.length);

        switch (frameType) {

            case FT_DATA: {
                Socket clientSock = clientConns.get(connId);
                if (clientSock == null || clientSock.isClosed()) return;
                try {
                    clientSock.getOutputStream().write(payload);
                    clientSock.getOutputStream().flush();
                    bytesDown.addAndGet(payload.length);
                } catch (Exception e) {
                    Log.d(TAG, "[Client] DATA write err: " + e.getMessage());
                }
                break;
            }

            case FT_CLOSE: {
                Socket clientSock = clientConns.remove(connId);
                if (clientSock != null) close(clientSock);
                break;
            }
        }
    }

    // ── LAN mode: direct CONNECT (no tunnel) ──────────────────────────────────

    private void handleConnectDirect(Socket client) {
        Socket remote = null;
        try {
            InputStream  cIn  = client.getInputStream();
            OutputStream cOut = client.getOutputStream();

            String firstLine = readLine(cIn);
            if (firstLine == null) return;

            if (firstLine.toUpperCase().startsWith("CONNECT ")) {
                String[] parts = firstLine.split(" ");
                String target = parts.length >= 2 ? parts[1] : null;
                if (target == null) { sendError(cOut, 400, "Bad Request"); return; }

                int colon = target.lastIndexOf(':');
                String host = colon > 0 ? target.substring(0, colon) : target;
                int port    = colon > 0 ? Integer.parseInt(target.substring(colon + 1)) : 443;

                drainHeaders(cIn);

                remote = new Socket();
                remote.setTcpNoDelay(true);
                remote.setSoTimeout(120_000);
                remote.connect(new InetSocketAddress(host, port), 15_000);

                cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
                cOut.flush();

                final Socket fr = remote, fc = client;
                Thread t1 = new Thread(() -> pipeDirect(fc, fr, true));
                Thread t2 = new Thread(() -> pipeDirect(fr, fc, false));
                t1.setDaemon(true); t2.setDaemon(true);
                t1.start(); t2.start();
                t1.join(); t2.join();

            } else {
                handlePlainHttpDirect(client, cIn, cOut, firstLine);
            }
        } catch (Exception e) {
            Log.d(TAG, "handleConnectDirect: " + e.getMessage());
        } finally {
            close(remote);
            close(client);
        }
    }

    private void handlePlainHttpDirect(Socket client, InputStream cIn,
                                       OutputStream cOut, String firstLine) {
        Socket remote = null;
        try {
            if (firstLine == null) return;
            String[] parts = firstLine.split(" ");
            if (parts.length < 2) { sendError(cOut, 400, "Bad Request"); return; }
            java.net.URL url = new java.net.URL(parts[1]);
            int port = url.getPort() > 0 ? url.getPort() : 80;

            remote = new Socket();
            remote.setTcpNoDelay(true);
            remote.connect(new InetSocketAddress(url.getHost(), port), 15_000);

            OutputStream rOut = remote.getOutputStream();
            String path = url.getFile().isEmpty() ? "/" : url.getFile();
            rOut.write((parts[0] + " " + path + " " + parts[2] + "\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            String line;
            while ((line = readLine(cIn)) != null && !line.isEmpty()) {
                rOut.write((line + "\r\n").getBytes(StandardCharsets.US_ASCII));
            }
            rOut.write("\r\n".getBytes(StandardCharsets.US_ASCII));
            rOut.flush();

            final Socket fr = remote, fc = client;
            Thread t1 = new Thread(() -> pipeDirect(fc, fr, true));
            Thread t2 = new Thread(() -> pipeDirect(fr, fc, false));
            t1.setDaemon(true); t2.setDaemon(true);
            t1.start(); t2.start();
            t1.join(); t2.join();

        } catch (Exception e) {
            Log.d(TAG, "handlePlainHttpDirect: " + e.getMessage());
        } finally {
            close(remote);
            close(client);
        }
    }

    private void pipeDirect(Socket src, Socket dst, boolean isUpload) {
        try {
            InputStream  in  = src.getInputStream();
            OutputStream out = dst.getOutputStream();
            byte[] buf = new byte[PIPE_BUF];
            int len;
            while ((len = in.read(buf)) != -1) {
                out.write(buf, 0, len);
                out.flush();
                if (isUpload) bytesUp.addAndGet(len);
                else          bytesDown.addAndGet(len);
            }
        } catch (Exception ignored) {}
        close(src); close(dst);
    }

    // ── Frame builder ─────────────────────────────────────────────────────────

    private static byte[] buildFrame(int connId, byte type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(5 + payload.length);
        buf.putInt(connId);
        buf.put(type);
        buf.put(payload);
        return buf.array();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String getWifiIp() {
        try {
            android.net.wifi.WifiManager wm =
                (android.net.wifi.WifiManager) reactCtx
                    .getApplicationContext()
                    .getSystemService(android.content.Context.WIFI_SERVICE);
            int ip4 = wm.getConnectionInfo().getIpAddress();
            return String.format("%d.%d.%d.%d",
                (ip4 & 0xFF), (ip4 >> 8 & 0xFF),
                (ip4 >> 16 & 0xFF), (ip4 >> 24 & 0xFF));
        } catch (Exception e) {
            try {
                java.util.Enumeration<java.net.NetworkInterface> ifaces =
                    java.net.NetworkInterface.getNetworkInterfaces();
                while (ifaces.hasMoreElements()) {
                    java.net.NetworkInterface iface = ifaces.nextElement();
                    java.util.Enumeration<java.net.InetAddress> addrs = iface.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        java.net.InetAddress addr = addrs.nextElement();
                        String ip = addr.getHostAddress();
                        if (!addr.isLoopbackAddress() && ip != null && ip.indexOf(':') < 0) {
                            return ip;
                        }
                    }
                }
            } catch (Exception e2) { Log.w(TAG, "getWifiIp fallback: " + e2.getMessage()); }
            return null;
        }
    }

    private String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') { in.read(); break; }
            if (b == '\n') break;
            sb.append((char) b);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    private void drainHeaders(InputStream in) throws Exception {
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) { /* drain */ }
    }

    private void sendError(OutputStream out, int code, String msg) {
        try {
            out.write(("HTTP/1.1 " + code + " " + msg + "\r\nContent-Length: 0\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            out.flush();
        } catch (Exception ignored) {}
    }

    private void close(Socket s) {
        if (s != null && !s.isClosed()) try { s.close(); } catch (Exception ignored) {}
    }

    private void stopProxyInternal() {
        running.set(false);
        try { if (serverSocket != null) serverSocket.close(); } catch (Exception ignored) {}
        if (executor != null) executor.shutdownNow();
        serverSocket = null;
        executor     = null;
        Log.i(TAG, "Proxy stopped");
    }

    // ── Static event emitter ──────────────────────────────────────────────────

    public static void emitEvent(String eventName, String data) {
        if (reactCtx == null) return;
        try {
            DeviceEventManagerModule.RCTDeviceEventEmitter emitter =
                reactCtx.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class);
            if (emitter != null) emitter.emit(eventName, data);
        } catch (Exception ignored) {}
    }
}
