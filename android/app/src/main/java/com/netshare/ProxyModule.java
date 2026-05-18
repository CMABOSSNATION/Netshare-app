package com.netshare;

import android.app.Activity;
import android.content.Intent;
import android.net.VpnService;
import android.util.Log;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.ActivityEventListener;
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

public class ProxyModule extends ReactContextBaseJavaModule implements ActivityEventListener {

    private static final String TAG        = "NetShareProxy";
    private static final int    PROXY_PORT = 8899;
    private static final int    PIPE_BUF   = 32 * 1024;
    private static final int    VPN_REQ    = 0x0F;

    private static final byte FT_OPEN  = 0x01;
    private static final byte FT_DATA  = 0x02;
    private static final byte FT_CLOSE = 0x03;
    private static final byte FT_READY = 0x04;

    private static ReactApplicationContext reactCtx;

    // ── Proxy (HOST) ──────────────────────────────────────────────────────────
    private ServerSocket    serverSocket;
    private ExecutorService executor;
    private final AtomicBoolean running   = new AtomicBoolean(false);
    private final AtomicLong    bytesUp   = new AtomicLong(0);
    private final AtomicLong    bytesDown = new AtomicLong(0);
    private final AtomicLong    lastUp    = new AtomicLong(0);
    private final AtomicLong    lastDown  = new AtomicLong(0);

    // ── Tunnel WS (HOST bridge) ───────────────────────────────────────────────
    private OkHttpClient  httpClient;
    private WebSocket     tunnelWs;
    private final AtomicBoolean tunnelRunning = new AtomicBoolean(false);

    private final ConcurrentHashMap<Integer, Socket> hostConns   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Socket> clientConns = new ConcurrentHashMap<>();
    private final AtomicInteger connIdCounter = new AtomicInteger(1);

    // ── VPN (CLIENT) ──────────────────────────────────────────────────────────
    private Promise vpnPromise;
    private String  pendingVpnWsUrl;

    // ── Constructor ───────────────────────────────────────────────────────────

    public ProxyModule(ReactApplicationContext context) {
        super(context);
        reactCtx = context;
        context.addActivityEventListener(this);
        httpClient = new OkHttpClient.Builder()
            .pingInterval(20, java.util.concurrent.TimeUnit.SECONDS)
            .build();
    }

    @NonNull @Override public String getName() { return "ProxyModule"; }
    @ReactMethod public void addListener(String e) {}
    @ReactMethod public void removeListeners(int n) {}

    // ── startProxy (HOST) ─────────────────────────────────────────────────────

    @ReactMethod
    public void startProxy(Promise promise) {
        if (running.get()) {
            WritableMap m = new WritableNativeMap();
            m.putString("ip",   getLocalIp());
            m.putInt("port",    PROXY_PORT);
            promise.resolve(m);
            return;
        }
        try {
            String ip = getLocalIp();

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

    @ReactMethod
    public void startTunnelBridge(String wsUrl, Promise promise) {
        if (tunnelRunning.get()) { promise.resolve(true); return; }
        tunnelRunning.set(true);

        Request req = new Request.Builder().url(wsUrl).build();
        tunnelWs = httpClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                Log.i(TAG, "[Host] Tunnel open → " + wsUrl);
                emitEvent("ProxyTunnelReady", "{}");
                promise.resolve(true);
            }

            @Override
            public void onMessage(WebSocket ws, ByteString bytes) {
                handleHostFrame(bytes.toByteArray());
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                Log.d(TAG, "[Host] text: " + text);
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response response) {
                Log.e(TAG, "[Host] Tunnel failure: " + t.getMessage());
                tunnelRunning.set(false);
                emitEvent("ProxyTunnelError", t.getMessage());
                promise.reject("TUNNEL_ERROR", t.getMessage());
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                tunnelRunning.set(false);
            }
        });
    }

    // ── stopTunnelBridge (HOST) ───────────────────────────────────────────────

    @ReactMethod
    public void stopTunnelBridge(Promise promise) {
        if (tunnelWs != null) { tunnelWs.close(1000, "stopped"); tunnelWs = null; }
        tunnelRunning.set(false);
        for (Socket s : hostConns.values()) closeSocket(s);
        hostConns.clear();
        promise.resolve(true);
    }

    // ── startVpn (CLIENT) ─────────────────────────────────────────────────────

    @ReactMethod
    public void startVpn(String wsUrl, Promise promise) {
        Activity activity = getCurrentActivity();
        if (activity == null) {
            promise.reject("NO_ACTIVITY", "No activity");
            return;
        }
        Intent intent = VpnService.prepare(activity);
        if (intent == null) {
            // Already have VPN permission
            launchVpnService(wsUrl);
            promise.resolve(true);
        } else {
            // Need to ask user
            vpnPromise      = promise;
            pendingVpnWsUrl = wsUrl;
            activity.startActivityForResult(intent, VPN_REQ);
        }
    }

    // ── stopVpn (CLIENT) ──────────────────────────────────────────────────────

    @ReactMethod
    public void stopVpn(Promise promise) {
        Intent i = new Intent(reactCtx, NetShareVpnService.class);
        i.setAction(NetShareVpnService.ACTION_STOP);
        reactCtx.startService(i);
        promise.resolve(true);
    }

    // ── startClientTunnel (fallback if VPN denied) ────────────────────────────

    @ReactMethod
    public void startClientTunnel(String wsUrl, Promise promise) {
        // Delegates to VPN approach
        startVpn(wsUrl, promise);
    }

    @ReactMethod
    public void stopClientTunnel(Promise promise) {
        stopVpn(promise);
    }

    // ── VPN permission result ─────────────────────────────────────────────────

    @Override
    public void onActivityResult(Activity activity, int requestCode, int resultCode, Intent data) {
        if (requestCode != VPN_REQ) return;
        if (resultCode == Activity.RESULT_OK) {
            if (pendingVpnWsUrl != null) launchVpnService(pendingVpnWsUrl);
            if (vpnPromise != null) vpnPromise.resolve(true);
        } else {
            if (vpnPromise != null) vpnPromise.reject("VPN_DENIED", "VPN permission denied");
        }
        vpnPromise      = null;
        pendingVpnWsUrl = null;
    }

    @Override public void onNewIntent(Intent intent) {}

    private void launchVpnService(String wsUrl) {
        Intent i = new Intent(reactCtx, NetShareVpnService.class);
        i.setAction(NetShareVpnService.ACTION_START);
        i.putExtra(NetShareVpnService.EXTRA_WS_URL, wsUrl);
        reactCtx.startForegroundService(i);
        Log.i(TAG, "VPN service launched");
    }

    // ── Accept loop (HOST) ────────────────────────────────────────────────────

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(120_000);
                executor.execute(() -> handleConnect(client));
            } catch (Exception e) {
                if (running.get()) Log.w(TAG, "accept: " + e.getMessage());
            }
        }
    }

    private void handleConnect(Socket clientSock) {
        if (tunnelRunning.get() && tunnelWs != null) {
            handleViaTunnel(clientSock);
        } else {
            handleDirect(clientSock);
        }
    }

    // ── Tunnel mode handler (HOST receives from client via Cloudflare) ─────────

    private void handleViaTunnel(Socket clientSock) {
        try {
            InputStream  cIn  = clientSock.getInputStream();
            OutputStream cOut = clientSock.getOutputStream();

            String firstLine = readLine(cIn);
            if (firstLine == null) { closeSocket(clientSock); return; }

            String target;
            boolean isConnect = firstLine.toUpperCase().startsWith("CONNECT ");
            if (isConnect) {
                String[] p = firstLine.split(" ");
                target = p.length >= 2 ? p[1] : null;
                drainHeaders(cIn);
            } else {
                String[] p = firstLine.split(" ");
                if (p.length < 2) { closeSocket(clientSock); return; }
                try {
                    java.net.URL url = new java.net.URL(p[1]);
                    int port = url.getPort() > 0 ? url.getPort() : 80;
                    target = url.getHost() + ":" + port;
                } catch (Exception e) { closeSocket(clientSock); return; }
                drainHeaders(cIn);
            }

            if (target == null) { closeSocket(clientSock); return; }

            int connId = connIdCounter.getAndIncrement();
            clientConns.put(connId, clientSock);

            tunnelWs.send(ByteString.of(buildFrame(connId, FT_OPEN,
                target.getBytes(StandardCharsets.UTF_8))));

            if (isConnect) {
                cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
                cOut.flush();
            }

            byte[] buf = new byte[PIPE_BUF];
            int len;
            while ((len = cIn.read(buf)) != -1) {
                if (!tunnelRunning.get() || tunnelWs == null) break;
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA,
                    Arrays.copyOf(buf, len))));
                bytesUp.addAndGet(len);
            }
            if (tunnelWs != null) {
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
            }
        } catch (Exception e) {
            Log.d(TAG, "handleViaTunnel: " + e.getMessage());
        } finally {
            closeSocket(clientSock);
        }
    }

    // ── Host incoming frame handler ───────────────────────────────────────────

    private void handleHostFrame(byte[] raw) {
        if (raw.length < 5) return;
        int    connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte   frameType = raw[4];
        byte[] payload   = Arrays.copyOfRange(raw, 5, raw.length);

        switch (frameType) {

            case FT_OPEN: {
                String target  = new String(payload, StandardCharsets.UTF_8);
                String[] parts = target.split(":");
                String host    = parts[0];
                int    port;
                try { port = Integer.parseInt(parts[parts.length - 1]); }
                catch (Exception e) { port = 443; }

                final String fh = host; final int fp = port;
                executor.execute(() -> {
                    try {
                        Socket remote = new Socket();
                        remote.setTcpNoDelay(true);
                        remote.setSoTimeout(120_000);
                        remote.connect(new InetSocketAddress(fh, fp), 15_000);
                        hostConns.put(connId, remote);
                        emitEvent("ProxyClientConnected", String.valueOf(connId));

                        InputStream rIn = remote.getInputStream();
                        byte[] buf = new byte[PIPE_BUF];
                        int len;
                        while ((len = rIn.read(buf)) != -1) {
                            if (!tunnelRunning.get() || tunnelWs == null) break;
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA,
                                Arrays.copyOf(buf, len))));
                            bytesDown.addAndGet(len);
                        }
                        if (tunnelWs != null) {
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
                        }
                    } catch (Exception e) {
                        Log.d(TAG, "[Host] OPEN err connId=" + connId + ": " + e.getMessage());
                        if (tunnelWs != null) {
                            tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
                        }
                    } finally {
                        Socket s = hostConns.remove(connId);
                        if (s != null) closeSocket(s);
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
                if (remote != null) closeSocket(remote);
                break;
            }
        }
    }

    // ── Direct (LAN) handler — kept minimal as internal fallback ──────────────

    private void handleDirect(Socket client) {
        Socket remote = null;
        try {
            InputStream  cIn  = client.getInputStream();
            OutputStream cOut = client.getOutputStream();
            String firstLine  = readLine(cIn);
            if (firstLine == null) return;

            if (firstLine.toUpperCase().startsWith("CONNECT ")) {
                String[] p = firstLine.split(" ");
                if (p.length < 2) { sendError(cOut, 400, "Bad Request"); return; }
                int colon  = p[1].lastIndexOf(':');
                String host = colon > 0 ? p[1].substring(0, colon) : p[1];
                int port    = colon > 0 ? Integer.parseInt(p[1].substring(colon + 1)) : 443;
                drainHeaders(cIn);

                remote = new Socket();
                remote.setTcpNoDelay(true);
                remote.connect(new InetSocketAddress(host, port), 15_000);
                cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
                cOut.flush();

                final Socket fr = remote, fc = client;
                Thread t1 = new Thread(() -> pipe(fc, fr, true));
                Thread t2 = new Thread(() -> pipe(fr, fc, false));
                t1.setDaemon(true); t2.setDaemon(true);
                t1.start(); t2.start();
                t1.join(); t2.join();
            }
        } catch (Exception e) {
            Log.d(TAG, "handleDirect: " + e.getMessage());
        } finally {
            closeSocket(remote);
            closeSocket(client);
        }
    }

    private void pipe(Socket src, Socket dst, boolean up) {
        try {
            InputStream  in  = src.getInputStream();
            OutputStream out = dst.getOutputStream();
            byte[] buf = new byte[PIPE_BUF];
            int len;
            while ((len = in.read(buf)) != -1) {
                out.write(buf, 0, len); out.flush();
                if (up) bytesUp.addAndGet(len); else bytesDown.addAndGet(len);
            }
        } catch (Exception ignored) {}
        closeSocket(src); closeSocket(dst);
    }

    // ── Frame builder ─────────────────────────────────────────────────────────

    private static byte[] buildFrame(int connId, byte type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(5 + payload.length);
        buf.putInt(connId); buf.put(type); buf.put(payload);
        return buf.array();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String getLocalIp() {
        try {
            android.net.wifi.WifiManager wm =
                (android.net.wifi.WifiManager) reactCtx.getApplicationContext()
                    .getSystemService(android.content.Context.WIFI_SERVICE);
            int ip4 = wm.getConnectionInfo().getIpAddress();
            if (ip4 != 0) {
                return String.format("%d.%d.%d.%d",
                    ip4 & 0xFF, (ip4 >> 8) & 0xFF,
                    (ip4 >> 16) & 0xFF, (ip4 >> 24) & 0xFF);
            }
        } catch (Exception ignored) {}
        try {
            java.util.Enumeration<java.net.NetworkInterface> ifaces =
                java.net.NetworkInterface.getNetworkInterfaces();
            while (ifaces.hasMoreElements()) {
                java.net.NetworkInterface iface = ifaces.nextElement();
                java.util.Enumeration<java.net.InetAddress> addrs = iface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    java.net.InetAddress a = addrs.nextElement();
                    String ip = a.getHostAddress();
                    if (!a.isLoopbackAddress() && ip != null && !ip.contains(":"))
                        return ip;
                }
            }
        } catch (Exception ignored) {}
        return "127.0.0.1";
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
        while ((line = readLine(in)) != null && !line.isEmpty()) {}
    }

    private void sendError(OutputStream out, int code, String msg) {
        try {
            out.write(("HTTP/1.1 " + code + " " + msg + "\r\nContent-Length: 0\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            out.flush();
        } catch (Exception ignored) {}
    }

    private void closeSocket(Socket s) {
        if (s != null && !s.isClosed()) try { s.close(); } catch (Exception ignored) {}
    }

    private void stopProxyInternal() {
        running.set(false);
        try { if (serverSocket != null) serverSocket.close(); } catch (Exception ignored) {}
        if (executor != null) executor.shutdownNow();
        serverSocket = null;
        executor     = null;
    }

    // ── Static event emitter ──────────────────────────────────────────────────

    public static void emitEvent(String event, String data) {
        if (reactCtx == null) return;
        try {
            DeviceEventManagerModule.RCTDeviceEventEmitter emitter =
                reactCtx.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class);
            if (emitter != null) emitter.emit(event, data);
        } catch (Exception ignored) {}
    }
}
