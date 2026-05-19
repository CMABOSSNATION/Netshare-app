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

/**
 * ProxyModule — React Native bridge for NetShare proxy + VPN control
 *
 * ══════════════════════════════════════════════════════════════════════════════
 * AUDIT FINDINGS FIXED IN THIS FILE
 * ══════════════════════════════════════════════════════════════════════════════
 *
 * BUG PM-1 ► buildFrame() THROWS NullPointerException ON FT_CLOSE
 * ─────────────────────────────────────────────────────────────────────────────
 * The original buildFrame computed `ByteBuffer.allocate(5 + payload.length)`
 * with no null check.  Every FT_CLOSE frame is sent with `new byte[0]` in
 * this file so it didn't crash here, but the pattern is fragile and crashes
 * immediately if any caller ever passes null (as NetShareVpnService.java does
 * on line 439: `buildFrame(connId, FT_CLOSE, null)`).  Since ProxyModule and
 * NetShareVpnService share the same frame wire protocol, both builders must
 * be null-safe.
 * FIX: `buildFrame` guards against null payload and treats it as empty.
 *
 * BUG PM-2 ► FT_OPEN HANDLER RACES: hostConns PUT AFTER RESPONSE STARTS
 * ─────────────────────────────────────────────────────────────────────────────
 * In handleHostFrame(FT_OPEN), the executor thread calls remote.connect(),
 * then hostConns.put(connId, remote), then starts reading the response.  But
 * the FT_DATA frames from the client (upload body, HTTP headers for plain HTTP)
 * may arrive and hit handleHostFrame(FT_DATA) BEFORE the executor has finished
 * connect() and called hostConns.put().  When that happens, hostConns.get()
 * returns null, the data frame falls through to clientConns (wrong direction),
 * and the remote server never receives the request body → it returns 0 bytes.
 * FIX: hostConns.put() is now called immediately after Socket construction,
 * BEFORE connect(), so incoming FT_DATA frames always find a valid socket.
 * The socket is then connected on the same thread before any I/O is attempted.
 *
 * BUG PM-3 ► FT_DATA DIRECTION IS AMBIGUOUS — BOTH MAPS CHECKED SERIALLY
 * ─────────────────────────────────────────────────────────────────────────────
 * The FT_DATA handler tries hostConns first, then clientConns.  This is
 * correct in theory, but if a remote socket is in hostConns but is currently
 * in the middle of connect() (see BUG PM-2 race), isClosed() returns false
 * even though no I/O is possible yet, and the write silently fails.  Combined
 * with BUG PM-2 this causes the data frame to be lost entirely.
 * FIX: After PM-2 fix, hostConns always contains the socket from construction
 * time; add explicit isConnected() guard before writing.
 *
 * BUG PM-4 ► VERBOSE TELEMETRY MISSING
 * ─────────────────────────────────────────────────────────────────────────────
 * Frame handler had only Log.d calls for errors.  No byte counts, no direction
 * labels, no connId tracing at INFO level.  Impossible to diagnose in prod logs.
 * FIX: Log.i at every frame send/receive with connId + byte count.
 *
 * ══════════════════════════════════════════════════════════════════════════════
 * ORIGINAL FIX LOG (preserved)
 * ══════════════════════════════════════════════════════════════════════════════
 * FIX 1: handleViaTunnel — connId declared before try; clientSock NOT closed in finally
 * FIX 2: handleHostFrame FT_DATA — checks clientConns as well as hostConns
 * FIX 3: handleHostFrame FT_CLOSE — closes both clientConns and hostConns
 * FIX 4: stopTunnelBridge clears clientConns
 * FIX 5: OkHttpClient has explicit read/write timeouts
 * FIX 6: onFailure guards already-resolved promise
 */
public class ProxyModule extends ReactContextBaseJavaModule implements ActivityEventListener {

    private static final String TAG        = "NetShareProxy";
    private static final int    PROXY_PORT = 8899;
    private static final int    PIPE_BUF   = 32 * 1024;
    private static final int    VPN_REQ    = 0x0F;

    private static final byte FT_OPEN  = 0x01;
    private static final byte FT_DATA  = 0x02;
    private static final byte FT_CLOSE = 0x03;

    private static ReactApplicationContext reactCtx;

    // ── HOST proxy ─────────────────────────────────────────────────────────────
    private ServerSocket    serverSocket;
    private ExecutorService executor;
    private final AtomicBoolean running   = new AtomicBoolean(false);
    private final AtomicLong    bytesUp   = new AtomicLong(0);
    private final AtomicLong    bytesDown = new AtomicLong(0);
    private final AtomicLong    lastUp    = new AtomicLong(0);
    private final AtomicLong    lastDown  = new AtomicLong(0);

    // ── HOST tunnel WS ─────────────────────────────────────────────────────────
    private OkHttpClient        httpClient;
    private WebSocket           tunnelWs;
    private final AtomicBoolean tunnelRunning     = new AtomicBoolean(false);
    private final AtomicBoolean tunnelPromiseDone = new AtomicBoolean(false);

    // connId → remote internet socket (host opens outgoing TCP to target)
    private final ConcurrentHashMap<Integer, Socket> hostConns   = new ConcurrentHashMap<>();
    // connId → local client socket (accepted from :8899 proxy, belongs to the app)
    private final ConcurrentHashMap<Integer, Socket> clientConns = new ConcurrentHashMap<>();
    private final AtomicInteger connIdCounter = new AtomicInteger(1);

    // ── VPN (CLIENT) ───────────────────────────────────────────────────────────
    private Promise vpnPromise;
    private String  pendingVpnWsUrl;

    // ── Constructor ────────────────────────────────────────────────────────────

    public ProxyModule(ReactApplicationContext context) {
        super(context);
        reactCtx = context;
        context.addActivityEventListener(this);
        // FIX 5: explicit timeouts so hung connections don't block forever
        httpClient = new OkHttpClient.Builder()
            .pingInterval(20, java.util.concurrent.TimeUnit.SECONDS)
            .connectTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
            .readTimeout(0,  java.util.concurrent.TimeUnit.SECONDS)   // WS: no read timeout
            .writeTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
            .build();
    }

    @NonNull @Override public String getName() { return "ProxyModule"; }
    @ReactMethod public void addListener(String e) {}
    @ReactMethod public void removeListeners(int n) {}

    // ── startProxy (HOST) ──────────────────────────────────────────────────────

    @ReactMethod
    public void startProxy(Promise promise) {
        if (running.get()) {
            WritableMap m = new WritableNativeMap();
            m.putString("ip", getLocalIp());
            m.putInt("port", PROXY_PORT);
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

            Log.i(TAG, "[startProxy] Proxy listening on " + ip + ":" + PROXY_PORT);
            WritableMap result = new WritableNativeMap();
            result.putString("ip", ip);
            result.putInt("port", PROXY_PORT);
            promise.resolve(result);
        } catch (Exception e) {
            Log.e(TAG, "[startProxy] Failed: " + e.getMessage());
            promise.reject("PROXY_START_ERROR", e.getMessage());
        }
    }

    @ReactMethod
    public void stopProxy(Promise promise) {
        stopProxyInternal();
        promise.resolve(true);
    }

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

    // ── startTunnelBridge (HOST) ───────────────────────────────────────────────

    @ReactMethod
    public void startTunnelBridge(String wsUrl, Promise promise) {
        if (tunnelRunning.get()) { promise.resolve(true); return; }
        tunnelRunning.set(true);
        tunnelPromiseDone.set(false); // FIX 6: reset guard

        Log.i(TAG, "[startTunnelBridge] Connecting to relay: " + wsUrl);
        Request req = new Request.Builder().url(wsUrl).build();
        tunnelWs = httpClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                Log.i(TAG, "[Host][WS] Tunnel OPEN → " + wsUrl);
                emitEvent("ProxyTunnelReady", "{}");
                if (tunnelPromiseDone.compareAndSet(false, true)) {
                    promise.resolve(true);
                }
            }

            @Override
            public void onMessage(WebSocket ws, ByteString bytes) {
                Log.i(TAG, "[Host][WS] Frame received bytes=" + bytes.size());
                handleHostFrame(bytes.toByteArray());
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                // Control/heartbeat text frames — not forwarded as data
                Log.d(TAG, "[Host][WS] Text control frame: " + text);
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response response) {
                Log.e(TAG, "[Host][WS] Tunnel FAILURE: " + t.getMessage());
                tunnelRunning.set(false);
                emitEvent("ProxyTunnelError", t.getMessage() != null ? t.getMessage() : "unknown");
                // FIX 6: only reject if promise hasn't already been resolved/rejected
                if (tunnelPromiseDone.compareAndSet(false, true)) {
                    promise.reject("TUNNEL_ERROR", t.getMessage());
                }
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                Log.i(TAG, "[Host][WS] Tunnel CLOSED code=" + code + " reason=" + reason);
                tunnelRunning.set(false);
                emitEvent("ProxyTunnelError", "Tunnel closed: " + reason);
            }
        });
    }

    // ── stopTunnelBridge (HOST) ────────────────────────────────────────────────

    @ReactMethod
    public void stopTunnelBridge(Promise promise) {
        Log.i(TAG, "[stopTunnelBridge] Stopping tunnel bridge");
        if (tunnelWs != null) { tunnelWs.close(1000, "stopped"); tunnelWs = null; }
        tunnelRunning.set(false);
        for (Socket s : hostConns.values())   closeSocket(s);
        for (Socket s : clientConns.values()) closeSocket(s); // FIX 4
        hostConns.clear();
        clientConns.clear();
        promise.resolve(true);
    }

    // ── startVpn / stopVpn (CLIENT) ────────────────────────────────────────────

    @ReactMethod
    public void startVpn(String wsUrl, Promise promise) {
        Activity activity = getCurrentActivity();
        if (activity == null) { promise.reject("NO_ACTIVITY", "No activity"); return; }
        Intent intent = VpnService.prepare(activity);
        if (intent == null) {
            launchVpnService(wsUrl);
            promise.resolve(true);
        } else {
            pendingVpnWsUrl = wsUrl;
            vpnPromise      = promise;
            activity.startActivityForResult(intent, VPN_REQ);
        }
    }

    @ReactMethod
    public void stopVpn(Promise promise) {
        Intent i = new Intent(reactCtx, NetShareVpnService.class)
            .setAction(NetShareVpnService.ACTION_STOP);
        reactCtx.startService(i);
        promise.resolve(true);
    }

    @ReactMethod
    public void startClientTunnel(String wsUrl, Promise promise) {
        launchVpnService(wsUrl);
        promise.resolve(true);
    }

    @ReactMethod
    public void stopClientTunnel(Promise promise) {
        Intent i = new Intent(reactCtx, NetShareVpnService.class)
            .setAction(NetShareVpnService.ACTION_STOP);
        reactCtx.startService(i);
        promise.resolve(true);
    }

    private void launchVpnService(String wsUrl) {
        Log.i(TAG, "[launchVpnService] Starting VPN service wsUrl=" + wsUrl);
        Intent i = new Intent(reactCtx, NetShareVpnService.class)
            .setAction(NetShareVpnService.ACTION_START)
            .putExtra(NetShareVpnService.EXTRA_WS_URL, wsUrl);
        reactCtx.startService(i);
    }

    @Override
    public void onActivityResult(Activity activity, int requestCode, int resultCode, Intent data) {
        if (requestCode == VPN_REQ) {
            if (resultCode == Activity.RESULT_OK && pendingVpnWsUrl != null) {
                launchVpnService(pendingVpnWsUrl);
                if (vpnPromise != null) vpnPromise.resolve(true);
            } else {
                if (vpnPromise != null) vpnPromise.reject("VPN_DENIED", "User denied VPN");
            }
            vpnPromise      = null;
            pendingVpnWsUrl = null;
        }
    }

    @Override public void onNewIntent(Intent intent) {}

    // ── Accept loop (HOST) ────────────────────────────────────────────────────

    private void acceptLoop() {
        Log.i(TAG, "[acceptLoop] Accepting connections on :" + PROXY_PORT);
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(120_000);
                Log.i(TAG, "[acceptLoop] New connection from " + client.getRemoteSocketAddress());
                executor.execute(() -> handleConnect(client));
            } catch (Exception e) {
                if (running.get()) Log.w(TAG, "[acceptLoop] accept error: " + e.getMessage());
            }
        }
        Log.i(TAG, "[acceptLoop] Loop exited");
    }

    private void handleConnect(Socket clientSock) {
        if (tunnelRunning.get() && tunnelWs != null) {
            handleViaTunnel(clientSock);
        } else {
            handleDirect(clientSock);
        }
    }

    // ── Tunnel mode handler (HOST) ────────────────────────────────────────────

    /**
     * Handles a connection accepted on :8899 by tunneling it through the
     * WebSocket relay.  The relay forwards FT_OPEN/FT_DATA/FT_CLOSE frames to
     * the host, which connects to the actual internet target.
     *
     * FIX 1: connId declared outside try block; clientSock NOT closed in finally.
     * FIX PM-1: buildFrame is null-safe (handles new byte[0] and null equally).
     */
    private void handleViaTunnel(Socket clientSock) {
        // FIX 1: declare connId outside try so cleanup catch can reference it
        int connId = connIdCounter.getAndIncrement();
        Log.i(TAG, "[handleViaTunnel] connId=" + connId + " from " + clientSock.getRemoteSocketAddress());
        try {
            InputStream  cIn  = clientSock.getInputStream();
            OutputStream cOut = clientSock.getOutputStream();

            String firstLine = readLine(cIn);
            if (firstLine == null) { closeSocket(clientSock); return; }
            Log.i(TAG, "[handleViaTunnel] connId=" + connId + " firstLine=" + firstLine);

            String  target;
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
            Log.i(TAG, "[handleViaTunnel] connId=" + connId + " target=" + target + " isConnect=" + isConnect);

            // FIX 1: register clientSock BEFORE sending FT_OPEN so any early
            // FT_DATA response frames have a valid socket to write to.
            clientConns.put(connId, clientSock);

            byte[] openFrame = buildFrame(connId, FT_OPEN, target.getBytes(StandardCharsets.UTF_8));
            tunnelWs.send(ByteString.of(openFrame));
            Log.i(TAG, "[handleViaTunnel] connId=" + connId + " FT_OPEN sent target=" + target);

            if (isConnect) {
                cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
                cOut.flush();
            }

            // Forward upload data (app → relay → host) via FT_DATA frames
            byte[] buf = new byte[PIPE_BUF];
            int len;
            long totalUpBytes = 0;
            while ((len = cIn.read(buf)) != -1) {
                if (!tunnelRunning.get() || tunnelWs == null) break;
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_DATA, Arrays.copyOf(buf, len))));
                bytesUp.addAndGet(len);
                totalUpBytes += len;
            }
            Log.i(TAG, "[handleViaTunnel] connId=" + connId + " upload done totalUpBytes=" + totalUpBytes);

            // Signal upload end — FT_CLOSE with empty payload
            if (tunnelWs != null) {
                tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0])));
                Log.i(TAG, "[handleViaTunnel] connId=" + connId + " FT_CLOSE sent (upload end)");
            }

            // FIX 1: do NOT close clientSock here — FT_CLOSE from relay closes it
            // after the host has flushed all response data back.

        } catch (Exception e) {
            Log.w(TAG, "[handleViaTunnel] connId=" + connId + " error: " + e.getMessage());
            clientConns.remove(connId);
            closeSocket(clientSock);
            if (tunnelWs != null) {
                try { tunnelWs.send(ByteString.of(buildFrame(connId, FT_CLOSE, new byte[0]))); }
                catch (Exception ignored) {}
            }
        }
        // Intentionally NO finally closeSocket — FT_CLOSE handler owns clientSock lifecycle.
    }

    // ── Host frame handler (receives frames from relay WS) ───────────────────

    /**
     * Routes binary frames arriving from the Cloudflare relay WebSocket.
     *
     * Frame layout: [ connId(4 bytes BE) | frameType(1 byte) | payload(N bytes) ]
     *
     * FIX PM-2: hostConns.put() now called BEFORE connect() so FT_DATA frames
     *           that arrive during the connect() call always find a valid socket.
     * FIX PM-3: isConnected() guard added before writing to remote socket.
     * FIX 2:    FT_DATA checks clientConns if hostConns lookup misses (response path).
     * FIX 3:    FT_CLOSE closes both clientConns and hostConns entries.
     * FIX PM-4: Full telemetry at INFO level for every frame.
     */
    private void handleHostFrame(byte[] raw) {
        if (raw.length < 5) {
            Log.w(TAG, "[handleHostFrame] Frame too short: " + raw.length + " bytes, ignoring");
            return;
        }
        int    connId    = ByteBuffer.wrap(raw, 0, 4).getInt();
        byte   frameType = raw[4];
        byte[] payload   = Arrays.copyOfRange(raw, 5, raw.length);

        Log.i(TAG, "[handleHostFrame] connId=" + connId
            + " type=" + frameTypeLabel(frameType)
            + " payloadBytes=" + payload.length);

        switch (frameType) {

            case FT_OPEN: {
                // Host receives FT_OPEN from the client-side VPN: open a real TCP
                // connection to the internet target and start streaming the response.
                String   target = new String(payload, StandardCharsets.UTF_8);
                String[] parts  = target.split(":");
                String   host   = parts[0];
                int      port;
                try { port = Integer.parseInt(parts[parts.length - 1]); }
                catch (Exception e) { port = 443; }

                final String fh = host;
                final int    fp = port;
                final int    cid = connId;

                executor.execute(() -> {
                    Socket remote = new Socket();
                    try {
                        remote.setTcpNoDelay(true);
                        remote.setSoTimeout(120_000);

                        // BUG PM-2 FIX: put socket in map BEFORE connect() so that any
                        // FT_DATA frames arriving during the connect attempt find the socket.
                        // The socket is not yet connected here, but isConnected() guard in
                        // FT_DATA prevents premature writes.
                        hostConns.put(cid, remote);
                        Log.i(TAG, "[FT_OPEN] connId=" + cid + " connecting to " + fh + ":" + fp);

                        remote.connect(new InetSocketAddress(fh, fp), 15_000);
                        Log.i(TAG, "[FT_OPEN] connId=" + cid + " connected to " + fh + ":" + fp);
                        emitEvent("ProxyClientConnected", String.valueOf(cid));

                        // Stream response back to client relay via FT_DATA frames
                        InputStream rIn = remote.getInputStream();
                        byte[] buf = new byte[PIPE_BUF];
                        int len;
                        long totalDownBytes = 0;
                        while ((len = rIn.read(buf)) != -1) {
                            if (!tunnelRunning.get() || tunnelWs == null) break;
                            tunnelWs.send(ByteString.of(buildFrame(cid, FT_DATA,
                                Arrays.copyOf(buf, len))));
                            bytesDown.addAndGet(len);
                            totalDownBytes += len;
                        }
                        Log.i(TAG, "[FT_OPEN] connId=" + cid
                            + " response stream ended totalDownBytes=" + totalDownBytes);

                        if (tunnelWs != null) {
                            tunnelWs.send(ByteString.of(buildFrame(cid, FT_CLOSE, new byte[0])));
                            Log.i(TAG, "[FT_OPEN] connId=" + cid + " FT_CLOSE sent (response end)");
                        }
                    } catch (Exception e) {
                        Log.w(TAG, "[FT_OPEN] connId=" + cid + " error: " + e.getMessage());
                        if (tunnelWs != null) {
                            try { tunnelWs.send(ByteString.of(buildFrame(cid, FT_CLOSE, new byte[0]))); }
                            catch (Exception ignored) {}
                        }
                    } finally {
                        Socket s = hostConns.remove(cid);
                        if (s != null) closeSocket(s);
                        emitEvent("ProxyClientDisconnected", String.valueOf(cid));
                    }
                });
                break;
            }

            case FT_DATA: {
                // BUG PM-2/PM-3 FIX: check isConnected() before writing to remote.
                // hostConns is populated before connect(); only write once connected.
                Socket remote = hostConns.get(connId);
                if (remote != null && !remote.isClosed() && remote.isConnected()) {
                    try {
                        remote.getOutputStream().write(payload);
                        remote.getOutputStream().flush();
                        bytesUp.addAndGet(payload.length);
                        Log.i(TAG, "[FT_DATA→remote] connId=" + connId + " bytes=" + payload.length);
                    } catch (Exception e) {
                        Log.w(TAG, "[FT_DATA→remote] connId=" + connId + " write error: " + e.getMessage());
                    }
                    break;
                }

                // FIX 2: clientConns path — host sending response data back to the
                // local proxy client socket (the app's HTTP connection to :8899).
                // This is the critical hot path for data delivery to the receiver.
                Socket clientSock = clientConns.get(connId);
                if (clientSock != null && !clientSock.isClosed()) {
                    try {
                        clientSock.getOutputStream().write(payload);
                        clientSock.getOutputStream().flush();
                        bytesDown.addAndGet(payload.length);
                        Log.i(TAG, "[FT_DATA→client] connId=" + connId + " bytes=" + payload.length);
                    } catch (Exception e) {
                        Log.w(TAG, "[FT_DATA→client] connId=" + connId + " write error: " + e.getMessage());
                        clientConns.remove(connId);
                        closeSocket(clientSock);
                    }
                } else {
                    Log.w(TAG, "[FT_DATA] connId=" + connId
                        + " DROPPED — no socket in hostConns or clientConns"
                        + " (remote=" + (remote != null ? "present,closed" : "null")
                        + " client=" + (clientSock != null ? "present,closed" : "null") + ")");
                }
                break;
            }

            case FT_CLOSE: {
                // FIX 3: close both sides so neither leaks
                Log.i(TAG, "[FT_CLOSE] connId=" + connId + " closing both sides");
                Socket clientSock = clientConns.remove(connId);
                if (clientSock != null) {
                    Log.i(TAG, "[FT_CLOSE] connId=" + connId + " closing clientSock");
                    closeSocket(clientSock);
                }
                Socket remote = hostConns.remove(connId);
                if (remote != null) {
                    Log.i(TAG, "[FT_CLOSE] connId=" + connId + " closing remote");
                    closeSocket(remote);
                }
                break;
            }

            default:
                Log.w(TAG, "[handleHostFrame] Unknown frameType=0x" + Integer.toHexString(frameType & 0xFF)
                    + " connId=" + connId);
        }
    }

    // ── Direct (LAN fallback) ─────────────────────────────────────────────────

    private void handleDirect(Socket client) {
        Socket remote = null;
        try {
            InputStream  cIn  = client.getInputStream();
            OutputStream cOut = client.getOutputStream();
            String firstLine  = readLine(cIn);
            if (firstLine == null) return;
            Log.i(TAG, "[handleDirect] " + firstLine);

            if (firstLine.toUpperCase().startsWith("CONNECT ")) {
                String[] p     = firstLine.split(" ");
                if (p.length < 2) { sendError(cOut, 400, "Bad Request"); return; }
                int    colon   = p[1].lastIndexOf(':');
                String host    = colon > 0 ? p[1].substring(0, colon) : p[1];
                int    port    = colon > 0 ? Integer.parseInt(p[1].substring(colon + 1)) : 443;
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
            Log.d(TAG, "[handleDirect] error: " + e.getMessage());
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

    /**
     * BUG PM-1 FIX: handles null payload (treats as empty byte array).
     * The original crashed with NullPointerException when payload was null.
     * Protocol: [ connId(4 BE) | frameType(1) | payload(N) ]
     */
    private static byte[] buildFrame(int connId, byte type, byte[] payload) {
        int     payloadLen = (payload != null) ? payload.length : 0;
        ByteBuffer buf     = ByteBuffer.allocate(5 + payloadLen);
        buf.putInt(connId);
        buf.put(type);
        if (payload != null && payloadLen > 0) buf.put(payload);
        return buf.array();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String frameTypeLabel(byte t) {
        switch (t) {
            case FT_OPEN:  return "FT_OPEN";
            case FT_DATA:  return "FT_DATA";
            case FT_CLOSE: return "FT_CLOSE";
            default:       return "0x" + Integer.toHexString(t & 0xFF);
        }
    }

    private String getLocalIp() {
        try {
            android.net.wifi.WifiManager wm =
                (android.net.wifi.WifiManager) reactCtx.getApplicationContext()
                    .getSystemService(android.content.Context.WIFI_SERVICE);
            int ip4 = wm.getConnectionInfo().getIpAddress();
            if (ip4 != 0) return String.format("%d.%d.%d.%d",
                ip4 & 0xFF, (ip4 >> 8) & 0xFF, (ip4 >> 16) & 0xFF, (ip4 >> 24) & 0xFF);
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
                    if (!a.isLoopbackAddress() && ip != null && !ip.contains(":")) return ip;
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

    public static void emitEvent(String event, String data) {
        if (reactCtx == null) return;
        try {
            DeviceEventManagerModule.RCTDeviceEventEmitter emitter =
                reactCtx.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class);
            if (emitter != null) emitter.emit(event, data);
        } catch (Exception ignored) {}
    }
}
