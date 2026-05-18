package com.netshare;

import android.app.Activity;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.IBinder;
import android.provider.Settings;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.WritableNativeMap;
import com.facebook.react.modules.core.DeviceEventManagerModule;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ProxyModule.java — NetShare Native Module
 *
 * Registered as "ProxyModule" in JS (ProxyService.js calls NativeModules.ProxyModule).
 *
 * Methods exposed to JS:
 *   startProxy()  → starts local HTTP CONNECT proxy on :8899, returns { ip, port }
 *   stopProxy()   → stops proxy server
 *   getStats()    → returns { up: bytes, down: bytes } since last call
 *   emitEvent()   → static helper for service → JS events
 *
 * How it works:
 *   The host phone runs a real HTTP CONNECT proxy server on port 8899.
 *   When a client sets their Android WiFi proxy to host-ip:8899, ALL their
 *   app traffic (TikTok, WhatsApp, Facebook, Spotify etc.) flows through
 *   this proxy automatically. Each CONNECT request opens a real TCP socket
 *   to the target server and pipes bytes both ways. TLS is end-to-end.
 */
public class ProxyModule extends ReactContextBaseJavaModule {

    private static final String TAG         = "NetShareProxy";
    private static final int    PROXY_PORT  = 8899;
    private static final int    PIPE_BUF    = 32 * 1024;
    private static final String CHANNEL_ID  = "netshare_proxy";
    private static final int    NOTIF_ID    = 3;

    private static ReactApplicationContext reactCtx;

    private ServerSocket    serverSocket;
    private ExecutorService executor;
    private AtomicBoolean   running     = new AtomicBoolean(false);
    private AtomicLong      bytesUp     = new AtomicLong(0);
    private AtomicLong      bytesDown   = new AtomicLong(0);
    private AtomicLong      lastUp      = new AtomicLong(0);
    private AtomicLong      lastDown    = new AtomicLong(0);

    public ProxyModule(ReactApplicationContext context) {
        super(context);
        reactCtx = context;
    }

    @NonNull
    @Override
    public String getName() { return "ProxyModule"; }

    // Required by RN NativeEventEmitter
    @ReactMethod public void addListener(String eventName) {}
    @ReactMethod public void removeListeners(int count) {}

    // ── startProxy ────────────────────────────────────────────────────────────
    /**
     * Starts the local HTTP CONNECT proxy server on 0.0.0.0:8899.
     * Returns { ip: "192.168.x.x", port: 8899 } to JS.
     * The IP is the phone's current WiFi IP so clients can connect to it.
     */
    @ReactMethod
    public void startProxy(Promise promise) {
        if (running.get()) {
            // Already running — just return current info
            try {
                WritableMap m = new WritableNativeMap();
                m.putString("ip",   getWifiIp());
                m.putInt("port",    PROXY_PORT);
                promise.resolve(m);
            } catch (Exception e) { promise.reject("PROXY_ERROR", e.getMessage()); }
            return;
        }

        try {
            String wifiIp = getWifiIp();
            if (wifiIp == null || wifiIp.equals("0.0.0.0")) {
                promise.reject("NO_WIFI", "Not connected to WiFi. Connect to WiFi first.");
                return;
            }

            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("0.0.0.0", PROXY_PORT));
            running.set(true);
            bytesUp.set(0); bytesDown.set(0);
            lastUp.set(0);  lastDown.set(0);

            executor = Executors.newCachedThreadPool();
            executor.execute(this::acceptLoop);

            startForeground();

            Log.i(TAG, "Proxy started on " + wifiIp + ":" + PROXY_PORT);

            WritableMap result = new WritableNativeMap();
            result.putString("ip",   wifiIp);
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
        stopInternal();
        promise.resolve(true);
    }

    // ── getStats ──────────────────────────────────────────────────────────────
    /**
     * Returns bytes transferred since the last call to getStats().
     * JS calls this every second to update the bandwidth display.
     */
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

    // ── Proxy server accept loop ──────────────────────────────────────────────

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(120_000); // 2 min idle timeout
                executor.execute(() -> handleConnect(client));
            } catch (Exception e) {
                if (running.get()) Log.w(TAG, "accept: " + e.getMessage());
            }
        }
    }

    /**
     * Handles one HTTP CONNECT request:
     *
     *   Reads:  "CONNECT api.whatsapp.net:443 HTTP/1.1\r\n..."
     *   Opens:  real TCP socket to api.whatsapp.net:443
     *   Writes: "HTTP/1.1 200 Connection established\r\n\r\n"
     *   Pipes:  raw bytes both directions until either side closes
     *
     * Everything after the 200 is raw bytes. TLS is end-to-end between
     * the client app and the remote server — we never see plaintext.
     */
    private void handleConnect(Socket client) {
        Socket remote = null;
        try {
            InputStream  cIn  = client.getInputStream();
            OutputStream cOut = client.getOutputStream();

            // Read first line: "CONNECT host:port HTTP/1.1"
            String firstLine = readLine(cIn);
            if (firstLine == null || !firstLine.toUpperCase().startsWith("CONNECT ")) {
                // Not a CONNECT — try plain HTTP proxy
                handlePlainHttp(client, cIn, cOut, firstLine);
                return;
            }

            // Parse host:port
            String[] parts    = firstLine.split(" ");
            String   target   = parts.length >= 2 ? parts[1] : null;
            if (target == null) { sendError(cOut, 400, "Bad Request"); return; }

            int    colon = target.lastIndexOf(':');
            String host  = colon > 0 ? target.substring(0, colon) : target;
            int    port  = colon > 0 ? Integer.parseInt(target.substring(colon + 1)) : 443;

            // Drain remaining headers
            drainHeaders(cIn);

            Log.d(TAG, "CONNECT → " + host + ":" + port);

            // Open real TCP socket to target
            remote = new Socket();
            remote.setTcpNoDelay(true);
            remote.setSoTimeout(120_000);
            remote.connect(new InetSocketAddress(host, port), 15_000);

            // Tell client the tunnel is open
            cOut.write("HTTP/1.1 200 Connection established\r\n\r\n"
                .getBytes(StandardCharsets.US_ASCII));
            cOut.flush();

            // Pipe both directions simultaneously
            final Socket  fr  = remote;
            final Socket  fc  = client;
            Thread t1 = new Thread(() -> pipe(fc, fr, true));   // client → remote
            Thread t2 = new Thread(() -> pipe(fr, fc, false));  // remote → client
            t1.setDaemon(true); t2.setDaemon(true);
            t1.start(); t2.start();
            // Wait for both directions to finish
            t1.join(); t2.join();

        } catch (Exception e) {
            Log.d(TAG, "handleConnect: " + e.getMessage());
        } finally {
            close(remote);
            close(client);
        }
    }

    /**
     * Handle plain HTTP (non-CONNECT) requests.
     * Parses "GET http://example.com/path HTTP/1.1" and forwards it.
     */
    private void handlePlainHttp(Socket client, InputStream cIn, OutputStream cOut, String firstLine) {
        Socket remote = null;
        try {
            if (firstLine == null) return;
            // e.g. "GET http://example.com/path HTTP/1.1"
            String[] parts = firstLine.split(" ");
            if (parts.length < 2) { sendError(cOut, 400, "Bad Request"); return; }
            URL url = new URL(parts[1]);
            int port = url.getPort() > 0 ? url.getPort() : 80;

            remote = new Socket();
            remote.setTcpNoDelay(true);
            remote.connect(new InetSocketAddress(url.getHost(), port), 15_000);

            // Forward original request line + remaining headers + body
            OutputStream rOut = remote.getOutputStream();
            String path = url.getFile().isEmpty() ? "/" : url.getFile();
            rOut.write((parts[0] + " " + path + " " + parts[2] + "\r\n")
                .getBytes(StandardCharsets.US_ASCII));
            // Forward remaining headers
            String line;
            while ((line = readLine(cIn)) != null && !line.isEmpty()) {
                rOut.write((line + "\r\n").getBytes(StandardCharsets.US_ASCII));
            }
            rOut.write("\r\n".getBytes(StandardCharsets.US_ASCII));
            rOut.flush();

            // Pipe remote response back to client
            final Socket fr = remote;
            final Socket fc = client;
            Thread t1 = new Thread(() -> pipe(fc, fr, true));
            Thread t2 = new Thread(() -> pipe(fr, fc, false));
            t1.setDaemon(true); t2.setDaemon(true);
            t1.start(); t2.start();
            t1.join(); t2.join();

        } catch (Exception e) {
            Log.d(TAG, "handlePlainHttp: " + e.getMessage());
        } finally {
            close(remote);
            close(client);
        }
    }

    /**
     * Pipes data from src to dst until EOF or error.
     * Counts bytes for stats.
     */
    private void pipe(Socket src, Socket dst, boolean isUpload) {
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
        // Close both sides when one direction ends
        close(src); close(dst);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String getWifiIp() {
        try {
            WifiManager wm = (WifiManager) reactCtx
                .getApplicationContext()
                .getSystemService(android.content.Context.WIFI_SERVICE);
            int ip4 = wm.getConnectionInfo().getIpAddress();
            return String.format(
                "%d.%d.%d.%d",
                (ip4 & 0xFF), (ip4 >> 8 & 0xFF),
                (ip4 >> 16 & 0xFF), (ip4 >> 24 & 0xFF)
            );
        } catch (Exception e) {
            // Fallback: enumerate network interfaces
            try {
                java.util.Enumeration<java.net.NetworkInterface> ifaces =
                    java.net.NetworkInterface.getNetworkInterfaces();
                while (ifaces.hasMoreElements()) {
                    java.net.NetworkInterface iface = ifaces.nextElement();
                    java.util.Enumeration<InetAddress> addrs = iface.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress addr = addrs.nextElement();
                        String ip = addr.getHostAddress();
                        if (!addr.isLoopbackAddress() && ip != null
                                && ip.indexOf(':') < 0 // IPv4 only
                                && ip.startsWith("192.168.")) {
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
            if (b == '\r') { in.read(); break; } // consume \n
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

    private void stopInternal() {
        running.set(false);
        try { if (serverSocket != null) serverSocket.close(); } catch (Exception ignored) {}
        if (executor != null) executor.shutdownNow();
        serverSocket = null;
        executor     = null;
        stopForeground();
        Log.i(TAG, "Proxy stopped");
    }

    // ── Foreground notification ───────────────────────────────────────────────

    private void startForeground() {
        try {
            Activity act = getCurrentActivity();
            if (act == null) return;

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                NotificationChannel ch = new NotificationChannel(
                    CHANNEL_ID, "NetShare Proxy", NotificationManager.IMPORTANCE_LOW);
                act.getSystemService(NotificationManager.class).createNotificationChannel(ch);
            }

            // No ongoing notification needed for a module (not a Service)
            // Just log — the foreground is handled by the React Native Activity
            Log.i(TAG, "Proxy running in background");
        } catch (Exception e) { Log.w(TAG, "startForeground: " + e.getMessage()); }
    }

    private void stopForeground() {
        Log.i(TAG, "Proxy stopped");
    }

    // ── Static event emitter ─────────────────────────────────────────────────

    public static void emitEvent(String eventName, String data) {
        if (reactCtx == null) return;
        try {
            DeviceEventManagerModule.RCTDeviceEventEmitter emitter =
                reactCtx.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class);
            if (emitter != null) emitter.emit(eventName, data);
        } catch (Exception ignored) {}
    }
}
