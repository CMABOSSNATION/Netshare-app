package com.netshare;

import android.content.Intent;
import android.net.wifi.WifiManager;
import android.content.Context;
import android.os.Build;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.Arguments;
import com.facebook.react.modules.core.DeviceEventManagerModule;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;

/**
 * ProxyModule — React Native bridge for ProxyService
 *
 * Exposes to JS:
 *   startProxy()  — starts the HTTP CONNECT proxy on port 8899
 *   stopProxy()   — stops the proxy
 *   getProxyInfo() — returns { ip, port } that clients should configure
 *   getStats()     — returns { bytesUp, bytesDown }
 *
 * JS uses these to:
 *   1. (HOST) Start the proxy, get IP:port, register with relay
 *   2. (CLIENT) Get IP:port from relay, show setup instructions to user
 *   3. Both sides poll getStats() to update the bandwidth display
 */
public class ProxyModule extends ReactContextBaseJavaModule {

    private static final String TAG = "ProxyModule";
    private final ReactApplicationContext ctx;

    public ProxyModule(ReactApplicationContext context) {
        super(context);
        this.ctx = context;
    }

    @NonNull @Override
    public String getName() { return "ProxyModule"; }

    // Required by RN NativeEventEmitter
    @ReactMethod public void addListener(String eventName) {}
    @ReactMethod public void removeListeners(int count) {}

    // ── Start proxy (HOST only) ──────────────────────────────────────────────

    @ReactMethod
    public void startProxy(Promise promise) {
        try {
            Intent intent = new Intent(ctx, ProxyService.class);
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                ctx.startForegroundService(intent);
            } else {
                ctx.startService(intent);
            }

            String ip = getWifiIp();
            WritableMap result = Arguments.createMap();
            result.putString("ip",   ip);
            result.putInt("port",    ProxyService.PROXY_PORT);
            result.putString("proxyUrl", ip + ":" + ProxyService.PROXY_PORT);
            promise.resolve(result);
        } catch (Exception e) {
            promise.reject("PROXY_START_ERROR", e.getMessage());
        }
    }

    // ── Stop proxy ───────────────────────────────────────────────────────────

    @ReactMethod
    public void stopProxy(Promise promise) {
        try {
            ctx.stopService(new Intent(ctx, ProxyService.class));
            promise.resolve(true);
        } catch (Exception e) {
            promise.reject("PROXY_STOP_ERROR", e.getMessage());
        }
    }

    // ── Get proxy connection info ─────────────────────────────────────────────

    @ReactMethod
    public void getProxyInfo(Promise promise) {
        try {
            String ip = getWifiIp();
            WritableMap result = Arguments.createMap();
            result.putString("ip",       ip);
            result.putInt("port",        ProxyService.PROXY_PORT);
            result.putString("proxyUrl", ip + ":" + ProxyService.PROXY_PORT);
            promise.resolve(result);
        } catch (Exception e) {
            promise.reject("PROXY_INFO_ERROR", e.getMessage());
        }
    }

    // ── Bandwidth stats ───────────────────────────────────────────────────────

    @ReactMethod
    public void getStats(Promise promise) {
        WritableMap stats = Arguments.createMap();
        stats.putDouble("bytesUp",   (double) ProxyService.bytesUp.get());
        stats.putDouble("bytesDown", (double) ProxyService.bytesDown.get());
        promise.resolve(stats);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * getWifiIp — returns the device's current Wi-Fi IP address.
     * Clients must be on the same Wi-Fi network as the host.
     */
    private String getWifiIp() {
        try {
            for (NetworkInterface iface :
                    Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (iface.isLoopback() || !iface.isUp()) continue;
                String name = iface.getName().toLowerCase();
                // Prefer wlan interfaces (Wi-Fi)
                if (!name.startsWith("wlan") && !name.startsWith("eth")) continue;
                for (InetAddress addr :
                        Collections.list(iface.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && addr.getAddress().length == 4) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {}

        // Fallback: use WifiManager (may return 0.0.0.0 on some Android 12+ devices)
        try {
            WifiManager wm = (WifiManager)
                ctx.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            if (wm != null) {
                int ip = wm.getConnectionInfo().getIpAddress();
                return String.format("%d.%d.%d.%d",
                    ip & 0xff, (ip >> 8) & 0xff, (ip >> 16) & 0xff, (ip >> 24) & 0xff);
            }
        } catch (Exception ignored) {}

        return "0.0.0.0";
    }
}
