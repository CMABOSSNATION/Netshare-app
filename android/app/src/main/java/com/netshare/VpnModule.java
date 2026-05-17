package com.netshare;

import android.app.Activity;
import android.content.Intent;
import android.net.VpnService;
import android.os.Build;
import android.provider.Settings;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.ActivityEventListener;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.modules.core.DeviceEventManagerModule;

/**
 * VpnModule.java — NetShare Native Module
 *
 * BUGS FIXED (TikTok / WhatsApp):
 *
 * FIX-TW-A: startVpn() now accepts a 6th parameter: deviceId (String).
 *   Previously the method signature had 5 args and always forwarded an empty
 *   deviceId to NetShareVpnService. The relay's CLIENT_JOIN handler requires
 *   a non-empty deviceId to enforce the one-device lock. Without it the relay
 *   returned "Device ID missing" and TikTok/WhatsApp clients never joined.
 *
 * FIX-TW-B: On first call, Java reads ANDROID_ID and writes it to the app's
 *   SharedPreferences under the key "netshare_device_id". React Native's
 *   AsyncStorage on Android uses the same SharedPreferences store, so JS
 *   (VpnService.js getDeviceId()) reads the exact same value. This ensures
 *   JS validate-code and Java CLIENT_JOIN always send the same deviceId,
 *   preventing claimedBy mismatches on reconnect.
 *
 * All prior bug fixes (1-4) are retained unchanged.
 */
public class VpnModule extends ReactContextBaseJavaModule implements ActivityEventListener {

    private static final int VPN_REQUEST_CODE = 0x0F;
    private Promise vpnPermissionPromise;
    private static ReactApplicationContext reactContext;

    // Shared prefs file name — AsyncStorage on Android uses this same file
    private static final String PREFS_NAME = "RCTAsyncLocalStorage_V1";
    private static final String DEVICE_ID_KEY = "netshare_device_id";

    // Reference to the running service — used for sendControlMessage()
    public static NetShareVpnService activeService = null;

    public VpnModule(ReactApplicationContext context) {
        super(context);
        reactContext = context;
        context.addActivityEventListener(this);
    }

    @NonNull
    @Override
    public String getName() {
        return "VpnModule";
    }

    // ── FIX 1: Required stubs for NativeEventEmitter ──────────────────────

    @ReactMethod
    public void addListener(String eventName) {
        // Required by RN NativeEventEmitter — no-op on the native side.
    }

    @ReactMethod
    public void removeListeners(int count) {
        // Required by RN NativeEventEmitter — no-op on the native side.
    }

    // ── FIX-TW-B: Get stable ANDROID_ID and write to shared prefs ────────
    // Called once per process. Stores the ID so JS (AsyncStorage) can read it.
    private String getAndStoreDeviceId() {
        try {
            // Try to read existing stored value first
            android.content.SharedPreferences prefs =
                reactContext.getSharedPreferences(PREFS_NAME, android.content.Context.MODE_PRIVATE);
            // AsyncStorage stores values JSON-encoded (quoted strings)
            String stored = prefs.getString(DEVICE_ID_KEY, null);
            if (stored != null) {
                // Strip JSON quotes if present: "\"abc\"" → "abc"
                if (stored.startsWith("\"") && stored.endsWith("\"") && stored.length() > 2) {
                    return stored.substring(1, stored.length() - 1);
                }
                return stored;
            }

            // Not stored yet — read ANDROID_ID and save it
            String androidId = Settings.Secure.getString(
                reactContext.getContentResolver(),
                Settings.Secure.ANDROID_ID
            );
            if (androidId == null || androidId.isEmpty() || androidId.equals("9774d56d682e549c")) {
                // Emulator or factory-reset device returns the sentinel value — generate a stable ID
                androidId = "android-" + System.currentTimeMillis();
            }

            // Store as JSON-encoded string (matching AsyncStorage format)
            prefs.edit().putString(DEVICE_ID_KEY, "\"" + androidId + "\"").apply();
            return androidId;
        } catch (Exception e) {
            return "unknown-" + System.currentTimeMillis();
        }
    }

    // ── VPN permission ────────────────────────────────────────────────────

    @ReactMethod
    public void prepare(Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                promise.reject("NO_ACTIVITY", "No current activity");
                return;
            }
            Intent intent = VpnService.prepare(activity);
            if (intent != null) {
                vpnPermissionPromise = promise;
                activity.startActivityForResult(intent, VPN_REQUEST_CODE);
            } else {
                promise.resolve(true);
            }
        } catch (Exception e) {
            promise.reject("VPN_PREPARE_ERROR", e.getMessage());
        }
    }

    // ── Start VPN service ─────────────────────────────────────────────────
    // FIX-TW-A: added deviceId (6th parameter).
    // JS passes the ANDROID_ID via VpnService.js startAsClient(); if empty,
    // Java reads it directly so CLIENT_JOIN always has a valid deviceId.

    @ReactMethod
    public void startVpn(String relayUrl, String sessionCode, String role,
                         String hostId, String netType, String deviceId,
                         String appPackagesJson, String appPortTimeoutsJson,
                         Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                promise.reject("NO_ACTIVITY", "No current activity");
                return;
            }

            // FIX-TW-B: ensure device ID is stored for JS to read, and use it
            // if JS didn't supply one (empty string = old JS build calling us).
            String resolvedDeviceId = (deviceId != null && !deviceId.isEmpty())
                ? deviceId
                : getAndStoreDeviceId();

            Intent serviceIntent = new Intent(activity, NetShareVpnService.class);
            serviceIntent.putExtra("RELAY_URL",         relayUrl);
            serviceIntent.putExtra("SESSION_CODE",      sessionCode != null ? sessionCode : "");
            serviceIntent.putExtra("ROLE",              role);
            serviceIntent.putExtra("HOST_ID",           hostId  != null ? hostId  : "");
            serviceIntent.putExtra("NET_TYPE",          netType != null ? netType : "WiFi");
            serviceIntent.putExtra("DEVICE_ID",         resolvedDeviceId);  // FIX-TW-A
            // WhatsApp FIX: forward per-app package list and port timeouts
            // from JS service files (WhatsApp.js, TikTok.js, etc.)
            if (appPackagesJson != null && !appPackagesJson.isEmpty())
                serviceIntent.putExtra("APP_PACKAGES",      appPackagesJson);
            if (appPortTimeoutsJson != null && !appPortTimeoutsJson.isEmpty())
                serviceIntent.putExtra("APP_PORT_TIMEOUTS", appPortTimeoutsJson);

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                activity.startForegroundService(serviceIntent);
            } else {
                activity.startService(serviceIntent);
            }
            promise.resolve(true);
        } catch (Exception e) {
            promise.reject("VPN_START_ERROR", e.getMessage());
        }
    }

    // ── Stop VPN service ──────────────────────────────────────────────────
    // FIX 4: Send STOP_VPN action intent.

    @ReactMethod
    public void stopVpn(Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                if (reactContext != null) {
                    Intent serviceIntent = new Intent(reactContext, NetShareVpnService.class);
                    serviceIntent.setAction("STOP_VPN");
                    reactContext.startService(serviceIntent);
                    promise.resolve(true);
                } else {
                    promise.reject("NO_ACTIVITY", "No current activity or context");
                }
                return;
            }
            Intent serviceIntent = new Intent(activity, NetShareVpnService.class);
            serviceIntent.setAction("STOP_VPN");
            activity.startService(serviceIntent);
            promise.resolve(true);
        } catch (Exception e) {
            promise.reject("VPN_STOP_ERROR", e.getMessage());
        }
    }

    // ── Send control message through active WS ────────────────────────────

    @ReactMethod
    public void sendControlMessage(String message) {
        if (activeService != null) {
            activeService.sendControlMessage(message);
        }
    }

    // ── Emit event to JS ──────────────────────────────────────────────────
    // FIX 2: null-guard on the emitter.

    public static void emitEvent(String eventName, String data) {
        if (reactContext == null) return;
        try {
            DeviceEventManagerModule.RCTDeviceEventEmitter emitter =
                reactContext.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class);
            if (emitter != null) {
                emitter.emit(eventName, data);
            }
        } catch (Exception e) {
            // Bridge may be torn down during app exit — ignore
        }
    }

    // ── Activity result (VPN permission dialog) ───────────────────────────

    @Override
    public void onActivityResult(Activity activity, int requestCode,
                                  int resultCode, Intent data) {
        if (requestCode == VPN_REQUEST_CODE && vpnPermissionPromise != null) {
            vpnPermissionPromise.resolve(resultCode == Activity.RESULT_OK);
            // FIX 3: always null the promise
            vpnPermissionPromise = null;
        }
    }

    @Override
    public void onNewIntent(Intent intent) {}
}
