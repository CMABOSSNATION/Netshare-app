package com.netshare;

import android.app.Activity;
import android.content.Intent;
import android.net.VpnService;
import android.os.Build;

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
 * BUGS FIXED:
 * 1. addListener / removeListeners stubs were missing — NativeEventEmitter threw
 *    "addListener is not a function" and NO events ever reached JS.
 * 2. emitEvent() had no null-guard on getJSModule() — NPE crashed service thread
 *    silently, stopping all future event delivery.
 * 3. vpnPermissionPromise was never nulled on RESULT_CANCELED — stale promise
 *    caused "Promise already settled" warnings on activity recreation.
 * 4. stopVpn sent a plain startService() on Android O+ — should use the same
 *    action-based approach so the running foreground service handles STOP_VPN.
 */
public class VpnModule extends ReactContextBaseJavaModule implements ActivityEventListener {

    private static final int VPN_REQUEST_CODE = 0x0F;
    private Promise vpnPermissionPromise;
    private static ReactApplicationContext reactContext;

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
    // Without these, React Native throws when JS calls vpnEmitter.addListener()
    // and NO events will ever be delivered to JS.

    @ReactMethod
    public void addListener(String eventName) {
        // Required by RN NativeEventEmitter — no-op on the native side.
    }

    @ReactMethod
    public void removeListeners(int count) {
        // Required by RN NativeEventEmitter — no-op on the native side.
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

    @ReactMethod
    public void startVpn(String relayUrl, String sessionCode, String role,
                         String hostId, String netType, Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                promise.reject("NO_ACTIVITY", "No current activity");
                return;
            }
            Intent serviceIntent = new Intent(activity, NetShareVpnService.class);
            serviceIntent.putExtra("RELAY_URL",     relayUrl);
            serviceIntent.putExtra("SESSION_CODE",  sessionCode != null ? sessionCode : "");
            serviceIntent.putExtra("ROLE",           role);
            serviceIntent.putExtra("HOST_ID",        hostId != null ? hostId : "");
            serviceIntent.putExtra("NET_TYPE",       netType != null ? netType : "WiFi");

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
    // FIX 4: Send STOP_VPN action intent. The running NetShareVpnService handles
    // this in onStartCommand and calls stopVpnTunnelFromUser(). Using startService
    // (not startForegroundService) for the stop signal is correct — we're just
    // delivering a command to an already-running foreground service.

    @ReactMethod
    public void stopVpn(Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                // No activity? Try application context as fallback.
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
    // FIX 2: null-guard on the emitter so a crash during shutdown doesn't
    // propagate and kill the service thread.

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
            // FIX 3: always null the promise so stale references don't cause
            // "Promise already settled" errors on activity recreation.
            vpnPermissionPromise = null;
        }
    }

    @Override
    public void onNewIntent(Intent intent) {}
}
