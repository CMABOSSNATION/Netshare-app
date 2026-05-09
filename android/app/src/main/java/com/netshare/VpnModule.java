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
 * NetShare VPN Native Module
 * Bridges React Native ↔ Android VpnService API
 *
 * This module:
 * 1. Requests VPN permission from the OS
 * 2. Starts/stops the NetShareVpnService
 * 3. Emits events back to JS (connected, disconnected, error)
 */
public class VpnModule extends ReactContextBaseJavaModule implements ActivityEventListener {

    private static final int VPN_REQUEST_CODE = 0x0F;
    private Promise vpnPermissionPromise;
    private static ReactApplicationContext reactContext;

    // Holds reference to running service so JS can send control messages
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

    /**
     * Prepare VPN — shows Android system dialog asking user to allow VPN.
     * Promise resolves true if granted, false if denied.
     */
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
                // Need to ask for permission
                vpnPermissionPromise = promise;
                activity.startActivityForResult(intent, VPN_REQUEST_CODE);
            } else {
                // Already granted
                promise.resolve(true);
            }
        } catch (Exception e) {
            promise.reject("VPN_PREPARE_ERROR", e.getMessage());
        }
    }

    /**
     * Start the VPN tunnel.
     * relayUrl: wss://your-render-backend.onrender.com/relay
     * sessionCode: 6-char code from host
     * role: "host" or "client"
     */
    @ReactMethod
    public void startVpn(String relayUrl, String sessionCode, String role, String hostId, String netType, Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                promise.reject("NO_ACTIVITY", "No current activity");
                return;
            }

            Intent serviceIntent = new Intent(activity, NetShareVpnService.class);
            serviceIntent.putExtra("RELAY_URL", relayUrl);
            serviceIntent.putExtra("SESSION_CODE", sessionCode);
            serviceIntent.putExtra("ROLE", role);
            serviceIntent.putExtra("HOST_ID", hostId != null ? hostId : "");
            serviceIntent.putExtra("NET_TYPE", netType != null ? netType : "WiFi");

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

    /**
     * Stop the VPN tunnel and disconnect from relay.
     */
    @ReactMethod
    public void stopVpn(Promise promise) {
        try {
            Activity activity = getCurrentActivity();
            if (activity == null) {
                promise.reject("NO_ACTIVITY", "No current activity");
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

    /**
     * Send a control message through the active WebSocket.
     * Called from JS (VpnService.js) to send PONG, HOST_LEAVE, CLIENT_LEAVE etc.
     */
    @ReactMethod
    public void sendControlMessage(String message) {
        if (activeService != null) {
            activeService.sendControlMessage(message);
        }
    }

    /**
     * Emit event to React Native JS layer.
     * Called from NetShareVpnService via static reference.
     */
    public static void emitEvent(String eventName, String data) {
        if (reactContext != null) {
            reactContext
                .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class)
                .emit(eventName, data);
        }
    }

    @Override
    public void onActivityResult(Activity activity, int requestCode, int resultCode, Intent data) {
        if (requestCode == VPN_REQUEST_CODE && vpnPermissionPromise != null) {
            if (resultCode == Activity.RESULT_OK) {
                vpnPermissionPromise.resolve(true);
            } else {
                vpnPermissionPromise.resolve(false);
            }
            vpnPermissionPromise = null;
        }
    }

    @Override
    public void onNewIntent(Intent intent) {}
}
