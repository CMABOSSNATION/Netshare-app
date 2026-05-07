# NetShare — Native VPN App (React Native)

Real WiFi sharing via Android VpnService + WebSocket relay.

---

## How It Works

```
HOST (has WiFi)          RELAY (Render)         CLIENT (no internet)
      │                       │                        │
      │── HOST_REGISTER ──────►│                        │
      │◄─ SESSION_CREATED ─────│                        │
      │   (code: ABC123)       │◄── CLIENT_JOIN(ABC123) ─│
      │◄─ CLIENT_CONNECTED ────│─── JOIN_SUCCESS ───────►│
      │                        │                        │
      │◄── IP packets ─────────│◄─── IP packets ─────────│
      │─── responses ─────────►│──── responses ─────────►│
```

1. HOST opens app → taps "START SHARING" → VPN dialog appears → tap OK
2. Relay server creates a 6-char session code (e.g. `ABC123`)
3. HOST shares the code with CLIENT (verbally, QR, message)
4. CLIENT enters code → taps "JOIN NETWORK" → VPN dialog → tap OK
5. CLIENT's ALL internet traffic now routes through HOST's WiFi

---

## Project Structure

```
netshare-rn/
├── App.jsx                          # Root component
├── src/
│   ├── screens/HomeScreen.jsx       # Main UI
│   ├── services/VpnService.js       # JS ↔ Native bridge
│   └── store/index.js               # Zustand state
├── android/app/src/main/
│   ├── AndroidManifest.xml          # VPN permissions
│   └── java/com/netshare/
│       ├── VpnModule.java           # RN Native Module
│       ├── VpnPackage.java          # Module registration
│       └── NetShareVpnService.java  # Actual VPN service
└── relay.js                         # Updated Render backend
```

---

## Setup Instructions

### 1. Initialize React Native project

```bash
npx react-native init NetShare --version 0.73.4
cd NetShare
```

### 2. Copy files into the project

Copy all files from this folder into your new React Native project:
- Replace `App.jsx`
- Copy `src/` folder
- Copy `android/app/src/main/java/com/netshare/` files
- Replace `android/app/src/main/AndroidManifest.xml`

### 3. Register VpnPackage in MainApplication.java

Open `android/app/src/main/java/com/netshare/MainApplication.java` and add:

```java
import com.netshare.VpnPackage; // add this import

// Inside getPackages():
packages.add(new VpnPackage());
```

### 4. Add java-websocket dependency

In `android/app/build.gradle`, add to dependencies:

```groovy
implementation 'org.java-websocket:Java-WebSocket:1.5.4'
```

### 5. Update relay URL

In `src/services/VpnService.js`, set your actual Render backend URL:

```js
export const RELAY_URL = 'wss://YOUR-APP.onrender.com/relay';
```

### 6. Update backend

Replace your Render backend's `src/relay.js` with the `relay.js` file in this folder.
Push to GitHub — Render will auto-deploy.

### 7. Install dependencies & build

```bash
npm install
cd android && ./gradlew assembleDebug
```

APK will be at: `android/app/build/outputs/apk/debug/app-debug.apk`

---

## Permissions Required

| Permission | Why |
|---|---|
| `BIND_VPN_SERVICE` | Core VPN functionality |
| `FOREGROUND_SERVICE` | Keep VPN alive in background |
| `INTERNET` | WebSocket to relay |
| `ACCESS_WIFI_STATE` | Show WiFi info |

---

## Known Limitations

| Limitation | Detail |
|---|---|
| Android only | iOS does not allow VPN apps without Apple entitlement ($299/yr enterprise) |
| VPN dialog | Android shows a system permission popup — user must accept |
| Speed | Depends on relay server bandwidth (Render free tier = limited) |
| Render sleep | Free tier sleeps after 15min inactivity — upgrade to paid for production |
