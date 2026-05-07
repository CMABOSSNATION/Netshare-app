import { useState, useEffect, useRef, useCallback } from "react";

/* ============================================================
   NETSHARE — Real Internet Sharing Frontend
   Stack: React + Tailwind (CDN) + WebSocket-ready
   
   ARCHITECTURE:
   - Person B (HOST): Connects to relay server via WebSocket,
     registers a session, shares their internet via a proxy tunnel
   - Person C (CLIENT): Enters session code or scans QR,
     routes traffic through Person B's relay
   
   TO MAKE THIS FULLY FUNCTIONAL:
   1. Deploy relay server (Node.js + ws + http-proxy)  ← next step
   2. Replace WS_URL with your server URL
   3. On Android: wrap in Capacitor + VpnService
   4. On iOS: wrap in Capacitor + NEPacketTunnelProvider
   ============================================================ */

const WS_URL = "wss://netshare-backend.onrender.com/relay";// Replace after backend deploy

// ── Design tokens ────────────────────────────────────────────
const T = {
  bg:      "#070b14",
  surface: "#0d1424",
  card:    "#111827",
  border:  "#1f2d4a",
  accent:  "#0ef",
  violet:  "#818cf8",
  green:   "#10ffa0",
  red:     "#ff4466",
  amber:   "#fbbf24",
  text:    "#f0f4ff",
  sub:     "#6b7fa3",
};

// ── Global styles injected once ───────────────────────────────
const GLOBAL_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap');

  *, *::before, *::after { box-sizing: border-box; }
  html, body, #root { height: 100%; margin: 0; padding: 0; }
  body {
    background: ${T.bg};
    color: ${T.text};
    font-family: 'Rajdhani', sans-serif;
    -webkit-font-smoothing: antialiased;
    overflow-x: hidden;
  }
  ::-webkit-scrollbar { width: 3px; }
  ::-webkit-scrollbar-thumb { background: ${T.border}; border-radius: 2px; }

  /* Animations */
  @keyframes fadeUp   { from { opacity:0; transform:translateY(18px) } to { opacity:1; transform:none } }
  @keyframes fadeIn   { from { opacity:0 } to { opacity:1 } }
  @keyframes pulse    { 0%,100% { opacity:1 } 50% { opacity:.35 } }
  @keyframes ripple   {
    0%   { transform:translate(-50%,-50%) scale(0); opacity:.6 }
    100% { transform:translate(-50%,-50%) scale(3.5); opacity:0 }
  }
  @keyframes spinCW   { to { transform:rotate(360deg) } }
  @keyframes scanline {
    0%   { top: 0% }
    100% { top: 100% }
  }
  @keyframes dash {
    to { stroke-dashoffset: 0 }
  }
  @keyframes glow {
    0%,100% { box-shadow: 0 0 8px ${T.accent}44 }
    50%      { box-shadow: 0 0 22px ${T.accent}99 }
  }
  @keyframes barSlide {
    from { width: 0 }
  }

  .fadeUp    { animation: fadeUp .45s cubic-bezier(.22,1,.36,1) both }
  .fadeIn    { animation: fadeIn .3s ease both }
  .pulse     { animation: pulse 2s ease infinite }
  .spinCW    { animation: spinCW 1.1s linear infinite }
  .glowBox   { animation: glow 2.5s ease infinite }

  .delay-1 { animation-delay:.07s }
  .delay-2 { animation-delay:.14s }
  .delay-3 { animation-delay:.21s }
  .delay-4 { animation-delay:.28s }
  .delay-5 { animation-delay:.35s }

  input:focus { outline: none }
  button { cursor: pointer }

  /* Glass card */
  .glass {
    background: rgba(13,20,36,.85);
    backdrop-filter: blur(14px);
    border: 1px solid ${T.border};
  }

  /* Noise overlay */
  .noise::after {
    content:'';
    position:absolute; inset:0;
    background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='.04'/%3E%3C/svg%3E");
    pointer-events:none; border-radius:inherit;
  }
`;

// ── Mini QR placeholder (real QR needs qrcode.js — shown as SVG grid) ─
function FakeQR({ value, size = 120 }) {
  // Deterministic hash → grid pattern
  const cells = 21;
  const grid = [];
  let h = 0;
  for (let i = 0; i < value.length; i++) h = (Math.imul(31, h) + value.charCodeAt(i)) | 0;
  for (let r = 0; r < cells; r++) {
    for (let c = 0; c < cells; c++) {
      const v = ((h ^ (r * 7 + c * 13) ^ (r ^ c)) & 1);
      // Always fill finder pattern corners
      const corner = (r < 7 && c < 7) || (r < 7 && c >= cells-7) || (r >= cells-7 && c < 7);
      grid.push(corner ? 1 : v);
    }
  }
  const cell = size / cells;
  return (
    <svg width={size} height={size} style={{ display:"block", borderRadius:6 }}>
      <rect width={size} height={size} fill="#fff" rx="6"/>
      {grid.map((on, i) => on ? (
        <rect key={i}
          x={(i % cells) * cell + .5}
          y={Math.floor(i / cells) * cell + .5}
          width={cell - 1} height={cell - 1}
          fill="#070b14" rx=".5"/>
      ) : null)}
    </svg>
  );
}

// ── Speedometer Arc ───────────────────────────────────────────
function SpeedArc({ value, max = 10, color = T.accent, label = "MB/s" }) {
  const r = 44, cx = 60, cy = 60;
  const startAngle = 215, sweepAngle = 290;
  const pct = Math.min(value / max, 1);
  const toRad = d => d * Math.PI / 180;
  const arcPt = (a) => ({
    x: cx + r * Math.cos(toRad(a)),
    y: cy + r * Math.sin(toRad(a))
  });
  const endA = startAngle + sweepAngle;
  const needleA = startAngle + pct * sweepAngle;
  const p0 = arcPt(startAngle), p1 = arcPt(endA), pN = arcPt(needleA);
  const largeArc = sweepAngle > 180 ? 1 : 0;
  const trackD = `M ${p0.x} ${p0.y} A ${r} ${r} 0 ${largeArc} 1 ${p1.x} ${p1.y}`;
  const fillLarge = pct * sweepAngle > 180 ? 1 : 0;
  const fillD = pct > 0
    ? `M ${p0.x} ${p0.y} A ${r} ${r} 0 ${fillLarge} 1 ${pN.x} ${pN.y}`
    : "";
  return (
    <svg width={120} height={75} viewBox="0 0 120 80" style={{overflow:"visible"}}>
      <path d={trackD} fill="none" stroke={T.border} strokeWidth="6" strokeLinecap="round"/>
      {fillD && <path d={fillD} fill="none" stroke={color} strokeWidth="6" strokeLinecap="round"
        style={{filter:`drop-shadow(0 0 6px ${color}99)`}}/>}
      <line x1={cx} y1={cy} x2={pN.x} y2={pN.y}
        stroke={color} strokeWidth="2" strokeLinecap="round"/>
      <circle cx={cx} cy={cy} r="4" fill={color}/>
      <text x={cx} y={cy+22} textAnchor="middle" fontSize="13" fontWeight="700"
        fontFamily="JetBrains Mono" fill={T.text}>{value.toFixed(1)}</text>
      <text x={cx} y={cy+34} textAnchor="middle" fontSize="8"
        fontFamily="Rajdhani" fill={T.sub}>{label}</text>
    </svg>
  );
}

// ── Stat Pill ─────────────────────────────────────────────────
function StatPill({ label, value, color = T.accent, icon }) {
  return (
    <div style={{
      background: T.card, border:`1px solid ${T.border}`, borderRadius:12,
      padding:"10px 14px", display:"flex", flexDirection:"column", gap:3
    }}>
      <div style={{ fontSize:10, color:T.sub, letterSpacing:2, fontFamily:"JetBrains Mono", textTransform:"uppercase" }}>
        {icon && <span style={{marginRight:5}}>{icon}</span>}{label}
      </div>
      <div style={{ fontSize:17, fontFamily:"JetBrains Mono", fontWeight:700, color }}>
        {value}
      </div>
    </div>
  );
}

// ── Log Feed ──────────────────────────────────────────────────
function LogFeed({ entries }) {
  const ref = useRef();
  useEffect(() => { if (ref.current) ref.current.scrollTop = ref.current.scrollHeight; }, [entries]);
  return (
    <div ref={ref} style={{
      maxHeight:110, overflowY:"auto", fontFamily:"JetBrains Mono",
      fontSize:10, lineHeight:1.7, color:T.sub,
      background:T.bg, borderRadius:8, padding:"8px 10px",
      border:`1px solid ${T.border}`
    }}>
      {entries.length === 0
        ? <span style={{color:T.sub}}>// Awaiting events...</span>
        : entries.map((e,i) => (
          <div key={i} style={{color: i===entries.length-1 ? T.green : T.sub}}>
            <span style={{color:T.violet}}>{e.ts}</span> {e.msg}
          </div>
        ))
      }
    </div>
  );
}

// ── Ripple Signal Visual ──────────────────────────────────────
function SignalRipple({ active, color = T.accent }) {
  return (
    <div style={{ position:"relative", width:80, height:80, display:"flex",
      alignItems:"center", justifyContent:"center" }}>
      {active && [0,1,2].map(i => (
        <div key={i} style={{
          position:"absolute", top:"50%", left:"50%",
          width:80, height:80, borderRadius:"50%",
          border:`1.5px solid ${color}`,
          animation:`ripple 2.4s ease-out ${i*0.8}s infinite`,
          pointerEvents:"none"
        }}/>
      ))}
      <div style={{
        width:44, height:44, borderRadius:"50%",
        background: active ? `radial-gradient(circle, ${color}33, ${color}11)` : T.card,
        border:`2px solid ${active ? color : T.border}`,
        display:"flex", alignItems:"center", justifyContent:"center",
        transition:"all .4s", zIndex:1,
        boxShadow: active ? `0 0 20px ${color}55` : "none"
      }}>
        <svg width={22} height={22} viewBox="0 0 24 24" fill="none">
          <path d="M1.42 9a16 16 0 0 1 21.16 0" stroke={active ? color : T.sub} strokeWidth="2" strokeLinecap="round" opacity=".5"/>
          <path d="M5 12.55a11 11 0 0 1 14.08 0" stroke={active ? color : T.sub} strokeWidth="2" strokeLinecap="round"/>
          <path d="M8.53 16.11a6 6 0 0 1 6.95 0" stroke={active ? color : T.sub} strokeWidth="2" strokeLinecap="round"/>
          <circle cx="12" cy="20" r="1.5" fill={active ? color : T.sub}/>
        </svg>
      </div>
    </div>
  );
}

// ── Progress Bar ──────────────────────────────────────────────
function Bar({ value, max, color }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div style={{ height:5, background:T.border, borderRadius:99, overflow:"hidden" }}>
      <div style={{
        height:"100%", width:`${pct}%`, borderRadius:99,
        background:`linear-gradient(90deg, ${color}88, ${color})`,
        transition:"width .5s ease",
        boxShadow:`0 0 6px ${color}66`
      }}/>
    </div>
  );
}

// ── Code Input ────────────────────────────────────────────────
function CodeInput({ value, onChange, disabled }) {
  const chars = value.padEnd(6," ").split("");
  const inputRef = useRef();
  return (
    <div style={{ display:"flex", gap:7, justifyContent:"center" }}
      onClick={() => inputRef.current?.focus()}>
      <input ref={inputRef} value={value}
        onChange={e => onChange(e.target.value.replace(/[^A-Za-z0-9]/g,"").toUpperCase().slice(0,6))}
        disabled={disabled}
        style={{ position:"absolute", opacity:0, width:1, height:1, pointerEvents: disabled?"none":"auto" }}
        maxLength={6} autoComplete="off" />
      {chars.map((ch, i) => (
        <div key={i} style={{
          width:40, height:50, borderRadius:10,
          background: T.bg,
          border:`2px solid ${ch.trim() ? T.accent : T.border}`,
          display:"flex", alignItems:"center", justifyContent:"center",
          fontFamily:"JetBrains Mono", fontSize:22, fontWeight:700,
          color: T.accent,
          boxShadow: ch.trim() ? `0 0 10px ${T.accent}33` : "none",
          transition:"all .2s",
          cursor: disabled ? "not-allowed" : "text"
        }}>{ch.trim()}</div>
      ))}
    </div>
  );
}

// ── HOST SCREEN ───────────────────────────────────────────────
function HostScreen({ ws, onLog, log }) {
  const [sharing, setSharing]     = useState(false);
  const [code, setCode]           = useState("");
  const [clients, setClients]     = useState([]);
  const [upload, setUpload]       = useState(0);
  const [totalUp, setTotalUp]     = useState(0);
  const [netType, setNetType]     = useState("WiFi");
  const [showQR, setShowQR]       = useState(false);
  const tickRef = useRef();

  // Generate code + register with relay server
  const startSharing = () => {
    const newCode = Math.random().toString(36).substring(2,8).toUpperCase();
    setCode(newCode);
    setSharing(true);
    setClients([]);
    onLog(`Session started · Code: ${newCode}`);
    onLog(`Relay socket: ${WS_URL}`);
    onLog(`Waiting for client connections...`);

    // Real WebSocket message to relay server:
    // ws.current?.send(JSON.stringify({ type:"HOST_REGISTER", code:newCode, netType }));
  };

  const stopSharing = () => {
    setSharing(false);
    setCode("");
    setClients([]);
    setUpload(0);
    setShowQR(false);
    clearInterval(tickRef.current);
    onLog("Session terminated · Relay disconnected");
    // ws.current?.send(JSON.stringify({ type:"HOST_LEAVE", code }));
  };

  // Simulate live metrics while sharing
  useEffect(() => {
    if (sharing) {
      // Simulate a client joining (replace with real ws message handler)
      const joinTimer = setTimeout(() => {
        setClients([{ id:"c1", name:"Person C", ip:"192.168.4.102", joined: Date.now(), rx:0 }]);
        onLog("Client joined · IP 192.168.4.102 · Relaying traffic...");
      }, 2500);

      tickRef.current = setInterval(() => {
        const spd = parseFloat((Math.random() * 5 + 0.3).toFixed(2));
        setUpload(spd);
        setTotalUp(p => parseFloat((p + spd * 0.08).toFixed(2)));
        setClients(p => p.map(c => ({ ...c, rx: parseFloat((c.rx + spd * 0.08).toFixed(2)) })));
      }, 800);

      return () => { clearTimeout(joinTimer); clearInterval(tickRef.current); };
    }
  }, [sharing]);

  const deeplink = `netshare://join?code=${code}`;

  return (
    <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

      {/* Network selector */}
      <div className="fadeUp" style={{ display:"flex", gap:8 }}>
        {["WiFi","4G LTE","5G"].map(n => (
          <button key={n} onClick={() => setNetType(n)} style={{
            flex:1, padding:"8px 0", borderRadius:9, fontSize:12, fontWeight:600,
            fontFamily:"Rajdhani", letterSpacing:1.5, border:"none",
            background: netType===n ? `linear-gradient(135deg,${T.violet},${T.accent})` : T.card,
            color: netType===n ? "#fff" : T.sub,
            border: `1px solid ${netType===n ? "transparent" : T.border}`,
            transition:"all .2s"
          }}>{n}</button>
        ))}
      </div>

      {/* Broadcast card */}
      <div className="fadeUp delay-1 glass noise" style={{
        borderRadius:18, padding:"24px 20px", textAlign:"center",
        position:"relative", overflow:"hidden"
      }}>
        {/* background grid */}
        <div style={{
          position:"absolute", inset:0, opacity:.04,
          backgroundImage:"linear-gradient(#0ef 1px,transparent 1px),linear-gradient(90deg,#0ef 1px,transparent 1px)",
          backgroundSize:"28px 28px", pointerEvents:"none"
        }}/>

        <SignalRipple active={sharing} color={sharing ? T.green : T.sub} />

        <div style={{
          marginTop:10, fontFamily:"Rajdhani", fontSize:11, fontWeight:700,
          letterSpacing:4, color: sharing ? T.green : T.sub, textTransform:"uppercase"
        }}>
          {sharing ? "● Broadcasting" : "○ Offline"}
        </div>

        {sharing && (
          <div className="fadeIn" style={{ marginTop:14 }}>
            <div style={{ fontSize:10, color:T.sub, letterSpacing:3, fontFamily:"JetBrains Mono" }}>SESSION CODE</div>
            <div style={{
              fontSize:38, fontFamily:"JetBrains Mono", fontWeight:700,
              color:T.accent, letterSpacing:10, marginTop:4,
              textShadow:`0 0 30px ${T.accent}66`
            }}>{code}</div>
            <div style={{ display:"flex", gap:8, justifyContent:"center", marginTop:10 }}>
              <button onClick={() => setShowQR(p=>!p)} style={{
                padding:"6px 16px", borderRadius:8, fontSize:11, fontFamily:"Rajdhani",
                fontWeight:600, letterSpacing:1, border:`1px solid ${T.border}`,
                background:T.card, color:T.text
              }}>{showQR ? "Hide QR" : "Show QR"}</button>
              <button onClick={() => navigator.clipboard?.writeText(deeplink)} style={{
                padding:"6px 16px", borderRadius:8, fontSize:11, fontFamily:"Rajdhani",
                fontWeight:600, letterSpacing:1, border:`1px solid ${T.border}`,
                background:T.card, color:T.text
              }}>Copy Link</button>
            </div>

            {showQR && (
              <div className="fadeIn" style={{ marginTop:14, display:"flex", justifyContent:"center" }}>
                <div style={{ padding:10, background:"#fff", borderRadius:10, display:"inline-block" }}>
                  <FakeQR value={deeplink} size={130}/>
                </div>
              </div>
            )}
          </div>
        )}

        <button onClick={sharing ? stopSharing : startSharing} style={{
          marginTop:18, padding:"12px 36px", borderRadius:30,
          fontSize:13, fontFamily:"Rajdhani", fontWeight:700, letterSpacing:3,
          border:"none", textTransform:"uppercase",
          background: sharing
            ? `linear-gradient(135deg,${T.red},#c0392b)`
            : `linear-gradient(135deg,${T.violet},${T.accent})`,
          color:"#fff",
          boxShadow: sharing ? `0 4px 20px ${T.red}44` : `0 4px 20px ${T.accent}44`,
          transition:"all .3s"
        }}>
          {sharing ? "Stop Sharing" : "Start Sharing"}
        </button>
      </div>

      {/* Live metrics */}
      {sharing && (
        <div className="fadeUp" style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10 }}>
          <div style={{ background:T.card, border:`1px solid ${T.border}`, borderRadius:14,
            padding:14, display:"flex", flexDirection:"column", alignItems:"center", gap:4 }}>
            <SpeedArc value={upload} max={10} color={T.accent} label="UL MB/s"/>
          </div>
          <div style={{ display:"grid", gridTemplateRows:"1fr 1fr", gap:10 }}>
            <StatPill label="Uploaded" value={`${totalUp} MB`} color={T.green} icon="↑"/>
            <StatPill label="Network" value={netType} color={T.violet} icon="📶"/>
          </div>
        </div>
      )}

      {/* Connected clients */}
      <div className="fadeUp delay-2 glass" style={{ borderRadius:14, padding:14 }}>
        <div style={{ display:"flex", alignItems:"center", marginBottom:10 }}>
          <span style={{ fontSize:11, letterSpacing:3, color:T.sub, fontFamily:"JetBrains Mono", flex:1 }}>
            CONNECTED CLIENTS
          </span>
          <span style={{
            fontSize:10, fontFamily:"JetBrains Mono", fontWeight:700,
            background: clients.length ? T.green : T.border, color: clients.length ? T.bg : T.sub,
            borderRadius:20, padding:"1px 9px"
          }}>{clients.length}</span>
        </div>

        {clients.length === 0 ? (
          <div style={{ fontSize:12, color:T.sub, textAlign:"center", padding:"12px 0" }}>
            {sharing ? "Waiting for clients to join..." : "Start sharing to accept connections"}
          </div>
        ) : clients.map(c => (
          <div key={c.id} className="fadeIn" style={{
            display:"flex", alignItems:"center", gap:10,
            background:T.bg, borderRadius:10, padding:"10px 12px",
            border:`1px solid ${T.border}`
          }}>
            <div style={{ width:36, height:36, borderRadius:"50%",
              background:`linear-gradient(135deg,${T.violet}55,${T.accent}55)`,
              display:"flex", alignItems:"center", justifyContent:"center",
              fontFamily:"Rajdhani", fontSize:16, fontWeight:700, color:T.accent }}>C</div>
            <div style={{ flex:1 }}>
              <div style={{ fontSize:13, fontWeight:700 }}>{c.name}</div>
              <div style={{ fontSize:10, color:T.sub, fontFamily:"JetBrains Mono" }}>{c.ip}</div>
            </div>
            <div style={{ textAlign:"right" }}>
              <div style={{ fontSize:13, fontFamily:"JetBrains Mono", color:T.green }}>{c.rx} MB</div>
              <div style={{ fontSize:9, color:T.sub }}>received</div>
            </div>
            <div style={{ width:7, height:7, borderRadius:"50%", background:T.green }} className="pulse"/>
          </div>
        ))}
      </div>

      {/* Event log */}
      <div className="fadeUp delay-3">
        <div style={{ fontSize:10, letterSpacing:3, color:T.sub, fontFamily:"JetBrains Mono", marginBottom:6 }}>EVENT LOG</div>
        <LogFeed entries={log}/>
      </div>
    </div>
  );
}

// ── CLIENT SCREEN ─────────────────────────────────────────────
function ClientScreen({ ws, onLog, log }) {
  const [phase, setPhase]         = useState("enter"); // enter | connecting | connected
  const [code, setCode]           = useState("");
  const [error, setError]         = useState("");
  const [dlSpeed, setDlSpeed]     = useState(0);
  const [totalRx, setTotalRx]     = useState(0);
  const [ping, setPing]           = useState(0);
  const [jitter, setJitter]       = useState(0);
  const [packetLoss, setPacket]   = useState(0);
  const tickRef = useRef();

  const connect = () => {
    if (code.length < 6) { setError("Code must be 6 characters"); return; }
    setError("");
    setPhase("connecting");
    onLog(`Resolving session: ${code}`);
    onLog(`Contacting relay at ${WS_URL}...`);

    // Real flow:
    // ws.current?.send(JSON.stringify({ type:"CLIENT_JOIN", code }));
    // ws.current.onmessage = (msg) => { ... handle HOST_FOUND / HOST_NOT_FOUND }

    setTimeout(() => {
      // Simulate success (replace with real WS response)
      onLog("Host found · Establishing tunnel...");
      setTimeout(() => {
        setPhase("connected");
        onLog("Tunnel active · Traffic routed through Person B");
      }, 800);
    }, 1800);
  };

  const disconnect = () => {
    setPhase("enter");
    setCode("");
    setDlSpeed(0);
    setTotalRx(0);
    setPing(0);
    clearInterval(tickRef.current);
    onLog("Disconnected from relay · Traffic restored to local interface");
  };

  useEffect(() => {
    if (phase === "connected") {
      tickRef.current = setInterval(() => {
        const spd = parseFloat((Math.random() * 4.5 + 0.2).toFixed(2));
        const p   = Math.floor(Math.random() * 28 + 6);
        const j   = parseFloat((Math.random() * 3).toFixed(1));
        const pl  = parseFloat((Math.random() * 1.2).toFixed(1));
        setDlSpeed(spd); setPing(p); setJitter(j); setPacket(pl);
        setTotalRx(r => parseFloat((r + spd * 0.08).toFixed(2)));
      }, 900);
    }
    return () => clearInterval(tickRef.current);
  }, [phase]);

  const signalQ = dlSpeed > 3 ? "Excellent" : dlSpeed > 1.5 ? "Good" : dlSpeed > 0.5 ? "Fair" : "Poor";
  const signalC = dlSpeed > 3 ? T.green : dlSpeed > 1.5 ? T.accent : dlSpeed > 0.5 ? T.amber : T.red;

  return (
    <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

      {/* Connection card */}
      {phase === "enter" && (
        <div className="fadeUp glass noise" style={{ borderRadius:18, padding:"22px 20px" }}>
          <div style={{ fontSize:13, letterSpacing:2, color:T.sub, fontFamily:"JetBrains Mono",
            marginBottom:18, textAlign:"center" }}>ENTER SESSION CODE</div>

          <CodeInput value={code} onChange={setCode} disabled={false}/>

          {error && (
            <div style={{ marginTop:10, fontSize:11, color:T.red, textAlign:"center",
              fontFamily:"JetBrains Mono" }}>⚠ {error}</div>
          )}

          <button onClick={connect} disabled={code.length < 6} style={{
            marginTop:18, width:"100%", padding:"13px", borderRadius:12,
            fontSize:13, fontFamily:"Rajdhani", fontWeight:700, letterSpacing:3,
            border:"none", textTransform:"uppercase",
            background: code.length===6
              ? `linear-gradient(135deg,${T.violet},${T.green})`
              : T.border,
            color: code.length===6 ? "#fff" : T.sub,
            opacity: code.length===6 ? 1 : .6,
            transition:"all .25s",
            boxShadow: code.length===6 ? `0 4px 20px ${T.green}33` : "none"
          }}>
            Connect to Host
          </button>

          <div style={{
            marginTop:14, padding:"10px 12px", background:T.bg, borderRadius:10,
            fontSize:11, color:T.sub, lineHeight:1.7, fontFamily:"Rajdhani"
          }}>
            💡 Ask <span style={{color:T.accent}}>Person B</span> to start sharing and share their 6-digit code or QR.
          </div>
        </div>
      )}

      {phase === "connecting" && (
        <div className="fadeIn glass" style={{
          borderRadius:18, padding:"40px 20px",
          display:"flex", flexDirection:"column", alignItems:"center", gap:14
        }}>
          <div style={{ width:50, height:50, border:`3px solid ${T.border}`,
            borderTopColor:T.accent, borderRadius:"50%" }} className="spinCW"/>
          <div style={{ fontFamily:"JetBrains Mono", fontSize:12, color:T.accent, letterSpacing:2 }}>
            CONNECTING...
          </div>
          <div style={{ fontFamily:"Rajdhani", fontSize:12, color:T.sub }}>
            Reaching relay server
          </div>
        </div>
      )}

      {phase === "connected" && (
        <>
          {/* Status banner */}
          <div className="fadeUp glass" style={{
            borderRadius:14, padding:"12px 16px",
            display:"flex", alignItems:"center", gap:12,
            border:`1px solid ${T.green}44`,
            background:`${T.green}08`
          }}>
            <div style={{ width:8, height:8, borderRadius:"50%", background:T.green }} className="pulse"/>
            <div style={{ flex:1 }}>
              <div style={{ fontSize:13, fontWeight:700, color:T.green }}>Tunnel Active</div>
              <div style={{ fontSize:10, fontFamily:"JetBrains Mono", color:T.sub }}>
                via session {code} · relayed through Person B
              </div>
            </div>
            <div style={{
              fontSize:11, fontFamily:"Rajdhani", fontWeight:700, letterSpacing:1,
              color:signalC, background:`${signalC}18`, padding:"3px 10px", borderRadius:20
            }}>{signalQ}</div>
          </div>

          {/* Speedometer + stats */}
          <div className="fadeUp delay-1" style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10 }}>
            <div style={{ background:T.card, border:`1px solid ${T.border}`, borderRadius:14,
              padding:14, display:"flex", flexDirection:"column", alignItems:"center" }}>
              <SpeedArc value={dlSpeed} max={10} color={T.green} label="DL MB/s"/>
            </div>
            <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
              <StatPill label="Ping"   value={`${ping} ms`}   color={ping < 20 ? T.green : ping < 60 ? T.amber : T.red}/>
              <StatPill label="Jitter" value={`${jitter} ms`} color={T.accent}/>
            </div>
          </div>

          {/* Extra stats */}
          <div className="fadeUp delay-2" style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10 }}>
            <StatPill label="Received"    value={`${totalRx} MB`}    color={T.green}  icon="↓"/>
            <StatPill label="Packet Loss" value={`${packetLoss}%`}   color={packetLoss>1?T.red:T.green} icon="⚠"/>
          </div>

          {/* Speed history bar */}
          <div className="fadeUp delay-3" style={{
            background:T.card, border:`1px solid ${T.border}`, borderRadius:14, padding:14
          }}>
            <div style={{ fontSize:10, letterSpacing:3, color:T.sub, fontFamily:"JetBrains Mono", marginBottom:8 }}>
              BANDWIDTH
            </div>
            <Bar value={dlSpeed} max={10} color={signalC}/>
            <div style={{ display:"flex", justifyContent:"space-between", marginTop:5,
              fontSize:9, fontFamily:"JetBrains Mono", color:T.sub }}>
              <span>0 MB/s</span><span>5 MB/s</span><span>10 MB/s</span>
            </div>
          </div>

          <button onClick={disconnect} style={{
            width:"100%", padding:"12px", borderRadius:12,
            fontSize:13, fontFamily:"Rajdhani", fontWeight:700, letterSpacing:3,
            border:"none", textTransform:"uppercase",
            background:`linear-gradient(135deg,${T.red},#c0392b)`,
            color:"#fff", boxShadow:`0 4px 20px ${T.red}44`
          }}>
            Disconnect
          </button>
        </>
      )}

      {/* Log */}
      <div className="fadeUp delay-4">
        <div style={{ fontSize:10, letterSpacing:3, color:T.sub, fontFamily:"JetBrains Mono", marginBottom:6 }}>
          CONNECTION LOG
        </div>
        <LogFeed entries={log}/>
      </div>
    </div>
  );
}

// ── ROOT APP ──────────────────────────────────────────────────
export default function NetShareApp() {
  const [tab, setTab]         = useState("host");
  const [hostLog, setHostLog] = useState([]);
  const [clientLog, setClientLog] = useState([]);
  const ws = useRef(null);

  const ts = () => new Date().toLocaleTimeString("en-US", { hour12:false });
  const addHostLog  = msg => setHostLog(p  => [...p.slice(-20), { ts:ts(), msg }]);
  const addClientLog= msg => setClientLog(p=> [...p.slice(-20), { ts:ts(), msg }]);

  // Real WebSocket init (uncomment when backend is ready):
  // useEffect(() => {
  //   ws.current = new WebSocket(WS_URL);
  //   ws.current.onopen  = () => addHostLog("Relay socket connected");
  //   ws.current.onclose = () => addHostLog("Relay socket closed");
  //   ws.current.onerror = () => addHostLog("Relay socket error");
  //   return () => ws.current?.close();
  // }, []);

  return (
    <>
      <style>{GLOBAL_CSS}</style>
      <div style={{
        minHeight:"100vh", background:T.bg,
        display:"flex", flexDirection:"column",
        alignItems:"center", padding:"20px 0 48px"
      }}>
        {/* ── Header ── */}
        <div style={{ width:"100%", maxWidth:420, padding:"0 16px", marginBottom:20 }}>
          <div style={{
            display:"flex", alignItems:"center", justifyContent:"space-between",
            padding:"12px 16px", borderRadius:14,
            background:T.surface, border:`1px solid ${T.border}`
          }}>
            <div>
              <div style={{
                fontFamily:"Rajdhani", fontWeight:700, fontSize:20, letterSpacing:5,
                background:`linear-gradient(90deg,${T.accent},${T.violet})`,
                WebkitBackgroundClip:"text", WebkitTextFillColor:"transparent"
              }}>NETSHARE</div>
              <div style={{ fontSize:9, color:T.sub, letterSpacing:3, fontFamily:"JetBrains Mono" }}>
                REAL-TIME RELAY PROTOCOL
              </div>
            </div>
            <div style={{
              fontSize:9, fontFamily:"JetBrains Mono", color:T.amber,
              background:`${T.amber}15`, border:`1px solid ${T.amber}44`,
              borderRadius:6, padding:"3px 8px"
            }}>v1.0.0-BETA</div>
          </div>
        </div>

        {/* ── Tab bar ── */}
        <div style={{ width:"100%", maxWidth:420, padding:"0 16px", marginBottom:16 }}>
          <div style={{
            display:"flex", background:T.surface,
            border:`1px solid ${T.border}`, borderRadius:50, padding:4
          }}>
            {[
              { key:"host",   label:"HOST",   sub:"Person B", color:T.violet },
              { key:"client", label:"CLIENT", sub:"Person C", color:T.green  },
            ].map(t => (
              <button key={t.key} onClick={() => setTab(t.key)} style={{
                flex:1, padding:"10px 0", borderRadius:50,
                fontSize:11, fontFamily:"Rajdhani", fontWeight:700, letterSpacing:2,
                border:"none",
                background: tab===t.key
                  ? `linear-gradient(135deg,${t.color}cc,${t.color})`
                  : "transparent",
                color: tab===t.key ? "#fff" : T.sub,
                transition:"all .25s"
              }}>
                {t.label} <span style={{ fontSize:9, opacity:.7 }}>· {t.sub}</span>
              </button>
            ))}
          </div>
        </div>

        {/* ── Panels ── */}
        <div style={{ width:"100%", maxWidth:420, padding:"0 16px" }}>
          {tab === "host"
            ? <HostScreen   ws={ws} onLog={addHostLog}   log={hostLog}/>
            : <ClientScreen ws={ws} onLog={addClientLog} log={clientLog}/>
          }
        </div>

        {/* ── Footer ── */}
        <div style={{
          marginTop:32, fontSize:9, color:T.sub,
          fontFamily:"JetBrains Mono", letterSpacing:3, textAlign:"center",
          lineHeight:2
        }}>
          NETSHARE © 2026<br/>
          <span style={{color:T.border}}>FRONTEND ONLY · BACKEND REQUIRED FOR LIVE RELAY</span>
        </div>
      </div>
    </>
  );
}
