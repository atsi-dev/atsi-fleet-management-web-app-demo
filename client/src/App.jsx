// client/src/App.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { io } from "socket.io-client";
import { format } from "timeago.js";

const WS_URL = import.meta.env.VITE_SERVER_URL || "http://localhost:8080";

/* ---------- utilities ---------- */
const NOW = () => Date.now();
const MPH = (v) => (Number.isFinite(v) ? v : null);
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
const cx = (...a) => a.filter(Boolean).join(" ");
const fmtLocation = (c, s) => (c && s ? `${c}, ${s}` : c || s || undefined);
const truncateMiddle = (str, max) => {
  const s = String(str ?? "");
  if (s.length <= max) return s;
  const head = Math.ceil((max - 1) / 2);
  const tail = Math.floor((max - 1) / 2);
  return `${s.slice(0, head)}…${s.slice(-tail)}`;
};
const signalTone = (ts) => {
  const age = NOW() - (ts || 0);
  if (age < 15_000) return { label: "Fresh", tone: "emerald" };
  if (age < 60_000) return { label: "Warm", tone: "amber" };
  return { label: "Stale", tone: "red" };
};
const sevTone = (s) =>
  s === "critical"
    ? "red"
    : s === "warning"
    ? "amber"
    : s === "info"
    ? "blue"
    : "neutral";

/* ---------- theme (Auto/Light/Dark) ---------- */
function useTheme() {
  const getInitial = () => localStorage.getItem("themeMode") || "auto";
  const [mode, setMode] = useState(getInitial);
  useEffect(() => {
    const root = document.documentElement;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => {
      const dark = mode === "dark" || (mode === "auto" && mq.matches);
      root.classList.toggle("dark", dark);
    };
    apply();
    if (mode === "auto") {
      mq.addEventListener?.("change", apply);
      return () => mq.removeEventListener?.("change", apply);
    }
  }, [mode]);
  useEffect(() => {
    localStorage.setItem("themeMode", mode);
  }, [mode]);
  return [mode, setMode];
}

/* ---------- App ---------- */
export default function App() {
  const [themeMode, setThemeMode] = useTheme();

  const [rows, setRows] = useState(() => new Map());
  const [connected, setConnected] = useState(false);
  const [now, setNow] = useState(() => new Date());

  // histories & events
  const historyRef = useRef(new Map()); // id -> mph[]
  const lastEventsRef = useRef(new Map()); // id -> [{time, mph, lat, lon}]

  // UI state (persisted)
  const [quick, setQuick] = useState(
    () => localStorage.getItem("quick") || "all"
  );
  const [query, setQuery] = useState(() => localStorage.getItem("query") || "");
  const [minMph, setMinMph] = useState(
    () => localStorage.getItem("minMph") || ""
  );
  const [sortBy, setSortBy] = useState(
    () => localStorage.getItem("sortBy") || "vin"
  );
  const [dense, setDense] = useState(
    () => localStorage.getItem("dense") === "1"
  );
  const [lastSnapshotCount, setLastSnapshotCount] = useState(0);
  const [coordPrec, setCoordPrec] = useState(() =>
    Number(localStorage.getItem("coordPrec") || 5)
  );
  const [mapProvider, setMapProvider] = useState(
    () => localStorage.getItem("mapProvider") || "google"
  );
  const [favorites, setFavorites] = useState(() => {
    try {
      return new Set(JSON.parse(localStorage.getItem("favorites") || "[]"));
    } catch {
      return new Set();
    }
  });
  const [onlyFav, setOnlyFav] = useState(
    () => localStorage.getItem("onlyFav") === "1"
  );

  // faults: alerting + snooze
  const [snoozeUntil, setSnoozeUntil] = useState(() =>
    Number(localStorage.getItem("snoozeUntil") || "0")
  );

  // modal
  const [active, setActive] = useState(null);

  // toast
  const [toast, setToast] = useState(null);
  const fireToast = (msg) => {
    setToast(msg);
    setTimeout(() => setToast(null), 1200);
  };

  // small clock
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  // persist UI
  useEffect(() => {
    localStorage.setItem("quick", quick);
  }, [quick]);
  useEffect(() => {
    localStorage.setItem("query", query);
  }, [query]);
  useEffect(() => {
    localStorage.setItem("minMph", String(minMph));
  }, [minMph]);
  useEffect(() => {
    localStorage.setItem("sortBy", sortBy);
  }, [sortBy]);
  useEffect(() => {
    localStorage.setItem("dense", dense ? "1" : "0");
  }, [dense]);
  useEffect(() => {
    localStorage.setItem("onlyFav", onlyFav ? "1" : "0");
  }, [onlyFav]);
  useEffect(() => {
    localStorage.setItem("coordPrec", String(coordPrec));
  }, [coordPrec]);
  useEffect(() => {
    localStorage.setItem("mapProvider", mapProvider);
  }, [mapProvider]);
  useEffect(() => {
    localStorage.setItem("snoozeUntil", String(snoozeUntil));
  }, [snoozeUntil]);
  useEffect(() => {
    try {
      localStorage.setItem("favorites", JSON.stringify([...favorites]));
    } catch {}
  }, [favorites]);

  // notifications permission (optional)
  const ensureNotifs = () => {
    if ("Notification" in window && Notification.permission === "default") {
      Notification.requestPermission();
    }
  };

  // audio (tiny beep)
  const beep = () => {
    try {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.type = "triangle";
      o.frequency.setValueAtTime(880, ctx.currentTime);
      g.gain.setValueAtTime(0.0001, ctx.currentTime);
      g.gain.exponentialRampToValueAtTime(0.1, ctx.currentTime + 0.02);
      g.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.3);
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      o.stop(ctx.currentTime + 0.32);
    } catch {}
  };

  // deep link open
  useEffect(() => {
    const hash = new URLSearchParams(window.location.hash.slice(1));
    const id = hash.get("truck");
    if (id) setActive(id);
  }, []);
  const openDetails = (id) => {
    setActive(id);
    const p = new URLSearchParams(window.location.hash.slice(1));
    p.set("truck", id);
    window.location.hash = p.toString();
  };
  const closeDetails = () => {
    setActive(null);
    const p = new URLSearchParams(window.location.hash.slice(1));
    p.delete("truck");
    window.location.hash = p.toString() || "";
  };

  // socket
  useEffect(() => {
    const socket = io(WS_URL, {
      transports: ["polling", "websocket"],
      timeout: 10000,
      withCredentials: false,
    });

    socket.on("connect", () => setConnected(true));
    socket.on("disconnect", () => setConnected(false));

    socket.on("snapshot", (items) => {
      const m = new Map();
      const hist = new Map(historyRef.current);
      const ev = new Map(lastEventsRef.current);

      for (const it of items) {
        m.set(it.id, it);
        if (!hist.has(it.id)) hist.set(it.id, []);
        const mph = MPH(it.mph);
        if (mph != null) {
          const arr = hist.get(it.id);
          arr.push(clamp(mph, 0, 120));
          if (arr.length > 60) arr.shift();
        }
        if (!ev.has(it.id)) ev.set(it.id, []);
        ev.get(it.id).push({
          time: it.time || new Date().toISOString(),
          mph: mph ?? null,
          lat: it.lat ?? null,
          lon: it.lon ?? null,
        });
        if (ev.get(it.id).length > 25) ev.get(it.id).shift();
      }
      historyRef.current = hist;
      lastEventsRef.current = ev;
      setRows(m);
      setLastSnapshotCount(items.length);
    });

    socket.on("update", (item) => {
      const now = NOW();
      setRows((prev) => {
        const next = new Map(prev);
        const merged = { ...(next.get(item.id) || {}), ...item, __hot: now };
        next.set(item.id, merged);
        return next;
      });

      const mph = MPH(item.mph);
      const hist = new Map(historyRef.current);
      if (!hist.has(item.id)) hist.set(item.id, []);
      if (mph != null) {
        const arr = hist.get(item.id);
        arr.push(clamp(mph, 0, 120));
        if (arr.length > 60) arr.shift();
      }
      historyRef.current = hist;

      const ev = new Map(lastEventsRef.current);
      if (!ev.has(item.id)) ev.set(item.id, []);
      ev.get(item.id).push({
        time: item.time || new Date().toISOString(),
        mph: mph ?? null,
        lat: item.lat ?? null,
        lon: item.lon ?? null,
      });
      if (ev.get(item.id).length > 25) ev.get(item.id).shift();
      lastEventsRef.current = ev;
    });

    // NEW: faults stream
    socket.on("fault", (payload) => {
      // Alert on critical, honor snooze
      const isCritical = (payload.severity || "").toLowerCase() === "critical";
      const snoozed = Date.now() < snoozeUntil;
      if (isCritical && !snoozed) {
        beep();
        ensureNotifs();
        if ("Notification" in window && Notification.permission === "granted") {
          new Notification(`Critical fault ${payload.code}`, {
            body: `${payload.vin || payload.id}: ${
              payload.description || "Fault"
            }`,
          });
        }
      }
    });

    // optional: someone clicked "request help"
    socket.on("help", (req) => {
      fireToast(`Help requested for ${req.vin || req.id}`);
    });

    return () => socket.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snoozeUntil]);

  // keyboard shortcuts
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "/" && document.activeElement?.tagName !== "INPUT") {
        e.preventDefault();
        document.getElementById("global-search")?.focus();
      } else if (e.key.toLowerCase() === "f") {
        setOnlyFav((v) => !v);
      } else if (e.key === "Escape") {
        if (active) closeDetails();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [active]);

  const all = useMemo(() => Array.from(rows.values()), [rows]);

  // de-dup by VIN
  const deduped = useMemo(() => {
    const byVin = new Map();
    for (const r of all) {
      const k = r.vin || r.id;
      const prev = byVin.get(k);
      if (!prev) byVin.set(k, r);
      else
        byVin.set(
          k,
          (r.lastUpdateTs || 0) >= (prev.lastUpdateTs || 0) ? r : prev
        );
    }
    return Array.from(byVin.values());
  }, [all]);

  // filters
  const quickFilter = (r) => {
    const mph = MPH(r.mph);
    const hasFaults = (r.faults?.active?.length || 0) > 0;
    const hasCritical = (r.faults?.active || []).some(
      (f) => f.severity === "critical"
    );
    switch (quick) {
      case "moving":
        return mph != null && mph >= 1;
      case "idle":
        return mph != null && mph < 1;
      case "nogps":
        return r.lat == null || r.lon == null;
      case "faults":
        return hasFaults;
      case "critical":
        return hasCritical;
      default:
        return true;
    }
  };

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const min = Number.isFinite(Number(minMph)) ? Number(minMph) : null;
    return deduped.filter((r) => {
      if (!quickFilter(r)) return false;
      if (onlyFav && !favorites.has(r.id)) return false;
      if (min != null && r.mph != null && r.mph < min) return false;
      if (!q) return true;
      const hay = [
        r.id,
        r.vin,
        r.serial,
        r.city,
        r.state,
        r.lastTopic,
        ...(r.faults?.active || []).flatMap((f) => [
          f.code,
          f.description,
          f.severity,
        ]),
        r.lat != null && r.lon != null ? `${r.lat},${r.lon}` : "",
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [deduped, query, minMph, quick, onlyFav, favorites]);

  const list = useMemo(() => {
    const arr = [...filtered];
    switch (sortBy) {
      case "vin":
        arr.sort((a, b) =>
          String(a.vin || "").localeCompare(String(b.vin || ""), undefined, {
            sensitivity: "base",
            numeric: true,
          })
        );
        break;
      case "speed":
        arr.sort((a, b) => (b.mph ?? -1) - (a.mph ?? -1));
        break;
      case "id":
        arr.sort((a, b) => String(a.id).localeCompare(String(b.id)));
        break;
      case "faults":
        arr.sort(
          (a, b) =>
            (b.faults?.active?.length || 0) - (a.faults?.active?.length || 0)
        );
        break;
      default:
        arr.sort((a, b) => (b.lastUpdateTs || 0) - (a.lastUpdateTs || 0));
    }
    return arr;
  }, [filtered, sortBy]);

  const total = deduped.length;
  const moving = deduped.filter((r) => (r.mph ?? 0) >= 1).length;
  const faultCount = deduped.reduce(
    (n, r) => n + (r.faults?.active?.length || 0),
    0
  );
  const criticalCount = deduped.reduce(
    (n, r) =>
      n +
      (r.faults?.active || []).filter((f) => f.severity === "critical").length,
    0
  );
  const avgSpeed =
    deduped.length > 0
      ? (
          deduped.reduce(
            (s, r) => s + (Number.isFinite(r.mph) ? r.mph : 0),
            0
          ) / deduped.length
        ).toFixed(1)
      : "0.0";

  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;

  /* ---------- UI ---------- */
  return (
    <div className="min-h-dvh bg-neutral-50 text-neutral-900 antialiased dark:bg-neutral-950 dark:text-neutral-100">
      {!connected && (
        <div className="sticky top-0 z-30 w-full bg-rose-600 text-white text-center py-2">
          Disconnected — retrying…
        </div>
      )}

      {/* Header */}
      <header className="sticky top-0 z-20 border-b border-neutral-200/70 bg-white/80 backdrop-blur supports-[backdrop-filter]:bg-white/60 dark:bg-neutral-900/80 dark:border-neutral-800">
        <div className="mx-auto max-w-7xl px-4 py-3 sm:px-6 lg:px-8">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-3 min-w-0">
              <div className="h-9 w-9 rounded-2xl bg-gradient-to-br from-emerald-500 to-cyan-500 text-white grid place-items-center font-semibold">
                A
              </div>
              <div className="min-w-0">
                <h1 className="text-lg sm:text-xl font-semibold tracking-tight truncate">
                  atsi.ai — Live Fleet Board
                </h1>
                <p className="text-xs text-neutral-500 dark:text-neutral-400">
                  {now.toLocaleTimeString()} • {tz} •{" "}
                  {new Intl.NumberFormat().format(total)} assets
                </p>
              </div>
            </div>

            {/* Theme */}
            <div className="flex items-center gap-2">
              <div className="rounded-2xl border border-neutral-200 bg-white px-2 py-1 shadow-sm dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800">
                {["auto", "light", "dark"].map((m) => (
                  <button
                    key={m}
                    className={cx(
                      "px-2 py-1 text-sm rounded-xl",
                      themeMode === m
                        ? "bg-neutral-900 text-white dark:bg-white dark:text-neutral-900"
                        : "text-neutral-700 dark:text-neutral-300"
                    )}
                    onClick={() => setThemeMode(m)}
                    title={`Theme: ${m}`}
                  >
                    {m[0].toUpperCase() + m.slice(1)}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-2 text-sm">
                <StatusDot ok={connected} />
                <span className="text-neutral-700 dark:text-neutral-300">
                  {connected ? "Live" : "Offline"}
                </span>
                <span className="hidden sm:inline text-neutral-300 dark:text-neutral-600">
                  •
                </span>
                <span className="truncate text-neutral-500 dark:text-neutral-400 max-w-[38ch]">
                  {WS_URL}
                </span>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Toolbar */}
      <div className="border-b border-neutral-200/70 bg-white dark:bg-neutral-900 dark:border-neutral-800">
        <div className="mx-auto max-w-7xl px-4 py-3 sm:px-6 lg:px-8">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div className="flex flex-1 flex-wrap items-center gap-2">
              <Input
                id="global-search"
                value={query}
                onChange={setQuery}
                placeholder="Search — VIN, ID, city, fault code…"
                className="min-w-[220px] md:min-w-[320px]"
              />
              <Input
                type="number"
                min="0"
                step="1"
                value={minMph}
                onChange={setMinMph}
                placeholder="Min mph"
                className="w-[110px]"
              />
              <Select
                value={sortBy}
                onChange={setSortBy}
                items={[
                  { value: "vin", label: "Sort: VIN (A→Z)" },
                  { value: "recent", label: "Sort: Recent" },
                  { value: "speed", label: "Sort: Speed" },
                  { value: "faults", label: "Sort: Fault count" },
                  { value: "id", label: "Sort: ID" },
                ]}
              />
              <QuickFilters value={quick} onChange={setQuick} />
              {/* precision */}
              <div className="flex items-center gap-2 text-sm text-neutral-600 dark:text-neutral-300 ml-2">
                <span>Coord</span>
                <input
                  type="range"
                  min="3"
                  max="7"
                  value={coordPrec}
                  onChange={(e) => setCoordPrec(Number(e.target.value))}
                />
                <span className="tabular-nums">{coordPrec}</span>
              </div>
              {/* snooze critical alerts */}
              <button
                type="button"
                onClick={() => setSnoozeUntil(Date.now() + 5 * 60 * 1000)}
                className="rounded-2xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 shadow-sm hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
                title="Snooze critical alerts for 5 minutes"
              >
                Snooze 5m
              </button>
            </div>

            <div className="flex items-center gap-2">
              <FaultCounters faults={faultCount} critical={criticalCount} />
              <button
                type="button"
                onClick={() => setOnlyFav((v) => !v)}
                className={cx(
                  "inline-flex items-center gap-2 rounded-2xl border px-3 py-2 text-sm shadow-sm transition",
                  onlyFav
                    ? "bg-yellow-400/90 text-black border-yellow-500 hover:bg-yellow-400"
                    : "bg-white text-neutral-700 border-neutral-200 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
                )}
                title="Show only favorites (f)"
              >
                ★ Favorites {onlyFav ? "On" : "Off"}
              </button>
              <button
                type="button"
                onClick={() => exportCsv(list)}
                className="inline-flex items-center gap-2 rounded-2xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 shadow-sm transition hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
                title="Export current list as CSV"
              >
                Export CSV ↧
              </button>
              <button
                type="button"
                onClick={() => exportFaultsCsv(list)}
                className="inline-flex items-center gap-2 rounded-2xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 shadow-sm transition hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
                title="Export active faults CSV"
              >
                Export Faults ↧
              </button>
              <DensityToggle dense={dense} onChange={setDense} />
              <KpiBar total={total} moving={moving} avgSpeed={avgSpeed} />
            </div>
          </div>
        </div>
      </div>

      {/* Main */}
      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        {list.length === 0 ? (
          <EmptyState
            connected={connected}
            ws={WS_URL}
            snapshot={lastSnapshotCount}
          />
        ) : (
          <div className="[grid-template-columns:repeat(auto-fit,minmax(300px,1fr))] grid gap-4">
            {list.map((r) => (
              <AssetCard
                key={r.id}
                row={r}
                dense={dense}
                history={historyRef.current.get(r.id) || []}
                favored={favorites.has(r.id)}
                onFavToggle={() => {
                  setFavorites((prev) => {
                    const next = new Set(prev);
                    if (next.has(r.id)) next.delete(r.id);
                    else next.add(r.id);
                    return next;
                  });
                }}
                onOpen={() => openDetails(r.id)}
                onCopy={(msg) => fireToast(msg)}
                coordPrec={coordPrec}
                mapProvider={mapProvider}
                setMapProvider={setMapProvider}
                onHelp={requestHelp}
              />
            ))}
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="mx-auto max-w-7xl px-4 pb-10 pt-2 sm:px-6 lg:px-8 text-xs text-neutral-500 dark:text-neutral-400">
        <span className="inline-flex items-center gap-2">
          <StatusDot ok={connected} />
          <span>
            {connected ? "Streaming updates" : "Disconnected"} •{" "}
            {now.toLocaleTimeString()}
          </span>
        </span>
      </footer>

      {/* Modal */}
      {active && (
        <DetailsModal
          row={rows.get(active)}
          history={historyRef.current.get(active) || []}
          events={lastEventsRef.current.get(active) || []}
          onClose={closeDetails}
          onCopy={(m) => fireToast(m)}
          coordPrec={coordPrec}
          mapProvider={mapProvider}
          onHelp={requestHelp}
        />
      )}

      {/* Toast */}
      <div
        className={cx(
          "fixed left-1/2 -translate-x-1/2 bottom-6 z-[60] rounded-2xl px-4 py-2 text-sm shadow-lg transition",
          toast
            ? "opacity-100 pointer-events-auto"
            : "opacity-0 pointer-events-none",
          "bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900"
        )}
        role="status"
      >
        {toast}
      </div>
    </div>
  );

  /* ---- actions ---- */
  async function requestHelp({ id, vin, faultId, code, note }) {
    try {
      const res = await fetch(`${WS_URL.replace("ws://", "http://")}/help`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id, vin, faultId, code, note }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "Failed");
      fireToast("Help requested");
    } catch (e) {
      fireToast(`Help failed: ${e.message}`);
    }
  }
}

/* ----------------------- UI atoms & small bits ----------------------- */

function StatusDot({ ok }) {
  return (
    <span
      className={cx(
        "inline-block h-2.5 w-2.5 rounded-full transition",
        ok
          ? "bg-emerald-500 shadow-[0_0_0_4px_rgba(16,185,129,0.18)]"
          : "bg-neutral-300 dark:bg-neutral-700"
      )}
      aria-hidden
    />
  );
}

function Badge({ children, tone = "neutral" }) {
  const tones = {
    neutral:
      "bg-neutral-100 text-neutral-700 ring-1 ring-inset ring-neutral-200 dark:bg-neutral-800 dark:text-neutral-200 dark:ring-neutral-700",
    blue: "bg-blue-50 text-blue-700 ring-1 ring-inset ring-blue-200 dark:bg-blue-400/10 dark:text-blue-300 dark:ring-blue-400/20",
    amber:
      "bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-200 dark:bg-amber-400/10 dark:text-amber-300 dark:ring-amber-400/20",
    red: "bg-rose-50 text-rose-700 ring-1 ring-inset ring-rose-200 dark:bg-rose-400/10 dark:text-rose-300 dark:ring-rose-400/20",
    emerald:
      "bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-200 dark:bg-emerald-400/10 dark:text-emerald-300 dark:ring-emerald-400/20",
    violet:
      "bg-violet-50 text-violet-700 ring-1 ring-inset ring-violet-200 dark:bg-violet-400/10 dark:text-violet-300 dark:ring-violet-400/20",
  };
  return (
    <span
      className={cx(
        "px-2 py-1 rounded-lg text-[11px] font-medium whitespace-nowrap",
        tones[tone]
      )}
    >
      {children}
    </span>
  );
}

function Kpi({ label, value }) {
  return (
    <div className="rounded-2xl bg-white/70 ring-1 ring-neutral-200 px-3 py-2 shadow-sm dark:bg-neutral-900/70 dark:ring-neutral-800">
      <div className="text-[10px] uppercase tracking-wide text-neutral-500 dark:text-neutral-400">
        {label}
      </div>
      <div className="text-sm font-semibold tabular-nums">{value}</div>
    </div>
  );
}
function KpiBar({ total, moving, avgSpeed }) {
  return (
    <div className="flex items-center gap-2">
      <Kpi label="Total" value={total} />
      <Kpi label="Moving" value={moving} />
      <Kpi label="Avg mph" value={avgSpeed} />
    </div>
  );
}
function FaultCounters({ faults, critical }) {
  return (
    <div className="flex items-center gap-2">
      <Badge tone="amber">Faults {faults}</Badge>
      <Badge tone="red">Critical {critical}</Badge>
    </div>
  );
}

function ResultCount({ count, total }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-2xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 shadow-sm dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800">
      Showing <strong className="tabular-nums">{count}</strong> / {total}
    </span>
  );
}

function DensityToggle({ dense, onChange }) {
  return (
    <button
      type="button"
      onClick={() => onChange(!dense)}
      className="inline-flex items-center gap-2 rounded-2xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 shadow-sm transition hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
      title={dense ? "Comfortable view" : "Compact view"}
    >
      <span className="font-medium">{dense ? "Compact" : "Comfort"}</span>
      <span aria-hidden>⇵</span>
    </button>
  );
}

function Input({
  id,
  value,
  onChange,
  placeholder,
  className = "",
  type = "text",
  min,
  step,
}) {
  return (
    <div
      className={cx(
        "relative rounded-2xl border border-neutral-200 bg-white shadow-sm dark:bg-neutral-900 dark:border-neutral-800",
        className
      )}
    >
      <input
        id={id}
        type={type}
        min={min}
        step={step}
        className="w-full rounded-2xl bg-transparent px-3 py-2 text-sm outline-none placeholder:text-neutral-400 dark:placeholder:text-neutral-500"
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </div>
  );
}

function Select({ value, onChange, items }) {
  return (
    <div className="relative rounded-2xl border border-neutral-200 bg-white shadow-sm dark:bg-neutral-900 dark:border-neutral-800">
      <select
        className="w-full rounded-2xl bg-transparent px-3 py-2 text-sm outline-none"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {items.map((it) => (
          <option key={it.value} value={it.value}>
            {it.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function QuickFilters({ value, onChange }) {
  const Opt = ({ v, label }) => (
    <button
      type="button"
      onClick={() => onChange(v)}
      className={cx(
        "rounded-2xl px-3 py-2 text-sm transition border",
        value === v
          ? "bg-neutral-900 text-white border-neutral-900 dark:bg-neutral-100 dark:text-neutral-900 dark:border-neutral-100"
          : "bg-white text-neutral-700 border-neutral-200 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-800 dark:hover:bg-neutral-800"
      )}
    >
      {label}
    </button>
  );
  return (
    <div className="flex items-center gap-2">
      <Opt v="all" label="All" />
      <Opt v="moving" label="Moving" />
      <Opt v="idle" label="Idle" />
      <Opt v="nogps" label="No GPS" />
      <Opt v="faults" label="Faulted" />
      <Opt v="critical" label="Critical" />
    </div>
  );
}

/* ------------------------------ Card ------------------------------ */

function AssetCard({
  row,
  dense,
  history,
  favored,
  onFavToggle,
  onOpen,
  onCopy,
  coordPrec,
  mapProvider,
  setMapProvider,
  onHelp,
}) {
  const {
    id,
    vin,
    serial,
    time,
    mph,
    city,
    state,
    lat,
    lon,
    heading,
    lastTopic,
    __hot,
    lastUpdateTs,
    faults,
  } = row;

  const location = fmtLocation(city, state);
  const coords =
    lat != null && lon != null
      ? `${lat.toFixed(coordPrec)}, ${lon.toFixed(coordPrec)}`
      : undefined;
  const mphTone =
    mph == null ? "neutral" : mph < 1 ? "amber" : mph < 45 ? "blue" : "emerald";
  const isHot = __hot && NOW() - __hot < 1200;
  const age = signalTone(lastUpdateTs);

  const activeFaults = faults?.active || [];
  const counts = faults?.counts || {
    critical: 0,
    warning: 0,
    info: 0,
    unknown: 0,
  };

  const mapUrl = () => {
    if (lat == null || lon == null) return null;
    return mapProvider === "apple"
      ? `https://maps.apple.com/?ll=${lat},${lon}&q=${encodeURIComponent(
          location || "Location"
        )}`
      : `https://www.google.com/maps?q=${lat},${lon}(${encodeURIComponent(
          location || "Location"
        )})`;
  };

  return (
    <article
      className={cx(
        "group relative rounded-2xl border overflow-hidden",
        "border-neutral-200 bg-white dark:bg-neutral-900 dark:border-neutral-800",
        isHot ? "ring-2 ring-emerald-400/70" : "ring-0",
        "shadow-[0_1px_0_0_rgba(0,0,0,0.02),0_10px_30px_-12px_rgba(0,0,0,0.15)]",
        "transition duration-200 hover:shadow-[0_1px_0_0_rgba(0,0,0,0.02),0_20px_50px_-16px_rgba(0,0,0,0.25)]",
        dense ? "p-3" : "p-4",
        "flex flex-col min-h-[280px]"
      )}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <Badge tone="neutral">VIN</Badge>
            <Mono value={vin || id} max={22} />
            <CopyBtn
              value={vin}
              title="Copy VIN"
              label="Copy"
              onCopy={() => onCopy?.("VIN copied")}
            />
            <button
              className={cx(
                "rounded-lg px-2 py-1 text-[11px] ring-1 transition",
                favored
                  ? "bg-yellow-400 text-black ring-yellow-500"
                  : "bg-white text-neutral-700 ring-neutral-200 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:ring-neutral-700 dark:hover:bg-neutral-800"
              )}
              title="Toggle favorite"
              onClick={onFavToggle}
            >
              ★
            </button>
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-neutral-500 dark:text-neutral-400">
            <span title={time ? new Date(time).toLocaleString() : undefined}>
              {time
                ? `${format(time)} • ${new Date(time).toLocaleTimeString()}`
                : "—"}
            </span>
            <span className="text-neutral-300 dark:text-neutral-600">•</span>
            <Badge tone={age.tone}>{age.label}</Badge>
            {lastTopic ? (
              <>
                <span className="text-neutral-300 dark:text-neutral-600">
                  •
                </span>
                <span className="truncate max-w-[22ch]" title={lastTopic}>
                  Topic:{" "}
                  <span className="font-mono">
                    {truncateMiddle(lastTopic, 20)}
                  </span>
                </span>
              </>
            ) : null}
          </div>
        </div>
        <div className="shrink-0 flex items-center gap-2">
          <Badge tone={mphTone}>
            {Number.isFinite(mph) ? `${mph.toFixed(1)} mph` : "No speed"}
          </Badge>
          <HeadingPill heading={heading} />
        </div>
      </div>

      {/* Fault badges row */}
      <div className="mt-2 flex flex-wrap items-center gap-2">
        {counts.critical > 0 && (
          <Badge tone="red">Critical {counts.critical}</Badge>
        )}
        {counts.warning > 0 && (
          <Badge tone="amber">Warn {counts.warning}</Badge>
        )}
        {counts.info > 0 && <Badge tone="blue">Info {counts.info}</Badge>}
        {activeFaults.length === 0 && <Badge tone="emerald">Healthy</Badge>}
      </div>

      {/* Body */}
      <div className={cx("grid grid-cols-1 gap-3", dense ? "pt-2" : "pt-3")}>
        <div className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 px-2 py-1.5 dark:bg-neutral-800 dark:ring-neutral-700">
          <Sparkline values={history} />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <Field label="Serial">
            <Mono value={serial} max={22} />
          </Field>
          <Field label="Location">
            <span className="truncate" title={location}>
              {location || "—"}
            </span>
          </Field>
          <Field label="Coordinates">
            <span className="truncate font-mono" title={coords}>
              {coords || "—"}
            </span>
          </Field>
          <div className="flex items-end gap-2">
            <CopyBtn
              value={coords}
              title="Copy coordinates"
              label="Copy"
              disabled={!coords}
              onCopy={() => onCopy?.("Coordinates copied")}
            />
            <button
              className="rounded-lg px-2 py-1 text-[11px] ring-1 ring-neutral-200 bg-white text-neutral-700 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:ring-neutral-700 dark:hover:bg-neutral-800"
              onClick={() => onOpen?.()}
              title="Open details"
            >
              Details
            </button>
          </div>
        </div>

        {/* Active faults (mini) */}
        {activeFaults.length > 0 && (
          <div className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 p-2 dark:bg-neutral-800 dark:ring-neutral-700">
            <div className="text-xs mb-1 text-neutral-600 dark:text-neutral-300">
              Active faults
            </div>
            <div className="flex flex-col gap-1 max-h-24 overflow-auto pr-1">
              {activeFaults.map((f) => (
                <div
                  key={f.id || f.code}
                  className="flex items-center gap-2 text-xs"
                >
                  <Badge tone={sevTone(f.severity)}>{f.code}</Badge>
                  <span className="truncate" title={f.description}>
                    {truncateMiddle(f.description, 48)}
                  </span>
                  <button
                    className="ml-auto rounded-md px-2 py-0.5 text-[11px] ring-1 ring-neutral-300 dark:ring-neutral-700"
                    onClick={() =>
                      onHelp?.({
                        id,
                        vin,
                        faultId: f.id,
                        code: f.code,
                        note: "",
                      })
                    }
                    title="Request help"
                  >
                    Help
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="mt-auto pt-3 border-t border-neutral-100 dark:border-neutral-800">
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="flex-1 inline-flex items-center justify-center gap-2 rounded-xl border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700 transition hover:bg-neutral-50 active:translate-y-[1px] disabled:opacity-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-700 dark:hover:bg-neutral-800"
            onClick={() => {
              const url =
                lat != null && lon != null
                  ? mapProvider === "apple"
                    ? `https://maps.apple.com/?ll=${lat},${lon}&q=${encodeURIComponent(
                        location || "Location"
                      )}`
                    : `https://www.google.com/maps?q=${lat},${lon}(${encodeURIComponent(
                        location || "Location"
                      )})`
                  : null;
              if (url) window.open(url, "_blank", "noopener,noreferrer");
            }}
            disabled={lat == null || lon == null}
          >
            <span>Open in Maps</span>
            <span aria-hidden>↗</span>
          </button>
          <CopyBtn
            value={mapUrl() || ""}
            title="Copy map link"
            label="Link"
            disabled={!mapUrl()}
          />
        </div>
      </div>
    </article>
  );
}

/* ---------------------------- Modal ---------------------------- */

function DetailsModal({
  row,
  history,
  events,
  onClose,
  onCopy,
  coordPrec,
  mapProvider,
  onHelp,
}) {
  if (!row) return null;
  const {
    id,
    vin,
    serial,
    time,
    mph,
    city,
    state,
    lat,
    lon,
    heading,
    lastTopic,
    lastUpdateTs,
    faults,
  } = row;
  const location = fmtLocation(city, state);
  const coords =
    lat != null && lon != null
      ? `${lat.toFixed(coordPrec)}, ${lon.toFixed(coordPrec)}`
      : "";
  const age = signalTone(lastUpdateTs);
  const active = faults?.active || [];
  const histFaults = faults?.history || [];

  return (
    <div className="fixed inset-0 z-[70]">
      <div
        className="absolute inset-0 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />
      <div className="absolute left-1/2 top-1/2 w-[min(100vw-2rem,1000px)] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-neutral-200 bg-white shadow-2xl dark:bg-neutral-900 dark:border-neutral-800">
        <div className="flex items-center justify-between gap-3 border-b border-neutral-100 p-4 dark:border-neutral-800">
          <div className="flex items-center gap-2">
            <Badge tone="violet">Details</Badge>
            <span className="font-mono text-sm">
              {truncateMiddle(vin || id, 28)}
            </span>
            <Badge tone={age.tone}>{age.label}</Badge>
          </div>
          <button
            className="rounded-xl border border-neutral-200 bg-white px-3 py-1.5 text-sm text-neutral-700 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:border-neutral-700 dark:hover:bg-neutral-800"
            onClick={onClose}
          >
            Close ✕
          </button>
        </div>

        <div className="grid grid-cols-1 gap-4 p-4 md:grid-cols-3">
          {/* Left facts */}
          <div className="space-y-3 md:col-span-1">
            <Fact label="VIN">
              <div className="flex items-center gap-2">
                <span className="font-mono">{vin || "—"}</span>
                <CopyBtn value={vin} onCopy={() => onCopy?.("VIN copied")} />
              </div>
            </Fact>
            <Fact label="Serial">
              <span className="font-mono">{serial || "—"}</span>
            </Fact>
            <Fact label="Last seen">
              {time
                ? `${format(time)} • ${new Date(time).toLocaleString()}`
                : "—"}
            </Fact>
            <Fact label="Location">{location || "—"}</Fact>
            <Fact label="Coordinates">
              <div className="flex items-center gap-2">
                <span className="font-mono truncate">{coords || "—"}</span>
                <CopyBtn
                  value={coords}
                  onCopy={() => onCopy?.("Coordinates copied")}
                />
              </div>
            </Fact>
            <Fact label="Speed">
              {Number.isFinite(mph) ? `${mph.toFixed(1)} mph` : "—"}
            </Fact>
            <Fact label="Heading">
              {heading != null ? `${Math.round(heading)}°` : "—"}
            </Fact>
            <Fact label="Topic">
              <span className="font-mono">{lastTopic || "—"}</span>
            </Fact>

            <div className="grid grid-cols-2 gap-2 pt-2">
              <Action
                label="Open in Maps"
                onClick={() => {
                  if (lat != null && lon != null) {
                    const url =
                      mapProvider === "apple"
                        ? `https://maps.apple.com/?ll=${lat},${lon}&q=${encodeURIComponent(
                            location || "Location"
                          )}`
                        : `https://www.google.com/maps?q=${lat},${lon}(${encodeURIComponent(
                            location || "Location"
                          )})`;
                    window.open(url, "_blank", "noopener,noreferrer");
                  }
                }}
                disabled={lat == null || lon == null}
              />
              <Action
                label="Request Help"
                onClick={() =>
                  onHelp?.({
                    id,
                    vin,
                    faultId: active[0]?.id,
                    code: active[0]?.code,
                    note: "",
                  })
                }
              />
            </div>
          </div>

          {/* Right: trends & faults */}
          <div className="space-y-3 md:col-span-2">
            <div className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 p-3 dark:bg-neutral-800 dark:ring-neutral-700">
              <div className="mb-2 text-xs text-neutral-500 dark:text-neutral-400">
                Speed (recent)
              </div>
              <Sparkline values={history} />
            </div>

            <div className="grid md:grid-cols-2 gap-3">
              <div className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 p-3 dark:bg-neutral-800 dark:ring-neutral-700">
                <div className="mb-2 text-xs text-neutral-500 dark:text-neutral-400">
                  Active faults
                </div>
                <div className="max-h-40 overflow-auto pr-1">
                  {active.length === 0 ? (
                    <div className="text-sm text-neutral-500">None</div>
                  ) : (
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-neutral-100 text-neutral-600 text-xs dark:bg-neutral-700/50 dark:text-neutral-300">
                        <tr>
                          <th className="text-left px-2 py-1">Code</th>
                          <th className="text-left px-2 py-1">Severity</th>
                          <th className="text-left px-2 py-1">When</th>
                          <th className="text-left px-2 py-1">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {active.map((f) => (
                          <tr
                            key={f.id}
                            className="odd:bg-white even:bg-neutral-50 dark:odd:bg-neutral-900 dark:even:bg-neutral-800/60"
                          >
                            <td className="px-2 py-1 font-mono">{f.code}</td>
                            <td className="px-2 py-1">
                              <Badge tone={sevTone(f.severity)}>
                                {f.severity}
                              </Badge>
                            </td>
                            <td className="px-2 py-1 whitespace-nowrap">
                              {f.time
                                ? new Date(f.time).toLocaleTimeString()
                                : "—"}
                            </td>
                            <td className="px-2 py-1">
                              <button
                                className="rounded-md px-2 py-1 text-[11px] ring-1 ring-neutral-300 dark:ring-neutral-700"
                                onClick={() =>
                                  onHelp?.({
                                    id,
                                    vin,
                                    faultId: f.id,
                                    code: f.code,
                                    note: "",
                                  })
                                }
                              >
                                Help
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>

              <div className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 p-3 dark:bg-neutral-800 dark:ring-neutral-700">
                <div className="mb-2 text-xs text-neutral-500 dark:text-neutral-400">
                  Fault history
                </div>
                <div className="max-h-40 overflow-auto pr-1">
                  {histFaults.length === 0 ? (
                    <div className="text-sm text-neutral-500">No history</div>
                  ) : (
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-neutral-100 text-neutral-600 text-xs dark:bg-neutral-700/50 dark:text-neutral-300">
                        <tr>
                          <th className="text-left px-2 py-1">Time</th>
                          <th className="text-left px-2 py-1">Code</th>
                          <th className="text-left px-2 py-1">Severity</th>
                          <th className="text-left px-2 py-1">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {[...histFaults]
                          .reverse()
                          .slice(0, 50)
                          .map((f, i) => (
                            <tr
                              key={i}
                              className="odd:bg-white even:bg-neutral-50 dark:odd:bg-neutral-900 dark:even:bg-neutral-800/60"
                            >
                              <td className="px-2 py-1 whitespace-nowrap">
                                {f.time
                                  ? new Date(f.time).toLocaleString()
                                  : "—"}
                              </td>
                              <td className="px-2 py-1 font-mono">{f.code}</td>
                              <td className="px-2 py-1">
                                <Badge tone={sevTone(f.severity)}>
                                  {f.severity}
                                </Badge>
                              </td>
                              <td className="px-2 py-1">
                                {f.active ? "Active" : "Cleared"}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>

            <details className="rounded-xl ring-1 ring-neutral-200/70 bg-neutral-50 p-3 dark:bg-neutral-800 dark:ring-neutral-700">
              <summary className="cursor-pointer text-sm text-neutral-700 dark:text-neutral-200">
                Raw JSON
              </summary>
              <pre className="mt-2 max-h-64 overflow-auto text-xs text-neutral-800 dark:text-neutral-200">
                {JSON.stringify(row, null, 2)}
              </pre>
            </details>
          </div>
        </div>
      </div>
    </div>
  );
}

function Fact({ label, children }) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wide text-neutral-500 dark:text-neutral-400">
        {label}
      </div>
      <div className="text-sm">{children}</div>
    </div>
  );
}
function Action({ label, onClick, disabled }) {
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      className={cx(
        "rounded-xl px-3 py-2 text-sm ring-1 transition",
        disabled
          ? "bg-neutral-100 text-neutral-400 ring-neutral-200 dark:bg-neutral-800 dark:text-neutral-600 dark:ring-neutral-700"
          : "bg-gradient-to-br from-emerald-500 to-cyan-500 text-white ring-emerald-400/40 hover:brightness-105"
      )}
    >
      {label}
    </button>
  );
}

/* ----------------------- small components ----------------------- */

function Field({ label, children }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] uppercase tracking-wide text-neutral-500 dark:text-neutral-400">
        {label}
      </div>
      <div className="text-sm text-neutral-900 dark:text-neutral-100 min-w-0 break-words truncate">
        {children || "—"}
      </div>
    </div>
  );
}
function Mono({ value, max = 20 }) {
  if (!value) return <span>—</span>;
  const text = String(value);
  const short = truncateMiddle(text, max);
  return (
    <span
      className="font-mono text-sm text-neutral-900 dark:text-neutral-100 truncate max-w-[26ch]"
      title={text}
    >
      {short}
    </span>
  );
}
function HeadingPill({ heading }) {
  if (heading == null) return <Badge tone="neutral">—</Badge>;
  const n = ((Math.round(heading) % 360) + 360) % 360;
  const arrow =
    n >= 337.5 || n < 22.5
      ? "⬆︎"
      : n < 67.5
      ? "↗︎"
      : n < 112.5
      ? "➡︎"
      : n < 157.5
      ? "↘︎"
      : n < 202.5
      ? "⬇︎"
      : n < 247.5
      ? "↙︎"
      : n < 292.5
      ? "⬅︎"
      : "↖︎";
  return (
    <span className="inline-flex select-none items-center gap-1 rounded-xl bg-neutral-100 px-2 py-1 text-[11px] font-medium text-neutral-800 ring-1 ring-inset ring-neutral-200 dark:bg-neutral-800 dark:text-neutral-200 dark:ring-neutral-700">
      <span aria-hidden>{arrow}</span>
      <span>{Math.round(heading)}°</span>
    </span>
  );
}
function CopyBtn({
  value,
  title = "Copy",
  label = "Copy",
  disabled = false,
  onCopy,
}) {
  return (
    <button
      type="button"
      disabled={disabled || !value}
      className={cx(
        "rounded-lg px-2 py-1 text-[11px] ring-1",
        disabled || !value
          ? "text-neutral-400 bg-neutral-50 ring-neutral-200 dark:bg-neutral-900/40 dark:text-neutral-600 dark:ring-neutral-800"
          : "text-neutral-700 bg-white ring-neutral-200 hover:bg-neutral-50 dark:bg-neutral-900 dark:text-neutral-200 dark:ring-neutral-700 dark:hover:bg-neutral-800"
      )}
      title={title}
      onClick={() => {
        if (value) {
          navigator.clipboard?.writeText(String(value));
          onCopy?.();
        }
      }}
    >
      {label}
    </button>
  );
}
function Sparkline({ values = [] }) {
  const W = 260,
    H = 34,
    pad = 2,
    n = values.length;
  if (n <= 1)
    return (
      <div className="h-[34px] text-[11px] text-neutral-400 dark:text-neutral-500 flex items-center">
        No speed history
      </div>
    );
  const maxVal = Math.max(1, ...values);
  const stepX = (W - pad * 2) / (n - 1);
  const points = values
    .map((v, i) => {
      const x = pad + i * stepX;
      const y = pad + (H - pad * 2) * (1 - v / maxVal);
      return `${x},${y}`;
    })
    .join(" ");
  const last = values[n - 1];
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-[34px]">
      <polyline
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        points={points}
        className="text-neutral-400 dark:text-neutral-500"
      />
      <circle
        cx={pad + (n - 1) * stepX}
        cy={pad + (H - pad * 2) * (1 - last / maxVal)}
        r="2.5"
        className="text-neutral-700 dark:text-neutral-300"
        fill="currentColor"
      />
    </svg>
  );
}
function EmptyState({ connected, ws, snapshot }) {
  return (
    <div className="rounded-3xl border border-dashed border-neutral-300 bg-white p-10 text-center shadow-sm dark:bg-neutral-900 dark:border-neutral-800">
      <div className="mx-auto mb-3 grid h-12 w-12 place-items-center rounded-2xl bg-neutral-100 dark:bg-neutral-800">
        <span aria-hidden className="text-lg">
          🚚
        </span>
      </div>
      <h2 className="text-lg font-semibold">Waiting for trucks…</h2>
      <p className="mx-auto mt-1 max-w-md text-sm text-neutral-600 dark:text-neutral-400">
        As soon as the server sends a snapshot or updates, your live cards will
        appear here.
      </p>
      <div className="mt-4 inline-flex items-center gap-2 rounded-xl bg-neutral-50 px-3 py-1.5 text-xs text-neutral-600 ring-1 ring-neutral-200 dark:bg-neutral-800 dark:text-neutral-300 dark:ring-neutral-700">
        <StatusDot ok={connected} />
        <span className="truncate max-w-[42ch]">{ws}</span>
        <span className="text-neutral-300 dark:text-neutral-600">•</span>
        <span>snapshot: {snapshot}</span>
      </div>
    </div>
  );
}

/* ----------------------- CSV helpers ----------------------- */

function exportCsv(list) {
  if (!list.length) return;
  const cols = [
    "id",
    "vin",
    "serial",
    "time",
    "mph",
    "lat",
    "lon",
    "heading",
    "city",
    "state",
    "lastTopic",
  ];
  const header = cols.join(",");
  const rows = list.map((r) =>
    cols
      .map((c) => {
        const v = r[c] == null ? "" : String(r[c]);
        return /[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
      })
      .join(",")
  );
  downloadBlob(
    [header, ...rows].join("\n"),
    `fleet_${new Date().toISOString().slice(0, 19)}.csv`,
    "text/csv;charset=utf-8"
  );
}
function exportFaultsCsv(list) {
  const rows = [];
  rows.push(
    ["vin", "id", "code", "severity", "active", "time", "city", "state"].join(
      ","
    )
  );
  for (const r of list) {
    for (const f of r.faults?.active || []) {
      const vals = [
        r.vin || "",
        r.id || "",
        f.code || "",
        f.severity || "",
        String(!!f.active),
        f.time || "",
        r.city || "",
        r.state || "",
      ].map((v) =>
        /[",\n]/.test(v) ? `"${String(v).replace(/"/g, '""')}"` : v
      );
      rows.push(vals.join(","));
    }
  }
  downloadBlob(
    rows.join("\n"),
    `faults_${new Date().toISOString().slice(0, 19)}.csv`,
    "text/csv;charset=utf-8"
  );
}
function downloadBlob(text, name, type) {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
