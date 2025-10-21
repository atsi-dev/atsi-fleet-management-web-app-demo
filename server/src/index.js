// server/src/index.js
import "dotenv/config.js";
import express from "express";
import http from "http";
import { Server as IOServer } from "socket.io";
import { buildKafka } from "./kafka.js";

/** ---------- Env ---------- */
const PORT = parseInt(process.env.PORT || "8080", 10);
const topics = (process.env.TOPICS || "")
  .split(",")
  .map((t) => t.trim())
  .filter(Boolean);
if (topics.length === 0) {
  console.error("No topics provided. Set TOPICS=topic1,topic2 in your .env");
  process.exit(1);
}
const groupId =
  process.env.KAFKA_GROUP_ID ||
  process.env.GROUP_ID ||
  `atsiai-realtime-${Math.random().toString(36).slice(2, 8)}`;

/** ---------- HTTP + WS ---------- */
const app = express();
const server = http.createServer(app);
const io = new IOServer(server, {
  cors: {
    origin: process.env.CORS_ORIGIN?.split(",").map((s) => s.trim()) || "*",
    methods: ["GET", "POST"],
  },
});
app.use(express.json({ limit: "2mb" }));

/** ---------- State ---------- */
const latestById = new Map(); // id -> asset snapshot
const helpRequests = [];
const partitionLastId = new Map(); // partition -> last seen id from location
const NOW = () => Date.now();

const safeJson = (buf) => {
  try {
    return typeof buf === "string"
      ? JSON.parse(buf)
      : JSON.parse(buf?.toString("utf8") ?? "{}");
  } catch {
    return null;
  }
};
const pickHeader = (headers, ...names) => {
  if (!headers) return undefined;
  for (const n of names) {
    const v = headers[n] || headers[n.toLowerCase()];
    if (v) return v.toString();
  }
  return undefined;
};

/** ---------- Identity helpers ---------- */
function extractIdentity(obj) {
  if (!obj || typeof obj !== "object") return {};
  const id = obj?.asset?.id ?? obj?.vehicle?.id ?? null;
  const vin =
    obj?.asset?.externalIds?.["samsara.vin"] ||
    obj?.vehicle?.externalIds?.["samsara.vin"];
  const serial =
    obj?.asset?.externalIds?.["samsara.serial"] ||
    obj?.vehicle?.externalIds?.["samsara.serial"];
  return { id, vin, serial };
}

function resolveIdentityFromObj(obj) {
  if (!obj || typeof obj !== "object") return {};
  const env = extractIdentity(obj);
  let id = env.id ?? obj.id ?? obj.vehicleId ?? obj.assetId ?? null;
  let vin =
    env.vin ??
    obj.vin ??
    obj.VIN ??
    obj?.vehicle?.vin ??
    obj?.asset?.vin ??
    null;
  let serial =
    env.serial ??
    obj.serial ??
    obj?.vehicle?.serial ??
    obj?.asset?.serial ??
    null;
  return { id, vin, serial };
}

/** ---------- Normalize: location / speed ---------- */
function normalizeLocation(obj, fallbackIso) {
  const { id, vin, serial } = extractIdentity(obj);
  let mph;
  if (obj?.ecuSpeedMph?.value != null) mph = Number(obj.ecuSpeedMph.value);
  if (obj?.speed?.ecuSpeedMetersPerSecond != null) {
    mph = Number(obj.speed.ecuSpeedMetersPerSecond) * 2.2369362920544;
  }
  const loc = obj?.location || {};
  const addr = loc?.address || {};
  const time =
    obj?.happenedAtTime || obj?.ecuSpeedMph?.time || obj?.time || fallbackIso;

  return {
    id,
    vin,
    serial,
    time,
    lat: loc?.latitude,
    lon: loc?.longitude,
    heading: loc?.headingDegrees,
    city: addr?.city,
    state: addr?.state,
    mph: Number.isFinite(mph) ? mph : undefined,
  };
}

/** ---------- Normalize: J1939 SPN/FMI faults ---------- */
function normalizeJ1939FaultItem(item, whenIso) {
  const spnId = item?.spnId;
  const fmiId = item?.fmiId;
  const code = [
    spnId != null ? `SPN ${spnId}` : null,
    fmiId != null ? `FMI ${fmiId}` : null,
  ]
    .filter(Boolean)
    .join(" ");

  const src = item?.sourceAddressName;
  const descBits = [
    item?.spnDescription || "",
    item?.fmiDescription ? ` — ${item.fmiDescription}` : "",
    src ? ` (${src})` : "",
  ];
  const description = descBits.join("").trim();

  const misfire =
    /misfire/i.test(item?.spnDescription || "") ||
    [1322, 1327, 1328].includes(Number(spnId));
  let severity = "info";
  if (Number(item?.milStatus) === 1) severity = "warning";
  if (Number(item?.milStatus) === 1 && misfire) severity = "critical";

  return {
    id: `${code || "UNKNOWN"}@${whenIso}`,
    code: code || "UNKNOWN",
    description: description || "Diagnostic fault",
    severity,
    active: true,
    time: whenIso,
    meta: {
      spn: spnId ?? null,
      fmi: fmiId ?? null,
      spnDescription: item?.spnDescription,
      fmiDescription: item?.fmiDescription,
      milStatus: item?.milStatus,
      occurrenceCount: item?.occurrenceCount,
      sourceAddressName: src,
      txId: item?.txId,
    },
  };
}

function deepFindSpnArray(root, maxDepth = 5) {
  if (!root || typeof root !== "object" || maxDepth < 0) return null;
  if (Array.isArray(root)) {
    if (
      root.length > 0 &&
      typeof root[0] === "object" &&
      (root[0].spnId != null || root[0].fmiId != null)
    ) {
      return root;
    }
  }
  for (const k of Object.keys(root)) {
    const v = root[k];
    if (!v) continue;
    if (Array.isArray(v)) {
      const arr = deepFindSpnArray(v, maxDepth - 1);
      if (arr) return arr;
    } else if (typeof v === "object") {
      const arr = deepFindSpnArray(v, maxDepth - 1);
      if (arr) return arr;
    }
  }
  return null;
}

function normalizeFaultPayload(body, whenIso) {
  let list = [];
  if (Array.isArray(body)) list = body;
  else if (Array.isArray(body?.faults)) list = body.faults;
  else if (Array.isArray(body?.items)) list = body.items;
  else if (Array.isArray(body?.j1939)) list = body.j1939;
  else if (body && (body.spnId != null || body.fmiId != null)) list = [body];
  else {
    const found = deepFindSpnArray(body);
    if (found) list = found;
  }
  return list.map((it) => normalizeJ1939FaultItem(it, whenIso)).filter(Boolean);
}

/** ---------- Upsert / Merge ---------- */
function upsertAssetBase(record, topic) {
  const id = record.id || record.vin || record.serial;
  if (!id) return null;

  const prev = latestById.get(id) || {};
  const merged = {
    id,
    vin: record.vin ?? prev.vin,
    serial: record.serial ?? prev.serial,
    time: record.time ?? prev.time,
    lat: record.lat ?? prev.lat,
    lon: record.lon ?? prev.lon,
    heading: record.heading ?? prev.heading,
    city: record.city ?? prev.city,
    state: record.state ?? prev.state,
    mph: record.mph ?? prev.mph,
    faults: prev.faults || { active: [], history: [], counts: {} },
    lastTopic: topic,
    lastUpdateTs: NOW(),
  };
  latestById.set(id, merged);
  return merged;
}

function handleFaultMerge(asset, faultObj) {
  if (!asset || !faultObj) return;

  asset.faults.history.push(faultObj);
  if (asset.faults.history.length > 600) asset.faults.history.shift();

  const idx = asset.faults.active.findIndex((f) => f.code === faultObj.code);
  if (faultObj.active) {
    if (idx === -1) asset.faults.active.push(faultObj);
    else asset.faults.active[idx] = faultObj;
  } else if (idx !== -1) {
    asset.faults.active.splice(idx, 1);
  }

  const counters = { critical: 0, warning: 0, info: 0, unknown: 0 };
  for (const f of asset.faults.active) {
    const s = ["critical", "warning", "info"].includes(f.severity)
      ? f.severity
      : "unknown";
    counters[s]++;
  }
  asset.faults.counts = counters;
  asset.milOn = asset.faults.active.some((f) => f.meta?.milStatus === 1);
}

/** ---------- Routes ---------- */
app.get("/health", (_, res) =>
  res.json({ ok: true, topics, groupId, size: latestById.size })
);

app.get("/state", (_, res) =>
  res.json({
    size: latestById.size,
    items: Array.from(latestById.values()).slice(0, 200),
  })
);

app.get("/faults", (_, res) => {
  const all = [];
  for (const a of latestById.values()) {
    for (const f of a.faults?.active || []) {
      all.push({
        id: a.id,
        vin: a.vin,
        serial: a.serial,
        ...f,
        city: a.city,
        state: a.state,
        lat: a.lat,
        lon: a.lon,
      });
    }
  }
  res.json({ count: all.length, items: all });
});

app.post("/help", (req, res) => {
  const { id, vin, faultId, code, note } = req.body || {};
  if (!id && !vin)
    return res.status(400).json({ ok: false, error: "id or vin required" });
  const createdAt = new Date().toISOString();
  const reqObj = {
    id,
    vin,
    faultId: faultId || null,
    code: code || null,
    note: note || "",
    createdAt,
    status: "open",
  };
  helpRequests.push(reqObj);
  io.emit("help", reqObj);
  res.json({ ok: true, request: reqObj });
});

/** ---------- Debug injectors (for testing) ---------- */
app.post("/debug/push", (req, res) => {
  const now = new Date().toISOString();
  const id = req.body?.id || `debug-${Date.now()}`;
  const mock = {
    id,
    vin: "3AKJHHDR0LSLL1245",
    serial: "G5W8BWXN9J",
    mph: Math.round(Math.random() * 65),
    city: "Testville",
    state: "TV",
    lat: 37.7749,
    lon: -122.4194,
    time: now,
  };
  const asset = upsertAssetBase(mock, "debug");
  latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });
  io.emit("update", latestById.get(asset.id));
  res.json({ ok: true, id: asset.id });
});

app.post("/debug/fault", (req, res) => {
  const now = new Date().toISOString();
  const id = req.body?.id || req.body?.vin || "1FUJHHDR9MLMJ5268";
  const vin = req.body?.vin || id;
  const payload = {
    id,
    vin,
    code: req.body?.code || "SPN 1327 FMI 11",
    description:
      req.body?.description || "Engine Cylinder 5 Misfire Rate (Engine #1)",
    severity: req.body?.severity || "critical",
    active: true,
    time: now,
  };

  const base = latestById.get(id) || { id, vin, time: now };
  const asset = upsertAssetBase(base, "debug");
  handleFaultMerge(asset, payload);
  latestById.set(id, { ...asset, lastUpdateTs: NOW() });

  io.emit("fault", { ...payload, id, vin });
  io.emit("update", latestById.get(id));
  res.json({ ok: true, injected: payload });
});

app.post("/debug/faultcodes", (req, res) => {
  const now = new Date().toISOString();
  const id = req.body?.id || req.body?.vin || "1FUJHHDR9MLMJ5268";
  const vin = req.body?.vin || id;
  const codes = Array.isArray(req.body?.codes)
    ? req.body.codes
    : [
        {
          fmiDescription: "Other Failure Mode",
          fmiId: 11,
          milStatus: 1,
          occurrenceCount: 1,
          sourceAddressName: "Engine #1",
          spnDescription: "Engine Cylinder 5 Misfire Rate",
          spnId: 1327,
          txId: 0,
        },
      ];

  const items = normalizeFaultPayload(codes, now);
  const base = latestById.get(id) || { id, vin, time: now };
  const asset = upsertAssetBase(base, "debug");
  for (const f of items) handleFaultMerge(asset, f);
  latestById.set(id, { ...asset, lastUpdateTs: NOW() });

  io.emit("faultcodes", { id, vin, codes });
  for (const f of items) io.emit("fault", { ...f, id, vin });
  io.emit("update", latestById.get(id));
  res.json({ ok: true, injected: { id, vin, count: items.length } });
});

/** ---------- Kafka ---------- */
const kafka = buildKafka();
const consumer = kafka.consumer({ groupId });

async function run() {
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const ts = message.timestamp
        ? new Date(Number(message.timestamp)).toISOString()
        : new Date().toISOString();
      const keyStr = message.key?.toString() || null;
      const headers = message.headers || {};
      const hdrVin = pickHeader(headers, "vin", "VIN");
      const hdrId = pickHeader(headers, "id", "vehicleId", "assetId");
      const obj = safeJson(message.value?.toString()) ?? {};

      console.log(
        JSON.stringify(
          {
            topic,
            partition,
            offset: message.offset,
            ts,
            key: keyStr,
            hdrVin,
            hdrId,
            valueShape: Array.isArray(obj)
              ? "array"
              : obj && typeof obj === "object"
              ? "object"
              : typeof obj,
          },
          null,
          0
        )
      );

      if (topic.toLowerCase().includes("fault")) {
        const codes = normalizeFaultPayload(obj, ts);

        let { id, vin, serial } = resolveIdentityFromObj(obj);
        if (!id)
          id =
            keyStr || hdrId || hdrVin || partitionLastId.get(partition) || null;
        if (!vin) vin = hdrVin || id || null;

        if (!id) {
          if (latestById.size === 1) {
            id = Array.from(latestById.keys())[0];
            vin = vin || latestById.get(id)?.vin || id;
          }
        }

        if (!id || codes.length === 0) {
          console.warn(
            "[faults] dropped message",
            JSON.stringify({
              reason: !id ? "missing id/vin" : "no codes found",
              partition,
              key: keyStr,
              hdrVin,
              hdrId,
              objKeys:
                obj && typeof obj === "object" ? Object.keys(obj) : typeof obj,
            })
          );
          return;
        }

        const base = upsertAssetBase({ id, vin, serial }, topic);
        for (const f of codes) {
          handleFaultMerge(base, f);
          io.emit("fault", {
            ...f,
            id: base.id,
            vin: base.vin,
            serial: base.serial,
          });
        }
        latestById.set(base.id, { ...base, lastUpdateTs: NOW() });

        // send normalized batch to client (with raw SPN/FMI fields)
        const rawForUi = codes.map((f) => ({
          spnId: f.meta?.spn ?? null,
          fmiId: f.meta?.fmi ?? null,
          spnDescription: f.meta?.spnDescription,
          fmiDescription: f.meta?.fmiDescription,
          milStatus: f.meta?.milStatus,
          occurrenceCount: f.meta?.occurrenceCount,
          sourceAddressName: f.meta?.sourceAddressName,
          txId: f.meta?.txId,
        }));
        io.emit("faultcodes", { id: base.id, vin: base.vin, codes: rawForUi });

        io.emit("update", latestById.get(base.id));
      } else {
        const rec = normalizeLocation(obj, ts);
        if (!rec.id) return;
        const asset = upsertAssetBase(rec, topic);
        latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });

        partitionLastId.set(partition, asset.id);

        io.emit("update", latestById.get(asset.id));
      }
    },
  });

  server.listen(PORT, () =>
    console.log(`HTTP+WS listening on http://localhost:${PORT}`)
  );
}

const shutdown = async (sig) => {
  console.log(`\n${sig} received; closing...`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(async (err) => {
  console.error("Fatal:", err);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(1);
  }
});
