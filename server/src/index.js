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
  cors: { origin: "*", methods: ["GET", "POST"] },
});
app.use(express.json());

const latestById = new Map(); // id -> asset snapshot (loc/speed/faults/etc.)
const helpRequests = []; // in-memory

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
  // item sample fields: spnId, fmiId, spnDescription, fmiDescription, milStatus, sourceAddressName...
  const spnId = item?.spnId;
  const fmiId = item?.fmiId;
  const code = [
    spnId != null ? `SPN ${spnId}` : null,
    fmiId != null ? `FMI ${fmiId}` : null,
  ]
    .filter(Boolean)
    .join(" / ");

  const description = [
    item?.spnDescription || "",
    item?.fmiDescription ? ` — ${item.fmiDescription}` : "",
    item?.sourceAddressName ? ` (${item.sourceAddressName})` : "",
  ]
    .join("")
    .trim();

  const severity = Number(item?.milStatus) === 1 ? "critical" : "warning";

  return {
    id: `${code || "UNKNOWN"}@${whenIso}`,
    code: code || "UNKNOWN",
    description: description || "Unknown fault",
    severity,
    active: true,
    time: whenIso,
  };
}

function normalizeFaultPayload(obj, whenIso, kafkaKey) {
  // identity first
  let { id, vin, serial } = extractIdentity(obj);
  if (!id) id = kafkaKey || null;

  // find array of fault items
  let list = [];
  if (Array.isArray(obj)) list = obj;
  else if (Array.isArray(obj?.faults)) list = obj.faults;
  else if (Array.isArray(obj?.items)) list = obj.items;
  else if (Array.isArray(obj?.j1939)) list = obj.j1939;
  else if (obj?.spnId != null) list = [obj]; // single item

  const faults = list
    .map((it) => normalizeJ1939FaultItem(it, whenIso))
    .filter(Boolean);
  return { id, vin, serial, faults };
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

  // history
  asset.faults.history.push(faultObj);
  if (asset.faults.history.length > 600) asset.faults.history.shift();

  // active: dedupe by code
  const idx = asset.faults.active.findIndex((f) => f.code === faultObj.code);
  if (faultObj.active) {
    if (idx === -1) asset.faults.active.push(faultObj);
    else asset.faults.active[idx] = faultObj;
  } else if (idx !== -1) {
    asset.faults.active.splice(idx, 1);
  }

  // counts
  const counters = { critical: 0, warning: 0, info: 0, unknown: 0 };
  for (const f of asset.faults.active) {
    const s = ["critical", "warning", "info"].includes(f.severity)
      ? f.severity
      : "unknown";
    counters[s]++;
  }
  asset.faults.counts = counters;
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

// optional debug
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

io.on("connection", (socket) => {
  console.log(`[socket] connected: ${socket.id} snapshot=${latestById.size}`);
  socket.emit("snapshot", Array.from(latestById.values()));
  socket.on("disconnect", (reason) =>
    console.log(`[socket] disconnected: ${reason}`)
  );
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
      const key = message.key?.toString() || null;
      const obj = safeJson(message.value?.toString()) ?? {};

      console.log(
        JSON.stringify(
          { topic, p: partition, off: message.offset, t: ts, key },
          null,
          0
        )
      );

      if (topic.toLowerCase().includes("fault")) {
        // ---- faults ----
        const { id, vin, serial, faults } = normalizeFaultPayload(obj, ts, key);
        if (!id || faults.length === 0) return;
        const asset = upsertAssetBase({ id, vin, serial }, topic);
        for (const f of faults) {
          handleFaultMerge(asset, f);
          io.emit("fault", {
            ...f,
            id: asset.id,
            vin: asset.vin,
            serial: asset.serial,
          });
        }
        latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });
        io.emit("update", latestById.get(asset.id));
      } else {
        // ---- location / speed ----
        const rec = normalizeLocation(obj, ts);
        if (!rec.id) return;
        const asset = upsertAssetBase(rec, topic);
        latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });
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
