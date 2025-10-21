// server/src/index.js
import "dotenv/config.js";
import express from "express";
import http from "http";
import { Server as IOServer } from "socket.io";
import { buildKafka } from "./kafka.js";

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || "8080", 10);
const topics = (process.env.TOPICS || "")
  .split(",")
  .map((t) => t.trim())
  .filter(Boolean);

if (topics.length === 0) {
  console.error("No topics provided. Set TOPICS=topic1,topic2 in your .env");
  process.exit(1);
}

// groupId (support GROUP_ID or KAFKA_GROUP_ID)
const groupId =
  process.env.KAFKA_GROUP_ID ||
  process.env.GROUP_ID ||
  `atsiai-realtime-${Math.random().toString(36).slice(2, 8)}`;

// ---------------------------------------------------------------------------
// HTTP + WS
// ---------------------------------------------------------------------------
const app = express();
const server = http.createServer(app);
const io = new IOServer(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.use(express.json());

// In-memory state
const latestById = new Map(); // id -> asset snapshot
const helpRequests = []; // basic queue in memory

// Helpers
const NOW = () => Date.now();
const safeJson = (buf) => {
  try {
    return typeof buf === "string"
      ? JSON.parse(buf)
      : JSON.parse(buf.toString("utf8"));
  } catch {
    return null;
  }
};
const pick = (o, k, d = undefined) => (o && k in o ? o[k] : d);

function extractIdentity(obj) {
  const assetId = pick(obj, "asset.id") ?? obj?.asset?.id;
  const vehId = pick(obj, "vehicle.id") ?? obj?.vehicle?.id;
  const id = assetId ?? vehId;
  const vin =
    obj?.asset?.externalIds?.["samsara.vin"] ||
    obj?.vehicle?.externalIds?.["samsara.vin"];
  const serial =
    obj?.asset?.externalIds?.["samsara.serial"] ||
    obj?.vehicle?.externalIds?.["samsara.serial"];
  return { id, vin, serial };
}

function normalizeLocation(obj, fallbackIso) {
  // Try Samsara location payload
  const identity = extractIdentity(obj);
  let mph = undefined;

  // speed can be mph directly or meters/s
  if (obj?.ecuSpeedMph?.value != null) mph = Number(obj.ecuSpeedMph.value);
  if (obj?.speed?.ecuSpeedMetersPerSecond != null) {
    mph = Number(obj.speed.ecuSpeedMetersPerSecond) * 2.2369362920544;
  }

  const loc = obj?.location || {};
  const addr = loc?.address || {};
  const time =
    obj?.happenedAtTime || obj?.ecuSpeedMph?.time || obj?.time || fallbackIso;

  return {
    ...identity,
    time,
    lat: loc?.latitude,
    lon: loc?.longitude,
    heading: loc?.headingDegrees,
    city: addr?.city,
    state: addr?.state,
    mph: Number.isFinite(mph) ? mph : undefined,
  };
}

function normalizeFault(obj, fallbackIso) {
  // we accept shapes like {asset:{id,...}} or {vehicle:{id,...}}
  const identity = extractIdentity(obj);

  // Flexible fault fields:
  const f = obj?.fault || obj?.dtc || obj?.code || obj?.event || {};
  const code = f?.code ?? obj?.code ?? obj?.faultCode ?? "UNKNOWN";
  const description =
    f?.description ?? obj?.description ?? obj?.message ?? "No description";
  const severity = (f?.severity || obj?.severity || "unknown")
    .toString()
    .toLowerCase();
  const active =
    typeof f?.active === "boolean"
      ? f.active
      : typeof obj?.active === "boolean"
      ? obj.active
      : true;

  const time = obj?.time || obj?.happenedAtTime || fallbackIso;

  return {
    ...identity,
    fault: {
      id: `${code}:${time}`, // unique-ish
      code: String(code),
      description: String(description),
      severity, // "critical" | "warning" | "info" | "unknown"
      active: Boolean(active),
      time,
    },
  };
}

function upsertAssetBase(record, topic) {
  const id = record.id || record.vin || record.serial;
  if (!id) return null;

  const prev = latestById.get(id) || {};
  const base = {
    id,
    vin: record.vin ?? prev.vin,
    serial: record.serial ?? prev.serial,
    // location fields
    time: record.time ?? prev.time,
    lat: record.lat ?? prev.lat,
    lon: record.lon ?? prev.lon,
    heading: record.heading ?? prev.heading,
    city: record.city ?? prev.city,
    state: record.state ?? prev.state,
    mph: record.mph ?? prev.mph,
    // faults container
    faults: prev.faults || { active: [], history: [] },
    lastTopic: topic,
    lastUpdateTs: NOW(),
  };
  latestById.set(id, base);
  return base;
}

function handleFaultMerge(asset, faultObj) {
  if (!asset || !faultObj) return;

  // history append
  asset.faults.history.push(faultObj);
  if (asset.faults.history.length > 200) asset.faults.history.shift();

  // active set update (dedupe by code)
  const idx = asset.faults.active.findIndex((f) => f.code === faultObj.code);
  if (faultObj.active) {
    if (idx === -1) asset.faults.active.push(faultObj);
    else asset.faults.active[idx] = faultObj;
  } else {
    if (idx !== -1) asset.faults.active.splice(idx, 1);
  }

  // severity counters
  const counters = { critical: 0, warning: 0, info: 0, unknown: 0 };
  for (const f of asset.faults.active) {
    const s = ["critical", "warning", "info"].includes(f.severity)
      ? f.severity
      : "unknown";
    counters[s]++;
  }
  asset.faults.counts = counters;
}

// routes
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
    faultId,
    code,
    note: note || "",
    createdAt,
    status: "open",
  };
  helpRequests.push(reqObj);
  io.emit("help", reqObj); // notify UIs
  res.json({ ok: true, request: reqObj });
});

// Debug endpoint (still handy)
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

// sockets
io.on("connection", (socket) => {
  console.log(`[socket] connected: ${socket.id} snapshot=${latestById.size}`);
  socket.emit("snapshot", Array.from(latestById.values()));
  socket.on("disconnect", (reason) =>
    console.log(`[socket] disconnected: ${reason}`)
  );
});

// ---------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------
const kafka = buildKafka();
const consumer = kafka.consumer({ groupId });

async function run() {
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawStr = message.value?.toString();
      const ts = message.timestamp
        ? new Date(Number(message.timestamp)).toISOString()
        : new Date().toISOString();

      // log (compact)
      console.log(
        JSON.stringify(
          {
            topic,
            p: partition,
            off: message.offset,
            t: ts,
            key: message.key?.toString(),
          },
          null,
          0
        )
      );

      const obj = safeJson(rawStr) || {};

      if (topic.includes("fault")) {
        // ---- faults pathway
        const f = normalizeFault(obj, ts);
        if (!f.id) return;
        const asset = upsertAssetBase(f, topic);
        const faultObj = {
          id: f.fault.id,
          code: f.fault.code,
          description: f.fault.description,
          severity: f.fault.severity,
          active: f.fault.active,
          time: f.fault.time,
        };

        handleFaultMerge(asset, faultObj);
        latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });

        // broadcast both the generic asset update and specific fault event
        io.emit("update", latestById.get(asset.id));
        io.emit("fault", {
          ...faultObj,
          id: asset.id,
          vin: asset.vin,
          serial: asset.serial,
        });
      } else {
        // ---- location/speed pathway
        const rec = normalizeLocation(obj, ts);
        if (!rec.id) return;
        const asset = upsertAssetBase(rec, topic);
        latestById.set(asset.id, { ...asset, lastUpdateTs: NOW() });
        io.emit("update", latestById.get(asset.id));
      }
    },
  });

  server.listen(PORT, () => {
    console.log(`HTTP+WS listening on http://localhost:${PORT}`);
  });
}

// shutdown
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
