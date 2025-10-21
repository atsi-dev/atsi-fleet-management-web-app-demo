import { z } from "zod";

const AssetLocationSchema = z.object({
  asset: z.object({
    id: z.string(),
    externalIds: z
      .object({
        "samsara.serial": z.string().optional(),
        "samsara.vin": z.string().optional(),
      })
      .partial()
      .optional(),
  }),
  happenedAtTime: z.string(),
  location: z.object({
    latitude: z.number(),
    longitude: z.number(),
    headingDegrees: z.number().optional(),
    accuracyMeters: z.number().optional(),
    address: z
      .object({
        city: z.string().optional(),
        state: z.string().optional(),
        country: z.string().optional(),
        postalCode: z.string().optional(),
        street: z.string().optional(),
        streetNumber: z.string().optional(),
        neighborhood: z.string().optional(),
        pointOfInterest: z.string().optional(),
      })
      .partial()
      .optional(),
  }),
  speed: z
    .object({
      ecuSpeedMetersPerSecond: z.number().optional(),
    })
    .partial()
    .optional(),
});

const VehicleSpeedSchema = z.object({
  vehicle: z.object({
    id: z.string(),
    externalIds: z
      .object({
        "samsara.serial": z.string().optional(),
        "samsara.vin": z.string().optional(),
      })
      .partial()
      .optional(),
  }),
  ecuSpeedMph: z.object({
    time: z.string(),
    value: z.number(),
  }),
});

function tryJSON(v) {
  if (v == null) return null;
  if (typeof v === "object") return v;
  if (Buffer.isBuffer(v)) v = v.toString("utf8");
  if (typeof v !== "string") return null;
  const trimmed = v.trim().replace(/,+$/, "");
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

export function normalizeKafkaMessage(rawValue) {
  const obj = tryJSON(rawValue);
  if (!obj) return null;

  const a = AssetLocationSchema.safeParse(obj);
  if (a.success) {
    const { asset, happenedAtTime, location, speed } = a.data;
    const mph =
      speed?.ecuSpeedMetersPerSecond != null
        ? speed.ecuSpeedMetersPerSecond * 2.2369362920544
        : undefined;

    return {
      kind: "assetLocation",
      id: asset.id,
      vin: asset.externalIds?.["samsara.vin"],
      serial: asset.externalIds?.["samsara.serial"],
      time: happenedAtTime,
      lat: location.latitude,
      lon: location.longitude,
      heading: location.headingDegrees,
      city: location.address?.city,
      state: location.address?.state,
      country: location.address?.country,
      postalCode: location.address?.postalCode,
      street: location.address?.street,
      mph,
    };
  }

  const v = VehicleSpeedSchema.safeParse(obj);
  if (v.success) {
    const { vehicle, ecuSpeedMph } = v.data;
    return {
      kind: "vehicleSpeed",
      id: vehicle.id,
      vin: vehicle.externalIds?.["samsara.vin"],
      serial: vehicle.externalIds?.["samsara.serial"],
      time: ecuSpeedMph.time,
      mph: ecuSpeedMph.value,
    };
  }
  return null;
}
