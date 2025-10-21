import { Kafka, logLevel } from "kafkajs";

function parseList(envName, def = "") {
  return (process.env[envName] || def)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function buildKafka() {
  const brokers = parseList("BOOTSTRAP_SERVERS");
  const ssl = (process.env.SSL || "true").toLowerCase() === "true";
  const rejectUnauth =
    (process.env.REJECT_UNAUTHORIZED || "true").toLowerCase() === "true";
  const useSCRAM = (process.env.USE_SCRAM || "false").toLowerCase() === "true";
  const useAwsIam =
    (process.env.USE_AWS_IAM || "false").toLowerCase() === "true";

  /** @type {import('kafkajs').KafkaConfig} */
  const cfg = {
    clientId: process.env.KAFKA_CLIENT_ID || "atsiai-realtime",
    brokers,
    ssl: ssl ? { rejectUnauthorized: rejectUnauth } : undefined,
    logLevel: logLevel.INFO,
  };

  if (useSCRAM && useAwsIam) {
    throw new Error(
      "Set only one of USE_SCRAM or USE_AWS_IAM to true, not both."
    );
  }

  if (useSCRAM) {
    const username = process.env.SCRAM_USERNAME;
    const password = process.env.SCRAM_PASSWORD;
    if (!username || !password) {
      throw new Error(
        "SCRAM enabled but SCRAM_USERNAME / SCRAM_PASSWORD not set."
      );
    }
    cfg.sasl = {
      mechanism: "scram-sha-512", // KafkaJS supports SCRAM-SHA-256 and SCRAM-SHA-512
      username,
      password,
    };
  } else if (useAwsIam) {
    if (!ssl) {
      throw new Error("USE_AWS_IAM requires SSL/TLS. Set SSL=true.");
    }
    // KafkaJS supports 'aws' mechanism natively.
    // If AWS creds are in env/role, you can omit explicit keys below.
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    const sessionToken = process.env.AWS_SESSION_TOKEN; // optional
    const authorizationIdentity = process.env.AWS_AUTHORIZATION_IDENTITY; // optional (aws:userid)

    cfg.sasl = {
      mechanism: "aws",
      ...(authorizationIdentity ? { authorizationIdentity } : {}),
      ...(accessKeyId && secretAccessKey
        ? {
            accessKeyId,
            secretAccessKey,
            ...(sessionToken ? { sessionToken } : {}),
          }
        : {}),
    };
  }

  return new Kafka(cfg);
}
