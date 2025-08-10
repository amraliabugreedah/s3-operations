#!/usr/bin/env node
/**
 * Split large JSON into chunks, optionally gzip, and upload to S3 in parallel.
 * - AWS creds come from .env as lowercase:
 *     aws_access_key_id, aws_secret_access_key, aws_session_token (optional)
 * - Region/bucket/prefix also from .env (lowercase or uppercase supported)
 * - CLI only controls input file, mode, and compression:
 *     --input <path>  --mode jsonl|array  [--compress gzip|none]
 *
 * Install:
 *   npm i dotenv @aws-sdk/client-s3 @aws-sdk/lib-storage stream-json
 *
 * Examples:
 *   node s3-upload.js --input ./big.jsonl --mode jsonl --compress gzip
 *   node s3-upload.js --input ./big.json   --mode array --compress none
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const zlib = require("zlib");
const { promisify } = require("util");
const { S3Client } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const { StreamArray } = require("stream-json/streamers/StreamArray");
require("dotenv").config();

const gzipAsync = promisify(zlib.gzip);

// --- helpers for env (support lowercase or uppercase) ---
function getEnv(key, fallback = undefined) {
  return (
    process.env[key] ??
    process.env[key.toLowerCase()] ??
    process.env[key.toUpperCase()] ??
    fallback
  );
}

// --- required AWS creds in lowercase (fallback to uppercase if present) ---
const ACCESS_KEY_ID = getEnv("aws_access_key_id");
const SECRET_ACCESS_KEY = getEnv("aws_secret_access_key");
const SESSION_TOKEN = getEnv("aws_session_token");

// --- region + s3 config (lower/upper supported) ---
const AWS_REGION = getEnv("aws_region", getEnv("AWS_DEFAULT_REGION"));
const S3_BUCKET = getEnv("s3_bucket");
const S3_PREFIX_RAW = getEnv("s3_prefix", "");
const CHUNK_BYTES = parseInt(getEnv("chunk_bytes", getEnv("CHUNK_BYTES", `${128 * 1024 * 1024}`)), 10);
const ITEMS_PER_CHUNK = parseInt(getEnv("items_per_chunk", getEnv("ITEMS_PER_CHUNK", "200000")), 10);
const MAX_CONCURRENCY = parseInt(getEnv("max_concurrency", getEnv("MAX_CONCURRENCY", "8")), 10);
const DEFAULT_COMPRESS = (getEnv("compress", "gzip") || "gzip").toLowerCase();

// minimal validation
if (!ACCESS_KEY_ID || !SECRET_ACCESS_KEY) {
  console.error("Missing aws_access_key_id or aws_secret_access_key in .env");
  process.exit(2);
}
if (!AWS_REGION) {
  console.error("Missing aws_region (or AWS_REGION/AWS_DEFAULT_REGION) in .env");
  process.exit(2);
}
if (!S3_BUCKET) {
  console.error("Missing s3_bucket in .env");
  process.exit(2);
}

const S3_PREFIX = S3_PREFIX_RAW ? (S3_PREFIX_RAW.endsWith("/") ? S3_PREFIX_RAW : S3_PREFIX_RAW + "/") : "";

// --- CLI: input/mode/compress only ---
const args = process.argv.slice(2);
const getArg = (name, fallback = null) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
};
const inputPath = getArg("input");
const mode = getArg("mode"); // jsonl | array
const compress = (getArg("compress", DEFAULT_COMPRESS) || "gzip").toLowerCase();

if (!inputPath || !mode || !["jsonl", "array"].includes(mode)) {
  console.error("Usage: node s3-upload.js --input <file> --mode <jsonl|array> [--compress gzip|none]");
  process.exit(2);
}
if (!["gzip", "none"].includes(compress)) {
  console.error("Invalid --compress. Use: gzip | none");
  process.exit(2);
}

// --- S3 client with explicit credentials from lowercase .env ---
const s3 = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    sessionToken: SESSION_TOKEN || undefined,
  },
});

// simple concurrency pool
class ConcurrencyPool {
  constructor(limit) {
    this.limit = limit;
    this.active = 0;
    this.queue = [];
  }
  run(task) {
    return new Promise((resolve, reject) => {
      const exec = async () => {
        this.active++;
        try {
          resolve(await task());
        } catch (e) {
          reject(e);
        } finally {
          this.active--;
          if (this.queue.length) this.queue.shift()();
        }
      };
      if (this.active < this.limit) exec();
      else this.queue.push(exec);
    });
  }
}

const pool = new ConcurrencyPool(MAX_CONCURRENCY);
let uploadedCount = 0;
const startTs = Date.now();

function partKey(idx, coreExt) {
  const base = path.basename(inputPath).replace(/\.[^.]+$/, "");
  const finalExt = compress === "gzip" ? `${coreExt}.gz` : coreExt;
  return `${S3_PREFIX}${base}/part-${String(idx).padStart(5, "0")}.${finalExt}`;
}

function contentHeaders(coreType) {
  // coreType: 'jsonl'|'json'
  const contentType = coreType === "jsonl" ? "application/x-ndjson" : "application/json";
  return {
    contentType,
    contentEncoding: compress === "gzip" ? "gzip" : undefined,
  };
}

async function uploadBufferToS3(key, buffer, { contentType, contentEncoding }) {
  const uploader = new Upload({
    client: s3,
    params: {
      Bucket: S3_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: contentType,
      ...(contentEncoding ? { ContentEncoding: contentEncoding } : {}),
    },
    queueSize: Math.min(4, MAX_CONCURRENCY), // multipart concurrency
    leavePartsOnError: false,
  });
  await uploader.done();
  uploadedCount++;
  if (uploadedCount % 10 === 0) {
    const secs = ((Date.now() - startTs) / 1000).toFixed(1);
    console.error(`Uploaded ${uploadedCount} parts in ${secs}s`);
  }
}

const gzipIfNeeded = async (buf) => (compress === "gzip" ? gzipAsync(buf) : buf);

// ---- JSONL MODE ----
async function processJsonl() {
  let idx = 0;
  let accSize = 0;
  let buffers = [];

  const rl = readline.createInterface({
    input: fs.createReadStream(inputPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const headers = contentHeaders("jsonl");

  const flush = async () => {
    if (!buffers.length) return;
    const raw = Buffer.from(buffers.join(""), "utf8");
    buffers = [];
    accSize = 0;

    const payload = await gzipIfNeeded(raw);
    const key = partKey(idx, "jsonl");
    idx++;

    await pool.run(() => uploadBufferToS3(key, payload, headers));
  };

  for await (const line of rl) {
    const toAdd = line + "\n";
    const b = Buffer.byteLength(toAdd, "utf8");

    if (accSize + b > CHUNK_BYTES && accSize > 0) {
      await flush();
    }
    buffers.push(toAdd);
    accSize += b;
  }
  await flush();

  // drain
  while (pool.active > 0 || pool.queue.length > 0) {
    await new Promise((r) => setTimeout(r, 50));
  }
}

// ---- ARRAY MODE ----
async function processArray() {
  let idx = 0;
  let batch = [];
  const headers = contentHeaders("json");

  await new Promise((resolve, reject) => {
    const inStream = fs.createReadStream(inputPath, { encoding: "utf8" });
    const stream = inStream.pipe(StreamArray.withParser());

    const pushBatch = async (items) => {
      const raw = Buffer.from(JSON.stringify(items), "utf8");
      const payload = await gzipIfNeeded(raw);
      const key = partKey(idx, "json");
      idx++;
      await pool.run(() => uploadBufferToS3(key, payload, headers));
    };

    stream.on("data", ({ value }) => {
      batch.push(value);
      if (batch.length >= ITEMS_PER_CHUNK) {
        stream.pause();
        pushBatch(batch)
          .then(() => {
            batch = [];
            stream.resume();
          })
          .catch(reject);
      }
    });

    stream.on("end", async () => {
      try {
        if (batch.length) {
          await pushBatch(batch);
          batch = [];
        }
        while (pool.active > 0 || pool.queue.length > 0) {
          await new Promise((r) => setTimeout(r, 50));
        }
        resolve();
      } catch (e) {
        reject(e);
      }
    });

    stream.on("error", reject);
  });
}

// ---- RUN ----
(async () => {
  console.error(`Starting: ${inputPath} | mode=${mode} | compress=${compress} | concurrency=${MAX_CONCURRENCY}`);
  try {
    if (mode === "jsonl") await processJsonl();
    else await processArray();

    const secs = ((Date.now() - startTs) / 1000).toFixed(1);
    console.error(`Done. Uploaded ${uploadedCount} parts in ${secs}s`);
  } catch (err) {
    console.error("FAILED:", err);
    process.exit(1);
  }
})();