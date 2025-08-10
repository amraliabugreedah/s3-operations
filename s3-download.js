#!/usr/bin/env node
"use strict";
/**
 * Download chunked parts from S3, gunzip if needed, and merge to a single local file.
 *
 * Modes:
 *   --mode jsonl  : each S3 object is NDJSON; output can be array (default) or NDJSON (--out-format jsonl)
 *   --mode array  : each S3 object is JSON array; output can be array (default) or NDJSON (--out-format jsonl)
 *
 * Usage:
 *   node download-merge-s3.js --output ./merged.json --mode jsonl [--out-format array|jsonl] [--compress gzip|none]
 *   node download-merge-s3.js --output ./merged.json --mode array [--out-format array|jsonl] [--compress gzip|none]
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const readline = require("readline");
const { promisify } = require("util");
const { Readable } = require("stream");
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { StreamArray } = require("stream-json/streamers/StreamArray");
require("dotenv").config();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// -------- ENV HELPERS (prefer lowercase, fallback to UPPER) --------
function getEnv(key, fallback = undefined) {
  return process.env[key] ?? process.env[key.toLowerCase()] ?? process.env[key.toUpperCase()] ?? fallback;
}

const AWS_REGION = getEnv("aws_region", getEnv("AWS_DEFAULT_REGION"));
const S3_BUCKET = getEnv("s3_bucket");
const RAW_PREFIX = getEnv("s3_prefix", "");
const S3_PREFIX = RAW_PREFIX ? (RAW_PREFIX.endsWith("/") ? RAW_PREFIX : RAW_PREFIX + "/") : "";

const ACCESS_KEY_ID = getEnv("aws_access_key_id");
const SECRET_ACCESS_KEY = getEnv("aws_secret_access_key");
const SESSION_TOKEN = getEnv("aws_session_token");

// Validate
if (!ACCESS_KEY_ID || !SECRET_ACCESS_KEY) {
  console.error("Missing aws_access_key_id or aws_secret_access_key in .env");
  process.exit(2);
}
if (!AWS_REGION) {
  console.error("Missing aws_region (or AWS_DEFAULT_REGION) in .env");
  process.exit(2);
}
if (!S3_BUCKET) {
  console.error("Missing s3_bucket in .env");
  process.exit(2);
}

// -------- CLI PARSE --------
const args = process.argv.slice(2);
const getArg = (name, fallback = null) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
};

const outPath = getArg("output");
const mode = getArg("mode"); // "jsonl" | "array"
const outFormat = (getArg("out-format", "array") || "array").toLowerCase(); // "array" | "jsonl"
const compressOverride = getArg("compress", null); // "gzip" | "none" | null (auto)

if (!outPath || !mode || !["jsonl", "array"].includes(mode)) {
  console.error("Usage: node download-merge-s3.js --output <file> --mode <jsonl|array> [--out-format array|jsonl] [--compress gzip|none]");
  process.exit(2);
}
if (!["array", "jsonl"].includes(outFormat)) {
  console.error("Invalid --out-format. Use: array | jsonl");
  process.exit(2);
}
if (compressOverride && !["gzip", "none"].includes(compressOverride)) {
  console.error("Invalid --compress. Use: gzip | none");
  process.exit(2);
}

// -------- S3 CLIENT --------
const s3 = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    sessionToken: SESSION_TOKEN || undefined,
  },
});

// -------- HELPERS --------
async function listAllKeys(bucket, prefix) {
  let token = undefined;
  const keys = [];
  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: token,
      })
    );
    (resp.Contents || []).forEach((o) => {
      if (o.Key && /part-\d{5}\./.test(o.Key)) keys.push(o.Key);
    });
    token = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (token);
  keys.sort(); // ensure part-00000, part-00001, ...
  return keys;
}

function isGzippedKeyOrHeader(key, contentEncoding) {
  if (compressOverride === "gzip") return true;
  if (compressOverride === "none") return false;
  return (contentEncoding || "").toLowerCase() === "gzip" || /\.gz$/i.test(key);
}

async function getBodyStream(bucket, key) {
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = resp.Body; // Node Readable
  const gz = isGzippedKeyOrHeader(key, resp.ContentEncoding);
  if (gz) {
    const gunzip = zlib.createGunzip();
    // Forward errors
    body.on("error", (e) => gunzip.emit("error", e));
    return body.pipe(gunzip);
  }
  return body;
}

function isReadable(obj) {
  return obj && typeof obj.pipe === "function";
}

// -------- MERGE WRITERS --------
async function mergeJsonl(keys, outFile, asArray) {
  let wroteAny = false;
  let count = 0;

  const out = fs.createWriteStream(outFile, { encoding: "utf8" });
  if (asArray) out.write("[");

  for (const key of keys) {
    const stream = await getBodyStream(S3_BUCKET, key);
    if (!isReadable(stream)) throw new Error(`S3 Body for ${key} is not a stream`);

    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      const trimmed = line.trim();
      if (!trimmed) continue; // skip empty
      if (asArray) {
        if (wroteAny) out.write(",");
        out.write(trimmed);
      } else {
        out.write(trimmed + "\n");
      }
      wroteAny = true;
      count++;
    }
    console.error(`Merged: ${key}`);
  }

  if (asArray) out.write("]\n");
  await new Promise((res) => out.end(res));
  console.error(`✅ Merge complete. Items: ${count}. Output: ${outFile}`);
}

async function mergeArrayChunks(keys, outFile, asArray) {
  let wroteAny = false;
  let count = 0;

  const out = fs.createWriteStream(outFile, { encoding: "utf8" });
  if (asArray) out.write("[");

  for (const key of keys) {
    const stream = await getBodyStream(S3_BUCKET, key);
    if (!isReadable(stream)) throw new Error(`S3 Body for ${key} is not a stream`);

    await new Promise((resolve, reject) => {
      const parser = StreamArray.withParser();
      parser.on("data", ({ value }) => {
        if (asArray) {
          if (wroteAny) out.write(",");
          out.write(JSON.stringify(value));
        } else {
          out.write(JSON.stringify(value) + "\n");
        }
        wroteAny = true;
        count++;
      });
      parser.on("end", resolve);
      parser.on("error", reject);
      stream.pipe(parser);
    });
    console.error(`Merged: ${key}`);
  }

  if (asArray) out.write("]\n");
  await new Promise((res) => out.end(res));
  console.error(`✅ Merge complete. Items: ${count}. Output: ${outFile}`);
}

// -------- MAIN --------
(async () => {
  try {
    const absOut = path.resolve(outPath);
    console.error(`Listing parts under s3://${S3_BUCKET}/${S3_PREFIX} ...`);
    const keys = await listAllKeys(S3_BUCKET, S3_PREFIX);
    if (keys.length === 0) {
      console.error("No part-* objects found under the given s3_prefix.");
      process.exit(1);
    }
    console.error(`Found ${keys.length} parts. Writing to: ${absOut}`);
    const asArray = outFormat === "array";

    if (mode === "jsonl") {
      await mergeJsonl(keys, absOut, asArray);
    } else {
      await mergeArrayChunks(keys, absOut, asArray);
    }
    console.error("🎉 Done.");
  } catch (err) {
    console.error("FAILED:", err);
    process.exit(1);
  }
})();