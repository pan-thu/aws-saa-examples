#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const { promisify } = require("util");
const stat = promisify(fs.stat);
const readdir = promisify(fs.readdir);
const mime = require("mime");

const {
  S3Client,
  CreateBucketCommand,
  DeleteBucketCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  DeleteObjectsCommand,
  HeadBucketCommand,
} = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");

// ---------- Helpers ----------
const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-1";
const s3 = new S3Client({ region });

function usage() {
  console.log(`
Usage:
  node s3-cli.js create-bucket <bucket> [--region us-east-1]
  node s3-cli.js delete-bucket <bucket>
  node s3-cli.js list-buckets
  node s3-cli.js list-objects <bucket> [--prefix path/to/]
  node s3-cli.js put-object <bucket> <filePath> [--key path/in/bucket.ext]
  node s3-cli.js delete-objects <bucket> [--prefix path/to/] [--keys file1,file2,...]
  node s3-cli.js sync <localDir> s3://<bucket>/<optional/prefix> [--delete]

Notes:
  • Credentials come from the default AWS provider chain (env, profile, IAM role, etc.)
  • --region overrides the SDK region for create-bucket only (S3 is global but bucket location matters).
  `);
}

function arg(flag) {
  const idx = process.argv.indexOf(flag);
  return idx > -1 ? process.argv[idx + 1] : undefined;
}
function hasFlag(flag) {
  return process.argv.includes(flag);
}
function parseS3Url(url) {
  // s3://bucket/prefix/
  if (!url.startsWith("s3://")) throw new Error("S3 URL must start with s3://");
  const rest = url.slice(5);
  const slash = rest.indexOf("/");
  if (slash === -1) return { bucket: rest, prefix: "" };
  return { bucket: rest.slice(0, slash), prefix: rest.slice(slash + 1).replace(/^\/+/, "") };
}

async function bucketExists(bucket) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: bucket }));
    return true;
  } catch {
    return false;
  }
}

// ---------- Commands ----------
async function createBucket(bucket) {
  const regionOverride = arg("--region") || region;
  if (await bucketExists(bucket)) {
    console.log(`Bucket already exists: ${bucket}`);
    return;
  }
  const params = { Bucket: bucket };
  // For non-us-east-1 you must set CreateBucketConfiguration
  if (regionOverride !== "us-east-1") {
    params.CreateBucketConfiguration = { LocationConstraint: regionOverride };
  }
  await s3.send(new CreateBucketCommand(params));
  console.log(`Created bucket: s3://${bucket} (region: ${regionOverride})`);
}

async function deleteBucket(bucket) {
  // Must be empty; user can call delete-objects first
  await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
  console.log(`Deleted bucket: s3://${bucket}`);
}

async function listBuckets() {
  const res = await s3.send(new ListBucketsCommand({}));
  res.Buckets?.forEach(b => console.log(b.Name));
}

async function listAllObjects(Bucket, Prefix) {
  const out = [];
  let ContinuationToken;
  do {
    const res = await s3.send(new ListObjectsV2Command({ Bucket, Prefix, ContinuationToken }));
    (res.Contents || []).forEach(o => out.push(o));
    ContinuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (ContinuationToken);
  return out;
}

async function listObjects(bucket) {
  const prefix = arg("--prefix") || "";
  const objects = await listAllObjects(bucket, prefix);
  if (objects.length === 0) {
    console.log("(no objects)");
    return;
  }
  objects.forEach(o =>
    console.log(`${o.Key}\t${o.Size}\t${o.LastModified?.toISOString?.() || ""}`)
  );
}

async function putObject(bucket, filePath, keyArg) {
  const fileStat = await stat(filePath);
  if (!fileStat.isFile()) throw new Error("path is not a file");
  const Key = keyArg || path.basename(filePath);
  const Body = fs.createReadStream(filePath);
  const ContentType = mime.getType(filePath) || "application/octet-stream";

  // Use high-level Upload for multipart on big files
  const upload = new Upload({
    client: s3,
    params: { Bucket: bucket, Key, Body, ContentType },
    queueSize: 4,
    partSize: 8 * 1024 * 1024,
  });
  await upload.done();
  console.log(`Uploaded: ${filePath} -> s3://${bucket}/${Key}`);
}

async function deleteObjects(bucket) {
  const prefix = arg("--prefix");
  const rawKeys = arg("--keys"); // comma-separated list
  let keys = [];

  if (rawKeys) {
    keys = rawKeys.split(",").map(k => k.trim()).filter(Boolean);
  } else if (prefix !== undefined) {
    const objs = await listAllObjects(bucket, prefix);
    keys = objs.map(o => o.Key);
  } else {
    console.error("Provide either --prefix or --keys file1,file2,...");
    process.exit(2);
  }

  if (keys.length === 0) {
    console.log("(no matching objects to delete)");
    return;
  }

  // Batch in 1000 (API limit)
  for (let i = 0; i < keys.length; i += 1000) {
    const chunk = keys.slice(i, i + 1000).map(Key => ({ Key }));
    const res = await s3.send(new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: { Objects: chunk, Quiet: false },
    }));
    const deleted = res.Deleted?.length || 0;
    const errs = res.Errors?.length || 0;
    console.log(`Deleted ${deleted} (errors: ${errs}) in batch ${i / 1000 + 1}`);
    if (errs && res.Errors) {
      res.Errors.forEach(e => console.error(`  ${e.Key}: ${e.Code} ${e.Message}`));
    }
  }
}

async function walkDir(root) {
  const results = [];
  async function walk(current) {
    const entries = await readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) await walk(full);
      else if (entry.isFile()) results.push(full);
    }
  }
  await walk(root);
  return results;
}

function relToKey(localRoot, filePath, prefix) {
  const rel = path.relative(localRoot, filePath).split(path.sep).join("/");
  return prefix ? `${prefix.replace(/\/+$/, "")}/${rel}` : rel;
}

async function fetchRemoteIndex(bucket, prefix) {
  const list = await listAllObjects(bucket, prefix);
  const map = new Map();
  list.forEach(o => map.set(o.Key, { size: o.Size, mtime: o.LastModified ? new Date(o.LastModified).getTime() : 0 }));
  return map;
}

async function sync(localDir, s3Url, doDelete) {
  const { bucket, prefix } = parseS3Url(s3Url);

  // 1) make sure bucket exists
  if (!(await bucketExists(bucket))) {
    throw new Error(`Bucket does not exist: ${bucket}`);
  }

  // 2) remote index
  const remote = await fetchRemoteIndex(bucket, prefix);

  // 3) upload or update
  const files = await walkDir(localDir);
  let uploaded = 0;
  for (const file of files) {
    const st = await stat(file);
    const key = relToKey(localDir, file, prefix);
    const remoteMeta = remote.get(key);
    // Simple policy: upload if missing OR size differs OR local mtime is newer
    if (!remoteMeta || remoteMeta.size !== st.size) {
      await putObject(bucket, file, key);
      uploaded++;
    }
    // mark as seen
    remote.delete(key);
  }

  // 4) optionally delete extra remote files
  let deleted = 0;
  if (doDelete && remote.size > 0) {
    const toDelete = Array.from(remote.keys());
    // reuse deleteObjects batching
    for (let i = 0; i < toDelete.length; i += 1000) {
      const chunk = toDelete.slice(i, i + 1000).map(Key => ({ Key }));
      const res = await s3.send(new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: chunk, Quiet: false },
      }));
      deleted += res.Deleted?.length || 0;
    }
  }

  console.log(`Sync complete. Uploaded/updated: ${uploaded}${doDelete ? `, deleted: ${deleted}` : ""}`);
}

// ---------- Main ----------
(async () => {
  const cmd = process.argv[2];
  try {
    switch (cmd) {
      case "create-bucket": {
        const bucket = process.argv[3];
        if (!bucket) return usage();
        await createBucket(bucket);
        break;
      }
      case "delete-bucket": {
        const bucket = process.argv[3];
        if (!bucket) return usage();
        await deleteBucket(bucket);
        break;
      }
      case "list-buckets": {
        await listBuckets();
        break;
      }
      case "list-objects": {
        const bucket = process.argv[3];
        if (!bucket) return usage();
        await listObjects(bucket);
        break;
      }
      case "put-object": {
        const bucket = process.argv[3];
        const file = process.argv[4];
        const key = arg("--key");
        if (!bucket || !file) return usage();
        await putObject(bucket, file, key);
        break;
      }
      case "delete-objects": {
        const bucket = process.argv[3];
        if (!bucket) return usage();
        await deleteObjects(bucket);
        break;
      }
      case "sync": {
        const localDir = process.argv[3];
        const dest = process.argv[4];
        const doDelete = hasFlag("--delete");
        if (!localDir || !dest) return usage();
        await sync(localDir, dest, doDelete);
        break;
      }
      default:
        usage();
    }
  } catch (err) {
    console.error("Error:", err.message || err);
    process.exit(1);
  }
})();
