// storageManager.js
// -------------------------------------------------
// Core shard logic for Archivolt — RAID-5 style
//
// SPLIT:       file bytes → 3 interleaved shards + 1 XOR parity shard
// RECONSTRUCT: 3 shards → original file (auto-recovers if 1 shard is missing)
//
// Shard formula:
//   Shard A (local)      → bytes where i % 3 === 0
//   Shard B (supabase)   → bytes where i % 3 === 1
//   Shard C (cloudinary) → bytes where i % 3 === 2
//   Shard P (local)      → XOR of every aligned byte across A, B, C
//
// Recovery formula (if one shard is lost):
//   Missing = P XOR the other two shards (byte by byte)

import { PassThrough, Readable } from "stream";

// ─────────────────────────────────────────────────
// SPLIT — interleave file into 3 shards + parity
// ─────────────────────────────────────────────────

/**
 * Splits a readable file stream into 3 data shards and 1 XOR parity shard.
 *
 * @param {Readable} fileStream
 * @returns {{ shardA, shardB, shardC, shardP }} — 4 PassThrough streams
 */
export function createShards(fileStream) {
  const shardA = new PassThrough(); // local      — i % 3 === 0
  const shardB = new PassThrough(); // supabase   — i % 3 === 1
  const shardC = new PassThrough(); // cloudinary — i % 3 === 2
  const shardP = new PassThrough(); // parity     — XOR of A, B, C (local)

  fileStream.on("data", (chunk) => {
    const lenA = Math.ceil(chunk.length / 3);
    const lenB = Math.ceil((chunk.length - 1) / 3);
    const lenC = Math.ceil((chunk.length - 2) / 3);
    const lenP = Math.max(lenA, lenB, lenC); // parity same length as largest shard

    const bufA = Buffer.allocUnsafe(Math.max(0, lenA));
    const bufB = Buffer.allocUnsafe(Math.max(0, lenB));
    const bufC = Buffer.allocUnsafe(Math.max(0, lenC));
    const bufP = Buffer.alloc(lenP, 0); // zero-filled so XOR is clean

    let aIdx = 0,
      bIdx = 0,
      cIdx = 0;

    for (let i = 0; i < chunk.length; i++) {
      const mod = i % 3;
      if (mod === 0) bufA[aIdx++] = chunk[i];
      else if (mod === 1) bufB[bIdx++] = chunk[i];
      else bufC[cIdx++] = chunk[i];
    }

    // XOR parity: P[i] = A[i] ^ B[i] ^ C[i]
    for (let i = 0; i < lenP; i++) {
      bufP[i] =
        (i < aIdx ? bufA[i] : 0) ^
        (i < bIdx ? bufB[i] : 0) ^
        (i < cIdx ? bufC[i] : 0);
    }

    if (aIdx > 0) shardA.write(bufA.subarray(0, aIdx));
    if (bIdx > 0) shardB.write(bufB.subarray(0, bIdx));
    if (cIdx > 0) shardC.write(bufC.subarray(0, cIdx));
    shardP.write(bufP);
  });

  fileStream.on("end", () => {
    shardA.end();
    shardB.end();
    shardC.end();
    shardP.end();
  });

  fileStream.on("error", (err) => {
    shardA.destroy(err);
    shardB.destroy(err);
    shardC.destroy(err);
    shardP.destroy(err);
  });

  return { shardA, shardB, shardC, shardP };
}

// ─────────────────────────────────────────────────
// COLLECT — drain a stream into a single Buffer
// ─────────────────────────────────────────────────

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

// ─────────────────────────────────────────────────
// RECOVER — rebuild a missing shard via XOR
// missingIndex: 0=A, 1=B, 2=C
// ─────────────────────────────────────────────────

function recoverShard(present1, present2, parity) {
  // recovered = P XOR present1 XOR present2
  const len = parity.length;
  const recovered = Buffer.alloc(len, 0);
  for (let i = 0; i < len; i++) {
    recovered[i] =
      parity[i] ^
      (i < present1.length ? present1[i] : 0) ^
      (i < present2.length ? present2[i] : 0);
  }
  return recovered;
}

// ─────────────────────────────────────────────────
// RECONSTRUCT — weave 3 shards back into one file
// Silently recovers if exactly one shard is null/failed
// ─────────────────────────────────────────────────

/**
 * @param {Readable|null} streamA  - null if node A is down
 * @param {Readable|null} streamB  - null if node B is down
 * @param {Readable|null} streamC  - null if node C is down
 * @param {Readable}      streamP  - parity shard (always local, always available)
 * @param {Writable}      dest     - Express res or any writable
 * @returns {Promise<{ recovered: boolean, failedShard: string|null }>}
 */
export async function reconstruct(streamA, streamB, streamC, streamP, dest) {
  // Collect all available shards into buffers first
  const [bufA, bufB, bufC, bufP] = await Promise.all([
    streamA ? streamToBuffer(streamA) : null,
    streamB ? streamToBuffer(streamB) : null,
    streamC ? streamToBuffer(streamC) : null,
    streamToBuffer(streamP),
  ]);

  let recoveredA = bufA;
  let recoveredB = bufB;
  let recoveredC = bufC;
  let recovered = false;
  let failedShard = null;

  // Detect which shard is missing and recover it
  if (!bufA) {
    recoveredA = recoverShard(bufB, bufC, bufP);
    recovered = true;
    failedShard = "A (local)";
  } else if (!bufB) {
    recoveredB = recoverShard(bufA, bufC, bufP);
    recovered = true;
    failedShard = "B (supabase)";
  } else if (!bufC) {
    recoveredC = recoverShard(bufA, bufB, bufP);
    recovered = true;
    failedShard = "C (cloudinary)";
  }

  // All 3 shards are now available — interleave them back
  const maxLen = Math.max(
    recoveredA.length,
    recoveredB.length,
    recoveredC.length,
  );
  const output = Buffer.allocUnsafe(
    recoveredA.length + recoveredB.length + recoveredC.length,
  );

  let outIdx = 0;
  for (let i = 0; i < maxLen; i++) {
    if (i < recoveredA.length) output[outIdx++] = recoveredA[i];
    if (i < recoveredB.length) output[outIdx++] = recoveredB[i];
    if (i < recoveredC.length) output[outIdx++] = recoveredC[i];
  }

  dest.write(output.subarray(0, outIdx));
  dest.end();

  return { recovered, failedShard };
}

// ─────────────────────────────────────────────────
// HELPER — Buffer → Readable (for multer memory uploads)
// ─────────────────────────────────────────────────

export function bufferToStream(buffer) {
  return Readable.from(buffer);
}
