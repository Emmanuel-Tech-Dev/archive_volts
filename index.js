// server.js
// -------------------------------------------------
// Archivolt Express API
// Routes:
//   POST   /upload           — shard & distribute a file
//   GET    /download/:id     — reconstruct & stream a file
//   GET    /health           — ping all 3 nodes
//   GET    /ledger           — recent file_ledger entries
//   DELETE /ledger/:id       — delete a ledger entry + shards

import express from "express";
import multer from "multer";
import cors from "cors";
import dotenv from "dotenv";
import { Readable } from "stream";

import db, { testConnection } from "./services/dbconnection.js";
import localDriver from "./drivers/local.js";
import supabaseDriver from "./drivers/supabase.js";
import cloudinaryDriver from "./drivers/cloudinary.js";
import {
  createShards,
  reconstruct,
  bufferToStream,
} from "./services/storageManager.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// ─────────────────────────────────────────────────
// Middleware
// ─────────────────────────────────────────────────

app.use(cors());
app.use(express.json());

// Multer — store upload in memory so we can stream it
// Max file size: 50MB (adjust as needed)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
});

// ─────────────────────────────────────────────────
// Driver map — used for dynamic lookup on download
// ─────────────────────────────────────────────────

const DRIVERS = {
  local: localDriver,
  supabase: supabaseDriver,
  cloudinary: cloudinaryDriver,
};

// ─────────────────────────────────────────────────
// POST /upload
// Accepts: multipart/form-data with field "file"
// Splits file into 3 shards and distributes them
// ─────────────────────────────────────────────────

app.post("/upload", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res
      .status(400)
      .json({ error: 'No file provided. Use field name "file".' });
  }

  try {
    const timestamp = Date.now();
    const keyA = `${timestamp}_a.shard`;
    const keyB = `${timestamp}_b.shard`;
    const keyC = `${timestamp}_c.shard`;
    const keyP = `${timestamp}_p.shard`; // XOR parity — always local

    // Wrap buffer → stream then split into 3 data shards + 1 parity shard
    const fileStream = bufferToStream(req.file.buffer);
    const { shardA, shardB, shardC, shardP } = createShards(fileStream);

    // Upload all 4 shards in parallel
    const [storedKeyA, storedKeyB, storedKeyC, storedKeyP] = await Promise.all([
      localDriver.upload(keyA, shardA),
      supabaseDriver.upload(keyB, shardB),
      cloudinaryDriver.upload(keyC, shardC),
      localDriver.upload(keyP, shardP), // parity always on local disk
    ]);

    // Record all 4 shard locations in ledger
    const [result] = await db.execute(
      `INSERT INTO file_ledger
        (filename, mime_type, file_size,
         shard_a_provider, shard_a_key,
         shard_b_provider, shard_b_key,
         shard_c_provider, shard_c_key,
         shard_p_provider, shard_p_key)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        req.file.originalname,
        req.file.mimetype,
        req.file.size,
        "local",
        storedKeyA,
        "supabase",
        storedKeyB,
        "cloudinary",
        storedKeyC,
        "local",
        storedKeyP,
      ],
    );

    return res.status(201).json({
      status: "success",
      message: "File sharded and distributed across 3 nodes.",
      file: {
        id: result.insertId,
        filename: req.file.originalname,
        size: req.file.size,
        shards: {
          a: { provider: "local", key: storedKeyA },
          b: { provider: "supabase", key: storedKeyB },
          c: { provider: "cloudinary", key: storedKeyC },
          p: { provider: "local", key: storedKeyP, role: "parity" },
        },
      },
    });
  } catch (err) {
    console.error("[upload] Error:", err.message);
    return res
      .status(500)
      .json({ error: "Upload failed.", detail: err.message });
  }
});

// ─────────────────────────────────────────────────
// GET /download/:id
// Fetches shard locations from ledger, reconstructs
// the original file and streams it to the client
// ─────────────────────────────────────────────────

app.get("/download/:id", async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM file_ledger WHERE id = ?", [
      req.params.id,
    ]);

    if (!rows.length) {
      return res.status(404).json({ error: "File not found in ledger." });
    }

    const file = rows[0];

    // Attempt to fetch all 3 data shards — null means that node failed
    const driverA = DRIVERS[file.shard_a_provider];
    const driverB = DRIVERS[file.shard_b_provider];
    const driverC = DRIVERS[file.shard_c_provider];
    const driverP = DRIVERS[file.shard_p_provider];

    if (!driverP) {
      return res
        .status(500)
        .json({ error: "Parity driver not found in ledger." });
    }

    // Try each data shard — catch failures and return null (triggers recovery)
    const tryDownload = async (driver, key) => {
      try {
        return await Promise.resolve(driver.download(key));
      } catch {
        return null;
      }
    };

    const [streamA, streamB, streamC, streamP] = await Promise.all([
      tryDownload(driverA, file.shard_a_key),
      tryDownload(driverB, file.shard_b_key),
      tryDownload(driverC, file.shard_c_key),
      Promise.resolve(driverP.download(file.shard_p_key)), // parity must always succeed
    ]);

    // Count how many data shards are available
    const available = [streamA, streamB, streamC].filter(Boolean).length;

    if (available < 2) {
      return res.status(503).json({
        error:
          "Too many nodes offline. Need at least 2 of 3 shards to reconstruct.",
      });
    }

    // Set download headers
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${file.filename}"`,
    );
    res.setHeader("Content-Type", file.mime_type || "application/octet-stream");

    // Reconstruct — silently recovers any single missing shard via XOR parity
    await reconstruct(streamA, streamB, streamC, streamP, res);
  } catch (err) {
    console.error("[download] Error:", err.message);
    // Only send error if headers haven't been sent yet
    if (!res.headersSent) {
      res.status(500).json({ error: "Download failed.", detail: err.message });
    }
  }
});

// ─────────────────────────────────────────────────
// GET /health
// Pings all 3 storage nodes and returns their status
// ─────────────────────────────────────────────────

app.get("/health", async (req, res) => {
  const [local, supabase, cloudinary] = await Promise.allSettled([
    localDriver.ping(),
    supabaseDriver.ping(),
    cloudinaryDriver.ping(),
  ]);

  const results = {
    local:
      local.status === "fulfilled"
        ? local.value
        : { ok: false, error: local.reason?.message },
    supabase:
      supabase.status === "fulfilled"
        ? supabase.value
        : { ok: false, error: supabase.reason?.message },
    cloudinary:
      cloudinary.status === "fulfilled"
        ? cloudinary.value
        : { ok: false, error: cloudinary.reason?.message },
  };

  const onlineCount = Object.values(results).filter((r) => r.ok).length;
  const meshStatus =
    onlineCount === 3 ? "healthy" : onlineCount >= 2 ? "degraded" : "critical";

  return res.status(onlineCount >= 2 ? 200 : 503).json({
    status: meshStatus,
    onlineNodes: onlineCount,
    totalNodes: 3,
    nodes: results,
    checkedAt: new Date().toISOString(),
  });
});

// ─────────────────────────────────────────────────
// GET /ledger?limit=20&offset=0
// Returns paginated file_ledger entries
// ─────────────────────────────────────────────────

app.get("/ledger", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;

    const [rows] = await db.execute(
      `SELECT id, filename, mime_type, file_size,
              shard_a_provider, shard_b_provider, shard_c_provider,
              created_at
       FROM file_ledger
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset],
    );

    const [[{ total }]] = await db.execute(
      "SELECT COUNT(*) AS total FROM file_ledger",
    );

    return res.json({
      data: rows,
      total,
      limit,
      offset,
    });
  } catch (err) {
    console.error("[ledger] Error:", err.message);
    return res
      .status(500)
      .json({ error: "Failed to fetch ledger.", detail: err.message });
  }
});

// ─────────────────────────────────────────────────
// DELETE /ledger/:id
// Removes ledger entry (does NOT delete shards from
// cloud storage — handle that separately if needed)
// ─────────────────────────────────────────────────

app.delete("/ledger/:id", async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM file_ledger WHERE id = ?", [
      req.params.id,
    ]);

    if (!rows.length) {
      return res.status(404).json({ error: "File not found in ledger." });
    }

    await db.execute("DELETE FROM file_ledger WHERE id = ?", [req.params.id]);

    return res.json({
      status: "success",
      message: `Ledger entry #${req.params.id} deleted.`,
    });
  } catch (err) {
    console.error("[delete] Error:", err.message);
    return res
      .status(500)
      .json({ error: "Delete failed.", detail: err.message });
  }
});

// ─────────────────────────────────────────────────
// 404 fallback
// ─────────────────────────────────────────────────

app.use((req, res) => {
  res.status(404).json({ error: `Route not found: ${req.method} ${req.path}` });
});

// ─────────────────────────────────────────────────
// Start server — test DB connection first
// ─────────────────────────────────────────────────

async function start() {
  await testConnection();
  app.listen(PORT, () => {
    console.log(`\n🗄  Archivolt Core running → http://localhost:${PORT}`);
    console.log(`   POST   /upload`);
    console.log(`   GET    /download/:id`);
    console.log(`   GET    /health`);
    console.log(`   GET    /ledger`);
    console.log(`   DELETE /ledger/:id\n`);
  });
}

start();
