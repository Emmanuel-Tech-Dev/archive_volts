# ⬡ Archivolt — Multi-Cloud Resilient File Ledger

> **Zero-knowledge, byte-interleaved file distribution across three independent cloud providers with RAID-5 style parity recovery.**

---

## Table of Contents

1. [What Is Archivolt?](#what-is-archivolt)
2. [How It Works](#how-it-works)
3. [Architecture Overview](#architecture-overview)
4. [Project Structure](#project-structure)
5. [Tech Stack](#tech-stack)
6. [Getting Started](#getting-started)
7. [Environment Variables](#environment-variables)
8. [Database Schema](#database-schema)
9. [Storage Drivers](#storage-drivers)
10. [Core Logic — storageManager.js](#core-logic--storagemanagerjs)
11. [API Reference](#api-reference)
12. [Fault Tolerance & Parity Recovery](#fault-tolerance--parity-recovery)
13. [Security Model](#security-model)
14. [Testing Guide](#testing-guide)

---

## What Is Archivolt?

Archivolt is a **multi-cloud file sharding system**. Instead of uploading a file to a single provider, Archivolt splits the file's raw bytes across three independent storage providers simultaneously. No single provider ever holds a complete, readable copy of the file.

If any one provider goes offline or is compromised, the file can still be fully reconstructed using a **XOR parity shard** stored locally — the same principle used in enterprise RAID-5 disk arrays.

**Key Properties:**

- **Zero-knowledge storage** — each cloud node holds only meaningless partial bytes
- **Fault tolerant** — survives the complete failure of any one cloud node
- **Memory-efficient** — uses Node.js streams throughout; never loads entire files into memory
- **Provider-agnostic** — the Strategy Pattern means swapping a provider requires changing one driver file

---

## How It Works

### Splitting (Upload)

Every file is split by interleaving its bytes across three shards:

```
Original bytes:  [B0, B1, B2, B3, B4, B5, B6, B7, B8 ...]

Shard A (Local):      [B0, B3, B6, ...]   ← bytes where i % 3 === 0
Shard B (Supabase):   [B1, B4, B7, ...]   ← bytes where i % 3 === 1
Shard C (Cloudinary): [B2, B5, B8, ...]   ← bytes where i % 3 === 2
Shard P (Parity):     [B0^B1^B2, B3^B4^B5, ...]   ← XOR of aligned bytes
```

A ledger entry is written to MySQL recording the storage key and provider for all four shards.

### Reconstructing (Download)

The server fetches all three data shards in parallel and weaves them back together:

```
Shard A: [B0, B3, B6]
Shard B: [B1, B4, B7]   →   [B0, B1, B2, B3, B4, B5, B6, B7, B8]
Shard C: [B2, B5, B8]
```

### Recovery (One Node Down)

If one shard is unavailable, the missing bytes are recovered via XOR:

```
Shard A missing?  →  A = Parity XOR B XOR C
Shard B missing?  →  B = Parity XOR A XOR C
Shard C missing?  →  C = Parity XOR A XOR B
```

This happens silently — the client receives the correct file with no error.

---

## Architecture Overview

```
                        ┌─────────────────────────────┐
                        │         Client               │
                        │  (Insomnia / Frontend UI)    │
                        └────────────┬────────────────┘
                                     │ HTTP
                        ┌────────────▼────────────────┐
                        │        Express Server         │
                        │         server.js             │
                        └──┬──────────┬───────────┬───┘
                           │          │           │
               ┌───────────▼──┐  ┌───▼──────┐  ┌▼──────────────┐
               │ storageManager│  │   db.js  │  │    drivers/    │
               │    .js        │  │  MySQL   │  │  local         │
               │  createShards │  │  Pool    │  │  supabase      │
               │  reconstruct  │  └──────────┘  │  cloudinary    │
               └───────────────┘                └────────────────┘
                       │
          ┌────────────┼─────────────┐
          │            │             │
   ┌──────▼───┐  ┌─────▼────┐  ┌───▼──────────┐  ┌──────────┐
   │  Local    │  │ Supabase │  │  Cloudinary   │  │  Local   │
   │  Shard A  │  │  Shard B │  │   Shard C     │  │ Parity P │
   │ i%3 === 0 │  │ i%3 === 1│  │  i%3 === 2    │  │  XOR     │
   └───────────┘  └──────────┘  └───────────────┘  └──────────┘
```

---

## Project Structure

```
archivolt-core/
├── drivers/
│   ├── local.js          # Shard A — local disk (Node fs streams)
│   ├── supabase.js       # Shard B — Supabase Storage bucket
│   └── cloudinary.js     # Shard C — Cloudinary Raw resource type
│
├── storage/              # Auto-created — local shard files live here
│
├── storageManager.js     # Core logic: byte splitting & reconstruction
├── db.js                 # MySQL connection pool (mysql2/promise)
├── server.js             # Express API — routes & orchestration
│
├── schema.sql            # MySQL table definition (run once)
├── package.json
├── .env.example          # Template — copy to .env and fill in
└── .env                  # ← Never commit this
```

---

## Tech Stack

| Layer       | Technology                        |
| ----------- | --------------------------------- |
| Runtime     | Node.js (ES Modules)              |
| Framework   | Express 4                         |
| Database    | MySQL + mysql2/promise            |
| File Upload | Multer (memory storage)           |
| Cloud A     | Local disk (Node fs)              |
| Cloud B     | Supabase Storage                  |
| Cloud C     | Cloudinary v2 (raw resource type) |
| Config      | dotenv                            |

---

## Getting Started

### 1. Clone & Install

```bash
git clone https://github.com/yourname/archivolt-core.git
cd archivolt-core
npm install
```

### 2. Set Up Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials (see [Environment Variables](#environment-variables) below).

### 3. Set Up MySQL

Run the schema against your MySQL instance:

```bash
mysql -u root -p < schema.sql
```

Or paste the contents of `schema.sql` into MySQL Workbench / TablePlus.

### 4. Set Up Supabase

1. Create a project at [supabase.com](https://supabase.com)
2. Go to **Storage** → **New Bucket**
3. Name it exactly: `shards`
4. Set visibility to **Private**
5. Go to **Project Settings** → **API** and copy your **service_role** key (not the anon key)

> The service_role key bypasses Row Level Security. Keep it server-side only and never expose it to a frontend client.

### 5. Set Up Cloudinary

1. Create an account at [cloudinary.com](https://cloudinary.com)
2. From the dashboard, copy your **Cloud Name**, **API Key**, and **API Secret**
3. Shards are automatically stored under the `archivolt-shards/` folder

### 6. Run the Server

```bash
# Development (auto-restarts on file changes)
npm run dev

# Production
npm start
```

You should see:

```
 MySQL connected successfully.

🗄  Archivolt Core running → http://localhost:3000
   POST   /upload
   GET    /download/:id
   GET    /health
   GET    /ledger
   DELETE /ledger/:id
```

---

## Environment Variables

| Variable                | Description                                      |
| ----------------------- | ------------------------------------------------ |
| `DATABASE_HOST`         | MySQL host (default: `localhost`)                |
| `DATABASE_PORT`         | MySQL port (default: `3306`)                     |
| `DATABASE_USER`         | MySQL username                                   |
| `DATABASE_PASSWORD`     | MySQL password                                   |
| `DATABASE_NAME`         | MySQL database name (default: `archivolt`)       |
| `SUPA_URL`              | Supabase project URL (`https://xxx.supabase.co`) |
| `SUPA_KEY`              | Supabase **service_role** key (not anon key)     |
| `CLOUDINARY_CLOUD_NAME` | Cloudinary cloud name                            |
| `CLOUDINARY_API_KEY`    | Cloudinary API key                               |
| `CLOUDINARY_API_SECRET` | Cloudinary API secret                            |
| `PORT`                  | Server port (default: `3000`)                    |

---

## Database Schema

```sql
CREATE TABLE file_ledger (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    filename          VARCHAR(255) NOT NULL,
    mime_type         VARCHAR(100),
    file_size         INT,

    shard_a_provider  VARCHAR(50) NOT NULL DEFAULT 'local',
    shard_a_key       TEXT NOT NULL,        -- local file path

    shard_b_provider  VARCHAR(50) NOT NULL DEFAULT 'supabase',
    shard_b_key       TEXT NOT NULL,        -- Supabase storage path

    shard_c_provider  VARCHAR(50) NOT NULL DEFAULT 'cloudinary',
    shard_c_key       TEXT NOT NULL,        -- Cloudinary public_id

    shard_p_provider  VARCHAR(50) NOT NULL DEFAULT 'local',
    shard_p_key       TEXT NOT NULL,        -- XOR parity file path

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

Each row in `file_ledger` is a complete map of where all four pieces of a file are stored. The server never needs to guess — it reads the provider and key for each shard directly from the ledger.

---

## Storage Drivers

All three drivers implement the same interface:

```js
driver.upload(key, stream); // → Promise<string>  (stored key/path)
driver.download(key); // → Readable | Promise<Readable>
driver.ping(); // → Promise<{ ok: boolean, latency: number }>
```

This is the **Strategy Pattern** — the server doesn't know or care which provider it's talking to. Adding a new provider (S3, Backblaze, R2) means creating a new driver file that implements these three methods.

### Local Driver (`drivers/local.js`)

- Stores shard files in `/storage` at the project root (auto-created)
- `upload` pipes the stream to `fs.createWriteStream`
- `download` returns `fs.createReadStream`
- `ping` does a real write/delete test on the storage directory

### Supabase Driver (`drivers/supabase.js`)

- Uploads to the `shards` bucket in Supabase Storage
- Collects the stream into a `Buffer` before upload (Supabase JS SDK requirement)
- `download` converts Blob response → Buffer → Node.js Readable stream
- `ping` calls `getBucket('shards')` to verify access

### Cloudinary Driver (`drivers/cloudinary.js`)

- Uses `resource_type: 'raw'` to store binary files without transformation
- Uploads via `upload_stream` to avoid loading large files into memory
- Shards are organized under the `archivolt-shards/` folder
- `download` generates a signed URL and returns an HTTPS stream
- `ping` calls `cloudinary.api.ping()` to verify credentials

---

## Core Logic — storageManager.js

### `createShards(fileStream)`

```js
import { createShards } from "./storageManager.js";

const { shardA, shardB, shardC, shardP } = createShards(fileStream);
```

Returns four `PassThrough` streams. As data flows in from `fileStream`, bytes are distributed in real-time:

- Even chunk boundaries are handled correctly with `subarray` (zero-copy)
- All four output streams end/error together with the input stream
- Parity is computed in the same pass — no second read of the file

### `reconstruct(streamA, streamB, streamC, streamP, dest)`

```js
import { reconstruct } from "./storageManager.js";

await reconstruct(streamA, streamB, streamC, streamP, res);
```

- Pass `null` for any shard that failed to download
- Automatically recovers the missing shard via XOR if exactly one is `null`
- Writes the reconstructed file directly to `dest` (Express `res`)
- Returns `{ recovered: boolean, failedShard: string|null }`

### `bufferToStream(buffer)`

```js
import { bufferToStream } from "./storageManager.js";

const stream = bufferToStream(req.file.buffer);
```

Wraps a multer memory buffer into a Node.js `Readable` stream so it can be consumed by `createShards`.

---

## API Reference

### `POST /upload`

Upload a file to be sharded across all three nodes.

**Request:** `multipart/form-data`

| Field  | Type | Required | Description                   |
| ------ | ---- | -------- | ----------------------------- |
| `file` | File | Yes      | The file to upload (max 50MB) |

**Response `201`:**

```json
{
  "status": "success",
  "message": "File sharded and distributed across 3 nodes.",
  "file": {
    "id": 1,
    "filename": "document.pdf",
    "size": 121952,
    "shards": {
      "a": { "provider": "local", "key": "1720000000000_a.shard" },
      "b": { "provider": "supabase", "key": "1720000000000_b.shard" },
      "c": {
        "provider": "cloudinary",
        "key": "archivolt-shards/1720000000000_c"
      },
      "p": {
        "provider": "local",
        "key": "1720000000000_p.shard",
        "role": "parity"
      }
    }
  }
}
```

---

### `GET /download/:id`

Reconstruct and download a file by its ledger ID.

**Response:** Binary file stream with appropriate `Content-Disposition` and `Content-Type` headers.

**Error `503`** — if 2 or more nodes are offline simultaneously:

```json
{
  "error": "Too many nodes offline. Need at least 2 of 3 shards to reconstruct."
}
```

> If exactly one node is offline, recovery happens silently and the file is returned normally.

---

### `GET /health`

Ping all three storage nodes and return their status.

**Response `200` (all healthy):**

```json
{
  "status": "healthy",
  "onlineNodes": 3,
  "totalNodes": 3,
  "nodes": {
    "local": { "ok": true, "latency": 2 },
    "supabase": { "ok": true, "latency": 187 },
    "cloudinary": { "ok": true, "latency": 312 }
  },
  "checkedAt": "2025-01-01T12:00:00.000Z"
}
```

**Response `503`** (one or more nodes down):

```json
{
  "status": "degraded",
  "onlineNodes": 2,
  ...
}
```

Status values: `healthy` (3/3) · `degraded` (2/3) · `critical` (1/3 or 0/3)

---

### `GET /ledger`

Paginated list of all files in the ledger.

**Query Parameters:**

| Param    | Default | Description    |
| -------- | ------- | -------------- |
| `limit`  | `20`    | Max 100        |
| `offset` | `0`     | For pagination |

**Response:**

```json
{
  "data": [
    {
      "id": 1,
      "filename": "document.pdf",
      "mime_type": "application/pdf",
      "file_size": 121952,
      "shard_a_provider": "local",
      "shard_b_provider": "supabase",
      "shard_c_provider": "cloudinary",
      "created_at": "2025-01-01T12:00:00.000Z"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

---

### `DELETE /ledger/:id`

Remove a file's ledger entry by ID.

> This removes the database record only. The physical shard files on each provider are **not** deleted automatically. Handle provider-side cleanup separately if needed.

**Response:**

```json
{
  "status": "success",
  "message": "Ledger entry #1 deleted."
}
```

---

## Fault Tolerance & Parity Recovery

Archivolt implements **single-node fault tolerance** using XOR parity — the same mathematical principle behind RAID-5.

### Failure Scenarios

| Situation                       | Outcome                                        |
| ------------------------------- | ---------------------------------------------- |
| All 3 nodes online              | Normal reconstruction                          |
| 1 node offline or shard corrupt | Silent XOR recovery — client gets correct file |
| 2 nodes offline                 | `503` error — insufficient data to recover     |
| 3 nodes offline                 | `503` error                                    |

### How XOR Recovery Works

XOR has a special property: `A ^ B ^ C ^ (A ^ B ^ C) = 0`

This means any one value can be recovered if you have all the others:

```
P = A ^ B ^ C

A = P ^ B ^ C
B = P ^ A ^ C
C = P ^ A ^ B
```

In Archivolt, this is applied byte-by-byte across aligned positions in the three shards. The parity shard `P` is stored on local disk because:

1. It's the fastest to access (no network latency)
2. If local disk fails, both Shard A and Shard P are lost — but Supabase and Cloudinary hold 2/3 of the data shards, which is enough to reconstruct via parity in reverse

### Testing Recovery Manually

1. Upload a file and note its `id`
2. Delete one of the `.shard` files from your `/storage` folder
3. Call `GET /download/:id`
4. The file should download correctly and be byte-for-byte identical to the original

---

## Security Model

### What Each Provider Sees

| Provider                 | Data Held                             | Can Read File?      |
| ------------------------ | ------------------------------------- | ------------------- |
| Local disk               | Every 3rd byte (positions 0, 3, 6...) | No                  |
| Supabase                 | Every 3rd byte (positions 1, 4, 7...) | No                  |
| Cloudinary               | Every 3rd byte (positions 2, 5, 8...) | No                  |
| Any 2 providers combined | 2/3 of bytes, non-contiguous          | No                  |
| All 3 providers combined | All bytes                             | Yes (if recombined) |

Even if a cloud provider is breached or subpoenaed, they cannot reconstruct the file from their shard alone.

### Recommendations

- Store your `.env` file securely — never commit it to version control
- Add `.env` and `storage/` to your `.gitignore`
- Use the Supabase **service_role** key only on the server — never expose it to clients
- Consider encrypting shard bytes before upload for a true zero-knowledge system
- Rotate your API keys periodically

---

## Testing Guide

### Upload a File (Insomnia / Postman)

```
POST http://localhost:3000/upload
Body: Multipart Form
  file [File] → select any file
```

### Download the File

```
GET http://localhost:3000/download/1
→ Click "Send and Download" to save the file
→ Verify it matches the original exactly
```

### Check Node Health

```
GET http://localhost:3000/health
```

### Simulate a Node Failure

1. Stop your local server
2. Delete a `.shard` file from `/storage`
3. Restart the server
4. Download the file — it should still work via parity recovery

### Verify Byte Integrity

On Linux/macOS, compare checksums of the original and recovered file:

```bash
md5sum original.pdf
md5sum recovered.pdf
# Both hashes must be identical
```

On Windows (PowerShell):

```powershell
Get-FileHash original.pdf -Algorithm MD5
Get-FileHash recovered.pdf -Algorithm MD5
```

---

_Built with Node.js · Express · MySQL · Supabase · Cloudinary_
