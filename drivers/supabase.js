// drivers/supabase.js
// -------------------------------------------------
// Shard B — Supabase Storage (file bucket)
// Bucket name: "shards" (create this in your Supabase dashboard)
// Bytes where: i % 3 === 1

import { createClient } from "@supabase/supabase-js";
import { Readable } from "stream";
import dotenv from "dotenv";

dotenv.config();

const supabase = createClient(process.env.SUPA_URL, process.env.SUPA_KEY);

const BUCKET = "shards";

const supabaseDriver = {
  name: "supabase",

  /**
   * Upload a shard stream to Supabase Storage
   * @param {string} key  - storage path (e.g. "1720000000000_b.shard")
   * @param {Readable} stream - readable stream of shard bytes
   * @returns {Promise<string>} resolves with the storage path on success
   */
  async upload(key, stream) {
    // Supabase Storage expects a Buffer or Blob — collect stream first
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);

    const { data, error } = await supabase.storage
      .from(BUCKET)
      .upload(key, buffer, {
        contentType: "application/octet-stream",
        upsert: true,
      });

    if (error) {
      throw new Error(
        `[supabase] Upload failed for "${key}": ${error.message}`,
      );
    }

    return data.path;
  },

  /**
   * Download a shard from Supabase Storage
   * @param {string} key - storage path
   * @returns {Promise<Readable>} readable stream of shard bytes
   */
  async download(key) {
    const { data, error } = await supabase.storage.from(BUCKET).download(key);

    if (error) {
      throw new Error(
        `[supabase] Download failed for "${key}": ${error.message}`,
      );
    }

    // Convert Blob → Node.js Readable stream
    const arrayBuffer = await data.arrayBuffer();
    return Readable.from(Buffer.from(arrayBuffer));
  },

  /**
   * Health check — verify bucket is accessible
   * @returns {Promise<{ ok: boolean, latency: number }>}
   */
  async ping() {
    const start = Date.now();
    try {
      const { error } = await supabase.storage.getBucket(BUCKET);
      if (error) throw error;
      return { ok: true, latency: Date.now() - start };
    } catch (err) {
      return { ok: false, latency: Date.now() - start, error: err.message };
    }
  },
};

export default supabaseDriver;
