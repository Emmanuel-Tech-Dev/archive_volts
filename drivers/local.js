// drivers/local.js
// -------------------------------------------------
// Shard A — local disk storage
// Stores shards in /storage folder at project root
// Bytes where: i % 3 === 0

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STORAGE_DIR = path.join(__dirname, "../storage");

// Ensure storage folder exists on startup
if (!fs.existsSync(STORAGE_DIR)) {
  fs.mkdirSync(STORAGE_DIR, { recursive: true });
}

const localDriver = {
  name: "local",

  /**
   * Upload a shard stream to local disk
   * @param {string} key  - filename to save as (e.g. "1720000000000_a.shard")
   * @param {Readable} stream - readable stream of shard bytes
   * @returns {Promise<string>} resolves with the key on success
   */
  upload(key, stream) {
    return new Promise((resolve, reject) => {
      const filePath = path.join(STORAGE_DIR, key);
      const writeStream = fs.createWriteStream(filePath);

      stream.pipe(writeStream);

      writeStream.on("finish", () => resolve(key));
      writeStream.on("error", (err) => {
        reject(new Error(`[local] Write failed for "${key}": ${err.message}`));
      });
    });
  },

  /**
   * Download a shard from local disk
   * @param {string} key - filename to read
   * @returns {fs.ReadStream}
   */
  download(key) {
    const filePath = path.join(STORAGE_DIR, key);

    if (!fs.existsSync(filePath)) {
      throw new Error(`[local] Shard not found: "${key}"`);
    }

    return fs.createReadStream(filePath);
  },

  /**
   * Health check — verify storage dir is writable
   * @returns {Promise<{ ok: boolean, latency: number }>}
   */
  async ping() {
    const start = Date.now();
    const testPath = path.join(STORAGE_DIR, ".ping");
    try {
      fs.writeFileSync(testPath, "ok");
      fs.unlinkSync(testPath);
      return { ok: true, latency: Date.now() - start };
    } catch (err) {
      return { ok: false, latency: Date.now() - start, error: err.message };
    }
  },
};

export default localDriver;
