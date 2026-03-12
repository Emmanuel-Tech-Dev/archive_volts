// drivers/cloudinary.js
// -------------------------------------------------
// Shard C — Cloudinary Raw Storage (v2 SDK)
// resource_type: "raw" — stores binary files as-is
// Bytes where: i % 3 === 2

import { v2 as cloudinary } from "cloudinary";
import { Readable, PassThrough } from "stream";
import https from "https";
import dotenv from "dotenv";

dotenv.config();

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
});

const cloudinaryDriver = {
  name: "cloudinary",

  /**
   * Upload a shard stream to Cloudinary as a raw file
   * @param {string} key  - used as the public_id (e.g. "1720000000000_c")
   * @param {Readable} stream - readable stream of shard bytes
   * @returns {Promise<string>} resolves with the Cloudinary public_id
   */
  upload(key, stream) {
    return new Promise((resolve, reject) => {
      // Strip extension from key to use as public_id
      const publicId = key.replace(/\.[^/.]+$/, "");

      const uploadStream = cloudinary.uploader.upload_stream(
        {
          public_id: publicId,
          resource_type: "raw",
          overwrite: true,
          folder: "archivolt-shards",
        },
        (error, result) => {
          if (error) {
            return reject(
              new Error(
                `[cloudinary] Upload failed for "${key}": ${error.message}`,
              ),
            );
          }
          // Return public_id — used to reconstruct download URL later
          resolve(result.public_id);
        },
      );

      stream.pipe(uploadStream);
      stream.on("error", reject);
    });
  },

  /**
   * Download a shard from Cloudinary
   * @param {string} publicId - Cloudinary public_id (stored in shard_c_key)
   * @returns {Promise<Readable>} readable stream of shard bytes
   */
  async download(publicId) {
    // Generate a signed URL for secure access
    const url = cloudinary.url(publicId, {
      resource_type: "raw",
      sign_url: true,
      type: "upload",
    });

    // Fetch the URL and return a stream
    return new Promise((resolve, reject) => {
      https
        .get(url, (res) => {
          if (res.statusCode !== 200) {
            return reject(
              new Error(
                `[cloudinary] Download failed for "${publicId}": HTTP ${res.statusCode}`,
              ),
            );
          }
          resolve(res); // res is already a Readable stream
        })
        .on("error", reject);
    });
  },

  /**
   * Health check — verify Cloudinary credentials are valid
   * @returns {Promise<{ ok: boolean, latency: number }>}
   */
  async ping() {
    const start = Date.now();
    try {
      await cloudinary.api.ping();
      return { ok: true, latency: Date.now() - start };
    } catch (err) {
      return { ok: false, latency: Date.now() - start, error: err.message };
    }
  },
};

export default cloudinaryDriver;
