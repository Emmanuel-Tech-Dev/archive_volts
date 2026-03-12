-- ============================================
-- ARCHIVOLT :: Global File Ledger Schema v2
-- 3-shard architecture: local + supabase + cloudinary
-- Run this once in your MySQL instance
-- ============================================

CREATE DATABASE IF NOT EXISTS archivolt;
USE archivolt;

-- Tracks every sharded file and where each shard lives
CREATE TABLE IF NOT EXISTS file_ledger (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    filename      VARCHAR(255)  NOT NULL,
    mime_type     VARCHAR(100),
    file_size     INT           NULL,          -- original size in bytes

    -- Shard A: local disk  (bytes where i % 3 === 0)
    shard_a_provider  VARCHAR(50)  NOT NULL DEFAULT 'local',
    shard_a_key       TEXT         NOT NULL,

    -- Shard B: Supabase Storage  (bytes where i % 3 === 1)
    shard_b_provider  VARCHAR(50)  NOT NULL DEFAULT 'supabase',
    shard_b_key       TEXT         NOT NULL,

    -- Shard C: Cloudinary Raw  (bytes where i % 3 === 2)
    shard_c_provider  VARCHAR(50)  NOT NULL DEFAULT 'cloudinary',
    shard_c_key       TEXT         NOT NULL,   -- Cloudinary public_id

    -- Shard P: XOR Parity (local disk — used to recover any single lost shard)
    shard_p_provider  VARCHAR(50)  NOT NULL DEFAULT 'local',
    shard_p_key       TEXT         NOT NULL,   -- parity file path

    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Index on filename for search lookups
CREATE INDEX idx_filename ON file_ledger (filename);
