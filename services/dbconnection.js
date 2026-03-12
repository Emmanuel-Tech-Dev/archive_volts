// db.js — MySQL connection pool (mysql2/promise)
// -------------------------------------------------
// Single pool instance shared across the whole app.
// Import `db` wherever you need to query.

import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

const db = mysql.createPool({
  host: process.env.DATABASE_HOST,
  port: Number(process.env.DATABASE_PORT) || 3306,
  user: process.env.DATABASE_USER,
  password: process.env.DATABASE_PASSWORD,
  database: process.env.DATABASE_NAME,
  waitForConnections: true,
  connectionLimit: 10, // max parallel connections
  queueLimit: 0, // unlimited queue
});

// Verify the connection is alive on startup
export async function testConnection() {
  try {
    const conn = await db.getConnection();
    console.log("✅ MySQL connected successfully.");
    conn.release();
  } catch (err) {
    console.error("❌ MySQL connection failed:", err.message);
    process.exit(1); // hard stop — no point running without a DATABASE
  }
}

export default db;
