import cluster from "node:cluster";
import dotenv from "dotenv";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema.js";

dotenv.config();

// Scale connection pool based on clustering mode.
// Primary gets 5 connections (cron/DB ops only). Each worker gets floor(20 / workerCount), min 5.
// Example with 4 workers: total = 4 × 5 (workers) + 5 (primary) = 25, well under PG's default 100.
// Single-process mode (workerCount=0): 20 connections, same as before clustering.
const workerCount = parseInt(process.env.CLUSTER_WORKERS || "0", 10);
const maxConnections = cluster.isWorker
  ? Math.max(5, Math.floor(20 / (workerCount || 1)))
  : cluster.isPrimary && workerCount > 0
    ? 5
    : 20;

// Postgres here holds configuration and accounts, never analytics, so no query
// should run long. Without a ceiling a stuck one holds its connection forever,
// and with as few as 5 per worker the requests behind it wait just as long.
// 0 disables the ceiling.
const statementTimeoutMs = parseInt(process.env.POSTGRES_STATEMENT_TIMEOUT_MS || "30000", 10);

// Create postgres connection
const client = postgres({
  host: process.env.POSTGRES_HOST || "postgres",
  port: parseInt(process.env.POSTGRES_PORT || "5432", 10),
  database: process.env.POSTGRES_DB,
  username: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  onnotice: () => {},
  max: maxConnections,
  connect_timeout: 10,
  connection: { statement_timeout: statementTimeoutMs },
});

// Create drizzle ORM instance
export const db = drizzle(client, { schema });

// For compatibility with raw SQL if needed
export const sql = client;
