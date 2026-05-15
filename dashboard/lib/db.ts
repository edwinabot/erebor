import { Pool } from "pg";

const g = globalThis as typeof globalThis & { __pgPool?: Pool };

export function getPool(): Pool {
  if (!g.__pgPool) {
    g.__pgPool = new Pool({ connectionString: process.env.DATABASE_DSN });
  }
  return g.__pgPool;
}
