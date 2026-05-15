import type { Pool } from "pg";
import type { SnapshotRow } from "./transforms";

export async function queryLatestSnapshot(
  pool: Pool,
  symbol: string
): Promise<SnapshotRow | null> {
  const result = await pool.query<SnapshotRow>(
    `SELECT snapshot_time, symbol, last_update_id, bids, asks
     FROM order_book_snapshots
     WHERE symbol = $1
     ORDER BY snapshot_time DESC
     LIMIT 1`,
    [symbol]
  );
  return result.rows[0] ?? null;
}

export async function queryRecentSnapshots(
  pool: Pool,
  symbol: string,
  limit: number
): Promise<SnapshotRow[]> {
  const result = await pool.query<SnapshotRow>(
    `SELECT snapshot_time, symbol, last_update_id, bids, asks
     FROM order_book_snapshots
     WHERE symbol = $1
     ORDER BY snapshot_time DESC
     LIMIT $2`,
    [symbol, limit]
  );
  return result.rows.slice().reverse();
}
