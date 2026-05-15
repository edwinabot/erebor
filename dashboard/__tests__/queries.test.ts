import { describe, it, expect, vi } from "vitest";
import { queryLatestSnapshot, queryRecentSnapshots } from "../lib/queries";
import type { Pool } from "pg";

const mockRow = {
  snapshot_time: new Date("2026-05-15T10:00:00.000Z"),
  symbol: "BTCUSDT",
  last_update_id: "4872910234",
  bids: [["94500.00", "0.532"]],
  asks: [["94501.00", "0.800"]],
};

function makePool(rows: object[]): Pool {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  } as unknown as Pool;
}

describe("queryLatestSnapshot", () => {
  it("returns the single row from the query result", async () => {
    const pool = makePool([mockRow]);
    const result = await queryLatestSnapshot(pool, "BTCUSDT");
    expect(result).toEqual(mockRow);
  });

  it("passes symbol as query parameter", async () => {
    const pool = makePool([mockRow]);
    await queryLatestSnapshot(pool, "ETHUSDT");
    const [, params] = (pool.query as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(params).toContain("ETHUSDT");
  });

  it("returns null when no rows found", async () => {
    const pool = makePool([]);
    const result = await queryLatestSnapshot(pool, "BTCUSDT");
    expect(result).toBeNull();
  });
});

describe("queryRecentSnapshots", () => {
  it("returns rows in chronological order (oldest first)", async () => {
    const older = { ...mockRow, snapshot_time: new Date("2026-05-15T10:00:00.000Z") };
    const newer = { ...mockRow, snapshot_time: new Date("2026-05-15T10:00:01.000Z") };
    // DB returns DESC (newest first), function must reverse
    const pool = makePool([newer, older]);
    const result = await queryRecentSnapshots(pool, "BTCUSDT", 2);
    expect(result[0].snapshot_time).toEqual(older.snapshot_time);
    expect(result[1].snapshot_time).toEqual(newer.snapshot_time);
  });

  it("passes symbol and limit as query parameters", async () => {
    const pool = makePool([mockRow]);
    await queryRecentSnapshots(pool, "ETHUSDT", 30);
    const [, params] = (pool.query as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(params).toContain("ETHUSDT");
    expect(params).toContain(30);
  });

  it("returns empty array when no rows found", async () => {
    const pool = makePool([]);
    const result = await queryRecentSnapshots(pool, "BTCUSDT", 60);
    expect(result).toEqual([]);
  });
});
