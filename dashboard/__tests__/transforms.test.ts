import { describe, it, expect } from "vitest";
import {
  toOrderBookResponse,
  toMarketDepthResponse,
  toSpreadData,
  toImbalanceData,
  type SnapshotRow,
} from "../lib/transforms";

const makeRow = (overrides: Partial<SnapshotRow> = {}): SnapshotRow => ({
  snapshot_time: new Date("2026-05-15T10:00:00.000Z"),
  symbol: "BTCUSDT",
  last_update_id: "4872910234",
  bids: [
    ["94500.00", "0.532"],
    ["94499.50", "1.234"],
    ["94498.00", "3.100"],
  ],
  asks: [
    ["94501.00", "0.800"],
    ["94502.50", "2.100"],
    ["94504.00", "0.450"],
  ],
  ...overrides,
});

describe("toOrderBookResponse", () => {
  it("maps symbol, timestamp, and last_update_id", () => {
    const result = toOrderBookResponse(makeRow());
    expect(result.symbol).toBe("BTCUSDT");
    expect(result.timestamp).toBe("2026-05-15T10:00:00.000Z");
    expect(result.last_update_id).toBe(4872910234);
  });

  it("maps bids as price/quantity pairs", () => {
    const result = toOrderBookResponse(makeRow());
    expect(result.bids).toEqual([
      { price: "94500.00", quantity: "0.532" },
      { price: "94499.50", quantity: "1.234" },
      { price: "94498.00", quantity: "3.100" },
    ]);
  });

  it("maps asks as price/quantity pairs", () => {
    const result = toOrderBookResponse(makeRow());
    expect(result.asks).toEqual([
      { price: "94501.00", quantity: "0.800" },
      { price: "94502.50", quantity: "2.100" },
      { price: "94504.00", quantity: "0.450" },
    ]);
  });
});

describe("toMarketDepthResponse", () => {
  it("maps symbol and timestamp", () => {
    const result = toMarketDepthResponse(makeRow());
    expect(result.symbol).toBe("BTCUSDT");
    expect(result.timestamp).toBe("2026-05-15T10:00:00.000Z");
  });

  it("computes cumulative bid quantities", () => {
    const result = toMarketDepthResponse(makeRow());
    expect(result.bids[0]).toEqual({ price: "94500.00", cumulative_quantity: "0.532" });
    expect(result.bids[1]).toEqual({ price: "94499.50", cumulative_quantity: "1.766" });
    expect(result.bids[2]).toEqual({ price: "94498.00", cumulative_quantity: "4.866" });
  });

  it("computes cumulative ask quantities", () => {
    const result = toMarketDepthResponse(makeRow());
    expect(result.asks[0]).toEqual({ price: "94501.00", cumulative_quantity: "0.800" });
    expect(result.asks[1]).toEqual({ price: "94502.50", cumulative_quantity: "2.900" });
    expect(result.asks[2]).toEqual({ price: "94504.00", cumulative_quantity: "3.350" });
  });
});

describe("toSpreadData", () => {
  const rows: SnapshotRow[] = [
    makeRow({
      snapshot_time: new Date("2026-05-15T10:00:00.000Z"),
      bids: [["94500.00", "0.532"]],
      asks: [["94501.00", "0.800"]],
    }),
    makeRow({
      snapshot_time: new Date("2026-05-15T10:00:01.000Z"),
      bids: [["94499.50", "1.234"]],
      asks: [["94501.50", "2.100"]],
    }),
  ];

  it("returns symbol from first row", () => {
    const result = toSpreadData(rows);
    expect(result.symbol).toBe("BTCUSDT");
  });

  it("returns empty samples for empty input", () => {
    expect(toSpreadData([]).samples).toEqual([]);
  });

  it("computes spread, mid_price, and spread_bps per sample", () => {
    const result = toSpreadData(rows);
    expect(result.samples).toHaveLength(2);

    const s0 = result.samples[0];
    expect(s0.timestamp).toBe("2026-05-15T10:00:00.000Z");
    expect(s0.best_bid).toBe("94500.00");
    expect(s0.best_ask).toBe("94501.00");
    expect(s0.mid_price).toBe("94500.50");
    expect(s0.spread).toBe("1.00");
    // spread_bps = (1 / 94500.50) * 10000 ≈ 0.11
    expect(parseFloat(s0.spread_bps)).toBeCloseTo(0.11, 1);
  });

  it("orders samples chronologically", () => {
    const result = toSpreadData(rows);
    expect(result.samples[0].timestamp).toBe("2026-05-15T10:00:00.000Z");
    expect(result.samples[1].timestamp).toBe("2026-05-15T10:00:01.000Z");
  });
});

describe("toImbalanceData", () => {
  const rows: SnapshotRow[] = [
    makeRow({
      snapshot_time: new Date("2026-05-15T10:00:00.000Z"),
      bids: [
        ["94500.00", "10.000"],
        ["94499.50", "5.000"],
      ],
      asks: [
        ["94501.00", "3.000"],
        ["94502.50", "2.000"],
      ],
    }),
  ];

  it("returns symbol and depth_levels", () => {
    const result = toImbalanceData(rows, 2);
    expect(result.symbol).toBe("BTCUSDT");
    expect(result.depth_levels).toBe(2);
  });

  it("returns empty samples for empty input", () => {
    expect(toImbalanceData([], 10).samples).toEqual([]);
  });

  it("computes bid_qty, ask_qty, and imbalance for given depth", () => {
    const result = toImbalanceData(rows, 2);
    const s = result.samples[0];
    expect(s.bid_qty).toBe("15.00"); // 10 + 5
    expect(s.ask_qty).toBe("5.00"); // 3 + 2
    // imbalance = (15 - 5) / (15 + 5) = 10/20 = 0.5000
    expect(s.imbalance).toBe("0.5000");
  });

  it("clamps imbalance to [-1, 1]", () => {
    const extremeRow = makeRow({
      bids: [["94500.00", "100.000"]],
      asks: [["94501.00", "0.000"]],
    });
    const result = toImbalanceData([extremeRow], 1);
    expect(parseFloat(result.samples[0].imbalance)).toBe(1);
  });

  it("respects depth_levels limit", () => {
    const result = toImbalanceData(rows, 1);
    const s = result.samples[0];
    // Only first level: bid=10, ask=3
    expect(s.bid_qty).toBe("10.00");
    expect(s.ask_qty).toBe("3.00");
  });
});
