/**
 * @vitest-environment jsdom
 *
 * Tests for the four data-fetching hooks.
 *
 * Runs in jsdom so renderHook can mount React components.
 * fetch is stubbed with vi.stubGlobal.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act } from "@testing-library/react";

// ── fetch stub helpers ────────────────────────────────────────────────────────

function makeOkFetch(payload: unknown) {
  return vi.fn().mockResolvedValue({
    ok: true,
    json: async () => payload,
  });
}

/** Returns a fetch mock that hangs until the returned `resolve` is called. */
function makeHangingFetch(payload: unknown): {
  fetchMock: ReturnType<typeof vi.fn>;
  resolve: () => void;
} {
  let resolve!: () => void;
  const p = new Promise<{ ok: boolean; json: () => Promise<unknown> }>((res) => {
    resolve = () => res({ ok: true, json: async () => payload });
  });
  const fetchMock = vi.fn().mockReturnValue(p);
  return { fetchMock, resolve };
}

// ── useOrderBookData ─────────────────────────────────────────────────────────
// Note: useOrderBookData uses a recursive setTimeout at 100ms (not setInterval)

import { useOrderBookData } from "../app/dashboard/hooks/useOrderBookData";

describe("useOrderBookData", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const payload = {
    symbol: "BTCUSDT",
    timestamp: "2026-05-15T10:00:00.000Z",
    last_update_id: 1,
    bids: [],
    asks: [],
  };

  it("fetches data on mount", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useOrderBookData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/orderbook?symbol=BTCUSDT")
    );
    expect(result.current).toEqual(payload);
    unmount();
  });

  it("fetches data again after the 100ms timeout", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { unmount } = renderHook(() => useOrderBookData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    unmount();
  });

  it("does not update state after unmount", async () => {
    const { fetchMock, resolve } = makeHangingFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useOrderBookData("BTCUSDT"));
    unmount();
    await act(async () => {
      resolve();
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(result.current).toBeNull();
  });
});

// ── useMarketDepthData ────────────────────────────────────────────────────────

import { useMarketDepthData } from "../app/dashboard/hooks/useMarketDepthData";

describe("useMarketDepthData", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const payload = {
    symbol: "BTCUSDT",
    timestamp: "2026-05-15T10:00:00.000Z",
    bids: [],
    asks: [],
  };

  it("fetches data on mount", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useMarketDepthData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/market-depth?symbol=BTCUSDT")
    );
    expect(result.current).toEqual(payload);
    unmount();
  });

  it("polls again after 500ms interval", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { unmount } = renderHook(() => useMarketDepthData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    unmount();
  });

  it("cancelled flag prevents state update after unmount", async () => {
    const { fetchMock, resolve } = makeHangingFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useMarketDepthData("BTCUSDT"));
    unmount();
    await act(async () => {
      resolve();
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(result.current).toBeNull();
  });
});

// ── useSpreadData ────────────────────────────────────────────────────────────

import { useSpreadData } from "../app/dashboard/hooks/useSpreadData";

describe("useSpreadData", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const payload = { symbol: "BTCUSDT", samples: [] };

  it("fetches data on mount", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useSpreadData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/api/spread?symbol=BTCUSDT"));
    expect(result.current).toEqual(payload);
    unmount();
  });

  it("polls again after 1000ms interval", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { unmount } = renderHook(() => useSpreadData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    unmount();
  });

  it("cancelled flag prevents state update after unmount", async () => {
    const { fetchMock, resolve } = makeHangingFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useSpreadData("BTCUSDT"));
    unmount();
    await act(async () => {
      resolve();
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(result.current).toBeNull();
  });
});

// ── useImbalanceData ─────────────────────────────────────────────────────────

import { useImbalanceData } from "../app/dashboard/hooks/useImbalanceData";

describe("useImbalanceData", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const payload = { symbol: "BTCUSDT", depth_levels: 10, samples: [] };

  it("fetches data on mount", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useImbalanceData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/imbalance?symbol=BTCUSDT")
    );
    expect(result.current).toEqual(payload);
    unmount();
  });

  it("polls again after 500ms interval", async () => {
    const fetchMock = makeOkFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { unmount } = renderHook(() => useImbalanceData("BTCUSDT"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    unmount();
  });

  it("cancelled flag prevents state update after unmount", async () => {
    const { fetchMock, resolve } = makeHangingFetch(payload);
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useImbalanceData("BTCUSDT"));
    unmount();
    await act(async () => {
      resolve();
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(result.current).toBeNull();
  });
});
