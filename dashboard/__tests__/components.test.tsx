/**
 * @vitest-environment jsdom
 *
 * Component tests for the four dashboard panels.
 *
 * Runs in jsdom. Canvas is stubbed so getContext returns null
 * (the useEffect guards skip drawing when ctx is null).
 */
import { describe, it, expect, vi, beforeAll, afterEach } from "vitest";
import { render, screen, cleanup } from "@testing-library/react";
import * as matchers from "@testing-library/jest-dom/matchers";
expect.extend(matchers);

// ── Canvas stub ──────────────────────────────────────────────────────────────
beforeAll(() => {
  HTMLCanvasElement.prototype.getContext = vi.fn(() => null) as never;
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

// ── Hook mocks ───────────────────────────────────────────────────────────────

vi.mock("../app/dashboard/hooks/useOrderBookData", () => ({
  useOrderBookData: vi.fn(),
}));
vi.mock("../app/dashboard/hooks/useMarketDepthData", () => ({
  useMarketDepthData: vi.fn(),
}));
vi.mock("../app/dashboard/hooks/useSpreadData", () => ({
  useSpreadData: vi.fn(),
}));
vi.mock("../app/dashboard/hooks/useImbalanceData", () => ({
  useImbalanceData: vi.fn(),
}));

import { useOrderBookData } from "../app/dashboard/hooks/useOrderBookData";
import { useMarketDepthData } from "../app/dashboard/hooks/useMarketDepthData";
import { useSpreadData } from "../app/dashboard/hooks/useSpreadData";
import { useImbalanceData } from "../app/dashboard/hooks/useImbalanceData";

import OrderBookLadder from "../app/dashboard/components/OrderBookLadder";
import MarketDepthChart from "../app/dashboard/components/MarketDepthChart";
import SpreadChart from "../app/dashboard/components/SpreadChart";
import ImbalanceChart from "../app/dashboard/components/ImbalanceChart";

const mockUseOrderBookData = vi.mocked(useOrderBookData);
const mockUseMarketDepthData = vi.mocked(useMarketDepthData);
const mockUseSpreadData = vi.mocked(useSpreadData);
const mockUseImbalanceData = vi.mocked(useImbalanceData);

// ── Fixtures ─────────────────────────────────────────────────────────────────

const orderBookFixture = {
  symbol: "BTCUSDT",
  timestamp: "2026-05-15T10:00:00.000Z",
  last_update_id: 999,
  bids: [
    { price: "94500.00", quantity: "0.532" },
    { price: "94499.50", quantity: "1.234" },
  ],
  asks: [
    { price: "94501.00", quantity: "0.800" },
    { price: "94502.50", quantity: "2.100" },
  ],
};

const marketDepthFixture = {
  symbol: "BTCUSDT",
  timestamp: "2026-05-15T10:00:00.000Z",
  bids: [{ price: "94500.00", cumulative_quantity: "0.532" }],
  asks: [{ price: "94501.00", cumulative_quantity: "0.800" }],
};

const spreadFixture = {
  symbol: "BTCUSDT",
  samples: [
    {
      timestamp: "2026-05-15T10:00:00.000Z",
      best_bid: "94500.00",
      best_ask: "94501.00",
      mid_price: "94500.50",
      spread: "1.00",
      spread_bps: "0.11",
    },
  ],
};

const imbalanceFixture = {
  symbol: "BTCUSDT",
  depth_levels: 10,
  samples: [
    {
      timestamp: "2026-05-15T10:00:00.000Z",
      bid_qty: "15.00",
      ask_qty: "5.00",
      imbalance: "0.5000",
    },
  ],
};

// ── OrderBookLadder ──────────────────────────────────────────────────────────

describe("OrderBookLadder", () => {
  it("renders loading state when data is null", () => {
    mockUseOrderBookData.mockReturnValue(null);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    expect(screen.getByText("LOADING ORDER BOOK...")).toBeInTheDocument();
  });

  it("renders bid price in the header when data is provided", () => {
    mockUseOrderBookData.mockReturnValue(orderBookFixture);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    // The spread info header shows the best bid price
    const bidItems = screen.getAllByText("94500.00");
    expect(bidItems.length).toBeGreaterThanOrEqual(1);
  });

  it("renders ask price when data is provided", () => {
    mockUseOrderBookData.mockReturnValue(orderBookFixture);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    const askItems = screen.getAllByText("94501.00");
    expect(askItems.length).toBeGreaterThanOrEqual(1);
  });

  it("renders SPREAD label", () => {
    mockUseOrderBookData.mockReturnValue(orderBookFixture);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    expect(screen.getByText("SPREAD")).toBeInTheDocument();
  });

  it("renders MID label", () => {
    mockUseOrderBookData.mockReturnValue(orderBookFixture);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    expect(screen.getByText("MID")).toBeInTheDocument();
  });

  it("renders PRICE column headers", () => {
    mockUseOrderBookData.mockReturnValue(orderBookFixture);
    render(<OrderBookLadder symbol="BTCUSDT" />);
    const priceHeaders = screen.getAllByText("PRICE");
    expect(priceHeaders.length).toBeGreaterThanOrEqual(1);
  });
});

// ── MarketDepthChart ─────────────────────────────────────────────────────────

describe("MarketDepthChart", () => {
  it("renders without crashing when data is null (shows canvas)", () => {
    mockUseMarketDepthData.mockReturnValue(null);
    const { container } = render(<MarketDepthChart symbol="BTCUSDT" />);
    expect(container.querySelector("canvas")).toBeInTheDocument();
  });

  it('renders "BID SIDE" legend when data is null', () => {
    mockUseMarketDepthData.mockReturnValue(null);
    render(<MarketDepthChart symbol="BTCUSDT" />);
    expect(screen.getByText("BID SIDE")).toBeInTheDocument();
  });

  it('renders "ASK SIDE" legend when data is null', () => {
    mockUseMarketDepthData.mockReturnValue(null);
    render(<MarketDepthChart symbol="BTCUSDT" />);
    expect(screen.getByText("ASK SIDE")).toBeInTheDocument();
  });

  it("renders a time string (not dash) when data is provided", () => {
    mockUseMarketDepthData.mockReturnValue(marketDepthFixture);
    render(<MarketDepthChart symbol="BTCUSDT" />);
    // The timestamp renders as a locale time string, not "—"
    expect(screen.queryByText("—")).not.toBeInTheDocument();
  });

  it("renders canvas element when data is provided", () => {
    mockUseMarketDepthData.mockReturnValue(marketDepthFixture);
    const { container } = render(<MarketDepthChart symbol="BTCUSDT" />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });
});

// ── SpreadChart ──────────────────────────────────────────────────────────────

describe("SpreadChart", () => {
  it("renders loading state when data is null", () => {
    mockUseSpreadData.mockReturnValue(null);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("LOADING SPREAD DATA...")).toBeInTheDocument();
  });

  it("renders SPREAD label when data is provided", () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("SPREAD")).toBeInTheDocument();
  });

  it("renders MID label when data is provided", () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("MID")).toBeInTheDocument();
  });

  it('renders "MID-PRICE" legend when data is provided', () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("MID-PRICE")).toBeInTheDocument();
  });

  it('renders "BID" legend when data is provided', () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("BID")).toBeInTheDocument();
  });

  it('renders "ASK" legend when data is provided', () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    render(<SpreadChart symbol="BTCUSDT" />);
    expect(screen.getByText("ASK")).toBeInTheDocument();
  });

  it("renders canvas element when data is provided", () => {
    mockUseSpreadData.mockReturnValue(spreadFixture);
    const { container } = render(<SpreadChart symbol="BTCUSDT" />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });
});

// ── ImbalanceChart ───────────────────────────────────────────────────────────

describe("ImbalanceChart", () => {
  it("renders loading state when data is null", () => {
    mockUseImbalanceData.mockReturnValue(null);
    render(<ImbalanceChart symbol="BTCUSDT" />);
    expect(screen.getByText("LOADING IMBALANCE DATA...")).toBeInTheDocument();
  });

  it("renders ORDER BOOK IMBALANCE via DEPTH label when data is provided", () => {
    mockUseImbalanceData.mockReturnValue(imbalanceFixture);
    render(<ImbalanceChart symbol="BTCUSDT" />);
    expect(screen.getByText("DEPTH: 10")).toBeInTheDocument();
  });

  it('renders "BID QTY" label when data is provided', () => {
    mockUseImbalanceData.mockReturnValue(imbalanceFixture);
    render(<ImbalanceChart symbol="BTCUSDT" />);
    expect(screen.getByText("BID QTY")).toBeInTheDocument();
  });

  it('renders "ASK QTY" label when data is provided', () => {
    mockUseImbalanceData.mockReturnValue(imbalanceFixture);
    render(<ImbalanceChart symbol="BTCUSDT" />);
    expect(screen.getByText("ASK QTY")).toBeInTheDocument();
  });

  it("renders imbalance percentage when data is provided", () => {
    mockUseImbalanceData.mockReturnValue(imbalanceFixture);
    render(<ImbalanceChart symbol="BTCUSDT" />);
    // 0.5 * 100 = 50.0%
    expect(screen.getByText("50.0%")).toBeInTheDocument();
  });

  it("renders canvas element when data is provided", () => {
    mockUseImbalanceData.mockReturnValue(imbalanceFixture);
    const { container } = render(<ImbalanceChart symbol="BTCUSDT" />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });
});
