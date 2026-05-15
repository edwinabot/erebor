import { NextRequest, NextResponse } from "next/server";
import { getPool } from "../../../lib/db";
import { queryLatestSnapshot } from "../../../lib/queries";
import { toOrderBookResponse } from "../../../lib/transforms";

export async function GET(request: NextRequest) {
  const symbol = request.nextUrl.searchParams.get("symbol");
  if (!symbol) {
    return NextResponse.json({ error: "symbol is required" }, { status: 400 });
  }

  try {
    const row = await queryLatestSnapshot(getPool(), symbol.toUpperCase());
    if (!row) {
      return NextResponse.json({ error: `no snapshot found for ${symbol}` }, { status: 404 });
    }
    return NextResponse.json(toOrderBookResponse(row));
  } catch (err) {
    console.error("orderbook query failed", err);
    return NextResponse.json({ error: "internal server error" }, { status: 500 });
  }
}
