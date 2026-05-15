import { NextRequest, NextResponse } from "next/server";
import { getPool } from "../../../lib/db";
import { queryRecentSnapshots } from "../../../lib/queries";
import { toImbalanceData } from "../../../lib/transforms";

const DEFAULT_LIMIT = 60;
const MAX_LIMIT = 500;
const DEFAULT_DEPTH = 10;
const MAX_DEPTH = 100;

export async function GET(request: NextRequest) {
  const symbol = request.nextUrl.searchParams.get("symbol");
  if (!symbol) {
    return NextResponse.json({ error: "symbol is required" }, { status: 400 });
  }

  const limit = Math.min(
    Math.max(1, parseInt(request.nextUrl.searchParams.get("limit") ?? "", 10) || DEFAULT_LIMIT),
    MAX_LIMIT
  );
  const depth = Math.min(
    Math.max(1, parseInt(request.nextUrl.searchParams.get("depth") ?? "", 10) || DEFAULT_DEPTH),
    MAX_DEPTH
  );

  try {
    const rows = await queryRecentSnapshots(getPool(), symbol.toUpperCase(), limit);
    return NextResponse.json(toImbalanceData(rows, depth));
  } catch (err) {
    console.error("imbalance query failed", err);
    return NextResponse.json({ error: "internal server error" }, { status: 500 });
  }
}
