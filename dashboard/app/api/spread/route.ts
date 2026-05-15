import { NextRequest, NextResponse } from "next/server";
import { getPool } from "../../../lib/db";
import { queryRecentSnapshots } from "../../../lib/queries";
import { toSpreadData } from "../../../lib/transforms";

const DEFAULT_LIMIT = 60;
const MAX_LIMIT = 500;

export async function GET(request: NextRequest) {
  const symbol = request.nextUrl.searchParams.get("symbol");
  if (!symbol) {
    return NextResponse.json({ error: "symbol is required" }, { status: 400 });
  }

  const limit = Math.min(
    Math.max(1, parseInt(request.nextUrl.searchParams.get("limit") ?? "", 10) || DEFAULT_LIMIT),
    MAX_LIMIT
  );

  try {
    const rows = await queryRecentSnapshots(getPool(), symbol.toUpperCase(), limit);
    return NextResponse.json(toSpreadData(rows));
  } catch (err) {
    console.error("spread query failed", err);
    return NextResponse.json({ error: "internal server error" }, { status: 500 });
  }
}
