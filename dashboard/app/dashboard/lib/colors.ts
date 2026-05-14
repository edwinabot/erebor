// Hardcoded hex equivalents of CSS custom properties.
// Canvas 2D does not resolve CSS variables — use these for ctx.fillStyle / ctx.strokeStyle.
export const COLORS = {
  bgPrimary: "#0a0e27",
  bgSecondary: "#0f1329",
  bgTertiary: "#151a35",
  border: "#1e2749",
  textPrimary: "#ffffff",
  textSecondary: "#a0a9be",
  buy: "#00d966",
  buyFill: "rgba(0, 217, 102, 0.25)",
  sell: "#ff1744",
  sellFill: "rgba(255, 23, 68, 0.25)",
  neutral: "#00bfff",
  accent: "#ffd700",
  accentFill: "rgba(255, 215, 0, 0.1)",
} as const;
