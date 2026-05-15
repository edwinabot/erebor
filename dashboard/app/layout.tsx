import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Erebor Trading Dashboard",
  description: "Real-time market data and trading visualizations",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
