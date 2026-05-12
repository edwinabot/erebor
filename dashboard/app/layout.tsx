import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'Erebor Trading Dashboard',
  description: 'Order book visualizations',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
