import "./globals.css"

export const metadata = {
  title: "Enterprise MAS",
  description: "Enterprise Multi-Agent System",
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-background font-sans antialiased">
        {children}
      </body>
    </html>
  )
}
