export const metadata = {
  title: 'Genomics Explorer',
  description: 'Tertiary genomic analysis UI'
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        {children}
      </body>
    </html>
  )
}