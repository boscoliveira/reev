export default async function Home() {
  const apiBase = process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000'
  const res = await fetch(`${apiBase}/api/healthz`, { cache: 'no-store' })
  const data = await res.json()
  return (
    <main style={{ padding: 24, fontFamily: 'sans-serif' }}>
      <h1>Genomics Explorer</h1>
      <p>API status: {data.status}</p>
      <a href="/variants">Go to Variants</a>
    </main>
  )
}