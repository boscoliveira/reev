'use client'

import { useState } from 'react'

type Item = Record<string, any>

export default function VariantsPage() {
  const [items, setItems] = useState<Item[]>([])
  const [projectId, setProjectId] = useState('demo')
  const [gene, setGene] = useState('')

  async function runQuery() {
    const body = {
      project_id: projectId,
      filters: gene
        ? { op: 'AND', clauses: [{ field: 'csq.symbol.keyword', op: 'term', value: gene }], groups: [] }
        : undefined,
      page: { size: 50 }
    }
    const res = await fetch('/api/filter/query', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body)
    })
    const data = await res.json()
    setItems(data.items || [])
  }

  return (
    <main style={{ padding: 24 }}>
      <h2>Variants</h2>
      <div style={{ display: 'flex', gap: 8 }}>
        <label>
          Project:
          <input value={projectId} onChange={(e) => setProjectId(e.target.value)} />
        </label>
        <label>
          Gene:
          <input value={gene} onChange={(e) => setGene(e.target.value)} />
        </label>
        <button onClick={runQuery}>Query</button>
      </div>
      <div style={{ marginTop: 16 }}>
        <table>
          <thead>
            <tr>
              <th>Variant</th>
              <th>Gene</th>
              <th>Consequence</th>
              <th>Impact</th>
            </tr>
          </thead>
          <tbody>
            {items.map((it) => (
              <tr key={it.variant_id}>
                <td>{it.chrom}:{it.pos} {it.ref}&gt;{it.alt}</td>
                <td>{it.csq?.SYMBOL || it.csq?.symbol}</td>
                <td>{it.csq?.Consequence || it.csq?.consequence}</td>
                <td>{it.csq?.IMPACT || it.csq?.impact}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </main>
  )
}