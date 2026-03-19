import React, { useEffect, useState, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

const COLORS = [
  '#f59e0b', '#ea580c', '#16a34a', '#3b82f6',
  '#a855f7', '#ec4899', '#14b8a6', '#f97316',
]

function completenessClass(pct) {
  if (pct >= 90) return 'green'
  if (pct >= 70) return 'amber'
  return 'red'
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload || !payload.length) return null
  return (
    <div style={{
      background: '#1a1510',
      border: '1px solid #92400e',
      borderRadius: 4,
      padding: '8px 12px',
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 11,
    }}>
      <div style={{ color: '#d97706', marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.fill, marginBottom: 2 }}>
          {p.name}: {p.value?.toFixed(2)}%
        </div>
      ))}
    </div>
  )
}

export default function CompareView({ datasets, compareSelected }) {
  const [details, setDetails]   = useState({})
  const [loading, setLoading]   = useState(false)

  const selectedDatasets = useMemo(
    () => datasets.filter(d => compareSelected.includes(d.name)),
    [datasets, compareSelected]
  )

  useEffect(() => {
    if (!compareSelected.length) return
    const toFetch = compareSelected.filter(name => !details[name])
    if (!toFetch.length) return
    setLoading(true)
    Promise.all(toFetch.map(name =>
      fetch(`/api/datasets/${encodeURIComponent(name)}`).then(r => r.json())
    )).then(results => {
      const map = {}
      results.forEach(d => { if (d && d.name) map[d.name] = d })
      setDetails(prev => ({ ...prev, ...map }))
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [compareSelected])

  // Build chart data: one entry per dataset
  const chartData = useMemo(() =>
    selectedDatasets.map(ds => ({
      name: ds.name.replace(/\.[^.]+$/, '').slice(0, 20),
      fullName: ds.name,
      completeness: ds.completeness || 0,
    })),
    [selectedDatasets]
  )

  if (compareSelected.length === 0) {
    return (
      <div>
        <div className="compare-header">
          <div className="compare-title">⇌ COMPARE DATASETS</div>
          <div className="compare-hint">
            Select datasets from the sidebar to compare
          </div>
        </div>
        <div className="empty-wrap" style={{ minHeight: '50vh' }}>
          <div className="empty-icon">⇌</div>
          <div className="empty-text">CHECK DATASETS IN THE SIDEBAR</div>
        </div>
      </div>
    )
  }

  return (
    <div>
      <div className="compare-header">
        <div className="compare-title">⇌ COMPARE DATASETS</div>
        <div className="compare-hint">
          {compareSelected.length} selected
        </div>
      </div>

      {loading && (
        <div className="loading-wrap" style={{ minHeight: 120 }}>
          <div className="loading-spinner" />
          <div className="loading-text">ANALYZING...</div>
        </div>
      )}

      {/* Completeness bar chart */}
      <div className="compare-section" style={{ marginBottom: 20 }}>
        <div className="section-heading">Completeness Comparison</div>
        <div style={{ width: '100%', height: 280 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              margin={{ top: 5, right: 20, left: -10, bottom: 60 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(42,30,16,0.8)" vertical={false} />
              <XAxis
                dataKey="name"
                tick={{ fill: '#78350f', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}
                angle={-35}
                textAnchor="end"
                interval={0}
                tickLine={false}
                axisLine={{ stroke: '#2a1e10' }}
              />
              <YAxis
                domain={[0, 100]}
                tick={{ fill: '#78350f', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => `${v}%`}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(245,158,11,0.06)' }} />
              <Bar
                dataKey="completeness"
                name="Completeness %"
                radius={[4, 4, 0, 0]}
                maxBarSize={50}
                fill="#f59e0b"
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Comparison table */}
      <div className="compare-section">
        <div className="section-heading">Dataset Summary Comparison</div>
        <div style={{ overflowX: 'auto' }}>
          <table className="compare-table">
            <thead>
              <tr>
                <th>Dataset</th>
                <th>Type</th>
                <th>Rows</th>
                <th>Columns</th>
                <th>Completeness</th>
                <th>Missing Total</th>
                <th>Wrong Type Cols</th>
              </tr>
            </thead>
            <tbody>
              {selectedDatasets.map(ds => (
                <tr key={ds.name}>
                  <td style={{ fontFamily: 'var(--font-data)', fontSize: 11, color: 'var(--text-secondary)', maxWidth: 200, wordBreak: 'break-all' }}>
                    {ds.name}
                  </td>
                  <td>
                    <span className={`file-badge ${(ds.type || '').toLowerCase()}`}>
                      {ds.type}
                    </span>
                  </td>
                  <td className="num">{(ds.rows || 0).toLocaleString()}</td>
                  <td className="num">{(ds.columns || 0).toLocaleString()}</td>
                  <td>
                    <span className={`completeness-pill ${completenessClass(ds.completeness || 0)}`}>
                      <span className={`status-dot ${completenessClass(ds.completeness || 0)}`} />
                      {(ds.completeness || 0).toFixed(2)}%
                    </span>
                  </td>
                  <td className="num" style={{ color: ds.missing_total > 0 ? '#f87171' : '#4ade80' }}>
                    {(ds.missing_total || 0).toLocaleString()}
                  </td>
                  <td className="num" style={{ color: ds.wrong_type_count > 0 ? '#f59e0b' : '#4ade80' }}>
                    {ds.wrong_type_count || 0}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Per-dataset column completeness mini charts */}
      {compareSelected.map((name, idx) => {
        const detail = details[name]
        if (!detail || !detail.column_details) return null
        const colData = detail.column_details.map(c => ({
          name: c.name,
          completeness: c.completeness_pct || 0,
        }))
        const minW = Math.max(400, colData.length * 30)
        return (
          <div className="compare-section" key={name} style={{ marginTop: 16 }}>
            <div className="section-heading" style={{ fontSize: 10 }}>
              {name}
            </div>
            <div style={{ overflowX: 'auto' }}>
              <div style={{ width: minW, height: 180 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={colData} margin={{ top: 5, right: 10, left: -15, bottom: 55 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(42,30,16,0.8)" vertical={false} />
                    <XAxis
                      dataKey="name"
                      tick={{ fill: '#78350f', fontSize: 8, fontFamily: "'JetBrains Mono', monospace" }}
                      angle={-50}
                      textAnchor="end"
                      interval={0}
                      tickLine={false}
                      axisLine={{ stroke: '#2a1e10' }}
                    />
                    <YAxis
                      domain={[0, 100]}
                      tick={{ fill: '#78350f', fontSize: 8, fontFamily: "'JetBrains Mono', monospace" }}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={v => `${v}%`}
                    />
                    <Tooltip
                      cursor={{ fill: 'rgba(245,158,11,0.06)' }}
                      contentStyle={{
                        background: '#1a1510',
                        border: '1px solid #92400e',
                        fontFamily: "'JetBrains Mono', monospace",
                        fontSize: 11,
                      }}
                      labelStyle={{ color: '#d97706' }}
                      itemStyle={{ color: '#fbbf24' }}
                    />
                    <Bar
                      dataKey="completeness"
                      name="Completeness %"
                      radius={[2, 2, 0, 0]}
                      maxBarSize={24}
                      fill={COLORS[idx % COLORS.length]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
