import React, { useEffect, useState } from 'react'

const DIR_COLOR = { HIGH: '#f87171', LOW: '#60a5fa' }

export default function LabAnomalyPanel({ datasetName }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    setLoading(true)
    setData(null)
    setError(null)
    fetch(`/api/datasets/${encodeURIComponent(datasetName)}/lab-anomalies`)
      .then(r => r.json())
      .then(d => { if (d.error) throw new Error(d.error); setData(d) })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [datasetName])

  if (loading) return (
    <div className="loading-wrap">
      <div className="loading-spinner" />
      <div className="loading-text">SCANNING LAB VALUES…</div>
    </div>
  )
  if (error) return <div className="error-text">Error: {error}</div>
  if (!data)  return null

  if (!data.has_lab_data) return (
    <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontFamily: 'var(--font-data)', fontSize: 12 }}>
      No lab reference columns detected in this dataset.<br />
      <span style={{ fontSize: 10, opacity: 0.6 }}>Expected pattern: param_value + param_ref_low / param_ref_high</span>
    </div>
  )

  const totalAnomalies = data.parameters.reduce((s, p) => s + p.anomaly_count, 0)

  return (
    <div style={{ padding: '16px 0' }}>
      {/* ── Summary bar ──────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
        <div className="stat-card">
          <div className="stat-icon">⚗</div>
          <div className="stat-label">Parameters</div>
          <div className="stat-value amber">{data.parameters.length}</div>
        </div>
        <div className="stat-card">
          <div className="stat-icon">⚠</div>
          <div className="stat-label">Out-of-range values</div>
          <div className="stat-value red">{totalAnomalies.toLocaleString()}</div>
        </div>
        <div className="stat-card">
          <div className="stat-icon">◉</div>
          <div className="stat-label">Affected rows</div>
          <div className="stat-value red">{data.total_anomaly_rows.toLocaleString()}</div>
        </div>
      </div>

      {/* ── Per-parameter breakdown ───────────────────────────── */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontFamily: 'var(--font-data)', fontSize: 10, color: 'var(--text-muted)', marginBottom: 8, letterSpacing: 1 }}>
          PARAMETERS WITH OUT-OF-RANGE VALUES
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {data.parameters.filter(p => p.anomaly_count > 0).map(p => {
            const pct = p.anomaly_pct
            const color = pct > 20 ? '#f87171' : pct > 5 ? '#fbbf24' : '#60a5fa'
            return (
              <div key={p.param} style={{
                background: 'var(--surface)', border: '1px solid var(--border)',
                borderRadius: 8, padding: '10px 14px',
                display: 'flex', alignItems: 'center', gap: 12,
              }}>
                <div style={{ fontFamily: 'var(--font-data)', fontSize: 11, fontWeight: 700, minWidth: 120, color: 'var(--text)' }}>
                  {p.param.toUpperCase()}
                </div>
                <div style={{ flex: 1, height: 4, background: 'var(--border)', borderRadius: 2 }}>
                  <div style={{ width: `${Math.min(pct, 100)}%`, height: '100%', background: color, borderRadius: 2 }} />
                </div>
                <div style={{ fontFamily: 'var(--font-data)', fontSize: 10, color, minWidth: 80, textAlign: 'right' }}>
                  {p.anomaly_count} / {p.total_measured} ({pct}%)
                </div>
                <div style={{ fontFamily: 'var(--font-data)', fontSize: 9, color: 'var(--text-muted)', minWidth: 60, textAlign: 'right' }}>
                  {p.low_col ? `↓${p.low_col}` : ''} {p.high_col ? `↑${p.high_col}` : ''}
                </div>
              </div>
            )
          })}
          {data.parameters.filter(p => p.anomaly_count > 0).length === 0 && (
            <div style={{ color: '#34c759', fontFamily: 'var(--font-data)', fontSize: 12, padding: 16 }}>
              ✓ All lab values within reference ranges.
            </div>
          )}
        </div>
      </div>

      {/* ── Anomaly rows table ────────────────────────────────── */}
      {data.anomaly_rows.length > 0 && (
        <div>
          <div style={{ fontFamily: 'var(--font-data)', fontSize: 10, color: 'var(--text-muted)', marginBottom: 8, letterSpacing: 1 }}>
            OUT-OF-RANGE ROWS
            {data.total_anomaly_rows > data.anomaly_rows.length && (
              <span style={{ color: '#fbbf24', marginLeft: 8 }}>
                (showing {data.anomaly_rows.length} of {data.total_anomaly_rows})
              </span>
            )}
          </div>
          <div className="missing-table-wrap">
            <table className="missing-table">
              <thead>
                <tr>
                  <th className="missing-table-th">#</th>
                  <th className="missing-table-th">PARAMETER</th>
                  <th className="missing-table-th">DIR</th>
                  <th className="missing-table-th">VALUE</th>
                  <th className="missing-table-th">REF LOW</th>
                  <th className="missing-table-th">REF HIGH</th>
                  <th className="missing-table-th">FLAG</th>
                  {(data.display_cols || []).slice(0, 4).map(c => (
                    <th key={c} className="missing-table-th">{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.anomaly_rows.map(row =>
                  Object.entries(row.anomalies).map(([param, info], i) => {
                    const color = DIR_COLOR[info.direction] || '#aeaeb2'
                    return (
                      <tr key={`${row.row_index}-${param}`} className="missing-table-row">
                        {i === 0 && (
                          <td className="missing-table-idx" rowSpan={Object.keys(row.anomalies).length}>
                            {row.row_index}
                          </td>
                        )}
                        <td className="missing-table-cell" style={{ fontWeight: 700, color: 'var(--accent)' }}>
                          {param.toUpperCase()}
                        </td>
                        <td className="missing-table-cell">
                          <span style={{
                            fontFamily: 'var(--font-data)', fontSize: 9, fontWeight: 700,
                            padding: '2px 6px', borderRadius: 3,
                            color, background: color + '20', border: `1px solid ${color}55`,
                          }}>
                            {info.direction}
                          </span>
                        </td>
                        <td className="missing-table-cell" style={{ color, fontWeight: 600 }}>
                          {info.value?.toFixed(2)}
                        </td>
                        <td className="missing-table-cell" style={{ color: 'var(--text-muted)' }}>
                          {info.ref_low != null ? info.ref_low : '—'}
                        </td>
                        <td className="missing-table-cell" style={{ color: 'var(--text-muted)' }}>
                          {info.ref_high != null ? info.ref_high : '—'}
                        </td>
                        <td className="missing-table-cell">
                          {info.flag
                            ? <span style={{ color: '#fbbf24', fontFamily: 'var(--font-data)', fontSize: 10 }}>{info.flag}</span>
                            : <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>—</span>}
                        </td>
                        {i === 0 && (data.display_cols || []).slice(0, 4).map(c => (
                          <td key={c} className="missing-table-cell" rowSpan={Object.keys(row.anomalies).length}>
                            {row.data[c] === null ? <span className="null-badge">NULL</span> : String(row.data[c] ?? '')}
                          </td>
                        ))}
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
