import React, { useEffect, useState } from 'react'

const REASON_META = {
  corrupt_chars:    { label: 'CORRUPT',  color: '#c4b5fd' },
  pattern_mismatch: { label: 'PATTERN',  color: '#60a5fa' },
  missing:          { label: 'MISSING',  color: '#f87171' },
  empty:            { label: 'EMPTY',    color: '#fbbf24' },
}

export default function PatientIDPanel({ datasetName, columnName, onClose }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    setLoading(true)
    fetch(`/api/datasets/${encodeURIComponent(datasetName)}/patient-id/issues`)
      .then(r => r.json())
      .then(d => {
        if (d.error) throw new Error(d.error)
        setData(d)
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [datasetName])

  return (
    <div className="missing-panel-overlay" onClick={onClose}>
      <div className="missing-panel" onClick={e => e.stopPropagation()}>
        <div className="missing-panel-header">
          <div className="missing-panel-title">
            {columnName} — Invalid Values
            {data && (
              <span className="missing-panel-subtitle">
                {data.invalid_count.toLocaleString()} invalid
                {data.rows.length < data.invalid_count && ` (showing first ${data.rows.length})`}
                {' · '}{data.total.toLocaleString()} total rows
              </span>
            )}
          </div>
          <button className="missing-panel-close" onClick={onClose}>✕</button>
        </div>

        <div className="missing-panel-body">
          {loading && (
            <div className="loading-wrap">
              <div className="loading-spinner" />
              <div className="loading-text">LOADING...</div>
            </div>
          )}
          {error && <div className="error-text">Error: {error}</div>}
          {data && data.rows.length === 0 && (
            <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-muted)' }}>
              No invalid rows found.
            </div>
          )}
          {data && data.rows.length > 0 && (
            <div className="missing-table-wrap">
              <table className="missing-table">
                <thead>
                  <tr>
                    <th className="missing-table-th">#</th>
                    <th className="missing-table-th" style={{ color: 'var(--accent)' }}>
                      {data.column_name} ⚑
                    </th>
                    <th className="missing-table-th">REASON</th>
                    {data.columns.slice(1, 8).map(col => (
                      <th key={col} className="missing-table-th">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map(row => {
                    const m = REASON_META[row.invalid_reason] || REASON_META.missing
                    return (
                      <tr key={row.row_index} className="missing-table-row">
                        <td className="missing-table-idx">{row.row_index}</td>
                        <td className="missing-table-cell" style={{ color: m.color, fontWeight: 600 }}>
                          {row.patient_id_value === null
                            ? <span className="null-badge">NULL</span>
                            : row.patient_id_value}
                        </td>
                        <td className="missing-table-cell">
                          <span style={{
                            fontFamily: 'var(--font-data)', fontSize: 9, fontWeight: 700,
                            padding: '2px 6px', borderRadius: 3,
                            color: m.color, background: m.color + '20',
                            border: `1px solid ${m.color}55`,
                          }}>
                            {m.label}
                          </span>
                        </td>
                        {data.columns.slice(1, 8).map(col => (
                          <td key={col} className="missing-table-cell">
                            {row.data[col] === null
                              ? <span className="null-badge">NULL</span>
                              : String(row.data[col] ?? '')}
                          </td>
                        ))}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
