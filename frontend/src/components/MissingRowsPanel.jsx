import React, { useEffect, useState } from 'react'

export default function MissingRowsPanel({ datasetName, onClose }) {
  const [data, setData]     = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]   = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`/api/datasets/${encodeURIComponent(datasetName)}/missing-rows`)
      .then(r => r.json())
      .then(d => {
        if (d.error) throw new Error(d.error)
        setData(d)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [datasetName])

  return (
    <div className="missing-panel-overlay" onClick={onClose}>
      <div className="missing-panel" onClick={e => e.stopPropagation()}>
        <div className="missing-panel-header">
          <div className="missing-panel-title">
            Missing Values
            {data && (
              <span className="missing-panel-subtitle">
                {data.total_missing_rows.toLocaleString()} rows affected
                {data.shown_rows < data.total_missing_rows && ` (showing first ${data.shown_rows})`}
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
              No rows with missing values.
            </div>
          )}
          {data && data.rows.length > 0 && (
            <div className="missing-table-wrap">
              <table className="missing-table">
                <thead>
                  <tr>
                    <th className="missing-table-th">#</th>
                    {data.columns.map(col => (
                      <th key={col} className="missing-table-th">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map(row => (
                    <tr key={row.row_index} className="missing-table-row">
                      <td className="missing-table-idx">{row.row_index}</td>
                      {data.columns.map(col => {
                        const val = row.data[col]
                        const isMissing = val === null || val === undefined
                        return (
                          <td
                            key={col}
                            className={`missing-table-cell${isMissing ? ' missing-cell-null' : ''}`}
                          >
                            {isMissing ? <span className="null-badge">NULL</span> : String(val)}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
