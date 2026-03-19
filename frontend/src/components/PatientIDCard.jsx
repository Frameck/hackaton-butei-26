import React, { useEffect, useState } from 'react'

export default function PatientIDCard({ datasetName, onClick }) {
  const [stats, setStats] = useState(null)

  useEffect(() => {
    fetch(`/api/datasets/${encodeURIComponent(datasetName)}/patient-id`)
      .then(r => r.json())
      .then(d => { if (!d.error) setStats(d) })
      .catch(() => {})
  }, [datasetName])

  if (!stats) return null

  const pct     = stats.invalid_pct
  const color   = pct > 10 ? '#f87171' : pct > 2 ? '#fbbf24' : '#34c759'
  const bgColor = pct > 10 ? 'rgba(220,38,38,0.08)' : pct > 2 ? 'rgba(245,158,11,0.08)' : 'rgba(52,199,89,0.08)'

  return (
    <div
      className="pid-card"
      style={{ borderColor: color + '55', background: bgColor }}
      onClick={onClick}
      title={`Click to view ${stats.invalid_count} invalid ${stats.column_name} values`}
    >
      <div className="pid-card-label">{stats.column_name}</div>
      <div className="pid-card-body">
        <span className="pid-card-count" style={{ color }}>
          {stats.invalid_count.toLocaleString()}
        </span>
        <span className="pid-card-suffix"> invalid</span>
      </div>
      <div className="pid-card-pct" style={{ color }}>
        {pct.toFixed(1)}% of {stats.total.toLocaleString()} rows
      </div>
      {stats.dominant_pattern && (
        <div className="pid-card-pattern">
          pattern: <code>{stats.dominant_pattern}</code>
        </div>
      )}
      <div className="pid-card-hint">click to inspect →</div>
    </div>
  )
}
