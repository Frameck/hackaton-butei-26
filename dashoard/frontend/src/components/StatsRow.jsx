import React from 'react'

function StatCard({ icon, label, value, valueClass, onClick }) {
  return (
    <div className={`stat-card${onClick ? ' stat-card-clickable' : ''}`} onClick={onClick}>
      <div className="stat-icon">{icon}</div>
      <div className="stat-label">{label}</div>
      <div className={`stat-value ${valueClass || ''}`}>{value}</div>
      {onClick && <div className="stat-hint">click to view rows</div>}
    </div>
  )
}

function completenessClass(pct) {
  if (pct >= 90) return 'green'
  if (pct >= 70) return 'amber'
  return 'red'
}

export default function StatsRow({ dataset, onMissingClick, children }) {
  if (!dataset) return null
  const { rows, columns, completeness, missing_total } = dataset

  return (
    <div className="stats-row">
      <StatCard
        icon="⊞"
        label="Total Rows"
        value={(rows || 0).toLocaleString()}
        valueClass="amber"
      />
      <StatCard
        icon="⊟"
        label="Total Columns"
        value={(columns || 0).toLocaleString()}
        valueClass="amber"
      />
      <StatCard
        icon="◉"
        label="Completeness"
        value={`${(completeness || 0).toFixed(1)}%`}
        valueClass={completenessClass(completeness || 0)}
      />
      <StatCard
        icon="⚠"
        label="Missing Values"
        value={(missing_total || 0).toLocaleString()}
        valueClass={missing_total > 0 ? 'red' : 'green'}
        onClick={missing_total > 0 ? onMissingClick : undefined}
      />
      {children}
    </div>
  )
}
