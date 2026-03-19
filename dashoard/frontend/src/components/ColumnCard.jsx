import React from 'react'

function typeBadgeClass(t) {
  if (!t) return 'default'
  const lower = t.toLowerCase()
  if (lower.includes('int'))      return 'integer'
  if (lower.includes('float'))    return 'float'
  if (lower.includes('bool'))     return 'boolean'
  if (lower.includes('date'))     return 'datetime'
  if (lower.includes('mixed'))    return 'mixed'
  if (lower.includes('string') || lower.includes('str')) return 'string'
  return 'default'
}

function completenessBarColor(pct) {
  if (pct >= 90) return '#16a34a'
  if (pct >= 70) return '#f59e0b'
  return '#dc2626'
}

export default function ColumnCard({ col }) {
  const {
    name, inferred_type, missing_pct, completeness_pct,
    unique_count, wrong_type, missing_total,
  } = col

  const hasWarning = wrong_type || missing_pct > 10

  return (
    <div className={`column-card ${hasWarning ? 'has-warning' : ''}`}>
      <div className="col-name" title={name}>{name}</div>

      <div className="col-badges">
        <span className={`type-badge ${typeBadgeClass(inferred_type)}`}>
          {inferred_type || 'unknown'}
        </span>
        {wrong_type && (
          <span className="warning-badge wrong-type">⚠ WRONG TYPE</span>
        )}
        {missing_pct > 10 && !wrong_type && (
          <span className="warning-badge missing">⚠ MISSING DATA</span>
        )}
        {missing_pct > 10 && wrong_type && (
          <span className="warning-badge missing">⚠ MISSING DATA</span>
        )}
      </div>

      <div className="completeness-bar-bg">
        <div
          className="completeness-bar-fill"
          style={{
            width: `${completeness_pct || 0}%`,
            background: completenessBarColor(completeness_pct || 0),
          }}
        />
      </div>

      <div className="col-stats" style={{ marginTop: 4 }}>
        <span>
          Completeness: <span style={{ color: completenessBarColor(completeness_pct || 0) }}>
            {(completeness_pct || 0).toFixed(1)}%
          </span>
        </span>
        <span>
          Missing: <span>{missing_total || 0}</span>
        </span>
      </div>
      <div className="col-stats">
        <span>
          Unique: <span>{(unique_count || 0).toLocaleString()}</span>
        </span>
        <span>
          {(missing_pct || 0).toFixed(1)}% miss
        </span>
      </div>
    </div>
  )
}
