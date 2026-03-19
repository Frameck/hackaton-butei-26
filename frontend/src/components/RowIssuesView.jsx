import React, { useEffect, useState } from 'react'

const LEVEL_META = {
  HIGH:   { color: '#f87171', bg: 'rgba(220,38,38,0.12)',  label: 'HIGH'  },
  MEDIUM: { color: '#fbbf24', bg: 'rgba(245,158,11,0.10)', label: 'MED'   },
  LOW:    { color: '#60a5fa', bg: 'rgba(37,99,235,0.10)',  label: 'LOW'   },
}

const TYPE_COLOR = {
  corrupt_chars:    '#c4b5fd',
  type_mismatch:    '#fb923c',
  outlier:          '#34d399',
  pattern_mismatch: '#60a5fa',
  anomaly:          '#a78bfa',
  null:             '#f87171',
  empty:            '#fbbf24',
}

function LevelBadge({ level }) {
  const m = LEVEL_META[level] || LEVEL_META.LOW
  return (
    <span style={{
      fontFamily: 'var(--font-data)', fontSize: 9, fontWeight: 700,
      padding: '2px 6px', borderRadius: 3,
      color: m.color, background: m.bg, border: `1px solid ${m.color}55`,
    }}>
      {m.label}
    </span>
  )
}

export default function RowIssuesView({ datasetName }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)
  const [needsTraining, setNeedsTraining] = useState(false)
  const [expandedRow, setExpandedRow]     = useState(null)
  const [levelFilter, setLevelFilter]     = useState('all')
  const [issueColsOnly, setIssueColsOnly] = useState(true)

  useEffect(() => {
    if (!datasetName) return
    setLoading(true)
    setData(null)
    setError(null)
    setNeedsTraining(false)
    setExpandedRow(null)

    fetch(`/api/ml/issues/${encodeURIComponent(datasetName)}`)
      .then(r => r.json())
      .then(d => {
        if (d.needs_training) { setNeedsTraining(true); setLoading(false); return }
        if (d.error) throw new Error(d.error)
        setData(d)
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [datasetName])

  if (loading) return (
    <div className="loading-wrap" style={{ minHeight: 200 }}>
      <div className="loading-spinner" />
      <div className="loading-text">ML SCANNING ROWS...</div>
    </div>
  )

  if (needsTraining) return (
    <div style={{ padding: 32, textAlign: 'center', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12 }}>
      <span style={{ fontSize: 32, opacity: 0.3 }}>◈</span>
      <span style={{ fontFamily: 'var(--font-body)', fontSize: 13, color: 'var(--text-secondary)' }}>
        ML model not trained yet.
      </span>
      <span style={{ fontFamily: 'var(--font-body)', fontSize: 11, color: 'var(--text-muted)' }}>
        Go to the <strong>ML QUALITY SCAN</strong> tab and click <strong>RETRAIN MODEL</strong>.
      </span>
    </div>
  )

  if (error) return <div className="error-text">Error: {error}</div>
  if (!data)  return null

  const { total_rows, total_issue_rows, shown_rows, issue_rows = [], columns = [] } = data

  const pct = total_rows > 0 ? ((total_issue_rows / total_rows) * 100).toFixed(1) : '0.0'

  // Confidence breakdown counts
  const levelCounts = { HIGH: 0, MEDIUM: 0, LOW: 0 }
  issue_rows.forEach(r => {
    const top = LEVEL_META[r.issues[0]?.level] ? r.issues[0].level : 'LOW'
    levelCounts[top] = (levelCounts[top] || 0) + 1
  })

  const filtered = levelFilter === 'all'
    ? issue_rows
    : issue_rows.filter(r => r.issues.some(i => i.level === levelFilter))

  return (
    <div className="row-issues-wrap">
      {/* ── Summary bar ─────────────────────────────────────────── */}
      <div className="row-issues-summary">
        <div className="ri-stat">
          <span className="ri-stat-val" style={{ color: total_issue_rows > 0 ? '#f87171' : '#4ade80' }}>
            {total_issue_rows.toLocaleString()}
          </span>
          <span className="ri-stat-label">ANOMALOUS ROWS</span>
        </div>
        <div className="ri-stat">
          <span className="ri-stat-val">{total_rows.toLocaleString()}</span>
          <span className="ri-stat-label">ROWS SCANNED</span>
        </div>
        <div className="ri-stat">
          <span className="ri-stat-val" style={{ color: parseFloat(pct) > 20 ? '#f87171' : parseFloat(pct) > 5 ? '#fbbf24' : '#4ade80' }}>
            {pct}%
          </span>
          <span className="ri-stat-label">ANOMALY RATE</span>
        </div>

        <div className="ri-divider" />

        {Object.entries(levelCounts).map(([level, count]) => {
          const m = LEVEL_META[level]
          return (
            <div key={level} className="ri-stat">
              <span className="ri-stat-val" style={{ color: m.color }}>{count}</span>
              <span className="ri-stat-label">{m.label} CONF.</span>
            </div>
          )
        })}

        {shown_rows < total_issue_rows && (
          <div className="ri-cap-notice">
            Showing top {shown_rows.toLocaleString()} of {total_issue_rows.toLocaleString()} rows
          </div>
        )}
      </div>

      {total_issue_rows === 0 ? (
        <div className="ri-clean">
          <span style={{ fontSize: 32 }}>✓</span>
          <span style={{ fontFamily: 'var(--font-heading)', color: '#4ade80', letterSpacing: '0.1em' }}>
            NO ANOMALIES DETECTED
          </span>
        </div>
      ) : (
        <>
          {/* ── Filter toolbar ──────────────────────────────────── */}
          <div className="ri-toolbar">
            <span className="col-toolbar-label">FILTER BY CONFIDENCE</span>
            {['all', 'HIGH', 'MEDIUM', 'LOW'].map(lvl => (
              <button
                key={lvl}
                className={`view-toggle-btn ${levelFilter === lvl ? 'active' : ''}`}
                onClick={() => { setLevelFilter(lvl); setExpandedRow(null) }}
                style={lvl !== 'all' ? { color: LEVEL_META[lvl]?.color } : {}}
              >
                {lvl === 'all' ? 'ALL' : LEVEL_META[lvl].label}
                {lvl !== 'all' && (
                  <span style={{ marginLeft: 4, opacity: 0.7 }}>({levelCounts[lvl] || 0})</span>
                )}
              </button>
            ))}
          </div>

          {/* ── Issue rows table ────────────────────────────────── */}
          <div className="preview-section" style={{ marginTop: 0 }}>
            <div className="preview-scroll" style={{ maxHeight: 520 }}>
              <table className="preview-table issue-table">
                <thead>
                  <tr>
                    <th style={{ minWidth: 60 }}>ROW #</th>
                    <th style={{ minWidth: 80 }}>CONF.</th>
                    <th style={{ minWidth: 220 }}>ANOMALOUS CELLS</th>
                    <th style={{ minWidth: 40 }} />
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(row => {
                    const isExpanded  = expandedRow === row.row_index
                    const issueCols   = [...new Set(row.issues.map(i => i.col))]
                    const topLevel    = row.issues[0]?.level || 'LOW'
                    const topMeta     = LEVEL_META[topLevel] || LEVEL_META.LOW

                    return (
                      <React.Fragment key={row.row_index}>
                        <tr
                          className={`issue-row ${isExpanded ? 'expanded' : ''}`}
                          onClick={() => setExpandedRow(isExpanded ? null : row.row_index)}
                        >
                          <td style={{ fontFamily: 'var(--font-data)', color: 'var(--text-muted)' }}>
                            {row.row_index + 1}
                          </td>
                          <td>
                            <span style={{
                              fontFamily: 'var(--font-data)', fontSize: 12, fontWeight: 700,
                              color: topMeta.color,
                            }}>
                              {(row.max_confidence * 100).toFixed(0)}%
                            </span>
                          </td>
                          <td>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                              {row.issues.slice(0, 5).map((issue, i) => {
                                const m = LEVEL_META[issue.level] || LEVEL_META.LOW
                                return (
                                  <span key={i} className="issue-col-chip" title={`${issue.value} — ${issue.issue_type.replace('_', ' ')}`}>
                                    <LevelBadge level={issue.level} />
                                    <span style={{ fontFamily: 'var(--font-data)', fontSize: 9, color: 'var(--text-muted)', marginLeft: 3 }}>
                                      {issue.col.length > 18 ? issue.col.slice(0, 17) + '…' : issue.col}
                                    </span>
                                  </span>
                                )
                              })}
                              {row.issues.length > 5 && (
                                <span style={{ fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-data)', alignSelf: 'center' }}>
                                  +{row.issues.length - 5} more
                                </span>
                              )}
                            </div>
                          </td>
                          <td style={{ textAlign: 'center', color: 'var(--text-muted)', fontSize: 10 }}>
                            {isExpanded ? '▲' : '▼'}
                          </td>
                        </tr>

                        {/* Expanded detail */}
                        {isExpanded && (
                          <tr className="issue-detail-row">
                            <td colSpan={4} style={{ padding: 0 }}>
                              <div className="issue-detail-wrap">
                                <div className="ri-toolbar" style={{ padding: '8px 12px', borderBottom: '1px solid var(--border)' }}>
                                  <span className="col-toolbar-label">SHOW COLUMNS</span>
                                  <button className={`view-toggle-btn ${issueColsOnly ? 'active' : ''}`}
                                    onClick={e => { e.stopPropagation(); setIssueColsOnly(true) }}>
                                    ANOMALOUS ONLY ({issueCols.length})
                                  </button>
                                  <button className={`view-toggle-btn ${!issueColsOnly ? 'active' : ''}`}
                                    onClick={e => { e.stopPropagation(); setIssueColsOnly(false) }}>
                                    ALL COLUMNS
                                  </button>
                                </div>

                                <div style={{ overflowX: 'auto' }}>
                                  <table className="preview-table" style={{ margin: 0 }}>
                                    <thead>
                                      <tr>
                                        {(issueColsOnly ? issueCols : columns).map(col => {
                                          const issue = row.issues.find(i => i.col === col)
                                          return (
                                            <th key={col} title={col} style={issue ? { color: LEVEL_META[issue.level]?.color || 'var(--text-primary)' } : {}}>
                                              {col.length > 16 ? col.slice(0, 15) + '…' : col}
                                              {issue && <span style={{ marginLeft: 3, fontSize: 8 }}>⚑</span>}
                                            </th>
                                          )
                                        })}
                                      </tr>
                                    </thead>
                                    <tbody>
                                      <tr>
                                        {(issueColsOnly ? issueCols : columns).map(col => {
                                          const val   = row.data[col]
                                          const issue = row.issues.find(i => i.col === col)
                                          const m     = issue ? (LEVEL_META[issue.level] || LEVEL_META.LOW) : null
                                          return (
                                            <td key={col}
                                              title={issue ? `${(issue.confidence * 100).toFixed(0)}% confidence — ${issue.issue_type.replace('_', ' ')}` : undefined}
                                              style={m ? { color: m.color, background: m.bg } : {}}>
                                              {val === null || val === undefined
                                                ? <span style={{ fontStyle: 'italic', opacity: 0.5 }}>NULL</span>
                                                : val}
                                            </td>
                                          )
                                        })}
                                      </tr>
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    )
                  })}
                </tbody>
              </table>
            </div>
            {filtered.length === 0 && (
              <div style={{ textAlign: 'center', padding: 24, color: 'var(--text-muted)', fontFamily: 'var(--font-data)', fontSize: 12 }}>
                No rows match this filter.
              </div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
