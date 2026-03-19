import React, { useState, useEffect } from 'react'

const LEVEL_META = {
  HIGH:   { color: '#f87171', glow: 'rgba(220,38,38,0.3)',   label: 'HIGH' },
  MEDIUM: { color: '#fbbf24', glow: 'rgba(245,158,11,0.3)',  label: 'MED'  },
  LOW:    { color: '#60a5fa', glow: 'rgba(37,99,235,0.25)',  label: 'LOW'  },
  NONE:   { color: '#4ade80', glow: 'rgba(22,163,74,0.2)',   label: 'OK'   },
}

const ISSUE_COLORS = {
  corrupt_chars:    '#c4b5fd',
  type_mismatch:    '#fb923c',
  outlier:          '#34d399',
  pattern_mismatch: '#60a5fa',
  anomaly:          '#a78bfa',
  null:             '#f87171',
  empty:            '#fbbf24',
  clean:            '#94a3b8',
}

function QualityRing({ score }) {
  const r = 38
  const circ = 2 * Math.PI * r
  const fill = circ * (score / 100)
  const color = score >= 80 ? '#4ade80' : score >= 55 ? '#fbbf24' : '#f87171'
  return (
    <svg width={100} height={100} style={{ display: 'block' }}>
      <circle cx={50} cy={50} r={r} fill="none" stroke="var(--border)" strokeWidth={8} />
      <circle
        cx={50} cy={50} r={r} fill="none"
        stroke={color} strokeWidth={8}
        strokeDasharray={`${fill} ${circ}`}
        strokeLinecap="round"
        transform="rotate(-90 50 50)"
        style={{ filter: `drop-shadow(0 0 6px ${color})`, transition: 'stroke-dasharray 0.6s ease' }}
      />
      <text x={50} y={50} textAnchor="middle" dominantBaseline="central"
        style={{ fontFamily: 'var(--font-data)', fontSize: 18, fontWeight: 700, fill: color }}>
        {score.toFixed(0)}
      </text>
      <text x={50} y={66} textAnchor="middle"
        style={{ fontFamily: 'var(--font-body)', fontSize: 7, letterSpacing: 0, fill: 'var(--text-muted)' }}>
        QUALITY
      </text>
    </svg>
  )
}

function ConfidenceBar({ value, max = 1 }) {
  const pct = Math.min(100, (value / max) * 100)
  const color = value >= 0.85 ? '#f87171' : value >= 0.55 ? '#fbbf24' : value >= 0.3 ? '#60a5fa' : '#4ade80'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ flex: 1, height: 6, background: 'var(--border)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{
          width: `${pct}%`, height: '100%', background: color, borderRadius: 3,
          boxShadow: `0 0 6px ${color}`, transition: 'width 0.5s ease',
        }} />
      </div>
      <span style={{ fontFamily: 'var(--font-data)', fontSize: 10, color, minWidth: 36, textAlign: 'right' }}>
        {(value * 100).toFixed(1)}%
      </span>
    </div>
  )
}

export default function MLPanel({ datasetName }) {
  const [mlStatus, setMlStatus]   = useState(null)   // global model status
  const [result,   setResult]     = useState(null)
  const [loading,  setLoading]    = useState(false)
  const [training, setTraining]   = useState(false)
  const [error,    setError]      = useState(null)
  const [tab,      setTab]        = useState('overview') // overview | columns | cells

  // Poll model status on mount
  useEffect(() => {
    fetch('/api/ml/status').then(r => r.json()).then(setMlStatus).catch(() => {})
  }, [])

  // Poll training status while training
  useEffect(() => {
    if (!training) return
    const id = setInterval(() => {
      fetch('/api/ml/status').then(r => r.json()).then(s => {
        setMlStatus(s)
        if (s.training_status?.state === 'done' || s.training_status?.state === 'error') {
          setTraining(false)
          clearInterval(id)
        }
      })
    }, 2000)
    return () => clearInterval(id)
  }, [training])

  function runScan() {
    setLoading(true)
    setError(null)
    setResult(null)
    fetch(`/api/ml/predict/${encodeURIComponent(datasetName)}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) throw new Error(d.needs_training ? 'Model not trained yet. Click TRAIN MODEL first.' : d.error)
        setResult(d)
        setLoading(false)
      })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  function startTraining() {
    setTraining(true)
    fetch('/api/ml/train', { method: 'POST' })
      .then(r => r.json())
      .catch(e => { setTraining(false); setError(e.message) })
  }

  const isModelTrained = mlStatus?.trained
  const trainingState  = mlStatus?.training_status?.state

  // Sort columns by mean_confidence desc
  const sortedCols = result
    ? Object.entries(result.column_results)
        .sort((a, b) => b[1].mean_confidence - a[1].mean_confidence)
    : []

  return (
    <div className="ml-panel">
      {/* ── Header bar ──────────────────────────────────────── */}
      <div className="ml-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span className="ml-logo">◈ ML</span>
          <div>
            <div style={{ fontFamily: 'var(--font-body)', fontSize: 11, color: 'var(--accent)', letterSpacing: 0 }}>
              ANOMALY SCANNER
            </div>
            <div style={{ fontFamily: 'var(--font-data)', fontSize: 9, color: 'var(--text-muted)', marginTop: 2 }}>
              Isolation Forest · {mlStatus?.metrics?.n_features ?? 15} features · trained on {
                mlStatus?.metrics?.n_train_cells
                  ? (mlStatus.metrics.n_train_cells / 1000).toFixed(0) + 'k clean cells'
                  : '—'
              }
              {mlStatus?.metrics?.n_train_files ? ` from ${mlStatus.metrics.n_train_files} files` : ''}
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 8, marginLeft: 'auto', alignItems: 'center' }}>
          {/* Model status badge */}
          <span style={{
            fontFamily: 'var(--font-data)', fontSize: 9, padding: '3px 8px', borderRadius: 4,
            background: isModelTrained ? 'rgba(22,163,74,0.15)' : 'rgba(220,38,38,0.15)',
            color: isModelTrained ? '#4ade80' : '#f87171',
            border: `1px solid ${isModelTrained ? 'rgba(22,163,74,0.4)' : 'rgba(220,38,38,0.4)'}`,
          }}>
            {trainingState === 'running' ? '⟳ TRAINING…' : isModelTrained ? '✓ MODEL READY' : '✗ NOT TRAINED'}
          </span>

          {/* Train button */}
          <button
            className="ml-btn secondary"
            onClick={startTraining}
            disabled={training || trainingState === 'running'}
          >
            {training || trainingState === 'running' ? 'TRAINING…' : 'RETRAIN MODEL'}
          </button>

          {/* Scan button */}
          <button
            className="ml-btn primary"
            onClick={runScan}
            disabled={loading || !isModelTrained}
          >
            {loading ? 'SCANNING…' : '▶ RUN SCAN'}
          </button>
        </div>
      </div>

      {error && (
        <div className="error-text" style={{ margin: '12px 0' }}>{error}</div>
      )}

      {loading && (
        <div className="loading-wrap" style={{ minHeight: 160 }}>
          <div className="loading-spinner" />
          <div className="loading-text">ML SCANNING…</div>
        </div>
      )}

      {trainingState === 'running' && !result && (
        <div style={{ padding: '20px', textAlign: 'center', fontFamily: 'var(--font-body)', color: 'var(--accent)', fontSize: 11, letterSpacing: 0, animation: 'pulse 1.2s infinite' }}>
          TRAINING IN PROGRESS… THIS MAY TAKE 1-2 MINUTES
        </div>
      )}

      {/* ── Results ──────────────────────────────────────────── */}
      {result && !loading && (
        <>
          {/* Tabs */}
          <div className="tab-bar" style={{ marginTop: 16 }}>
            {[['overview','OVERVIEW'],['columns','COLUMNS'],['cells','TOP ISSUES']].map(([k,l]) => (
              <button key={k} className={`tab-btn ${tab===k?'active':''}`} onClick={() => setTab(k)}>{l}</button>
            ))}
          </div>

          {/* OVERVIEW tab */}
          {tab === 'overview' && (
            <div className="ml-overview">
              {/* Quality ring + summary */}
              <div className="ml-summary-card">
                <QualityRing score={result.quality_score} />
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <div className="ml-stat-row">
                    <div className="ml-stat">
                      <span className="ml-stat-val" style={{ color: result.overall_issue_rate > 0.2 ? '#f87171' : '#fbbf24' }}>
                        {(result.overall_issue_rate * 100).toFixed(1)}%
                      </span>
                      <span className="ml-stat-label">ISSUE RATE</span>
                    </div>
                    <div className="ml-stat">
                      <span className="ml-stat-val">{result.total_cells_analyzed.toLocaleString()}</span>
                      <span className="ml-stat-label">CELLS SCANNED</span>
                    </div>
                    <div className="ml-stat">
                      <span className="ml-stat-val" style={{ color: '#f87171' }}>
                        {result.top_issue_cells.filter(c => c.level === 'HIGH').length}
                      </span>
                      <span className="ml-stat-label">HIGH CONF. ISSUES</span>
                    </div>
                    <div className="ml-stat">
                      <span className="ml-stat-val" style={{ color: '#4ade80' }}>
                        {(result.quality_score).toFixed(1)}
                      </span>
                      <span className="ml-stat-label">QUALITY SCORE</span>
                    </div>
                  </div>

                  {/* Issue type breakdown from top cells */}
                  <div>
                    <div style={{ fontFamily: 'var(--font-body)', fontSize: 9, letterSpacing: 0, color: 'var(--text-muted)', marginBottom: 6 }}>
                      ISSUE TYPES DETECTED
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {Object.entries(
                        result.top_issue_cells.reduce((acc, c) => {
                          acc[c.issue_type] = (acc[c.issue_type] || 0) + 1
                          return acc
                        }, {})
                      ).map(([type, count]) => (
                        <span key={type} style={{
                          fontFamily: 'var(--font-data)', fontSize: 10, padding: '3px 8px',
                          borderRadius: 4, border: `1px solid ${ISSUE_COLORS[type] || '#888'}55`,
                          background: `${ISSUE_COLORS[type] || '#888'}15`,
                          color: ISSUE_COLORS[type] || '#aaa',
                        }}>
                          {type.replace('_', ' ')} ({count})
                        </span>
                      ))}
                    </div>
                  </div>

                  {/* How the model works */}
                  <div style={{ background: 'var(--bg-primary)', borderRadius: 8, padding: '10px 14px', border: '1px solid var(--border)' }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 6 }}>
                      How anomaly detection works
                    </div>
                    {[
                      ['◎', 'Trained only on clean data — learns what normal looks like'],
                      ['⊘', 'At scan time, each cell is scored against that clean baseline'],
                      ['⚑', 'High confidence = cell looks very different from any clean data seen in training'],
                      ['✓', 'No labels or dirty data needed — no circular logic'],
                    ].map(([icon, text]) => (
                      <div key={text} style={{ display: 'flex', gap: 8, marginBottom: 4, alignItems: 'flex-start' }}>
                        <span style={{ color: 'var(--accent)', fontSize: 11, flexShrink: 0, marginTop: 1 }}>{icon}</span>
                        <span style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.4 }}>{text}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* COLUMNS tab */}
          {tab === 'columns' && (
            <div className="ml-col-list">
              <div style={{ fontFamily: 'var(--font-body)', fontSize: 9, color: 'var(--text-muted)', letterSpacing: 0, marginBottom: 10 }}>
                COLUMNS RANKED BY MEAN ISSUE CONFIDENCE ↓
              </div>
              {sortedCols.map(([col, stats]) => {
                const meta = LEVEL_META[stats.confidence_level] || LEVEL_META.NONE
                return (
                  <div key={col} className="ml-col-row">
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                      <span className="ml-level-badge" style={{ color: meta.color, borderColor: meta.color + '55', background: meta.color + '15' }}>
                        {meta.label}
                      </span>
                      <span style={{ fontFamily: 'var(--font-data)', fontSize: 11, color: 'var(--text-primary)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={col}>
                        {col}
                      </span>
                      <span style={{ fontFamily: 'var(--font-data)', fontSize: 9, color: 'var(--text-muted)' }}>
                        {stats.issue_count}/{stats.total_cells} cells
                      </span>
                      {stats.dominant_issue !== 'clean' && (
                        <span style={{
                          fontFamily: 'var(--font-data)', fontSize: 8, padding: '1px 5px', borderRadius: 3,
                          background: `${ISSUE_COLORS[stats.dominant_issue] || '#888'}15`,
                          color: ISSUE_COLORS[stats.dominant_issue] || '#aaa',
                          border: `1px solid ${ISSUE_COLORS[stats.dominant_issue] || '#888'}40`,
                        }}>
                          {stats.dominant_issue.replace('_', ' ')}
                        </span>
                      )}
                    </div>
                    <ConfidenceBar value={stats.mean_confidence} />
                  </div>
                )
              })}
            </div>
          )}

          {/* CELLS tab */}
          {tab === 'cells' && (
            <div>
              <div style={{ fontFamily: 'var(--font-body)', fontSize: 9, color: 'var(--text-muted)', letterSpacing: 0, marginBottom: 10 }}>
                TOP SUSPICIOUS CELLS (MAX 50, SORTED BY CONFIDENCE)
              </div>
              <div className="preview-scroll" style={{ maxHeight: 460 }}>
                <table className="preview-table">
                  <thead>
                    <tr>
                      <th style={{ minWidth: 50 }}>ROW</th>
                      <th style={{ minWidth: 120 }}>COLUMN</th>
                      <th style={{ minWidth: 70 }}>CONFIDENCE</th>
                      <th style={{ minWidth: 80 }}>ISSUE TYPE</th>
                      <th>VALUE</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.top_issue_cells.map((cell, i) => {
                      const meta = LEVEL_META[cell.level] || LEVEL_META.NONE
                      return (
                        <tr key={i}>
                          <td style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-data)' }}>{cell.row_index + 1}</td>
                          <td style={{ fontFamily: 'var(--font-data)', fontSize: 10 }} title={cell.col}>
                            {cell.col.length > 22 ? cell.col.slice(0,21) + '…' : cell.col}
                          </td>
                          <td>
                            <span style={{
                              fontFamily: 'var(--font-data)', fontSize: 11, fontWeight: 700,
                              color: meta.color, textShadow: `0 0 8px ${meta.color}`,
                            }}>
                              {(cell.confidence * 100).toFixed(1)}%
                            </span>
                          </td>
                          <td>
                            <span style={{
                              fontFamily: 'var(--font-data)', fontSize: 9, padding: '2px 6px', borderRadius: 3,
                              background: `${ISSUE_COLORS[cell.issue_type] || '#888'}15`,
                              color: ISSUE_COLORS[cell.issue_type] || '#aaa',
                              border: `1px solid ${ISSUE_COLORS[cell.issue_type] || '#888'}40`,
                            }}>
                              {cell.issue_type.replace('_', ' ')}
                            </span>
                          </td>
                          <td style={{
                            fontFamily: 'var(--font-data)', fontSize: 10, maxWidth: 200,
                            overflow: 'hidden', textOverflow: 'ellipsis',
                            color: cell.value === null ? 'var(--accent-red)' : 'var(--text-primary)',
                            fontStyle: cell.value === null ? 'italic' : 'normal',
                          }}>
                            {cell.value === null ? 'NULL' : cell.value}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {/* Empty state */}
      {!result && !loading && !error && !training && (
        <div style={{
          padding: '32px', textAlign: 'center', display: 'flex', flexDirection: 'column',
          alignItems: 'center', gap: 12,
        }}>
          <span style={{ fontSize: 36, opacity: 0.3 }}>◈</span>
          <span style={{ fontFamily: 'var(--font-body)', fontSize: 11, color: 'var(--text-muted)', letterSpacing: 0 }}>
            {isModelTrained ? 'CLICK ▶ RUN SCAN TO ANALYSE THIS DATASET' : 'TRAIN MODEL FIRST, THEN RUN SCAN'}
          </span>
        </div>
      )}
    </div>
  )
}
