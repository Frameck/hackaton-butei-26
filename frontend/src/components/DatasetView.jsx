import React, { useEffect, useState } from 'react'
import StatsRow from './StatsRow.jsx'
import CompletenessChart from './CompletenessChart.jsx'
import ColumnCard from './ColumnCard.jsx'
import DataPreview from './DataPreview.jsx'
import RowIssuesView from './RowIssuesView.jsx'
import MLPanel from './MLPanel.jsx'
import MissingRowsPanel from './MissingRowsPanel.jsx'
import PatientIDCard from './PatientIDCard.jsx'
import PatientIDPanel from './PatientIDPanel.jsx'
import AIRepairPanel from './AIRepairPanel.jsx'
import LabAnomalyPanel from './LabAnomalyPanel.jsx'

function typeToExt(type) {
  return (type || '').toLowerCase()
}

// ── Column group thresholds ────────────────────────────────────────
const GROUPS = [
  { key: 'complete',   label: 'COMPLETE',   min: 90,  max: 101, color: '#4ade80', glow: 'rgba(22,163,74,0.25)' },
  { key: 'partial',    label: 'PARTIAL',    min: 70,  max: 90,  color: '#fbbf24', glow: 'rgba(245,158,11,0.25)' },
  { key: 'incomplete', label: 'INCOMPLETE', min: 0,   max: 70,  color: '#f87171', glow: 'rgba(220,38,38,0.25)' },
]

function groupColumns(columns) {
  return GROUPS.map(g => ({
    ...g,
    cols: columns.filter(c => {
      const p = c.completeness_pct ?? 0
      return p >= g.min && p < g.max
    }),
  }))
}

export default function DatasetView({ name, dbConn }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)

  // tabs: 'columns' | 'rows' | 'ml'
  const [activeTab, setActiveTab] = useState('columns')
  // which groups are collapsed
  const [collapsed, setCollapsed] = useState({})
  // missing rows panel
  const [showMissing, setShowMissing]   = useState(false)
  // patient id panel
  const [showPatientID, setShowPatientID] = useState(false)

  useEffect(() => {
    if (!name) return
    setLoading(true)
    setError(null)
    setData(null)
    setActiveTab('columns')
    setCollapsed({})
    setShowMissing(false)
    setShowPatientID(false)

    fetch(`/api/datasets/${encodeURIComponent(name)}`)
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
  }, [name])

  if (loading) {
    return (
      <div className="loading-wrap">
        <div className="loading-spinner" />
        <div className="loading-text">ANALYZING...</div>
      </div>
    )
  }
  if (error) return <div className="error-text">Error: {error}</div>
  if (!data)  return null

  const groups = groupColumns(data.column_details || [])

  // Detect PatientID column (case-insensitive)
  const PID_ALIASES = ['patientid', 'patient_id', 'patid', 'pid']
  const hasPatientIdCol = (data.column_details || []).some(c =>
    PID_ALIASES.includes(c.name.toLowerCase().replace(/[\s\-]/g, '_'))
  )

  function toggleCollapse(key) {
    setCollapsed(prev => ({ ...prev, [key]: !prev[key] }))
  }

  return (
    <div>
      {/* ── Header ─────────────────────────────────────────────── */}
      <div className="dataset-header">
        <span className={`file-badge ${typeToExt(data.type)}`} style={{ fontSize: 12, padding: '4px 10px' }}>
          {data.type}
        </span>
        <div className="dataset-title">{data.name}</div>
        <div style={{ fontFamily: 'var(--font-data)', fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
          {(data.rows || 0).toLocaleString()} rows × {data.columns || 0} cols
        </div>
      </div>

      {/* ── Stats ──────────────────────────────────────────────── */}
      <StatsRow dataset={data} onMissingClick={() => setShowMissing(true)}>
        {hasPatientIdCol && (
          <PatientIDCard
            datasetName={name}
            onClick={() => setShowPatientID(true)}
          />
        )}
      </StatsRow>

      {/* ── Completeness chart ─────────────────────────────────── */}
      {data.column_details?.length > 0 && (
        <CompletenessChart columns={data.column_details} />
      )}

      {/* ── Tab bar ────────────────────────────────────────────── */}
      <div className="tab-bar">
        <button
          className={`tab-btn ${activeTab === 'columns' ? 'active' : ''}`}
          onClick={() => setActiveTab('columns')}
        >
          ▦ COLUMN ANALYSIS
          <span className="tab-count">{data.column_details?.length || 0}</span>
        </button>
        <button
          className={`tab-btn ${activeTab === 'rows' ? 'active' : ''}`}
          onClick={() => setActiveTab('rows')}
        >
          ⚠ ROW ISSUES
        </button>
        <button
          className={`tab-btn ${activeTab === 'ml' ? 'active' : ''}`}
          onClick={() => setActiveTab('ml')}
        >
          ◈ ML QUALITY SCAN
        </button>
        <button
          className={`tab-btn ${activeTab === 'lab' ? 'active' : ''}`}
          onClick={() => setActiveTab('lab')}
        >
          ⚗ LAB ANOMALIES
        </button>
        <button
          className={`tab-btn ${activeTab === 'ai' ? 'active' : ''}`}
          onClick={() => setActiveTab('ai')}
        >
          ✦ AI REPAIR
        </button>
      </div>

      {/* ── Column Analysis tab — always grouped ───────────────── */}
      {activeTab === 'columns' && data.column_details?.length > 0 && (
        <>
          <div className="col-toolbar">
            <span className="col-toolbar-label">Grouped by completeness</span>
            <div className="col-toolbar-legend">
              {groups.map(g => (
                <span key={g.key} className="legend-chip">
                  <span className="legend-dot" style={{ background: g.color }} />
                  <span style={{ color: g.color }}>{g.label}</span>
                  <span style={{ color: 'var(--text-muted)' }}>({g.cols.length})</span>
                </span>
              ))}
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', marginBottom: 16 }}>
            {groups.map(g => (
              <div key={g.key} className="col-group-section">
                <button className="col-group-header" onClick={() => toggleCollapse(g.key)}>
                  <span className="col-group-dot" style={{ background: g.color }} />
                  <span className="col-group-label">{g.label}</span>
                  <span className="col-group-count">{g.cols.length} column{g.cols.length !== 1 ? 's' : ''}</span>
                  <span className="col-group-range">
                    {g.key === 'complete' ? '≥ 90%' : g.key === 'partial' ? '70 – 89%' : '< 70%'}
                  </span>
                  <span className="col-group-chevron">{collapsed[g.key] ? '▶' : '▼'}</span>
                </button>
                {!collapsed[g.key] && (
                  g.cols.length === 0
                    ? <div className="col-group-empty">No columns in this range</div>
                    : <div className="column-grid col-group-grid">
                        {g.cols.map(col => <ColumnCard key={col.name} col={col} />)}
                      </div>
                )}
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── Row Issues tab ─────────────────────────────────────── */}
      {activeTab === 'rows' && (
        <RowIssuesView datasetName={name} totalRows={data.rows} />
      )}

      {/* ── ML tab ─────────────────────────────────────────────── */}
      {activeTab === 'ml' && (
        <MLPanel datasetName={name} />
      )}

      {/* ── Lab Anomaly tab ─────────────────────────────────────── */}
      {activeTab === 'lab' && (
        <LabAnomalyPanel datasetName={name} />
      )}

      {/* ── AI Repair tab ───────────────────────────────────────── */}
      {activeTab === 'ai' && (
        <AIRepairPanel datasetName={name} dbConn={dbConn} />
      )}

      {/* ── Missing Rows Panel ──────────────────────────────────── */}
      {showMissing && (
        <MissingRowsPanel datasetName={name} onClose={() => setShowMissing(false)} />
      )}

      {/* ── PatientID Panel ─────────────────────────────────────── */}
      {showPatientID && (
        <PatientIDPanel
          datasetName={name}
          columnName={(data.column_details || []).find(c =>
            PID_ALIASES.includes(c.name.toLowerCase().replace(/[\s\-]/g, '_'))
          )?.name || 'PatientID'}
          onClose={() => setShowPatientID(false)}
        />
      )}

      {/* ── Data Preview (always shown below) ──────────────────── */}
      {activeTab === 'columns' && data.preview_rows && data.preview_columns && (
        <DataPreview
          columns={data.preview_columns}
          rows={data.preview_rows}
          wrongTypeCols={data.wrong_type_cols || []}
        />
      )}
    </div>
  )
}
