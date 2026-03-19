import React from 'react'

function completenessColor(pct) {
  if (pct >= 90) return '#16a34a'
  if (pct >= 70) return '#f59e0b'
  return '#dc2626'
}

function FileBadge({ type }) {
  const t = (type || '').toLowerCase()
  return <span className={`file-badge ${t}`}>{type || '?'}</span>
}

export default function Sidebar({
  datasets, loading, error,
  workspaceMode, onSelectWorkspace,
  selected, onSelect,
  compareMode, onToggleCompare,
  compareSelected, onToggleCompareItem,
  dbConn, onOpenDBModal,
}) {
  const isConnected = dbConn?.status === 'connected'

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="sidebar-title">◈ DQ Dashboard</div>
        <div className="sidebar-subtitle">
          {loading ? 'loading...' : `${datasets.length} datasets`}
        </div>
      </div>

      <div className="sidebar-mode-wrap">
        <button
          className={`sidebar-mode-card ${workspaceMode === 'patient360' ? 'active' : ''}`}
          onClick={() => onSelectWorkspace('patient360')}
        >
          <span className="sidebar-mode-eyebrow">LIVE DEMO</span>
          <span className="sidebar-mode-title">Patient 360</span>
          <span className="sidebar-mode-copy">Unified patient view, NLP notes, and harmonized field mapping.</span>
        </button>
        <button
          className={`sidebar-mode-card ${workspaceMode === 'datasets' ? 'active' : ''}`}
          onClick={() => onSelectWorkspace('datasets')}
        >
          <span className="sidebar-mode-eyebrow">WORKBENCH</span>
          <span className="sidebar-mode-title">Dataset Explorer</span>
          <span className="sidebar-mode-copy">Inspect issues, compare sources, run AI repair, and export to SQLite.</span>
        </button>
      </div>

      <div className="sidebar-list">
        {loading && (
          <div style={{ padding: '20px 14px' }}>
            <div className="loading-spinner" style={{ width: 20, height: 20, borderWidth: 2 }} />
          </div>
        )}
        {error && (
          <div className="error-text" style={{ margin: '10px 14px', fontSize: 11 }}>
            {error}
          </div>
        )}
        {datasets.map(ds => {
          const isActive  = workspaceMode === 'datasets' && !compareMode && selected === ds.name
          const isChecked = compareSelected.includes(ds.name)
          const col = completenessColor(ds.completeness)

          return (
            <div
              key={ds.name}
              className={`sidebar-item ${isActive ? 'active' : ''}`}
              onClick={() => compareMode ? onToggleCompareItem(ds.name) : onSelect(ds.name)}
            >
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: 7 }}>
                {compareMode && (
                  <input
                    type="checkbox"
                    className="sidebar-item-checkbox"
                    checked={isChecked}
                    onChange={() => onToggleCompareItem(ds.name)}
                    onClick={e => e.stopPropagation()}
                    style={{ marginTop: 2 }}
                  />
                )}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div className="sidebar-item-name">{ds.name}</div>
                  <div className="sidebar-item-meta">
                    <FileBadge type={ds.type} />
                    <span className="sidebar-rows">{(ds.rows || 0).toLocaleString()} rows</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 5 }}>
                    <div className="mini-completeness">
                      <div
                        className="mini-completeness-fill"
                        style={{ width: `${ds.completeness || 0}%`, background: col }}
                      />
                    </div>
                    <span style={{
                      fontFamily: 'var(--font-data)',
                      fontSize: 10,
                      color: col,
                      whiteSpace: 'nowrap',
                    }}>
                      {(ds.completeness || 0).toFixed(1)}%
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* ── DB Connection button ──────────────────────────── */}
      <div className="db-conn-btn-wrap">
        <button className="db-conn-btn" onClick={onOpenDBModal}>
          <span
            className="db-conn-dot"
            style={{ background: isConnected ? '#34c759' : '#ff3b30' }}
          />
          <div className="db-conn-btn-text">
            <span className="db-conn-btn-label">SQLITE</span>
            <span className="db-conn-btn-sub">
              {isConnected
                ? dbConn.database
                : 'Not connected'}
            </span>
          </div>
          <span className="db-conn-btn-edit">✎</span>
        </button>
      </div>
    </aside>
  )
}
