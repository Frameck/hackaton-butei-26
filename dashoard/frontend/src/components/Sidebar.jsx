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
  selected, onSelect,
  compareMode, onToggleCompare,
  compareSelected, onToggleCompareItem,
}) {
  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="sidebar-title">◈ MedMap</div>
        <div className="sidebar-subtitle">
          {loading ? 'loading...' : `${datasets.length} datasets`}
        </div>
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
          const isActive  = !compareMode && selected === ds.name
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

    </aside>
  )
}
