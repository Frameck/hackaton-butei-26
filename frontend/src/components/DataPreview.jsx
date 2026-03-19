import React from 'react'

const SENTINEL_LOWER = new Set([
  'missing', 'unknown', 'n/a', 'null', '?', '-', 'na', 'none', 'nan', '',
])

function cellClass(value, colName, wrongTypeCols) {
  if (value === null || value === undefined) return 'cell-null'
  const strVal = String(value).trim().toLowerCase()
  if (SENTINEL_LOWER.has(strVal)) return 'cell-sentinel'
  if (wrongTypeCols && wrongTypeCols.includes(colName)) return 'cell-wrong-type'
  return ''
}

function cellDisplay(value) {
  if (value === null || value === undefined) return 'NULL'
  return String(value)
}

export default function DataPreview({ columns, rows, wrongTypeCols }) {
  if (!columns || !rows) return null

  return (
    <div className="preview-section">
      <div className="section-heading">
        Data Preview
        <span style={{
          fontFamily: 'var(--font-data)',
          fontSize: 10,
          color: 'var(--text-muted)',
          fontWeight: 400,
          marginLeft: 8,
          letterSpacing: 0,
        }}>
          (first 20 rows · max 30 columns)
        </span>
      </div>
      <div style={{ display: 'flex', gap: 12, marginBottom: 10, flexWrap: 'wrap' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, fontFamily: 'var(--font-data)', color: 'var(--text-muted)' }}>
          <span style={{ display: 'inline-block', width: 10, height: 10, background: 'rgba(220,38,38,0.3)', borderRadius: 2 }} />
          NULL value
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, fontFamily: 'var(--font-data)', color: 'var(--text-muted)' }}>
          <span style={{ display: 'inline-block', width: 10, height: 10, background: 'rgba(220,38,38,0.15)', borderRadius: 2 }} />
          Sentinel (missing/unknown/n/a…)
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, fontFamily: 'var(--font-data)', color: 'var(--text-muted)' }}>
          <span style={{ display: 'inline-block', width: 10, height: 10, background: 'rgba(245,158,11,0.15)', borderRadius: 2 }} />
          Wrong type column
        </span>
      </div>
      <div className="preview-scroll">
        <table className="preview-table">
          <thead>
            <tr>
              <th style={{ color: 'var(--text-muted)', minWidth: 36 }}>#</th>
              {columns.map(col => (
                <th key={col} title={col}>
                  {col.length > 18 ? col.slice(0, 17) + '…' : col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                <td style={{ color: 'var(--text-muted)', minWidth: 36 }}>{ri + 1}</td>
                {columns.map(col => {
                  const val = row[col]
                  const cls = cellClass(val, col, wrongTypeCols)
                  return (
                    <td key={col} className={cls} title={cellDisplay(val)}>
                      {cellDisplay(val)}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
