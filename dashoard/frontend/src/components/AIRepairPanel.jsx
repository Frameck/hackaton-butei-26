import React, { useState } from 'react'

const CONF_COLOR = { high: '#34c759', medium: '#fbbf24', low: '#f87171' }

export default function AIRepairPanel({ datasetName, dbConn }) {
  const [repairStatus, setRepairStatus] = useState('idle')
  const [corrections, setCorrections]   = useState([])
  const [summary, setSummary]           = useState('')
  const [repairError, setRepairError]   = useState(null)
  const [exportStatus, setExportStatus] = useState('idle')
  const [exportResult, setExportResult] = useState(null)

  const isConnected = dbConn?.status === 'connected'

  async function handleRepair() {
    setRepairStatus('loading')
    setCorrections([])
    setSummary('')
    setRepairError(null)
    setExportStatus('idle')
    setExportResult(null)
    try {
      const r = await fetch(
        `/api/datasets/${encodeURIComponent(datasetName)}/ai-repair`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ max_rows: 50 }),
        }
      )
      const data = await r.json()
      if (data.error) throw new Error(data.error)
      setCorrections(data.corrections || [])
      setSummary(data.summary || '')
      setRepairStatus('done')
    } catch (e) {
      setRepairError(e.message)
      setRepairStatus('error')
    }
  }

  async function handleExport() {
    setExportStatus('loading')
    setExportResult(null)
    try {
      const r = await fetch(
        `/api/datasets/${encodeURIComponent(datasetName)}/export-db`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({
            corrections,
            connection: {
              server:   dbConn.server,
              port:     parseInt(dbConn.port, 10) || 1433,
              database: dbConn.database,
              auth:     dbConn.auth,
              username: dbConn.username,
              password: dbConn.password,
              driver:   dbConn.driver,
            },
          }),
        }
      )
      const data = await r.json()
      if (data.error) throw new Error(data.error)
      setExportResult(data)
      setExportStatus('done')
    } catch (e) {
      setExportResult({ error: e.message })
      setExportStatus('error')
    }
  }

  return (
    <div className="ai-repair-panel">

      {/* ── Header ─────────────────────────────────────────── */}
      <div className="ai-repair-header">
        <div className="ai-repair-header-left">
          <span className="ai-repair-icon">✦</span>
          <span className="ai-repair-title">AI DATA REPAIR</span>
          <span className="ai-repair-badge">powered by Claude Opus</span>
        </div>
        {repairStatus === 'done' && corrections.length > 0 && (
          <span className="ai-repair-count-badge">{corrections.length} corrections</span>
        )}
      </div>

      <div className="ai-repair-desc">
        Claude analyzes corrupted, missing, and invalid values and suggests
        context-aware corrections. Corrected data is then exported to the connected SQL Server.
      </div>

      {/* ── Analyze button ─────────────────────────────────── */}
      {repairStatus !== 'loading' && (
        <button
          className="ai-repair-analyze-btn"
          onClick={handleRepair}
          disabled={repairStatus === 'loading'}
        >
          {repairStatus === 'done' ? '↺ Re-analyze with AI' : '✦ Analyze & Repair with AI'}
        </button>
      )}

      {/* ── Loading ────────────────────────────────────────── */}
      {repairStatus === 'loading' && (
        <div className="ai-repair-loading">
          <div className="loading-spinner" />
          <div className="ai-repair-loading-text">
            Claude is analyzing the data…
            <span className="ai-repair-loading-sub">This may take 15–30 seconds</span>
          </div>
        </div>
      )}

      {/* ── Error ──────────────────────────────────────────── */}
      {repairStatus === 'error' && (
        <div className="ai-repair-error">
          <span className="ai-repair-error-icon">⚠</span>
          {repairError}
        </div>
      )}

      {/* ── Results ────────────────────────────────────────── */}
      {repairStatus === 'done' && (
        <div className="ai-repair-results">
          {summary && <div className="ai-repair-summary">{summary}</div>}

          {corrections.length === 0 ? (
            <div className="ai-repair-empty">✓ No corrections needed — dataset looks clean.</div>
          ) : (
            <div className="ai-corrections-table-wrap">
              <table className="ai-corrections-table">
                <thead>
                  <tr>
                    <th className="ai-ct-th">#</th>
                    <th className="ai-ct-th">COLUMN</th>
                    <th className="ai-ct-th">ORIGINAL</th>
                    <th className="ai-ct-th"></th>
                    <th className="ai-ct-th">CORRECTED</th>
                    <th className="ai-ct-th">CONF</th>
                    <th className="ai-ct-th">REASON</th>
                  </tr>
                </thead>
                <tbody>
                  {corrections.map((c, i) => (
                    <tr key={i} className="ai-ct-row">
                      <td className="ai-ct-idx">{c.row_index}</td>
                      <td className="ai-ct-col">{c.column}</td>
                      <td className="ai-ct-orig">
                        {c.original_value === 'NULL'
                          ? <span className="null-badge">NULL</span>
                          : c.original_value}
                      </td>
                      <td className="ai-ct-arrow">→</td>
                      <td className="ai-ct-new">
                        {c.corrected_value === 'NULL'
                          ? <span className="null-badge">NULL</span>
                          : <span style={{ color: '#34c759', fontWeight: 600 }}>
                              {c.corrected_value}
                            </span>}
                      </td>
                      <td className="ai-ct-conf">
                        <span
                          className="ai-conf-badge"
                          style={{
                            color:      CONF_COLOR[c.confidence] || '#aeaeb2',
                            background: (CONF_COLOR[c.confidence] || '#aeaeb2') + '20',
                            border:     `1px solid ${(CONF_COLOR[c.confidence] || '#aeaeb2')}55`,
                          }}
                        >
                          {c.confidence}
                        </span>
                      </td>
                      <td className="ai-ct-reason">{c.reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── Export section ─────────────────────────────────── */}
      {(repairStatus === 'done' || repairStatus === 'idle' || repairStatus === 'error') && (
        <div className="ai-export-section">
          <div className="ai-export-divider" />
          <div className="ai-export-header">
            <span className="ai-export-title">↓ EXPORT TO SQL SERVER</span>
            {repairStatus === 'done' && corrections.length > 0 && (
              <span className="ai-export-hint">
                {corrections.length} corrections will be applied
              </span>
            )}
          </div>

          {/* Not connected warning */}
          {!isConnected && (
            <div className="ai-export-no-conn">
              <span>⬡</span>
              No SQL Server connection — configure it from the bottom-left button.
            </div>
          )}

          {/* Connected — show export button */}
          {isConnected && exportStatus !== 'done' && (
            <>
              <div className="ai-export-target">
                <span className="db-conn-dot" style={{ background: '#34c759', width: 7, height: 7, borderRadius: '50%', display: 'inline-block', marginRight: 6 }} />
                <code className="ai-export-filename">{dbConn.server}</code>
                {' / '}
                <code className="ai-export-filename">{dbConn.database}</code>
              </div>
              <button
                className="ai-export-btn"
                onClick={handleExport}
                disabled={exportStatus === 'loading'}
              >
                {exportStatus === 'loading'
                  ? 'Exporting…'
                  : repairStatus === 'done' && corrections.length > 0
                    ? `Export corrected data (${corrections.length} fixes) → SQL Server`
                    : 'Export raw data → SQL Server'}
              </button>
            </>
          )}

          {/* Success */}
          {exportStatus === 'done' && exportResult && !exportResult.error && (
            <div className="ai-export-result">
              <span className="ai-export-ok">✓</span>
              <div className="ai-export-result-text">
                <strong>{exportResult.rows_exported.toLocaleString()} rows</strong> exported to{' '}
                <code className="ai-export-filename">
                  [{exportResult.database}].[dbo].[{exportResult.table_name}]
                </code>
                {exportResult.corrections_applied > 0 && (
                  <span className="ai-export-applied">
                    {' '}· {exportResult.corrections_applied} corrections applied
                  </span>
                )}
                <button
                  className="ai-export-again-btn"
                  onClick={() => { setExportStatus('idle'); setExportResult(null) }}
                >
                  Export again
                </button>
              </div>
            </div>
          )}

          {/* Export error */}
          {exportStatus === 'error' && (
            <div className="ai-repair-error" style={{ marginTop: 10 }}>
              <span className="ai-repair-error-icon">⚠</span>
              <div>
                Export failed: {exportResult?.error}
                <button
                  className="ai-export-again-btn"
                  onClick={() => { setExportStatus('idle'); setExportResult(null) }}
                >
                  Try again
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
