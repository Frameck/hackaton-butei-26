import React, { useEffect, useRef, useState } from 'react'

const CONF_COLOR = { high: '#34c759', medium: '#fbbf24', low: '#f87171' }

export default function AIRepairPanel({ datasetName, dbConn }) {
  const [repairStatus, setRepairStatus] = useState('idle')
  const [corrections, setCorrections]   = useState([])
  const [summary, setSummary]           = useState('')
  const [repairMeta, setRepairMeta]     = useState(null)
  const [repairError, setRepairError]   = useState(null)
  const [exportStatus, setExportStatus] = useState('idle')
  const [exportResult, setExportResult] = useState(null)
  const repairAbortRef = useRef(null)
  const exportAbortRef = useRef(null)

  const isConnected = dbConn?.status === 'connected'

  useEffect(() => {
    return () => {
      repairAbortRef.current?.abort()
      exportAbortRef.current?.abort()
    }
  }, [])

  useEffect(() => {
    repairAbortRef.current?.abort()
    exportAbortRef.current?.abort()
    setRepairStatus('idle')
    setCorrections([])
    setSummary('')
    setRepairMeta(null)
    setRepairError(null)
    setExportStatus('idle')
    setExportResult(null)
  }, [datasetName])

  async function handleRepair() {
    repairAbortRef.current?.abort()
    const controller = new AbortController()
    repairAbortRef.current = controller

    setRepairStatus('loading')
    setCorrections([])
    setSummary('')
    setRepairMeta(null)
    setRepairError(null)
    setExportStatus('idle')
    setExportResult(null)

    try {
      const r = await fetch(
        `/api/datasets/${encodeURIComponent(datasetName)}/ai-repair`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({}),
          signal:  controller.signal,
        }
      )
      const data = await r.json().catch(() => ({}))
      if (!r.ok || data.error) throw new Error(data.error || `Request failed (${r.status})`)
      if (controller.signal.aborted) return

      setCorrections(data.corrections || [])
      setSummary(data.summary || '')
      setRepairMeta(data.meta || data.demo || null)
      setRepairStatus('done')
    } catch (e) {
      if (e.name === 'AbortError') return
      setRepairError(e.message)
      setRepairStatus('error')
    } finally {
      if (repairAbortRef.current === controller) {
        repairAbortRef.current = null
      }
    }
  }

  async function handleExport() {
    exportAbortRef.current?.abort()
    const controller = new AbortController()
    exportAbortRef.current = controller

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
              database: dbConn.database,
            },
          }),
          signal: controller.signal,
        }
      )
      const data = await r.json().catch(() => ({}))
      if (!r.ok || data.error) throw new Error(data.error || `Export failed (${r.status})`)
      if (controller.signal.aborted) return

      setExportResult(data)
      setExportStatus('done')
    } catch (e) {
      if (e.name === 'AbortError') return
      setExportResult({ error: e.message })
      setExportStatus('error')
    } finally {
      if (exportAbortRef.current === controller) {
        exportAbortRef.current = null
      }
    }
  }

  return (
    <div className="ai-repair-panel">

      {/* ── Header ─────────────────────────────────────────── */}
      <div className="ai-repair-header">
        <div className="ai-repair-header-left">
          <span className="ai-repair-icon">✦</span>
          <span className="ai-repair-title">AI DATA REPAIR</span>
          <span className="ai-repair-badge">powered by Claude Sonnet</span>
        </div>
        {repairStatus === 'done' && corrections.length > 0 && (
          <span className="ai-repair-count-badge">{corrections.length} corrections</span>
        )}
      </div>

      <div className="ai-repair-desc">
        Claude analyzes corrupted, missing, and invalid values and suggests
        context-aware corrections. Corrected data is then exported to the connected SQLite database.
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
            Claude is running an optimized repair pass…
            <span className="ai-repair-loading-sub">Usually 5–20 seconds depending on dataset size</span>
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
          {repairMeta && (
            <div className="ai-repair-meta">
              <span className="ai-repair-meta-title">OPTIMIZED CLAUDE PASS</span>
              <span className="ai-repair-meta-note">{repairMeta.note}</span>
              <span className="ai-repair-meta-stats">
                {repairMeta.rows_analysed ?? 0} rows analysed
                {repairMeta.issue_cells_analysed ? ` · ${repairMeta.issue_cells_analysed} flagged cells` : ''}
                {' · '}
                {repairMeta.total_issue_rows ?? 0} issue rows detected
                {repairMeta.duration_ms ? ` · ${repairMeta.duration_ms} ms` : ''}
                {repairMeta.payload_chars ? ` · ${repairMeta.payload_chars} chars sent` : ''}
              </span>
              {repairMeta.model && (
                <span className="ai-repair-meta-model">{repairMeta.model}</span>
              )}
            </div>
          )}
          {(repairMeta?.issue_columns?.length > 0 || repairMeta?.context_columns?.length > 0) && (
            <div className="ai-repair-context">
              Focus columns: {(repairMeta.issue_columns || repairMeta.context_columns).join(', ')}
            </div>
          )}

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
            <span className="ai-export-title">↓ EXPORT TO SQLITE</span>
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
              No SQLite database selected — configure it from the bottom-left button.
            </div>
          )}

          {/* Connected — show export button */}
          {isConnected && exportStatus !== 'done' && (
            <>
              <div className="ai-export-target">
                <span className="db-conn-dot" style={{ background: '#34c759', width: 7, height: 7, borderRadius: '50%', display: 'inline-block', marginRight: 6 }} />
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
                    ? `Export corrected data (${corrections.length} fixes) → SQLite`
                    : 'Export raw data → SQLite'}
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
                  {exportResult.database}:{exportResult.table_name}
                </code>
                {exportResult.path && (
                  <span className="ai-export-applied"> {' '}· {exportResult.path}</span>
                )}
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
