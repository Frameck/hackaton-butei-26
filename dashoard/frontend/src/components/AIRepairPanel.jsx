import React, { useState } from 'react'

const CONF_COLOR = { high: '#34c759', medium: '#fbbf24', low: '#f87171', none: '#636366' }

function initDecisions(corrections) {
  return corrections.map(c => ({
    status:    'pending',
    editValue: c.corrected_value, // '' for no-suggestion items
    editing:   false,
  }))
}

export default function AIRepairPanel({ datasetName }) {
  const [repairStatus, setRepairStatus] = useState('idle')
  const [corrections, setCorrections]   = useState([])
  const [decisions, setDecisions]       = useState([])
  const [summary, setSummary]           = useState('')
  const [repairError, setRepairError]   = useState(null)
  const [exportStatus, setExportStatus] = useState('idle')
  const [exportResult, setExportResult] = useState(null)

  // ── decision helpers ────────────────────────────────────────────────────
  function setDecision(i, patch) {
    setDecisions(prev => prev.map((d, idx) => idx === i ? { ...d, ...patch } : d))
  }
  function accept(i)  { setDecision(i, { status: 'accepted', editing: false }) }
  function reject(i)  { setDecision(i, { status: 'rejected', editing: false }) }
  function restore(i) { setDecision(i, { status: 'pending',  editing: false }) }
  function startEdit(i)   { setDecision(i, { editing: true }) }
  function confirmEdit(i) { setDecision(i, { editing: false, status: 'accepted' }) }
  function cancelEdit(i)  {
    setDecision(i, { editing: false, editValue: corrections[i].corrected_value })
  }

  // All AI-suggestion items are currently accepted
  const allSuggestionsAccepted = corrections.length > 0 &&
    corrections.every((c, i) =>
      c.confidence === 'none' || decisions[i]?.status === 'accepted'
    )

  function toggleAcceptAll() {
    if (allSuggestionsAccepted) {
      // Undo: restore all accepted AI-suggestion items back to pending
      setDecisions(prev =>
        prev.map((d, i) =>
          corrections[i]?.confidence !== 'none' && d.status === 'accepted'
            ? { ...d, status: 'pending', editing: false }
            : d
        )
      )
    } else {
      // Accept all pending AI-suggestion items
      setDecisions(prev =>
        prev.map((d, i) =>
          d.status === 'rejected' || corrections[i]?.confidence === 'none'
            ? d
            : { ...d, status: 'accepted', editing: false }
        )
      )
    }
  }

  const noSugg     = i => corrections[i]?.confidence === 'none'
  // accepted WITH an actual value to write (AI suggestion or manual edit)
  const willApply  = i => decisions[i]?.status === 'accepted' && decisions[i]?.editValue !== ''

  const suggestedCount = corrections.filter(c => c.confidence !== 'none').length
  const noSuggCount    = corrections.filter(c => c.confidence === 'none').length
  const acceptedCount  = decisions.filter((_, i) => willApply(i)).length
  const pendingCount   = decisions.filter(d => d.status === 'pending').length
  const rejectedCount  = decisions.filter(d => d.status === 'rejected').length

  // ── API calls ───────────────────────────────────────────────────────────
  async function handleRepair() {
    setRepairStatus('loading')
    setCorrections([])
    setDecisions([])
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
          body:    JSON.stringify({ max_rows: 20 }),
        }
      )
      const data = await r.json()
      if (data.error) throw new Error(data.error)
      const corrs = data.corrections || []
      setCorrections(corrs)
      setDecisions(initDecisions(corrs))
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
    // Only send corrections that have an actual value to write
    const approved = corrections
      .map((c, i) => ({ ...c, corrected_value: decisions[i].editValue }))
      .filter((_, i) => willApply(i))
    try {
      const r = await fetch(
        `/api/datasets/${encodeURIComponent(datasetName)}/export-db`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ corrections: approved }),
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

  const rowBg = { accepted: 'rgba(52,199,89,0.07)', rejected: 'rgba(255,59,48,0.06)', pending: 'transparent' }

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
          <span className="ai-repair-count-badge">
            {acceptedCount} accepted · {pendingCount} pending · {rejectedCount} rejected
          </span>
        )}
      </div>

      <div className="ai-repair-desc">
        Claude analyzes corrupted, missing, and invalid values and suggests corrections.
        Review each suggestion before exporting.
      </div>

      {/* ── Analyze button ─────────────────────────────────── */}
      {repairStatus !== 'loading' && (
        <button className="ai-repair-analyze-btn" onClick={handleRepair}>
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
            <>
              {/* Toolbar */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8, flexWrap: 'wrap' }}>
                <button
                  className="ai-export-again-btn"
                  style={{ background: 'rgba(52,199,89,0.12)', color: '#34c759' }}
                  onClick={toggleAcceptAll}
                  disabled={suggestedCount === 0}
                >
                  {allSuggestionsAccepted ? '↩ Undo all acceptances' : `✓ Accept AI suggestions (${suggestedCount})`}
                </button>
                {noSuggCount > 0 && (
                  <span style={{ fontSize: 11, color: '#636366' }}>
                    {noSuggCount} need manual review
                  </span>
                )}
                {rejectedCount > 0 && (
                  <span style={{ fontSize: 11, color: '#ff3b30' }}>
                    · {rejectedCount} rejected
                  </span>
                )}
              </div>

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
                      <th className="ai-ct-th">ACTIONS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {corrections.map((c, i) => {
                      const d = decisions[i]
                      if (!d) return null
                      const isNoSugg = noSugg(i)
                      return (
                        <tr
                          key={i}
                          className="ai-ct-row"
                          style={{
                            background: isNoSugg && d.status === 'pending'
                              ? 'rgba(99,99,102,0.05)'
                              : rowBg[d.status],
                            opacity: d.status === 'rejected' ? 0.45 : 1,
                          }}
                        >
                          <td className="ai-ct-idx">{c.row_index}</td>
                          <td className="ai-ct-col">{c.column}</td>
                          <td className="ai-ct-orig">
                            {c.original_value === 'NULL'
                              ? <span className="null-badge">NULL</span>
                              : c.original_value}
                          </td>
                          <td className="ai-ct-arrow" style={{ color: isNoSugg ? '#636366' : undefined }}>→</td>

                          {/* Corrected value — editable */}
                          <td className="ai-ct-new">
                            {d.editing ? (
                              <span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>
                                <input
                                  className="ai-conn-input"
                                  style={{ padding: '2px 6px', fontSize: 12, width: 100 }}
                                  value={d.editValue}
                                  placeholder="enter value…"
                                  onChange={e => setDecision(i, { editValue: e.target.value })}
                                  onKeyDown={e => {
                                    if (e.key === 'Enter')  confirmEdit(i)
                                    if (e.key === 'Escape') cancelEdit(i)
                                  }}
                                  autoFocus
                                />
                                <button className="ai-export-again-btn"
                                  style={{ color: '#34c759', padding: '1px 6px' }}
                                  onClick={() => confirmEdit(i)}>✓</button>
                                <button className="ai-export-again-btn"
                                  style={{ color: '#aeaeb2', padding: '1px 6px' }}
                                  onClick={() => cancelEdit(i)}>✕</button>
                              </span>
                            ) : isNoSugg && !d.editValue ? (
                              <span style={{ color: '#636366', fontStyle: 'italic', fontSize: 10 }}>
                                — edit to set
                              </span>
                            ) : d.editValue === 'NULL' ? (
                              <span className="null-badge">NULL</span>
                            ) : (
                              <span style={{
                                color:      d.status === 'accepted' ? '#34c759' : '#aeaeb2',
                                fontWeight: d.status === 'accepted' ? 600 : 400,
                              }}>
                                {d.editValue}
                              </span>
                            )}
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
                              {c.confidence === 'none' ? '—' : c.confidence}
                            </span>
                          </td>
                          <td className="ai-ct-reason" style={{ color: isNoSugg ? '#636366' : undefined }}>
                            {c.reason}
                          </td>

                          {/* Action buttons */}
                          <td className="ai-ct-actions">
                            {d.status === 'rejected' ? (
                              <button className="ai-export-again-btn"
                                style={{ color: '#aeaeb2', fontSize: 11 }}
                                onClick={() => restore(i)}>↩ Restore</button>
                            ) : (
                              <span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>
                                {/* Accept toggle — disabled for no-suggestion items with no edit */}
                                <button
                                  className="ai-export-again-btn"
                                  title={d.status === 'accepted' ? 'Undo accept' : 'Accept'}
                                  disabled={isNoSugg && !d.editValue}
                                  style={{
                                    color:      '#34c759',
                                    background: d.status === 'accepted' ? 'rgba(52,199,89,0.22)' : 'rgba(52,199,89,0.08)',
                                    fontWeight: d.status === 'accepted' ? 700 : 400,
                                    opacity:    isNoSugg && !d.editValue ? 0.3 : 1,
                                  }}
                                  onClick={() => d.status === 'accepted' ? restore(i) : accept(i)}
                                >✓</button>
                                <button className="ai-export-again-btn"
                                  title="Edit value"
                                  style={{ color: '#fbbf24', background: 'rgba(251,191,36,0.08)' }}
                                  onClick={() => startEdit(i)}>✎</button>
                                <button className="ai-export-again-btn"
                                  title="Reject"
                                  style={{ color: '#ff3b30', background: 'rgba(255,59,48,0.08)' }}
                                  onClick={() => reject(i)}>✕</button>
                              </span>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}

      {/* ── Export section ─────────────────────────────────── */}
      <div className="ai-export-section">
        <div className="ai-export-divider" />
        <div className="ai-export-header">
          <span className="ai-export-title">↓ EXPORT TO SQLITE</span>
          {repairStatus === 'done' && acceptedCount > 0 && exportStatus !== 'done' && (
            <span className="ai-export-hint">{acceptedCount} corrections will be applied</span>
          )}
        </div>

        {exportStatus !== 'done' && (
          <button className="ai-export-btn" onClick={handleExport} disabled={exportStatus === 'loading'}>
            {exportStatus === 'loading'
              ? 'Exporting…'
              : acceptedCount > 0
                ? `Export with ${acceptedCount} correction${acceptedCount > 1 ? 's' : ''} → SQLite`
                : 'Export data → SQLite'}
          </button>
        )}

        {exportStatus === 'done' && exportResult && !exportResult.error && (
          <div className="ai-export-result">
            <span className="ai-export-ok">✓</span>
            <div className="ai-export-result-text">
              <strong>{exportResult.rows_exported.toLocaleString()} rows</strong> exported to{' '}
              <code className="ai-export-filename">{exportResult.file_name}</code>
              {exportResult.corrections_applied > 0 && (
                <span className="ai-export-applied"> · {exportResult.corrections_applied} corrections applied</span>
              )}
              <div style={{ marginTop: 6, display: 'flex', gap: 8 }}>
                <a href={`/api/datasets/${encodeURIComponent(datasetName)}/download-sqlite`}
                  className="ai-export-again-btn" download>↓ Download .sqlite</a>
                <button className="ai-export-again-btn"
                  onClick={() => { setExportStatus('idle'); setExportResult(null) }}>Export again</button>
              </div>
            </div>
          </div>
        )}

        {exportStatus === 'error' && (
          <div className="ai-repair-error" style={{ marginTop: 10 }}>
            <span className="ai-repair-error-icon">⚠</span>
            <div>
              Export failed: {exportResult?.error}
              <button className="ai-export-again-btn"
                onClick={() => { setExportStatus('idle'); setExportResult(null) }}>Try again</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
