import React, { useEffect, useRef, useState } from 'react'

function StatPill({ label, value, tone }) {
  return (
    <div className={`patient360-stat-pill ${tone || ''}`}>
      <span className="patient360-stat-label">{label}</span>
      <span className="patient360-stat-value">{value}</span>
    </div>
  )
}

function SummaryMetric({ label, value }) {
  return (
    <div className="patient360-metric">
      <span className="patient360-metric-label">{label}</span>
      <strong className="patient360-metric-value">{value}</strong>
    </div>
  )
}

function DataTable({ columns, rows }) {
  if (!columns?.length || !rows?.length) {
    return <div className="patient360-table-empty">No matched rows</div>
  }

  return (
    <div className="patient360-table-wrap">
      <table className="patient360-table">
        <thead>
          <tr>
            <th>#</th>
            {columns.map(col => <th key={col}>{col}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <tr key={`${row.row_index}`}>
              <td>{row.row_index}</td>
              {columns.map(col => (
                <td key={`${row.row_index}-${col}`}>
                  {row.data?.[col] == null || row.data?.[col] === ''
                    ? <span className="patient360-null">NULL</span>
                    : String(row.data[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function Patient360View() {
  const [overview, setOverview] = useState(null)
  const [overviewStatus, setOverviewStatus] = useState('loading')
  const [overviewError, setOverviewError] = useState(null)
  const [query, setQuery] = useState('')
  const [lookupStatus, setLookupStatus] = useState('idle')
  const [lookupError, setLookupError] = useState(null)
  const [lookup, setLookup] = useState(null)
  const seededRef = useRef(false)

  async function runLookup(rawQuery) {
    const value = (rawQuery || '').trim()
    if (!value) return

    setLookupStatus('loading')
    setLookupError(null)

    try {
      const response = await fetch(`/api/unified/patient-lookup?q=${encodeURIComponent(value)}`)
      const data = await response.json().catch(() => ({}))
      if (!response.ok || data.error) throw new Error(data.error || `Lookup failed (${response.status})`)
      setLookup(data)
      setLookupStatus('done')
    } catch (error) {
      setLookupError(error.message)
      setLookupStatus('error')
    }
  }

  useEffect(() => {
    fetch('/api/unified/overview')
      .then(r => r.json())
      .then(data => {
        if (data.error) throw new Error(data.error)
        setOverview(data)
        setOverviewStatus('done')
      })
      .catch(error => {
        setOverviewError(error.message)
        setOverviewStatus('error')
      })
  }, [])

  useEffect(() => {
    if (seededRef.current) return
    if (!overview?.quick_picks?.length) return
    const seed = overview.quick_picks[0].lookup_id
    if (!seed) return

    seededRef.current = true
    setQuery(seed)
    runLookup(seed)
  }, [overview])

  const harmonization = lookup?.harmonization || overview?.harmonization
  const presentation = lookup?.presentation || overview?.presentation

  return (
    <div className="patient360-view">
      <section className="patient360-hero">
        <div className="patient360-hero-copy">
          <div className="patient360-eyebrow">Unified Patient View</div>
          <h1 className="patient360-title">Map every source to one patient-centric model, then demo the full repair-to-SQL story.</h1>
          <p className="patient360-subtitle">
            Search by patient ID or case ID to join labs, medication, nursing notes, device streams, and coding data in one place.
          </p>
          <div className="patient360-stat-row">
            <StatPill label="Challenge Savings" value={presentation?.market_savings || '€800M-€1.6B'} tone="accent" />
            <StatPill label="Deployment" value="Offline / on-prem ready" />
            <StatPill label="AI Layer" value="Claude + fallback NLP" />
          </div>
        </div>

        <div className="patient360-search-card">
          <form
            className="patient360-search-form"
            onSubmit={event => {
              event.preventDefault()
              runLookup(query)
            }}
          >
            <label className="patient360-search-label" htmlFor="patient360-query">Patient or case lookup</label>
            <div className="patient360-search-row">
              <input
                id="patient360-query"
                className="patient360-search-input"
                value={query}
                onChange={event => setQuery(event.target.value)}
                placeholder="Try 000078, PAT_0044, CASE-0135-01..."
              />
              <button className="patient360-search-btn" type="submit" disabled={lookupStatus === 'loading'}>
                {lookupStatus === 'loading' ? 'Loading...' : 'Open 360'}
              </button>
            </div>
          </form>

          <div className="patient360-quick-picks">
            {(overview?.quick_picks || []).slice(0, 6).map(item => (
              <button
                key={item.lookup_id}
                className="patient360-chip"
                onClick={() => {
                  setQuery(item.lookup_id)
                  runLookup(item.lookup_id)
                }}
              >
                <span>{item.lookup_id}</span>
                <small>{item.dataset_count} datasets</small>
              </button>
            ))}
          </div>

          <div className="patient360-flow">
            {(presentation?.workflow || []).map(step => (
              <span key={step} className="patient360-flow-step">{step}</span>
            ))}
          </div>
        </div>
      </section>

      {overviewStatus === 'loading' && !overview && (
        <div className="patient360-loading-card">
          <div className="loading-spinner" />
          <span>Building the patient index and harmonization map…</span>
        </div>
      )}

      {overviewStatus === 'error' && (
        <div className="patient360-error-card">Overview error: {overviewError}</div>
      )}

      {lookupError && (
        <div className="patient360-error-card">Lookup error: {lookupError}</div>
      )}

      {lookup && (
        <>
          <section className="patient360-summary-grid">
            <SummaryMetric label="Canonical patient" value={lookup.canonical_patient_id || lookup.query} />
            <SummaryMetric label="Datasets linked" value={lookup.summary?.matched_dataset_count || 0} />
            <SummaryMetric label="Sections populated" value={lookup.summary?.section_count || 0} />
            <SummaryMetric label="Nursing notes parsed" value={lookup.summary?.nursing_note_count || 0} />
          </section>

          <section className="patient360-id-strip">
            <div>
              <span className="patient360-id-label">Patient IDs</span>
              <div className="patient360-id-values">{(lookup.patient_ids || []).join(' · ') || 'None linked'}</div>
            </div>
            <div>
              <span className="patient360-id-label">Case IDs</span>
              <div className="patient360-id-values">{(lookup.case_ids || []).join(' · ') || 'None linked'}</div>
            </div>
            <div>
              <span className="patient360-id-label">Encounter IDs</span>
              <div className="patient360-id-values">{(lookup.encounter_ids || []).join(' · ') || 'None linked'}</div>
            </div>
          </section>

          <section className="patient360-sections-grid">
            {(lookup.sections || []).map(section => (
              <article key={section.key} className="patient360-section-card">
                <div className="patient360-section-header">
                  <div>
                    <div className="patient360-section-title">{section.label}</div>
                    <div className="patient360-section-meta">
                      {section.datasets?.length || 0} dataset{section.datasets?.length === 1 ? '' : 's'} · {section.total_rows || 0} matched rows
                    </div>
                  </div>
                </div>

                <div className="patient360-section-stack">
                  {(section.datasets || []).map(dataset => (
                    <div key={dataset.dataset_name} className="patient360-dataset-card">
                      <div className="patient360-dataset-head">
                        <strong>{dataset.dataset_name}</strong>
                        <span className="patient360-match-badge">matched on {dataset.matched_on}</span>
                      </div>
                      <DataTable columns={dataset.display_columns} rows={dataset.rows} />
                    </div>
                  ))}
                </div>
              </article>
            ))}
          </section>

          <section className="patient360-bottom-grid">
            <article className="patient360-panel-card">
              <div className="patient360-panel-title">AI / NLP Layer</div>
              <div className="patient360-panel-subtitle">Structured entities extracted from nursing notes for symptoms, interventions, and fall risk.</div>
              <div className="patient360-note-stack">
                {(lookup.nursing_notes || []).length === 0 && (
                  <div className="patient360-table-empty">No nursing notes linked to this lookup.</div>
                )}
                {(lookup.nursing_notes || []).map(note => (
                  <div key={`${note.dataset_name}-${note.row_index}`} className="patient360-note-card">
                    <div className="patient360-note-head">
                      <strong>{note.dataset_name}</strong>
                      <span className={`patient360-risk-badge ${note.entities?.fall_risk || 'low'}`}>
                        fall risk: {note.entities?.fall_risk || 'low'}
                      </span>
                    </div>
                    <div className="patient360-note-meta">
                      {[note.report_date, note.shift, note.ward].filter(Boolean).join(' · ') || 'No timing metadata'}
                      <span className="patient360-note-source">{note.extraction_source}</span>
                    </div>
                    <p className="patient360-note-text">{note.note_text || 'No note text'}</p>
                    <div className="patient360-entity-row">
                      <span className="patient360-entity-label">Symptoms</span>
                      <div className="patient360-entity-values">
                        {(note.entities?.symptoms || []).map(value => <span key={value} className="patient360-entity-chip">{value}</span>)}
                        {!(note.entities?.symptoms || []).length && <span className="patient360-entity-empty">none</span>}
                      </div>
                    </div>
                    <div className="patient360-entity-row">
                      <span className="patient360-entity-label">Interventions</span>
                      <div className="patient360-entity-values">
                        {(note.entities?.interventions || []).map(value => <span key={value} className="patient360-entity-chip">{value}</span>)}
                        {!(note.entities?.interventions || []).length && <span className="patient360-entity-empty">none</span>}
                      </div>
                    </div>
                    <div className="patient360-entity-row">
                      <span className="patient360-entity-label">Summary</span>
                      <div className="patient360-note-summary">{note.entities?.summary || 'No summary generated'}</div>
                    </div>
                  </div>
                ))}
              </div>
            </article>

            <article className="patient360-panel-card">
              <div className="patient360-panel-title">Field Harmonization</div>
              <div className="patient360-panel-subtitle">The UI shows exactly how raw fields like <code>PID</code>, <code>PatientID</code>, and <code>pat_id</code> map to one canonical target model.</div>
              <div className="patient360-harmonization-table-wrap">
                <table className="patient360-harmonization-table">
                  <thead>
                    <tr>
                      <th>Canonical field</th>
                      <th>Datasets</th>
                      <th>Observed raw columns</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(harmonization?.fields || []).map(field => (
                      <tr key={field.canonical_field}>
                        <td>{field.canonical_field}</td>
                        <td>{field.dataset_count}</td>
                        <td>{(field.source_columns || []).join(', ') || 'None detected'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
          </section>
        </>
      )}
    </div>
  )
}
