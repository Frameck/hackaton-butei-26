import React, { useState } from 'react'

const DEFAULT = {
  database: 'dashboard.sqlite',
}

export default function DBConnectionModal({ current, onSave, onClose }) {
  const [form, setForm]       = useState(current || DEFAULT)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState(null) // null | {ok, error}

  function set(field, value) {
    setForm(prev => ({ ...prev, [field]: value }))
    setTestResult(null)
  }

  async function handleTest() {
    setTesting(true)
    setTestResult(null)
    try {
      const r = await fetch('/api/db/test-connection', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          database: form.database,
        }),
      })
      const data = await r.json()
      setTestResult(data)
    } catch (e) {
      setTestResult({ ok: false, error: e.message })
    } finally {
      setTesting(false)
    }
  }

  function handleSave() {
    onSave({ ...form, status: testResult?.ok ? 'connected' : 'untested' })
    onClose()
  }

  const canTest = form.database.trim()

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header">
          <div className="modal-title">
            <span className="modal-icon">⬡</span>
            SQLITE DATABASE
          </div>
          <button className="missing-panel-close" onClick={onClose}>✕</button>
        </div>

        {/* Body */}
        <div className="modal-body">
          <div className="ai-conn-row">
            <div className="ai-conn-field ai-conn-wide">
              <label className="ai-conn-label">Database File</label>
              <input
                className="ai-conn-input"
                placeholder="dashboard.sqlite or /full/path/dashboard.sqlite"
                value={form.database}
                onChange={e => set('database', e.target.value)}
              />
            </div>
          </div>

          <div className="ai-conn-row">
            <div className="ai-conn-field">
              <label className="ai-conn-label">How It Works</label>
              <div className="ai-conn-input" style={{ lineHeight: 1.5 }}>
                Relative names are stored in the local <code>databases/</code> folder.
                Absolute paths work too.
              </div>
            </div>
          </div>

          {/* Test result */}
          {testResult && (
            <div className={`modal-test-result ${testResult.ok ? 'ok' : 'fail'}`}>
              {testResult.ok
                ? `✓ Ready: ${testResult.path || testResult.database}`
                : `✕ ${testResult.error}`}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="modal-footer">
          <button
            className="modal-btn-test"
            onClick={handleTest}
            disabled={testing || !canTest}
          >
            {testing ? 'Testing…' : '⚡ Test Connection'}
          </button>
          <div style={{ flex: 1 }} />
          <button className="modal-btn-cancel" onClick={onClose}>Cancel</button>
          <button
            className="modal-btn-save"
            onClick={handleSave}
            disabled={!canTest}
          >
            Save
          </button>
        </div>
      </div>
    </div>
  )
}
