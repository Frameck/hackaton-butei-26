import React, { useState } from 'react'

const DEFAULT = {
  server:   '',
  port:     '1433',
  database: '',
  auth:     'sql',
  username: '',
  password: '',
  driver:   'SQL Server',
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
          server:   form.server,
          port:     parseInt(form.port, 10) || 1433,
          database: form.database,
          auth:     form.auth,
          username: form.username,
          password: form.password,
          driver:   form.driver,
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

  const canTest = form.server.trim() && form.database.trim() &&
    (form.auth === 'windows' || form.username.trim())

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header">
          <div className="modal-title">
            <span className="modal-icon">⬡</span>
            SQL SERVER CONNECTION
          </div>
          <button className="missing-panel-close" onClick={onClose}>✕</button>
        </div>

        {/* Body */}
        <div className="modal-body">
          <div className="ai-conn-row">
            <div className="ai-conn-field ai-conn-wide">
              <label className="ai-conn-label">Server / Host</label>
              <input
                className="ai-conn-input"
                placeholder="localhost or 192.168.1.10"
                value={form.server}
                onChange={e => set('server', e.target.value)}
              />
            </div>
            <div className="ai-conn-field ai-conn-narrow">
              <label className="ai-conn-label">Port</label>
              <input
                className="ai-conn-input"
                type="number"
                placeholder="1433"
                value={form.port}
                onChange={e => set('port', e.target.value)}
              />
            </div>
          </div>

          <div className="ai-conn-row">
            <div className="ai-conn-field">
              <label className="ai-conn-label">Database</label>
              <input
                className="ai-conn-input"
                placeholder="MyDatabase"
                value={form.database}
                onChange={e => set('database', e.target.value)}
              />
            </div>
          </div>

          <div className="ai-conn-row">
            <div className="ai-conn-field">
              <label className="ai-conn-label">Authentication</label>
              <select
                className="ai-conn-input ai-conn-select"
                value={form.auth}
                onChange={e => set('auth', e.target.value)}
              >
                <option value="sql">SQL Server Authentication</option>
                <option value="windows">Windows Authentication</option>
              </select>
            </div>
            <div className="ai-conn-field">
              <label className="ai-conn-label">ODBC Driver</label>
              <select
                className="ai-conn-input ai-conn-select"
                value={form.driver}
                onChange={e => set('driver', e.target.value)}
              >
                <option value="SQL Server">SQL Server (built-in)</option>
                <option value="ODBC Driver 17 for SQL Server">ODBC Driver 17</option>
                <option value="ODBC Driver 18 for SQL Server">ODBC Driver 18</option>
              </select>
            </div>
          </div>

          {form.auth === 'sql' && (
            <div className="ai-conn-row">
              <div className="ai-conn-field">
                <label className="ai-conn-label">Username</label>
                <input
                  className="ai-conn-input"
                  placeholder="sa"
                  value={form.username}
                  onChange={e => set('username', e.target.value)}
                />
              </div>
              <div className="ai-conn-field">
                <label className="ai-conn-label">Password</label>
                <input
                  className="ai-conn-input"
                  type="password"
                  placeholder="••••••••"
                  value={form.password}
                  onChange={e => set('password', e.target.value)}
                />
              </div>
            </div>
          )}

          {/* Test result */}
          {testResult && (
            <div className={`modal-test-result ${testResult.ok ? 'ok' : 'fail'}`}>
              {testResult.ok
                ? '✓ Connection successful'
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
