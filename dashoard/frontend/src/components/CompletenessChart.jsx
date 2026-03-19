import React, { useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell,
} from 'recharts'

function barColor(pct) {
  if (pct >= 90) return '#16a34a'
  if (pct >= 70) return '#f59e0b'
  return '#dc2626'
}

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null
  const d = payload[0].payload
  return (
    <div style={{
      background: '#1a1510',
      border: '1px solid #92400e',
      borderRadius: 4,
      padding: '8px 12px',
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 11,
    }}>
      <div style={{ color: '#d97706', marginBottom: 3, wordBreak: 'break-all', maxWidth: 200 }}>
        {d.name}
      </div>
      <div style={{ color: '#fbbf24', fontWeight: 600 }}>
        {d.completeness.toFixed(2)}% complete
      </div>
      {d.missing_pct > 0 && (
        <div style={{ color: '#f87171', fontSize: 10, marginTop: 2 }}>
          {d.missing_pct.toFixed(2)}% missing
        </div>
      )}
    </div>
  )
}

export default function CompletenessChart({ columns }) {
  const data = useMemo(() =>
    (columns || []).map(c => ({
      name:         c.name,
      completeness: c.completeness_pct || 0,
      missing_pct:  c.missing_pct || 0,
    })),
    [columns]
  )

  if (!data.length) return null

  return (
    <div className="chart-section">
      <div className="section-heading">Completeness Per Column</div>
      <div style={{ width: '100%', height: 220 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 5, right: 10, left: -10, bottom: 60 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="rgba(42,30,16,0.8)"
                vertical={false}
              />
              <XAxis
                dataKey="name"
                tick={{ fill: '#78350f', fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }}
                angle={-50}
                textAnchor="end"
                interval={0}
                tickLine={false}
                axisLine={{ stroke: '#2a1e10' }}
              />
              <YAxis
                domain={[0, 100]}
                tick={{ fill: '#78350f', fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => `${v}%`}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(245,158,11,0.06)' }} />
              <Bar dataKey="completeness" radius={[3, 3, 0, 0]} maxBarSize={32}>
                {data.map((entry, index) => (
                  <Cell key={index} fill={barColor(entry.completeness)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
      </div>
    </div>
  )
}
