import React, { useState, useEffect } from 'react'
import Sidebar from './components/Sidebar.jsx'
import DatasetView from './components/DatasetView.jsx'
import CompareView from './components/CompareView.jsx'
export default function App() {
  const [datasets, setDatasets]           = useState([])
  const [loadingList, setLoadingList]     = useState(true)
  const [listError, setListError]         = useState(null)
  const [selected, setSelected]           = useState(null)
  const [compareMode, setCompareMode]     = useState(false)
  const [compareSelected, setCompareSelected] = useState([])

  useEffect(() => {
    fetch('/api/datasets')
      .then(r => r.json())
      .then(data => {
        setDatasets(data)
        setLoadingList(false)
        if (data.length > 0) setSelected(data[0].name)
      })
      .catch(err => {
        setListError(err.message)
        setLoadingList(false)
      })
  }, [])

  const toggleCompare = () => {
    setCompareMode(m => !m)
    setCompareSelected([])
  }

  const toggleCompareItem = (name) => {
    setCompareSelected(prev =>
      prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]
    )
  }

  return (
    <div className="app-layout">
      <Sidebar
        datasets={datasets}
        loading={loadingList}
        error={listError}
        selected={selected}
        onSelect={(name) => { setSelected(name); setCompareMode(false) }}
        compareMode={compareMode}
        onToggleCompare={toggleCompare}
        compareSelected={compareSelected}
        onToggleCompareItem={toggleCompareItem}
      />
      <main className="main-content">
        {compareMode ? (
          <CompareView
            datasets={datasets}
            compareSelected={compareSelected}
          />
        ) : selected ? (
          <DatasetView name={selected} />
        ) : (
          <div className="empty-wrap">
            <div className="empty-icon">◈</div>
            <div className="empty-text">SELECT A DATASET</div>
          </div>
        )}
      </main>

    </div>
  )
}
