import { useState, useRef } from 'react'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

interface QueryResult {
  status: string
  query: string
  result?: {
    s3_result_link?: string
    columns?: string[]
    rows?: Record<string, string>[]
    row_count?: number
  }
  error_message?: string
  execution_time_ms?: number
}

function App() {
  const [sql, setSql] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<QueryResult | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!sql.trim() || !file) {
      return
    }

    setLoading(true)
    setResult(null)

    const formData = new FormData()
    formData.append('sql_query', sql)
    formData.append('file', file)

    try {
      const response = await fetch(`${API_URL}/api/v1/query`, {
        method: 'POST',
        body: formData,
      })

      const data = await response.json()
      setResult(data)
    } catch (err) {
      setResult({
        status: 'error',
        query: sql,
        error_message: err instanceof Error ? err.message : 'Request failed',
      })
    } finally {
      setLoading(false)
    }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      setFile(selectedFile)
    }
  }

  return (
    <div className="container">
      <h1>OptiSQL</h1>

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="sql">SQL Query</label>
          <textarea
            id="sql"
            rows={6}
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            placeholder="SELECT * FROM data WHERE column > 10"
          />
        </div>

        <div className="form-group">
          <label>Data File (.csv, .json, .parquet)</label>
          <input
            type="file"
            ref={fileInputRef}
            onChange={handleFileChange}
            accept=".csv,.json,.parquet"
            style={{ display: 'none' }}
          />
          <div
            className={`file-input ${file ? 'has-file' : ''}`}
            onClick={() => fileInputRef.current?.click()}
          >
            {file ? file.name : 'Click to select a file'}
          </div>
        </div>

        <button type="submit" disabled={loading || !sql.trim() || !file}>
          {loading ? 'Executing...' : 'Execute Query'}
        </button>
      </form>

      {loading && <div className="loading">Processing query...</div>}

      {result && (
        <div className={`result ${result.status === 'success' ? 'success' : 'error'}`}>
          <h3>{result.status === 'success' ? 'Result' : 'Error'}</h3>

          {result.error_message && (
            <p className="error-message">{result.error_message}</p>
          )}

          {result.result?.columns && result.result?.rows && (
            <div className="table-container">
              <table className="result-table">
                <thead>
                  <tr>
                    {result.result.columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.result.rows.map((row, idx) => (
                    <tr key={idx}>
                      {result.result!.columns!.map((col) => (
                        <td key={col}>{row[col]}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="row-count">{result.result.row_count} rows returned</p>
            </div>
          )}

          {result.execution_time_ms && (
            <p className="execution-time">
              Execution time: {result.execution_time_ms.toFixed(2)}ms
            </p>
          )}
        </div>
      )}
    </div>
  )
}

export default App
