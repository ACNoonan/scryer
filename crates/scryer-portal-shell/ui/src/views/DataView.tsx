import { useEffect, useMemo, useState } from "react";
import {
  api,
  downloadBlob,
  type DatasetSchema,
  type QueryResult,
} from "../lib/api";
import { ResultTable } from "../components/ResultTable";
import "./DataView.css";

type Mode = "browse" | "sql";

export function DataView() {
  const [mode, setMode] = useState<Mode>("browse");
  const [schemas, setSchemas] = useState<DatasetSchema[]>([]);
  const [datasetRoot, setDatasetRoot] = useState<string>("");
  const [selected, setSelected] = useState<DatasetSchema | null>(null);
  const [preview, setPreview] = useState<QueryResult | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const [sql, setSql] = useState<string>(STARTER_SQL);
  const [sqlResult, setSqlResult] = useState<QueryResult | null>(null);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlRunning, setSqlRunning] = useState(false);

  useEffect(() => {
    api.listDatasets().then((r) => {
      setSchemas(r.schemas);
      setDatasetRoot(r.dataset_root);
      if (r.schemas.length > 0 && !selected) {
        setSelected(r.schemas[0]);
      }
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!selected || mode !== "browse") return;
    setPreviewLoading(true);
    setPreview(null);
    api
      .preview(selected.venue, selected.data_type, selected.version, 50)
      .then((r) => setPreview(r))
      .finally(() => setPreviewLoading(false));
  }, [selected, mode]);

  const totalBytes = useMemo(
    () => schemas.reduce((acc, s) => acc + s.total_bytes, 0),
    [schemas],
  );
  const totalFiles = useMemo(
    () => schemas.reduce((acc, s) => acc + s.partition_count, 0),
    [schemas],
  );

  async function runSql() {
    setSqlRunning(true);
    setSqlError(null);
    try {
      const r = await api.query(sql, 10_000);
      setSqlResult(r);
    } catch (e) {
      setSqlError(String(e));
      setSqlResult(null);
    } finally {
      setSqlRunning(false);
    }
  }

  async function exportAs(format: "csv" | "xlsx" | "parquet") {
    const targetSql = mode === "sql" ? sql : currentBrowseSql(selected);
    if (!targetSql) return;
    try {
      const { blob, filename } = await api.exportDownload(
        targetSql,
        format,
        suggestedName(selected, mode),
      );
      downloadBlob(blob, filename);
    } catch (e) {
      setSqlError(String(e));
    }
  }

  return (
    <div className="section data-view">
      <div className="head-row">
        <div>
          <span className="eyebrow">
            <span className="bar" /> Data
          </span>
          <h1>
            Parquet <span style={{ color: "var(--green)" }}>explorer</span>
          </h1>
          <p className="lede mono">
            {datasetRoot} · {totalFiles} parquet files · {humanBytes(totalBytes)}
          </p>
        </div>
        <div className="mode-toggle">
          <button
            className={mode === "browse" ? "primary" : ""}
            onClick={() => setMode("browse")}
          >
            Browse
          </button>
          <button
            className={mode === "sql" ? "primary" : ""}
            onClick={() => setMode("sql")}
          >
            SQL
          </button>
        </div>
      </div>

      <div className="data-grid">
        <aside className="schema-sidebar">
          <div className="tagnum" style={{ marginBottom: 12 }}>
            Schemas · {schemas.length}
          </div>
          {schemas.length === 0 && (
            <p className="dim mono">No parquet found under dataset root.</p>
          )}
          {schemas.map((s) => {
            const active =
              selected?.venue === s.venue &&
              selected?.data_type === s.data_type &&
              selected?.version === s.version;
            return (
              <button
                key={`${s.venue}/${s.data_type}/${s.version}`}
                className={`schema-pill ${active ? "active" : ""}`}
                onClick={() => setSelected(s)}
              >
                <div className="line-1">
                  <span>{s.venue}</span>
                  <span className="dim">/</span>
                  <span>{s.data_type}</span>
                </div>
                <div className="line-2 mono dim">
                  {s.version} · {s.partition_count} files ·{" "}
                  {humanBytes(s.total_bytes)}
                </div>
              </button>
            );
          })}
        </aside>

        <main className="explorer-main">
          {mode === "browse" && (
            <BrowsePane
              schema={selected}
              preview={preview}
              loading={previewLoading}
            />
          )}
          {mode === "sql" && (
            <SqlPane
              sql={sql}
              setSql={setSql}
              result={sqlResult}
              error={sqlError}
              running={sqlRunning}
              onRun={runSql}
              schemas={schemas}
              datasetRoot={datasetRoot}
            />
          )}

          <div className="export-bar">
            <span className="tagnum">Export</span>
            <button onClick={() => exportAs("csv")}>CSV</button>
            <button onClick={() => exportAs("xlsx")}>XLSX</button>
            <button onClick={() => exportAs("parquet")}>Parquet</button>
            <span className="dim mono" style={{ marginLeft: "auto" }}>
              {mode === "browse" && selected
                ? `from ${selected.venue}/${selected.data_type}/${selected.version} (preview SQL)`
                : "from current SQL editor"}
            </span>
          </div>
        </main>
      </div>
    </div>
  );
}

function BrowsePane({
  schema,
  preview,
  loading,
}: {
  schema: DatasetSchema | null;
  preview: QueryResult | null;
  loading: boolean;
}) {
  if (!schema) {
    return <p className="dim">Pick a schema in the sidebar.</p>;
  }
  return (
    <div className="browse-pane">
      <div className="tile-row">
        <Tile k="Files" v={schema.partition_count.toLocaleString()} />
        <Tile k="Total bytes" v={humanBytes(schema.total_bytes)} />
        <Tile k="Version" v={schema.version} />
        <Tile k="Venue" v={schema.venue} />
      </div>
      <div className="card" style={{ marginTop: 16 }}>
        <div className="tagnum" style={{ marginBottom: 12 }}>
          Recent rows · top 50 by _fetched_at
        </div>
        {loading && <p className="dim mono">Loading…</p>}
        {!loading && preview && <ResultTable result={preview} />}
      </div>
    </div>
  );
}

function SqlPane({
  sql,
  setSql,
  result,
  error,
  running,
  onRun,
  schemas,
  datasetRoot,
}: {
  sql: string;
  setSql: (s: string) => void;
  result: QueryResult | null;
  error: string | null;
  running: boolean;
  onRun: () => void;
  schemas: DatasetSchema[];
  datasetRoot: string;
}) {
  return (
    <div className="sql-pane">
      <div className="sql-toolbar">
        <span className="tagnum">SQL · DuckDB</span>
        <button className="primary" onClick={onRun} disabled={running}>
          {running ? "Running…" : "Run"}
        </button>
        <details className="snippets">
          <summary className="mono">Snippets</summary>
          <div className="snippet-list">
            {schemas.map((s) => {
              const path = `${datasetRoot}/${s.venue}/${s.data_type}/${s.version}/**/*.parquet`;
              const insert = `SELECT * FROM read_parquet('${path}', hive_partitioning = true) LIMIT 100`;
              return (
                <button
                  key={`${s.venue}/${s.data_type}/${s.version}`}
                  onClick={() => setSql(insert)}
                >
                  {s.venue}/{s.data_type}
                </button>
              );
            })}
          </div>
        </details>
      </div>
      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        spellCheck={false}
        rows={6}
      />
      {error && <div className="err">{error}</div>}
      {result && <ResultTable result={result} />}
    </div>
  );
}

function Tile({ k, v }: { k: string; v: string }) {
  return (
    <div className="tile">
      <div className="k mono">{k}</div>
      <div className="v">{v}</div>
    </div>
  );
}

function currentBrowseSql(s: DatasetSchema | null): string {
  if (!s) return "";
  return `SELECT * FROM read_parquet('${s.root}/**/*.parquet', hive_partitioning = true) LIMIT 10000`;
}

function suggestedName(s: DatasetSchema | null, mode: Mode): string {
  if (mode === "sql") return "scryer-query";
  if (!s) return "scryer-export";
  return `${s.venue}_${s.data_type}_${s.version}`;
}

function humanBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

const STARTER_SQL = `-- DuckDB SQL. Hive partitioning is on for read_parquet by default in your
-- snippets. The portal preserves _schema_version, _fetched_at, _source on
-- every scryer parquet row.
SELECT 1 + 1 as two;`;
