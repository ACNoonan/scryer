import type { QueryResult } from "../lib/api";
import "./ResultTable.css";

interface Props {
  result: QueryResult;
  maxBodyHeight?: number;
}

export function ResultTable({ result, maxBodyHeight = 480 }: Props) {
  const { columns, rows, query_ms, row_count, truncated } = result;
  return (
    <div className="result">
      <div className="result-meta mono">
        <span>{row_count.toLocaleString()} rows</span>
        <span className="dim">·</span>
        <span>{query_ms} ms</span>
        {truncated && (
          <span className="dim">· truncated to {row_count.toLocaleString()}</span>
        )}
      </div>
      <div className="result-scroll" style={{ maxHeight: maxBodyHeight }}>
        <table className="result-table">
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c.name} title={c.ty}>
                  <span>{c.name}</span>
                  <span className="ty mono">{c.ty}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                {row.map((cell, ci) => {
                  const col = columns[ci];
                  const meta = col?.name.startsWith("_");
                  return (
                    <td key={ci} className={meta ? "meta" : ""}>
                      {renderCell(cell)}
                    </td>
                  );
                })}
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={Math.max(1, columns.length)} className="empty">
                  no rows
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (typeof v === "boolean") return v ? "true" : "false";
  return JSON.stringify(v);
}
