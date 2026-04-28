// Typed client for the scryer-portal axum backend. The base URL is read from
// localStorage so the same UI works against a local sidecar or a future
// IP-allowlisted Linux deploy without a rebuild.

const STORAGE_KEY = "scryer.portal.backendUrl";

export function getBackendUrl(): string {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored) return stored.replace(/\/$/, "");
  // In dev (vite proxy) we hit /api on the same origin; in a Tauri bundle, the
  // sidecar listens on 47777 by default.
  if (import.meta.env.DEV) return "";
  return "http://127.0.0.1:47777";
}

export function setBackendUrl(url: string): void {
  localStorage.setItem(STORAGE_KEY, url.replace(/\/$/, ""));
}

async function call<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const url = `${getBackendUrl()}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
  });
  if (!res.ok) {
    let detail = "";
    try {
      detail = (await res.json()).error ?? "";
    } catch {
      detail = await res.text();
    }
    throw new Error(`${res.status} ${res.statusText}: ${detail}`);
  }
  return res.json();
}

// ── types ───────────────────────────────────────────────────────────────────

export type JobGroup = "scryer" | "other";
export type JobStatus =
  | "running"
  | "idle"
  | "failed"
  | "not_loaded"
  | "unknown";
export type ScheduleKind =
  | "interval"
  | "calendar"
  | "run_at_load_only"
  | "on_demand"
  | "unknown";

export interface Schedule {
  kind: ScheduleKind;
  summary: string;
  next_fires: number[];
}

export interface JobSummary {
  label: string;
  group: JobGroup;
  schedule: Schedule;
  status: JobStatus;
  last_exit: number | null;
  last_run: number | null;
  plist_path: string;
  program: string | null;
  /** Last non-empty stderr line, populated only when status === "failed". */
  last_error: string | null;
}

export interface JobDetail {
  summary: JobSummary;
  plist_xml: string;
  stdout_path: string | null;
  stderr_path: string | null;
  recent_stdout: string[];
  recent_stderr: string[];
}

export interface DatasetSchema {
  venue: string;
  data_type: string;
  version: string;
  root: string;
  partition_count: number;
  total_bytes: number;
}

export interface DatasetsResponse {
  dataset_root: string;
  schemas: DatasetSchema[];
}

export interface ColumnInfo {
  name: string;
  ty: string;
}

export interface QueryResult {
  columns: ColumnInfo[];
  rows: unknown[][];
  row_count: number;
  query_ms: number;
  truncated: boolean;
}

export interface HealthResponse {
  ok: boolean;
  version: string;
  backend_kind: string;
  dataset_root: string;
}

// ── endpoints ───────────────────────────────────────────────────────────────

export const api = {
  health: () => call<HealthResponse>("/api/health"),
  listJobs: () => call<JobSummary[]>("/api/jobs"),
  jobDetail: (label: string) =>
    call<JobDetail>(`/api/jobs/${encodeURIComponent(label)}`),
  runJob: (label: string) =>
    call<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(label)}/run`, {
      method: "POST",
    }),
  loadJob: (label: string) =>
    call<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(label)}/load`, {
      method: "POST",
    }),
  unloadJob: (label: string) =>
    call<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(label)}/unload`, {
      method: "POST",
    }),
  listDatasets: () => call<DatasetsResponse>("/api/datasets"),
  preview: (
    venue: string,
    dataType: string,
    version: string,
    limit = 50,
  ) =>
    call<QueryResult>(
      `/api/datasets/${encodeURIComponent(venue)}/${encodeURIComponent(dataType)}/${encodeURIComponent(version)}/preview?limit=${limit}`,
    ),
  query: (sql: string, limit = 10_000) =>
    call<QueryResult>("/api/query", {
      method: "POST",
      body: JSON.stringify({ sql, limit }),
    }),
  exportDownload: async (
    sql: string,
    format: "csv" | "xlsx" | "parquet",
    name = "scryer-export",
  ): Promise<{ blob: Blob; filename: string }> => {
    const url = `${getBackendUrl()}/api/export`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql, format, name }),
    });
    if (!res.ok) {
      let detail = "";
      try {
        detail = (await res.json()).error ?? "";
      } catch {
        detail = await res.text();
      }
      throw new Error(`${res.status}: ${detail}`);
    }
    const blob = await res.blob();
    const cd = res.headers.get("content-disposition") ?? "";
    const m = /filename="([^"]+)"/.exec(cd);
    const filename = m?.[1] ?? `${name}.${format}`;
    return { blob, filename };
  },
};

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
