import { useEffect, useMemo, useState } from "react";
import { api, type JobDetail, type JobSummary } from "../lib/api";
import { JobDetailPanel } from "../components/JobDetailPanel";
import { JobTimeline } from "../components/JobTimeline";
import "./JobsView.css";

export function JobsView() {
  const [jobs, setJobs] = useState<JobSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showOther, setShowOther] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<JobDetail | null>(null);
  const [refreshTick, setRefreshTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    api
      .listJobs()
      .then((j) => {
        if (!cancelled) {
          setJobs(j);
          setError(null);
        }
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [refreshTick]);

  useEffect(() => {
    if (!selected) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    api
      .jobDetail(selected)
      .then((d) => {
        if (!cancelled) setDetail(d);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [selected, refreshTick]);

  const { scryer, other, failures } = useMemo(() => {
    const s: JobSummary[] = [];
    const o: JobSummary[] = [];
    const f: JobSummary[] = [];
    for (const j of jobs ?? []) {
      if (j.group === "scryer") s.push(j);
      else o.push(j);
      // Banner shouts only about scryer-track failures. Non-scryer jobs
      // (soothsayer, google, etc) are still visible in the Other table when
      // expanded — but they don't hijack the top of the portal.
      if (j.status === "failed" && j.group === "scryer") f.push(j);
    }
    return { scryer: s, other: o, failures: f };
  }, [jobs]);

  // Other group never auto-expands — it's always opt-in. Keeps the portal
  // calm about non-scryer jobs the operator may not own.
  const otherExpanded = showOther;

  return (
    <div className="section jobs-view">
      <div className="head-row">
        <div>
          <span className="eyebrow g">
            <span className="bar" /> Jobs
          </span>
          <h1>
            Scheduled <span style={{ color: "var(--green)" }}>fetcher tapes</span>
          </h1>
          <p className="lede">
            Read-only on plist contents. Run / Load / Unload via launchctl. Edits
            still go through your editor — open the plist for that, then reload.
          </p>
        </div>
        <button onClick={() => setRefreshTick((t) => t + 1)}>Refresh</button>
      </div>

      {error && <div className="err">{error}</div>}

      {failures.length > 0 && (
        <div className="failure-banner" role="alert">
          <div className="failure-banner-head">
            <span className="status-pill red">
              <span className="led" />
              {failures.length} failing
            </span>
            <span className="dim mono">
              click a row below to inspect logs
            </span>
          </div>
          <ul>
            {failures.map((j) => (
              <li key={j.label}>
                <button
                  className="failure-row"
                  onClick={() => setSelected(j.label)}
                >
                  <span className="failure-label">{j.label}</span>
                  <span className="failure-msg mono">
                    {j.last_error ?? "exited non-zero"}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {jobs && (
        <>
          <div className="card" style={{ marginBottom: 16 }}>
            <div className="tagnum" style={{ marginBottom: 14 }}>
              Next 24 hours · Gantt strip
            </div>
            <JobTimeline
              jobs={jobs}
              hours={24}
              onSelect={(label) => setSelected(label)}
            />
          </div>

          <JobsTable
            title="Scryer"
            jobs={scryer}
            onSelect={(label) => setSelected(label)}
            selected={selected}
          />
          <div style={{ marginTop: 14 }} />
          <button
            onClick={() => setShowOther((v) => !v)}
            style={{ marginBottom: 12 }}
            title="Show / hide non-scryer launchd agents on this machine"
          >
            {otherExpanded ? "Hide" : "Show"} other launchd agents (
            {other.length})
          </button>
          {otherExpanded && (
            <JobsTable
              title="Other"
              jobs={other}
              onSelect={(label) => setSelected(label)}
              selected={selected}
              muted
            />
          )}
        </>
      )}

      {selected && detail && (
        <JobDetailPanel
          detail={detail}
          onClose={() => setSelected(null)}
          onAction={async (kind) => {
            try {
              if (kind === "run") await api.runJob(selected);
              if (kind === "load") await api.loadJob(selected);
              if (kind === "unload") await api.unloadJob(selected);
              setRefreshTick((t) => t + 1);
            } catch (e) {
              setError(String(e));
            }
          }}
        />
      )}
    </div>
  );
}

function JobsTable({
  title,
  jobs,
  onSelect,
  selected,
  muted,
}: {
  title: string;
  jobs: JobSummary[];
  onSelect: (label: string) => void;
  selected: string | null;
  muted?: boolean;
}) {
  if (jobs.length === 0) {
    return (
      <div className={`card ${muted ? "muted" : ""}`}>
        <div className="tagnum">{title}</div>
        <p className="dim" style={{ marginTop: 12 }}>
          No jobs in this group.
        </p>
      </div>
    );
  }
  return (
    <div className={`card ${muted ? "muted" : ""}`}>
      <div className="tagnum" style={{ marginBottom: 14 }}>
        {title} · {jobs.length}
      </div>
      <table className="jobs-table">
        <thead>
          <tr>
            <th>Label</th>
            <th>Schedule</th>
            <th>Status</th>
            <th>Last exit</th>
            <th>Last run</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => {
            const failed = j.status === "failed";
            const rowClass = [
              selected === j.label ? "selected" : "",
              failed ? "failed" : "",
            ]
              .filter(Boolean)
              .join(" ");
            const tooltip = failed && j.last_error
              ? `Last error:\n${j.last_error}`
              : j.label;
            return (
              <tr
                key={j.label}
                className={rowClass}
                onClick={() => onSelect(j.label)}
                title={tooltip}
              >
                <td className="label">
                  {stripPrefix(j.label)}
                  {failed && j.last_error && (
                    <div className="error-preview mono">{j.last_error}</div>
                  )}
                </td>
                <td className="mono dim">{j.schedule.summary}</td>
                <td>
                  <span className={`status-pill ${pillColor(j.status)}`}>
                    <span className="led" />
                    {statusLabel(j.status)}
                  </span>
                </td>
                <td className="mono dim">
                  {j.last_exit === null
                    ? "—"
                    : j.last_exit === 0
                      ? "0"
                      : String(j.last_exit)}
                </td>
                <td className="mono dim">{formatTs(j.last_run)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function stripPrefix(label: string) {
  return label.replace(/^com\.adamnoonan\.scryer\./, "");
}

function pillColor(s: JobSummary["status"]): string {
  switch (s) {
    case "running":
      return "green";
    case "failed":
      return "red";
    case "idle":
      return "gray";
    case "not_loaded":
      return "gray dim";
    default:
      return "amber";
  }
}

function statusLabel(s: JobSummary["status"]): string {
  if (s === "not_loaded") return "not loaded";
  return s;
}

function formatTs(ts: number | null): string {
  if (ts === null || ts === 0) return "—";
  const d = new Date(ts * 1000);
  const now = Date.now();
  const diff = (now - d.getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return d.toISOString().slice(0, 16).replace("T", " ");
}
