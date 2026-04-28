import { useState } from "react";
import type { JobDetail } from "../lib/api";
import "./JobDetailPanel.css";

interface Props {
  detail: JobDetail;
  onClose: () => void;
  onAction: (kind: "run" | "load" | "unload") => void;
}

export function JobDetailPanel({ detail, onClose, onAction }: Props) {
  const [tab, setTab] = useState<"plist" | "stdout" | "stderr">("stdout");
  const { summary, plist_xml, recent_stdout, recent_stderr, stdout_path, stderr_path } =
    detail;
  const [confirmUnload, setConfirmUnload] = useState(false);

  return (
    <div className="job-panel-overlay" onClick={onClose}>
      <aside className="job-panel" onClick={(e) => e.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">
              <span className="bar" /> Job detail
            </span>
            <h2>{summary.label}</h2>
            <div className="meta">
              <span className="mono dim">{summary.schedule.summary}</span>
              <span className="mono dim">·</span>
              <span className="mono dim">{summary.program ?? "—"}</span>
            </div>
          </div>
          <button onClick={onClose} className="close">
            ×
          </button>
        </header>

        <div className="actions">
          <button
            className="primary"
            onClick={() => onAction("run")}
            title="launchctl kickstart -k — fire the job immediately, ignoring the schedule. Restarts it if already running."
          >
            Run now
          </button>
          <button
            onClick={() => onAction("load")}
            title="launchctl bootstrap — register the plist with launchd's runtime so it begins firing on its schedule. Use after editing the plist or to bring back an unloaded agent."
          >
            Load
          </button>
          {!confirmUnload ? (
            <button
              className="danger"
              onClick={() => setConfirmUnload(true)}
              title="launchctl bootout — de-register from runtime so the agent stops firing. The plist file stays on disk and reloads on next login unless you remove it."
            >
              Unload
            </button>
          ) : (
            <button
              className="danger"
              onClick={() => {
                onAction("unload");
                setConfirmUnload(false);
              }}
            >
              Confirm unload
            </button>
          )}
          <a
            className="ghost-action"
            href={`file://${summary.plist_path}`}
            onClick={(e) => {
              e.stopPropagation();
            }}
            title="Open the plist file location"
          >
            Reveal plist
          </a>
        </div>
        <div className="actions-help mono dim">
          run · trigger now &nbsp;·&nbsp; load · register with launchd &nbsp;·&nbsp;
          unload · stop firing (file stays on disk)
        </div>

        <nav className="tabs">
          <button
            className={`tab ${tab === "stdout" ? "active" : ""}`}
            onClick={() => setTab("stdout")}
          >
            stdout ({recent_stdout.length})
          </button>
          <button
            className={`tab ${tab === "stderr" ? "active" : ""}`}
            onClick={() => setTab("stderr")}
          >
            stderr ({recent_stderr.length})
          </button>
          <button
            className={`tab ${tab === "plist" ? "active" : ""}`}
            onClick={() => setTab("plist")}
          >
            plist
          </button>
        </nav>

        <div className="body">
          {tab === "stdout" && <LogPane lines={recent_stdout} path={stdout_path} />}
          {tab === "stderr" && <LogPane lines={recent_stderr} path={stderr_path} />}
          {tab === "plist" && (
            <pre className="plist">{plist_xml}</pre>
          )}
        </div>
      </aside>
    </div>
  );
}

function LogPane({ lines, path }: { lines: string[]; path: string | null }) {
  if (lines.length === 0) {
    return (
      <div className="empty">
        <p className="dim">No log lines.</p>
        {path && <p className="mono faint">{path}</p>}
      </div>
    );
  }
  return (
    <div className="log-pane">
      {path && <div className="log-path mono faint">{path}</div>}
      <pre>{lines.join("\n")}</pre>
    </div>
  );
}
