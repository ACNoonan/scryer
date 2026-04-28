import { useEffect, useState } from "react";
import { JobsView } from "./views/JobsView";
import { DataView } from "./views/DataView";
import { SettingsView } from "./views/SettingsView";
import { api, type HealthResponse } from "./lib/api";

type Tab = "jobs" | "data" | "settings";

export default function App() {
  const [tab, setTab] = useState<Tab>("jobs");
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [healthErr, setHealthErr] = useState<string | null>(null);

  useEffect(() => {
    api
      .health()
      .then((h) => {
        setHealth(h);
        setHealthErr(null);
      })
      .catch((e) => setHealthErr(String(e)));
    const interval = setInterval(() => {
      api
        .health()
        .then((h) => {
          setHealth(h);
          setHealthErr(null);
        })
        .catch((e) => setHealthErr(String(e)));
    }, 10_000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app">
      <nav className="top">
        <a className="brand" href="#" onClick={(e) => e.preventDefault()}>
          <svg
            className="mark"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              d="M3 6 L21 6 M3 18 L21 18"
              stroke="#14F195"
              strokeWidth="2"
              strokeLinecap="round"
              fill="none"
            />
            <circle cx="12" cy="12" r="3" fill="#14F195" />
          </svg>
          <span>
            Scryer<span className="dot">.</span>
          </span>
        </a>
        <div className="links">
          <button
            className={`tab ${tab === "jobs" ? "active" : ""}`}
            onClick={() => setTab("jobs")}
          >
            Jobs
          </button>
          <button
            className={`tab ${tab === "data" ? "active" : ""}`}
            onClick={() => setTab("data")}
          >
            Data
          </button>
          <button
            className={`tab ${tab === "settings" ? "active" : ""}`}
            onClick={() => setTab("settings")}
          >
            Settings
          </button>
        </div>
        <div
          className={`status-pill ${healthErr ? "red" : "green"}`}
          title={healthErr ?? "Backend connected"}
        >
          <span className="led" />
          {health
            ? `${health.backend_kind} · v${health.version}`
            : healthErr
              ? "backend offline"
              : "connecting…"}
        </div>
      </nav>
      {tab === "jobs" && <JobsView />}
      {tab === "data" && <DataView />}
      {tab === "settings" && <SettingsView health={health} />}
    </div>
  );
}
