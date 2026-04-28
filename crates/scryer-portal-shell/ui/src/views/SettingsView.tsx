import { useState } from "react";
import {
  getBackendUrl,
  setBackendUrl,
  type HealthResponse,
} from "../lib/api";

export function SettingsView({ health }: { health: HealthResponse | null }) {
  const [url, setUrl] = useState(getBackendUrl() || "(same origin)");
  const [saved, setSaved] = useState(false);

  return (
    <div className="section">
      <div className="head-row">
        <div>
          <span className="eyebrow p">
            <span className="bar" /> Settings
          </span>
          <h1>Configuration</h1>
        </div>
      </div>

      <div
        className="card"
        style={{ display: "grid", gap: 18, maxWidth: 720 }}
      >
        <div>
          <div className="tagnum" style={{ marginBottom: 8 }}>
            Backend URL
          </div>
          <input
            type="text"
            value={url}
            onChange={(e) => {
              setUrl(e.target.value);
              setSaved(false);
            }}
            placeholder="http://127.0.0.1:47777"
            style={{ width: "100%" }}
          />
          <p className="dim" style={{ fontSize: 12, margin: "8px 0 0" }}>
            Empty / "(same origin)" uses Vite's dev proxy. Set to your remote
            scryer-portal server URL when accessing a deployed instance.
          </p>
          <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
            <button
              className="primary"
              onClick={() => {
                const v = url.trim();
                setBackendUrl(v === "(same origin)" ? "" : v);
                setSaved(true);
              }}
            >
              Save
            </button>
            {saved && (
              <span className="dim" style={{ alignSelf: "center" }}>
                Saved · reload the page to apply
              </span>
            )}
          </div>
        </div>

        {health && (
          <div>
            <div className="tagnum" style={{ marginBottom: 8 }}>
              Connected backend
            </div>
            <pre className="mono dim" style={{ margin: 0, fontSize: 12 }}>
              {JSON.stringify(health, null, 2)}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
