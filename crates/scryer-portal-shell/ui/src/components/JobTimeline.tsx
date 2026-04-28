import type { JobSummary } from "../lib/api";
import "./JobTimeline.css";

interface Props {
  jobs: JobSummary[];
  hours: number;
  onSelect: (label: string) => void;
}

/** Two fires count as colliding if they're within this many seconds. */
const COLLISION_TOLERANCE_S = 30;
/** Cadence-mismatch budget: 5% relative difference. */
const CADENCE_TOLERANCE = 0.05;

/** Infer cadence (seconds between fires) from the next-fires list. */
function inferInterval(fires: number[]): number | null {
  if (fires.length < 2) return null;
  const delta = fires[1] - fires[0];
  return delta > 0 ? delta : null;
}

/**
 * For each non-scryer job, find scryer job(s) it likely duplicates.
 *
 * Heuristic: same cadence (interval matches within {@link CADENCE_TOLERANCE})
 * AND at least one fire pair within {@link COLLISION_TOLERANCE_S}. Cadence
 * matching is required because scryer has high-frequency tapes (every 60s)
 * that would otherwise collide with any round-minute launchd agent. Two jobs
 * on the same cadence that fire in lock-step are the actual duplicate signal.
 */
function findConflicts(jobs: JobSummary[]): Map<string, JobSummary[]> {
  const conflicts = new Map<string, JobSummary[]>();
  const scryerJobs = jobs.filter((j) => j.group === "scryer");
  for (const j of jobs) {
    if (j.group === "scryer") continue;
    const intJ = inferInterval(j.schedule.next_fires);
    if (intJ === null) continue;
    const seen = new Set<string>();
    const overlaps: JobSummary[] = [];
    for (const s of scryerJobs) {
      const intS = inferInterval(s.schedule.next_fires);
      if (intS === null) continue;
      const ratio = Math.abs(intJ - intS) / Math.max(intJ, intS);
      if (ratio > CADENCE_TOLERANCE) continue;
      const jFires = j.schedule.next_fires.slice(0, 5);
      const sFires = s.schedule.next_fires.slice(0, 5);
      const colliding = jFires.some((jf) =>
        sFires.some((sf) => Math.abs(jf - sf) <= COLLISION_TOLERANCE_S),
      );
      if (colliding && !seen.has(s.label)) {
        seen.add(s.label);
        overlaps.push(s);
      }
    }
    if (overlaps.length > 0) conflicts.set(j.label, overlaps);
  }
  return conflicts;
}

/**
 * Horizontal Gantt-style strip showing next-fire markers for each job over
 * the next N hours. Scryer jobs are highlighted green; "other" jobs are
 * muted grey by default, but rendered amber if their fires overlap with a
 * scryer job (likely-duplicate signal — see methodology Portal section).
 */
export function JobTimeline({ jobs, hours, onSelect }: Props) {
  const now = Math.floor(Date.now() / 1000);
  const horizon = now + hours * 3600;

  const conflicts = findConflicts(jobs);

  const lanes = jobs.slice().sort((a, b) => {
    if (a.group !== b.group) return a.group === "scryer" ? -1 : 1;
    return a.label.localeCompare(b.label);
  });

  return (
    <div className="timeline">
      <div className="timeline-axis">
        {Array.from({ length: 7 }).map((_, i) => {
          const t = now + Math.round((i / 6) * hours * 3600);
          return (
            <span key={i} className="axis-mark mono">
              {formatHourLabel(t)}
            </span>
          );
        })}
      </div>
      <div className="timeline-lanes">
        {lanes.map((j) => {
          const dupes = conflicts.get(j.label);
          const hasConflict = !!dupes;
          const tooltip = hasConflict
            ? `${j.label} — ${j.schedule.summary}\nLikely duplicates: ${dupes!.map((d) => d.label).join(", ")}`
            : `${j.label} — ${j.schedule.summary}`;
          const isNotLoaded = j.status === "not_loaded";
          // Suppress ticks for unloaded jobs — the plist's StartInterval would
          // *say* it fires every N seconds, but launchd isn't actually
          // scheduling it. Showing ticks would be misleading.
          const visibleFires = isNotLoaded
            ? []
            : j.schedule.next_fires.filter((t) => t >= now && t <= horizon);
          const laneClasses = [
            "lane",
            j.group === "scryer" ? "scryer" : "other",
            hasConflict ? "conflict" : "",
            isNotLoaded ? "not-loaded" : "",
          ]
            .filter(Boolean)
            .join(" ");
          return (
            <div
              key={j.label}
              className={laneClasses}
              onClick={() => onSelect(j.label)}
              title={
                isNotLoaded
                  ? `${j.label} — plist on disk but not loaded into launchd`
                  : tooltip
              }
            >
              <div className="lane-name">
                {shortLabel(j)}
                {hasConflict && (
                  <span className="conflict-tag mono">conflict</span>
                )}
                {isNotLoaded && (
                  <span className="status-tag mono">not loaded</span>
                )}
              </div>
              <div className="lane-track">
                {visibleFires.map((t) => {
                  const pct = ((t - now) / (horizon - now)) * 100;
                  return (
                    <span
                      key={t}
                      className="tick"
                      style={{ left: `${pct}%` }}
                    />
                  );
                })}
                {visibleFires.length === 0 && (
                  <span className="lane-empty">
                    {isNotLoaded
                      ? "plist on disk · not loaded"
                      : j.schedule.summary}
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function shortLabel(j: JobSummary): string {
  return j.label.replace(/^com\.adamnoonan\.scryer\./, "").replace(/^com\./, "");
}

function formatHourLabel(unix: number): string {
  const d = new Date(unix * 1000);
  const h = d.getHours().toString().padStart(2, "0");
  const m = d.getMinutes().toString().padStart(2, "0");
  return `${h}:${m}`;
}
