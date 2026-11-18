import { useEffect, useMemo, useState } from "react";
import type { AgentObservability } from "./types";

type EvidenceState =
  | { phase: "loading" }
  | { phase: "error"; message: string }
  | { phase: "ready"; evidence: AgentObservability };

const GRAPH_STAGES = [
  {
    label: "Incident context",
    detail: "Telemetry, threshold evidence and the maintenance objective enter one run."
  },
  {
    label: "Governed procedure",
    detail: "The graph loads the Unity Catalog response Skill before deciding what to do."
  },
  {
    label: "Reason and select",
    detail: "The model evaluates evidence and chooses from tools discovered at runtime."
  },
  {
    label: "Governed tool call",
    detail: "Unity Gateway applies service policy before the MCP capability can execute."
  }
] as const;

async function loadObservability(signal: AbortSignal): Promise<AgentObservability> {
  const response = await fetch("/api/agent-observability", { signal, cache: "no-store" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: "Request failed" }));
    throw new Error(body.detail ?? "Unable to load agent observability");
  }
  return response.json() as Promise<AgentObservability>;
}

function ExternalLink({ href, children }: { href: string; children: string }) {
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}<span aria-hidden="true">↗</span></a>;
}

export function AgentGraphView() {
  const [reload, setReload] = useState(0);
  const [evidenceState, setEvidenceState] = useState<EvidenceState>({ phase: "loading" });

  useEffect(() => {
    const controller = new AbortController();
    setEvidenceState({ phase: "loading" });
    void loadObservability(controller.signal)
      .then((evidence) => setEvidenceState({ phase: "ready", evidence }))
      .catch((caught: unknown) => {
        if (caught instanceof DOMException && caught.name === "AbortError") return;
        setEvidenceState({
          phase: "error",
          message: caught instanceof Error ? caught.message : "Unable to load agent observability"
        });
      });
    return () => controller.abort();
  }, [reload]);

  const evidence = evidenceState.phase === "ready" ? evidenceState.evidence : null;
  const experimentUrl = evidence?.experiment_url ?? null;
  const tracesUrl = evidence?.traces_url ?? experimentUrl;
  const evidenceIsPartial = useMemo(() => Boolean(
    evidence && (
      evidence.status === "unavailable"
      || !evidence.experiment_id
      || !evidence.experiment_url
      || !evidence.traces_url
    )
  ), [evidence]);

  return <section className="agent-graph-page" aria-labelledby="agent-graph-title">
    <header className="agent-graph-heading">
      <div>
        <p>Agent orchestration</p>
        <h2 id="agent-graph-title">A governed LangGraph turns asset risk into a traceable maintenance decision.</h2>
        <span>The graph shows the designed control flow; MLflow provides evidence from actual executions.</span>
      </div>
      <span className="agent-framework-badge">LangGraph</span>
    </header>

    <div className="agent-graph-layout">
      <section className="agent-design-panel" aria-labelledby="agent-design-title">
        <div className="agent-panel-heading">
          <div><span>Static architecture</span><h3 id="agent-design-title">Governed agent control flow</h3></div>
          <p>Design view · not a live execution trace</p>
        </div>
        <div className="agent-flow" aria-label="LangGraph control-flow design">
          {GRAPH_STAGES.map((stage, index) => <div className="agent-flow-step" key={stage.label}>
            <article>
              <span>{String(index + 1).padStart(2, "0")}</span>
              <div><strong>{stage.label}</strong><p>{stage.detail}</p></div>
            </article>
            {index < GRAPH_STAGES.length - 1 && <span className="agent-flow-arrow" aria-hidden="true">→</span>}
          </div>)}
        </div>
        <div className="agent-branch-grid">
          <article className="agent-branch-card agent-branch-loop">
            <span>Continue</span><strong>Tool evidence returns to reasoning</strong>
            <p>The graph can select another discovered capability without a UI-imposed tool order.</p>
          </article>
          <article className="agent-branch-card agent-branch-ask">
            <span>Native ASK</span><strong>Pause for human approval</strong>
            <p>The exact pending request resumes once after Unity Gateway records approval.</p>
          </article>
          <article className="agent-branch-card agent-branch-complete">
            <span>Complete</span><strong>Return evidence and outcome</strong>
            <p>The run closes with its response, work order and immutable governance receipt.</p>
          </article>
        </div>
      </section>

      <aside className="agent-evidence-panel" aria-labelledby="agent-evidence-title">
        <div className="agent-panel-heading">
          <div><span>Live runtime evidence</span><h3 id="agent-evidence-title">MLflow experiment and traces</h3></div>
          <p>Links returned by the deployed App</p>
        </div>

        {evidenceState.phase === "loading" && <div className="agent-evidence-state" role="status" aria-live="polite">
          <i className="agent-evidence-spinner" aria-hidden="true" />
          <div><strong>Loading MLflow evidence…</strong><p>Checking the deployed agent configuration.</p></div>
        </div>}

        {evidenceState.phase === "error" && <div className="agent-evidence-state agent-evidence-error" role="alert">
          <span aria-hidden="true">!</span>
          <div><strong>MLflow evidence could not be loaded</strong><p>{evidenceState.message}</p><button type="button" onClick={() => setReload((value) => value + 1)}>Retry</button></div>
        </div>}

        {evidence && <>
          <div className={`agent-evidence-status ${evidence.status === "configured" ? "agent-evidence-configured" : "agent-evidence-unavailable"}`}>
            <span>{evidence.status === "configured" ? "Configured" : "Unavailable"}</span>
            <strong>{evidence.framework}</strong>
          </div>

          {evidenceIsPartial && <div className="agent-evidence-partial" role="status">
            <strong>Partial runtime evidence</strong>
            <p>{evidence.detail ?? (experimentUrl
              ? "A dedicated traces link was not returned, so trace navigation uses the verified MLflow experiment page."
              : "The deployed runtime has not returned a verified MLflow experiment link yet.")}</p>
          </div>}

          <dl className="agent-evidence-details">
            <div><dt>Framework</dt><dd>{evidence.framework}</dd></div>
            <div><dt>Experiment ID</dt><dd>{evidence.experiment_id ?? "Not available"}</dd></div>
          </dl>

          {(experimentUrl || tracesUrl) ? <div className="agent-evidence-actions">
            {experimentUrl && <ExternalLink href={experimentUrl}>Open MLflow experiment</ExternalLink>}
            {tracesUrl && <ExternalLink href={tracesUrl}>View MLflow traces</ExternalLink>}
          </div> : <div className="agent-evidence-empty">
            <strong>No verified MLflow link is available</strong>
            <p>Run evidence will appear only after the deployed runtime returns its configured experiment.</p>
          </div>}
        </>}
      </aside>
    </div>
  </section>;
}
