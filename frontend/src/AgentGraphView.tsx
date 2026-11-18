import { useEffect, useMemo, useState } from "react";
import type { AgentObservability } from "./types";

type EvidenceState =
  | { phase: "loading" }
  | { phase: "error"; message: string }
  | { phase: "ready"; evidence: AgentObservability };

const GRAPH_STAGES = [
  {
    label: "Incident context",
    detail: "Telemetry, threshold evidence and the governed response procedure enter one run."
  },
  {
    label: "MCP runtime discovery",
    detail: "The graph discovers governed maintenance capabilities at runtime; the UI does not prescribe a tool order."
  },
  {
    label: "LangGraph model reasoning",
    detail: "LangGraph invokes the Foundation Model API through Unity Gateway Model Service to evaluate evidence and choose the next action."
  },
  {
    label: "Governed MCP tool execution",
    detail: "Unity Gateway applies service policy before the selected MCP maintenance capability can execute."
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
            <p>LangGraph can reason over the result and select another capability discovered at runtime.</p>
          </article>
          <article className="agent-branch-card agent-branch-ask">
            <span>Native Gateway ASK</span><strong>Human approval and exact replay</strong>
            <p>The graph pauses for approval, then replays the exact pending request once through Unity Gateway.</p>
          </article>
          <article className="agent-branch-card agent-branch-complete">
            <span>Complete / no action</span><strong>Return the governed outcome</strong>
            <p>The run closes with a work order when warranted, or a recorded no-action decision.</p>
          </article>
        </div>
        <div className="agent-evidence-persistence">
          <span>MLflow evidence persistence</span>
          <p>Execution traces preserve the model, orchestration and governed tool evidence produced by actual runs.</p>
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
