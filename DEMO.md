# Asset Reliability Command Centre demo

## Audience and business outcome

This ten-minute demo is for process-manufacturing, energy, and utilities leaders responsible for
asset availability. It shows how Databricks continuously ingests equipment telemetry, identifies
a bearing-risk condition without waiting for an operator prompt, and turns that insight into a
governed maintenance action before an avoidable outage.

The executive outcome is protected uptime and production throughput. The reliability owner sees
faster detection, fewer emergency repairs, and a complete audit trail from measurement to action.

## Presenter flow

1. Open the command centre on your personal estate of eight circulatory pumps streaming live
   operating measurements. Point out that every authenticated user receives the same isolated
   experience, and show the measured Spark-to-Lakebase, Lakebase-to-App, and
   end-to-end latency. The status is based on real freshness and component heartbeats.

2. Choose **Clear history & replay**. Only your sandbox restarts, its previous activity leaves the
   active UI, and P-202 and P-207 each produce one normal transient. Each
   threshold crossing creates a visible alert and autonomously launches an independent agent
   evaluation. The presenter does not type a prompt.

3. Open one reviewed event. The incident conversation shows the machine-authored initiating
   prompt and every runtime-emitted model, Skill, MCP discovery, tool, result, and policy event.
   Both transients resolve with no maintenance action and remain in invocation history.

4. Choose **Trigger P-204 excursion**. P-204 moves smoothly from a stable feed into rising
   bearing vibration. The one-second App evaluator detects the threshold crossing and launches
   the same autonomous workflow.

5. The single Claude Sonnet 4.5 agent uses the governed response Skill, discovers tools on the
   configured `asset_reliability_mcp` service, and assesses a high likelihood of failure before
   the next maintenance window. Tool choice and call count are runtime decisions, not scripted
   UI stages.

6. Immediately before `submit_work_order`, Unity Gateway returns native `ASK`. The activity card
   says the action was not executed, and the agent stops. Follow the real Gateway handoff and
   approve as the current user; no supervisor role-play or simulated App approval is needed.

7. The exact pending request resumes once. A work order to replace the bearing is created, an
   immutable governance receipt is recorded, and the agent produces its final finding, evidence,
   risk, and action summary. Close the review to return to the live command centre with P-204
   showing maintenance scheduled and both no-action histories retained.

8. Close on the complete journey: a shared RTM workload efficiently serves many isolated personal
   sandboxes, while every consequential write still stops at a policy-controlled human decision.
   The Architecture tab shows how production plant telemetry would replace the synthetic Rate
   adapter with Kafka while preserving the downstream design.

## Experience invariants

- The command centre—not chat—is the primary screen.
- Every user sees and controls only their personal sandbox.
- Alerts formulate the initiating prompt and start agent evaluations automatically.
- The UI never predicts a model call, Skill, MCP server, tool, tool order, or tool-call count.
- Every actual tool call appears as its own activity event.
- The incident footer shows only total tokens, estimated cost, and tool calls.
- Independent incidents can execute concurrently.
- Unity Gateway is the sole approval authority.
- **Clear history & replay** cancels only that sandbox's old agent activity before creating a new
  scenario whose two normal transients run once.
