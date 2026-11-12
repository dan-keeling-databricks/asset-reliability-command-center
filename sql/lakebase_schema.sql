-- Canonical operational schema for the Asset Reliability Command Center.
-- This migration is intentionally idempotent because every App startup applies it.

-- Fleet reference data and scenario lifecycle.
CREATE SCHEMA IF NOT EXISTS asset_reliability;

CREATE TABLE IF NOT EXISTS asset_reliability.pumps (
    pump_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    area_name TEXT NOT NULL,
    asset_class TEXT NOT NULL DEFAULT 'Centrifugal pump',
    criticality TEXT NOT NULL CHECK (criticality IN ('low', 'medium', 'high')),
    next_maintenance_window TIMESTAMPTZ NOT NULL
);

INSERT INTO asset_reliability.pumps
    (pump_id, display_name, area_name, criticality, next_maintenance_window)
VALUES
    ('P-201', 'Cooling Water Pump P-201', 'Cooling loop A', 'medium', now() + interval '12 days'),
    ('P-202', 'Cooling Water Pump P-202', 'Cooling loop A', 'medium', now() + interval '14 days'),
    ('P-203', 'Cooling Water Pump P-203', 'Cooling loop A', 'medium', now() + interval '11 days'),
    ('P-204', 'Cooling Water Pump P-204', 'Cooling loop A', 'high', now() + interval '9 days'),
    ('P-205', 'Cooling Water Pump P-205', 'Cooling loop B', 'medium', now() + interval '13 days'),
    ('P-206', 'Cooling Water Pump P-206', 'Cooling loop B', 'medium', now() + interval '10 days'),
    ('P-207', 'Cooling Water Pump P-207', 'Cooling loop B', 'medium', now() + interval '15 days'),
    ('P-208', 'Cooling Water Pump P-208', 'Cooling loop B', 'medium', now() + interval '12 days')
ON CONFLICT (pump_id) DO UPDATE
SET display_name = EXCLUDED.display_name,
    area_name = EXCLUDED.area_name,
    criticality = EXCLUDED.criticality;

CREATE TABLE IF NOT EXISTS asset_reliability.scenario_runs (
    scenario_id UUID PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('active', 'archived')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    requested_by TEXT,
    p204_excursion_started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Shared shift-operator identity and immutable handover audit.
CREATE TABLE IF NOT EXISTS asset_reliability.app_users (
    email TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    operator_email TEXT REFERENCES asset_reliability.app_users(email),
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ,
    updated_by TEXT
);

INSERT INTO asset_reliability.shift_operator (singleton)
VALUES (true) ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator_audit (
    audit_id UUID PRIMARY KEY,
    previous_operator_email TEXT,
    new_operator_email TEXT NOT NULL,
    initiated_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    previous_version BIGINT NOT NULL,
    new_version BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_reliability.scenario_commands (
    command_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES asset_reliability.scenario_runs(scenario_id),
    command_type TEXT NOT NULL CHECK (
        command_type IN ('restart_scenario', 'start_p204_excursion')
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'claimed', 'completed', 'failed')
    ),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    requested_by TEXT,
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT,
    lease_expires_at TIMESTAMPTZ,
    claim_version BIGINT NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ,
    error TEXT,
    UNIQUE (scenario_id, command_type)
);

-- Older installations predate command leases. These additive migrations keep an active
-- producer compatible while enabling another producer to reclaim work abandoned by a failed
-- process. Completion is fenced by both owner and claim_version in the producer code.
ALTER TABLE asset_reliability.scenario_commands
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

-- A command claimed before lease support was deployed has no expiry and would otherwise be
-- stranded forever. Expiring it in place lets the version-fenced producer reclaim it safely.
UPDATE asset_reliability.scenario_commands
SET lease_expires_at = clock_timestamp() - interval '1 second'
WHERE status = 'claimed' AND lease_expires_at IS NULL;

CREATE INDEX IF NOT EXISTS scenario_commands_pending_idx
    ON asset_reliability.scenario_commands (requested_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS scenario_commands_reclaim_idx
    ON asset_reliability.scenario_commands (lease_expires_at)
    WHERE status = 'claimed';

-- Append-oriented telemetry plus the bounded latest-state read model.
CREATE TABLE IF NOT EXISTS asset_reliability.telemetry_events (
    event_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    pump_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    flow_m3h DOUBLE PRECISION NOT NULL,
    discharge_pressure_bar DOUBLE PRECISION NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    bearing_temperature_c DOUBLE PRECISION NOT NULL,
    motor_current_a DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE asset_reliability.telemetry_events
    ALTER COLUMN ingested_at SET DEFAULT clock_timestamp();

CREATE INDEX IF NOT EXISTS telemetry_events_scenario_pump_time_idx
    ON asset_reliability.telemetry_events (scenario_id, pump_id, event_time DESC);

CREATE TABLE IF NOT EXISTS asset_reliability.pump_live_state (
    scenario_id TEXT NOT NULL,
    pump_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    flow_m3h DOUBLE PRECISION NOT NULL,
    discharge_pressure_bar DOUBLE PRECISION NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    bearing_temperature_c DOUBLE PRECISION NOT NULL,
    motor_current_a DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (scenario_id, pump_id)
);

CREATE TABLE IF NOT EXISTS asset_reliability.alert_rules (
    rule_id TEXT PRIMARY KEY,
    metric TEXT NOT NULL,
    comparison TEXT NOT NULL CHECK (comparison IN ('gt', 'gte', 'lt', 'lte')),
    warning_threshold DOUBLE PRECISION NOT NULL,
    clear_threshold DOUBLE PRECISION NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    description TEXT NOT NULL
);

INSERT INTO asset_reliability.alert_rules
    (rule_id, metric, comparison, warning_threshold, clear_threshold, description)
VALUES
    (
        'bearing-vibration-warning',
        'vibration_mm_s',
        'gte',
        4.4,
        3.6,
        'Bearing vibration has crossed the fleet warning threshold.'
    )
ON CONFLICT (rule_id) DO UPDATE
SET warning_threshold = EXCLUDED.warning_threshold,
    clear_threshold = EXCLUDED.clear_threshold,
    enabled = true,
    description = EXCLUDED.description;

-- Durable incident, agent execution, conversation, and governed-action journal.
CREATE TABLE IF NOT EXISTS asset_reliability.incidents (
    incident_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES asset_reliability.scenario_runs(scenario_id),
    pump_id TEXT NOT NULL REFERENCES asset_reliability.pumps(pump_id),
    rule_id TEXT NOT NULL REFERENCES asset_reliability.alert_rules(rule_id),
    status TEXT NOT NULL CHECK (
        status IN ('reviewing', 'waiting_approval', 'resolved_no_action', 'maintenance_scheduled', 'failed')
    ),
    observed_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    resolution_summary TEXT,
    UNIQUE (scenario_id, pump_id, rule_id)
);

CREATE INDEX IF NOT EXISTS incidents_scenario_time_idx
    ON asset_reliability.incidents (scenario_id, triggered_at DESC);

CREATE TABLE IF NOT EXISTS asset_reliability.agent_runs (
    run_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'waiting_approval', 'completed', 'failed')
    ),
    trigger_prompt TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    pending_approval_url TEXT,
    pending_tool_request JSONB,
    approval_replay_state TEXT CHECK (
        approval_replay_state IN ('pending', 'claimed', 'completed')
    ),
    approval_claimed_at TIMESTAMPTZ,
    approval_claimed_by TEXT,
    approval_claim_id UUID,
    approval_claim_version BIGINT NOT NULL DEFAULT 0,
    total_input_tokens BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    estimated_cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

ALTER TABLE asset_reliability.agent_runs
    ADD COLUMN IF NOT EXISTS approval_replay_state TEXT,
    ADD COLUMN IF NOT EXISTS approval_claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approval_claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS approval_claim_id UUID,
    ADD COLUMN IF NOT EXISTS approval_claim_version BIGINT NOT NULL DEFAULT 0;

-- CREATE TABLE constraints are not retroactively applied to an existing live table. Add the
-- replay-state contract explicitly and validate it after the additive column migration.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agent_runs_approval_replay_state_check'
          AND conrelid = 'asset_reliability.agent_runs'::regclass
    ) THEN
        ALTER TABLE asset_reliability.agent_runs
            ADD CONSTRAINT agent_runs_approval_replay_state_check
            CHECK (approval_replay_state IN ('pending', 'claimed', 'completed')) NOT VALID;
    END IF;
END;
$$;

-- Also finishes validation if an earlier deployment added the constraint as NOT VALID but was
-- interrupted before the scan completed.
ALTER TABLE asset_reliability.agent_runs
    VALIDATE CONSTRAINT agent_runs_approval_replay_state_check;

-- Each successful Gateway replay emits one immutable receipt. The request hash binds the
-- human decision to the exact JSON-RPC bytes that were paused by Unity Gateway without
-- persisting the operator's OBO bearer token.
CREATE TABLE IF NOT EXISTS asset_reliability.governance_receipts (
    receipt_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES asset_reliability.agent_runs(run_id),
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    elicitation_id TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    approving_subject TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('ALLOW', 'DENY')),
    requested_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION asset_reliability.reject_governance_receipt_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'governance receipts are immutable';
END;
$$;

DROP TRIGGER IF EXISTS governance_receipts_are_immutable
    ON asset_reliability.governance_receipts;

CREATE TRIGGER governance_receipts_are_immutable
BEFORE UPDATE OR DELETE ON asset_reliability.governance_receipts
FOR EACH ROW EXECUTE FUNCTION asset_reliability.reject_governance_receipt_mutation();

CREATE TABLE IF NOT EXISTS asset_reliability.activity_events (
    event_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES asset_reliability.agent_runs(run_id),
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT,
    status TEXT,
    runtime_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, sequence)
);

CREATE INDEX IF NOT EXISTS activity_events_run_sequence_idx
    ON asset_reliability.activity_events (run_id, sequence);

CREATE TABLE IF NOT EXISTS asset_reliability.conversation_messages (
    message_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    sequence INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('agent', 'operator', 'system')),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (incident_id, sequence)
);

CREATE INDEX IF NOT EXISTS conversation_messages_incident_sequence_idx
    ON asset_reliability.conversation_messages (incident_id, sequence);

-- Model output is canonical in activity_events. Remove legacy copies that predate the
-- single-source response model while retaining operator and deterministic runtime messages.
DELETE FROM asset_reliability.conversation_messages AS message
USING asset_reliability.agent_runs AS run, asset_reliability.activity_events AS activity
WHERE message.incident_id = run.incident_id
  AND activity.run_id = run.run_id
  AND message.role = 'agent'
  AND activity.event_type = 'model_call'
  AND activity.runtime_metadata ->> 'summary' = message.content;

CREATE TABLE IF NOT EXISTS asset_reliability.work_orders (
    work_order_id TEXT PRIMARY KEY,
    incident_id UUID NOT NULL UNIQUE REFERENCES asset_reliability.incidents(incident_id),
    pump_id TEXT NOT NULL REFERENCES asset_reliability.pumps(pump_id),
    action TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'submitted', 'scheduled')),
    scheduled_for TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    submitted_by TEXT,
    governance_receipt JSONB
);

CREATE TABLE IF NOT EXISTS asset_reliability.component_heartbeats (
    component_name TEXT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Maintain latest pump state and notify the App after every accepted telemetry write.
-- The sequence guard prevents a delayed Kafka record from replacing newer live state.
CREATE OR REPLACE FUNCTION asset_reliability.apply_telemetry_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO asset_reliability.pump_live_state (
        scenario_id, pump_id, event_id, sequence, event_time, ingested_at,
        flow_m3h, discharge_pressure_bar, vibration_mm_s,
        bearing_temperature_c, motor_current_a
    ) VALUES (
        NEW.scenario_id, NEW.pump_id, NEW.event_id, NEW.sequence,
        NEW.event_time, NEW.ingested_at, NEW.flow_m3h,
        NEW.discharge_pressure_bar, NEW.vibration_mm_s,
        NEW.bearing_temperature_c, NEW.motor_current_a
    )
    ON CONFLICT (scenario_id, pump_id) DO UPDATE
    SET event_id = EXCLUDED.event_id,
        sequence = EXCLUDED.sequence,
        event_time = EXCLUDED.event_time,
        ingested_at = EXCLUDED.ingested_at,
        flow_m3h = EXCLUDED.flow_m3h,
        discharge_pressure_bar = EXCLUDED.discharge_pressure_bar,
        vibration_mm_s = EXCLUDED.vibration_mm_s,
        bearing_temperature_c = EXCLUDED.bearing_temperature_c,
        motor_current_a = EXCLUDED.motor_current_a
    WHERE EXCLUDED.sequence > asset_reliability.pump_live_state.sequence;

    PERFORM pg_notify(
        'asset_reliability_telemetry',
        json_build_object(
            'scenario_id', NEW.scenario_id,
            'pump_id', NEW.pump_id,
            'sequence', NEW.sequence,
            'ingested_at', NEW.ingested_at
        )::text
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS telemetry_event_applied
    ON asset_reliability.telemetry_events;

CREATE TRIGGER telemetry_event_applied
AFTER INSERT OR UPDATE ON asset_reliability.telemetry_events
FOR EACH ROW EXECUTE FUNCTION asset_reliability.apply_telemetry_event();

-- The streaming table is write-heavy and retention deletes continuously create dead tuples.
ALTER TABLE asset_reliability.telemetry_events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0
);

-- Personal sandbox tenancy. The fixed legacy sandbox keeps the migration additive and lets the
-- previously deployed Kafka writer continue during the ordered cutover. New App requests never
-- resolve this row because it has no real authenticated owner and is permanently expired.
CREATE TABLE IF NOT EXISTS asset_reliability.sandboxes (
    sandbox_id UUID PRIMARY KEY,
    owner_email TEXT NOT NULL UNIQUE,
    generator_slot INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

INSERT INTO asset_reliability.sandboxes
    (sandbox_id, owner_email, generator_slot, status, expires_at)
VALUES
    ('00000000-0000-0000-0000-000000000001', '__legacy_demo__@internal.invalid', -1,
     'expired', '-infinity'::timestamptz)
ON CONFLICT (sandbox_id) DO NOTHING;

ALTER TABLE asset_reliability.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_generator_slot_key;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_sandbox_per_generator_slot
    ON asset_reliability.sandboxes (generator_slot) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS asset_reliability.sandbox_slots (
    generator_slot INTEGER PRIMARY KEY,
    sandbox_id UUID UNIQUE REFERENCES asset_reliability.sandboxes(sandbox_id),
    leased_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0
);

INSERT INTO asset_reliability.sandbox_slots (generator_slot)
SELECT generate_series(0, 31)
ON CONFLICT (generator_slot) DO NOTHING;

CREATE TABLE IF NOT EXISTS asset_reliability.sandbox_controls (
    sandbox_id UUID NOT NULL REFERENCES asset_reliability.sandboxes(sandbox_id),
    pump_id TEXT NOT NULL REFERENCES asset_reliability.pumps(pump_id),
    control_type TEXT NOT NULL CHECK (control_type IN ('excursion')),
    status TEXT NOT NULL CHECK (status IN ('active', 'cleared')),
    initiated_by TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sandbox_id, pump_id, control_type)
);

ALTER TABLE asset_reliability.scenario_runs
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001'
        REFERENCES asset_reliability.sandboxes(sandbox_id);
UPDATE asset_reliability.scenario_runs
SET sandbox_id = '00000000-0000-0000-0000-000000000001'
WHERE sandbox_id IS NULL;
ALTER TABLE asset_reliability.scenario_runs ALTER COLUMN sandbox_id SET NOT NULL;
DROP INDEX IF EXISTS asset_reliability.one_active_asset_reliability_scenario;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_scenario_per_sandbox
    ON asset_reliability.scenario_runs (sandbox_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS scenario_runs_sandbox_started_idx
    ON asset_reliability.scenario_runs (sandbox_id, started_at DESC);

ALTER TABLE asset_reliability.telemetry_events
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001',
    ADD COLUMN IF NOT EXISTS generator_slot INTEGER;
ALTER TABLE asset_reliability.pump_live_state
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.scenario_commands
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.incidents
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.agent_runs
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.governance_receipts
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.activity_events
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.conversation_messages
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.work_orders
    ADD COLUMN IF NOT EXISTS sandbox_id UUID
        DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE asset_reliability.component_heartbeats
    ADD COLUMN IF NOT EXISTS sandbox_id UUID;

UPDATE asset_reliability.telemetry_events telemetry
SET sandbox_id = scenario.sandbox_id
FROM asset_reliability.scenario_runs scenario
WHERE telemetry.scenario_id = scenario.scenario_id::text
  AND telemetry.sandbox_id IS DISTINCT FROM scenario.sandbox_id;
UPDATE asset_reliability.pump_live_state live
SET sandbox_id = scenario.sandbox_id
FROM asset_reliability.scenario_runs scenario
WHERE live.scenario_id = scenario.scenario_id::text
  AND live.sandbox_id IS DISTINCT FROM scenario.sandbox_id;
UPDATE asset_reliability.scenario_commands command
SET sandbox_id = scenario.sandbox_id
FROM asset_reliability.scenario_runs scenario
WHERE command.scenario_id = scenario.scenario_id
  AND command.sandbox_id IS DISTINCT FROM scenario.sandbox_id;
UPDATE asset_reliability.incidents incident
SET sandbox_id = scenario.sandbox_id
FROM asset_reliability.scenario_runs scenario
WHERE incident.scenario_id = scenario.scenario_id
  AND incident.sandbox_id IS DISTINCT FROM scenario.sandbox_id;
UPDATE asset_reliability.agent_runs run
SET sandbox_id = incident.sandbox_id
FROM asset_reliability.incidents incident
WHERE run.incident_id = incident.incident_id
  AND run.sandbox_id IS DISTINCT FROM incident.sandbox_id;
UPDATE asset_reliability.activity_events event
SET sandbox_id = run.sandbox_id
FROM asset_reliability.agent_runs run
WHERE event.run_id = run.run_id
  AND event.sandbox_id IS DISTINCT FROM run.sandbox_id;
UPDATE asset_reliability.conversation_messages message
SET sandbox_id = incident.sandbox_id
FROM asset_reliability.incidents incident
WHERE message.incident_id = incident.incident_id
  AND message.sandbox_id IS DISTINCT FROM incident.sandbox_id;
UPDATE asset_reliability.work_orders work_order
SET sandbox_id = incident.sandbox_id
FROM asset_reliability.incidents incident
WHERE work_order.incident_id = incident.incident_id
  AND work_order.sandbox_id IS DISTINCT FROM incident.sandbox_id;
UPDATE asset_reliability.governance_receipts receipt
SET sandbox_id = incident.sandbox_id
FROM asset_reliability.incidents incident
WHERE receipt.incident_id = incident.incident_id
  AND receipt.sandbox_id IS DISTINCT FROM incident.sandbox_id;

-- Validate without taking a long ACCESS EXCLUSIVE scan lock while the old writer is live. Once
-- PostgreSQL has the validated check, SET NOT NULL reuses that proof and is a metadata operation.
DO $$
DECLARE
    table_name TEXT;
    check_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'telemetry_events', 'pump_live_state', 'scenario_commands', 'incidents', 'agent_runs',
        'governance_receipts', 'activity_events', 'conversation_messages', 'work_orders'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = format('asset_reliability.%I', table_name)::regclass
              AND attname = 'sandbox_id' AND attnotnull
        ) THEN
            check_name := table_name || '_sandbox_id_nn';
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = check_name
                  AND connamespace = 'asset_reliability'::regnamespace
            ) THEN
                EXECUTE format(
                    'ALTER TABLE asset_reliability.%I ADD CONSTRAINT %I '
                    'CHECK (sandbox_id IS NOT NULL) NOT VALID', table_name, check_name
                );
            END IF;
            EXECUTE format(
                'ALTER TABLE asset_reliability.%I VALIDATE CONSTRAINT %I',
                table_name, check_name
            );
            EXECUTE format(
                'ALTER TABLE asset_reliability.%I ALTER COLUMN sandbox_id SET NOT NULL',
                table_name
            );
            EXECUTE format(
                'ALTER TABLE asset_reliability.%I DROP CONSTRAINT %I', table_name, check_name
            );
        END IF;
    END LOOP;
END;
$$;

DO $$
DECLARE
    target RECORD;
BEGIN
    FOR target IN SELECT * FROM (VALUES
        ('telemetry_events', 'telemetry_events_sandbox_fk'),
        ('pump_live_state', 'pump_live_state_sandbox_fk'),
        ('scenario_commands', 'scenario_commands_sandbox_fk'),
        ('incidents', 'incidents_sandbox_fk'),
        ('agent_runs', 'agent_runs_sandbox_fk'),
        ('governance_receipts', 'governance_receipts_sandbox_fk'),
        ('activity_events', 'activity_events_sandbox_fk'),
        ('conversation_messages', 'conversation_messages_sandbox_fk'),
        ('work_orders', 'work_orders_sandbox_fk')
    ) AS target_constraints(table_name, constraint_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = target.constraint_name
              AND connamespace = 'asset_reliability'::regnamespace
        ) THEN
            EXECUTE format(
                'ALTER TABLE asset_reliability.%I ADD CONSTRAINT %I '
                'FOREIGN KEY (sandbox_id) REFERENCES asset_reliability.sandboxes(sandbox_id) '
                'NOT VALID', target.table_name, target.constraint_name
            );
        END IF;
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS telemetry_events_sandbox_pump_time_idx
    ON asset_reliability.telemetry_events (sandbox_id, pump_id, event_time DESC);
CREATE INDEX IF NOT EXISTS pump_live_state_sandbox_scenario_idx
    ON asset_reliability.pump_live_state (sandbox_id, scenario_id, pump_id);
CREATE INDEX IF NOT EXISTS incidents_sandbox_time_idx
    ON asset_reliability.incidents (sandbox_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS scenario_commands_sandbox_status_idx
    ON asset_reliability.scenario_commands (sandbox_id, status, requested_at);

-- The raw context never appears in the model-visible schema. The runtime persists only its
-- SHA-256 digest and injects the opaque value immediately before the Gateway call.
CREATE TABLE IF NOT EXISTS asset_reliability.agent_run_contexts (
    run_context_sha256 TEXT PRIMARY KEY CHECK (length(run_context_sha256) = 64),
    run_id UUID NOT NULL UNIQUE REFERENCES asset_reliability.agent_runs(run_id),
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    scenario_id UUID NOT NULL REFERENCES asset_reliability.scenario_runs(scenario_id),
    sandbox_id UUID NOT NULL REFERENCES asset_reliability.sandboxes(sandbox_id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'terminal')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS agent_run_contexts_sandbox_run_idx
    ON asset_reliability.agent_run_contexts (sandbox_id, run_id);

-- Resolve Rate-source slots and apply the sandbox-local excursion before the existing live-state
-- trigger. Returning NULL deliberately discards telemetry for unallocated or expired slots.
CREATE OR REPLACE FUNCTION asset_reliability.resolve_personal_sandbox_telemetry()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_sandbox UUID;
    resolved_scenario UUID;
    excursion_started TIMESTAMPTZ;
BEGIN
    IF NEW.generator_slot IS NULL THEN
        SELECT scenario.sandbox_id INTO resolved_sandbox
        FROM asset_reliability.scenario_runs scenario
        WHERE scenario.scenario_id::text = NEW.scenario_id;
        NEW.sandbox_id := COALESCE(resolved_sandbox, NEW.sandbox_id);
        RETURN NEW;
    END IF;

    SELECT sandbox.sandbox_id, scenario.scenario_id
      INTO resolved_sandbox, resolved_scenario
    FROM asset_reliability.sandbox_slots slot
    JOIN asset_reliability.sandboxes sandbox
      ON sandbox.sandbox_id = slot.sandbox_id
    JOIN asset_reliability.scenario_runs scenario
      ON scenario.sandbox_id = sandbox.sandbox_id AND scenario.status = 'active'
    WHERE slot.generator_slot = NEW.generator_slot
      AND sandbox.status = 'active'
      AND sandbox.expires_at > clock_timestamp()
      AND slot.lease_expires_at > clock_timestamp();

    IF resolved_sandbox IS NULL THEN
        RETURN NULL;
    END IF;

    NEW.sandbox_id := resolved_sandbox;
    NEW.scenario_id := resolved_scenario::text;
    IF NEW.pump_id = 'P-204' THEN
        SELECT started_at INTO excursion_started
        FROM asset_reliability.sandbox_controls
        WHERE sandbox_id = resolved_sandbox AND pump_id = 'P-204'
          AND control_type = 'excursion' AND status = 'active';
        IF excursion_started IS NOT NULL THEN
            NEW.vibration_mm_s := NEW.vibration_mm_s + LEAST(
                6.4,
                0.25 + GREATEST(
                    0, EXTRACT(EPOCH FROM (NEW.event_time - excursion_started))
                ) * 0.12
            );
            NEW.bearing_temperature_c := NEW.bearing_temperature_c + LEAST(
                18.0,
                GREATEST(0, EXTRACT(EPOCH FROM (NEW.event_time - excursion_started))) * 0.25
            );
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS resolve_personal_sandbox_telemetry
    ON asset_reliability.telemetry_events;
CREATE TRIGGER resolve_personal_sandbox_telemetry
BEFORE INSERT OR UPDATE ON asset_reliability.telemetry_events
FOR EACH ROW EXECUTE FUNCTION asset_reliability.resolve_personal_sandbox_telemetry();

-- PostgreSQL executes every BEFORE ROW trigger before any AFTER ROW trigger, so sandbox
-- resolution and the excursion overlay always complete before ``telemetry_event_applied``
-- updates the live-state read model.

-- One statement-level heartbeat proves that the native PostgreSQL sink is actively committing
-- Rate epochs without turning every telemetry row into a heartbeat write.
CREATE OR REPLACE FUNCTION asset_reliability.record_rate_rtm_heartbeat()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO asset_reliability.component_heartbeats (
        component_name, observed_at, status, details, sandbox_id
    ) VALUES (
        'rate-rtm', NOW(), 'healthy', '{"source":"spark-rate-demo"}'::jsonb, NULL
    )
    ON CONFLICT (component_name) DO UPDATE
    SET observed_at = EXCLUDED.observed_at,
        status = EXCLUDED.status,
        details = EXCLUDED.details,
        sandbox_id = NULL;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS telemetry_rate_rtm_heartbeat
    ON asset_reliability.telemetry_events;
CREATE TRIGGER telemetry_rate_rtm_heartbeat
AFTER INSERT OR UPDATE ON asset_reliability.telemetry_events
FOR EACH STATEMENT EXECUTE FUNCTION asset_reliability.record_rate_rtm_heartbeat();

CREATE OR REPLACE FUNCTION asset_reliability.apply_telemetry_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO asset_reliability.pump_live_state (
        sandbox_id, scenario_id, pump_id, event_id, sequence, event_time, ingested_at,
        flow_m3h, discharge_pressure_bar, vibration_mm_s,
        bearing_temperature_c, motor_current_a
    ) VALUES (
        NEW.sandbox_id, NEW.scenario_id, NEW.pump_id, NEW.event_id, NEW.sequence,
        NEW.event_time, NEW.ingested_at, NEW.flow_m3h,
        NEW.discharge_pressure_bar, NEW.vibration_mm_s,
        NEW.bearing_temperature_c, NEW.motor_current_a
    )
    ON CONFLICT (scenario_id, pump_id) DO UPDATE
    SET sandbox_id = EXCLUDED.sandbox_id,
        event_id = EXCLUDED.event_id,
        sequence = EXCLUDED.sequence,
        event_time = EXCLUDED.event_time,
        ingested_at = EXCLUDED.ingested_at,
        flow_m3h = EXCLUDED.flow_m3h,
        discharge_pressure_bar = EXCLUDED.discharge_pressure_bar,
        vibration_mm_s = EXCLUDED.vibration_mm_s,
        bearing_temperature_c = EXCLUDED.bearing_temperature_c,
        motor_current_a = EXCLUDED.motor_current_a
    WHERE EXCLUDED.sequence > asset_reliability.pump_live_state.sequence;
    PERFORM pg_notify('asset_reliability_telemetry', json_build_object(
        'sandbox_id', NEW.sandbox_id, 'scenario_id', NEW.scenario_id,
        'pump_id', NEW.pump_id, 'sequence', NEW.sequence,
        'ingested_at', NEW.ingested_at)::text);
    RETURN NEW;
END;
$$;
