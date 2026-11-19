-- Least-privilege Unity Catalog grants for the Fleet Telemetry Analyst.
--
-- This SQL is intentionally separate from the pipeline graph. Run it only after the
-- pipeline has published `pump_telemetry_history`. Re-running it is safe because GRANT is
-- idempotent. Databricks SQL does not accept parameter markers or IDENTIFIER() in a GRANT
-- principal clause, so the target group is backtick-escaped before dynamic execution. The checked
-- deploy entry point verifies the operator is a member before this job runs; the script creates no
-- identities.

DECLARE OR REPLACE VARIABLE telemetry_catalog STRING DEFAULT :catalog_name;
DECLARE OR REPLACE VARIABLE telemetry_schema_fqn STRING DEFAULT :schema_fqn;
DECLARE OR REPLACE VARIABLE telemetry_view_fqn STRING DEFAULT :view_fqn;
DECLARE OR REPLACE VARIABLE telemetry_reader_group STRING DEFAULT :telemetry_analysis_group;

EXECUTE IMMEDIATE concat(
  'GRANT USE CATALOG ON CATALOG IDENTIFIER(?) TO `',
  replace(telemetry_reader_group, '`', '``'), '`'
) USING telemetry_catalog;

EXECUTE IMMEDIATE concat(
  'GRANT USE SCHEMA ON SCHEMA IDENTIFIER(?) TO `',
  replace(telemetry_reader_group, '`', '``'), '`'
) USING telemetry_schema_fqn;

EXECUTE IMMEDIATE concat(
  'GRANT SELECT ON VIEW IDENTIFIER(?) TO `',
  replace(telemetry_reader_group, '`', '``'), '`'
) USING telemetry_view_fqn;

-- Text-readable verification output for deployment logs.
SHOW GRANTS
ON VIEW IDENTIFIER(telemetry_view_fqn);
