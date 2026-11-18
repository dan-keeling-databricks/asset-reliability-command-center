-- Unity Catalog privileges required by the code-defined workload identity.
--
-- The principal is the bundle-managed Databricks App service principal for this target.
-- Databricks SQL does not accept parameter markers or IDENTIFIER() in a GRANT principal clause,
-- so each object and principal identifier is backtick-escaped before dynamic execution. This
-- script does not create identities or groups. It runs before the continuous producer, RTM
-- ingestion, and governed pipeline runner are switched to that principal. Every GRANT is
-- idempotent.

DECLARE OR REPLACE VARIABLE runtime_catalog STRING DEFAULT :catalog_name;
DECLARE OR REPLACE VARIABLE runtime_schema_fqn STRING DEFAULT :schema_fqn;
DECLARE OR REPLACE VARIABLE runtime_volume_fqn STRING DEFAULT :volume_fqn;
DECLARE OR REPLACE VARIABLE runtime_principal STRING DEFAULT :app_service_principal;

EXECUTE IMMEDIATE concat(
  'GRANT USE CATALOG ON CATALOG IDENTIFIER(?) TO `',
  replace(runtime_principal, '`', '``'), '`'
) USING runtime_catalog;

EXECUTE IMMEDIATE concat(
  'GRANT USE SCHEMA ON SCHEMA IDENTIFIER(?) TO `',
  replace(runtime_principal, '`', '``'), '`'
) USING runtime_schema_fqn;

EXECUTE IMMEDIATE concat(
  'GRANT CREATE TABLE ON SCHEMA IDENTIFIER(?) TO `',
  replace(runtime_principal, '`', '``'), '`'
) USING runtime_schema_fqn;

EXECUTE IMMEDIATE concat(
  'GRANT READ VOLUME ON VOLUME IDENTIFIER(?) TO `',
  replace(runtime_principal, '`', '``'), '`'
) USING runtime_volume_fqn;

EXECUTE IMMEDIATE concat(
  'GRANT WRITE VOLUME ON VOLUME IDENTIFIER(?) TO `',
  replace(runtime_principal, '`', '``'), '`'
) USING runtime_volume_fqn;

-- These statements produce text-readable deployment evidence of the effective grants.
SHOW GRANTS
ON CATALOG IDENTIFIER(runtime_catalog);

SHOW GRANTS
ON SCHEMA IDENTIFIER(runtime_schema_fqn);

SHOW GRANTS
ON VOLUME IDENTIFIER(runtime_volume_fqn);
