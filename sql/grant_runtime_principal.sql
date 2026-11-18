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

-- Produce portable, text-readable evidence without relying on SHOW GRANTS support for
-- parameterised object identifiers.
SELECT 'CATALOG' AS object_type, catalog_name AS object_name, privilege_type
FROM system.information_schema.catalog_privileges
WHERE catalog_name = runtime_catalog AND grantee = runtime_principal
UNION ALL
SELECT 'SCHEMA', concat(schema_catalog, '.', schema_name), privilege_type
FROM system.information_schema.schema_privileges
WHERE concat(schema_catalog, '.', schema_name) = runtime_schema_fqn
  AND grantee = runtime_principal
UNION ALL
SELECT 'VOLUME', concat(volume_catalog, '.', volume_schema, '.', volume_name), privilege_type
FROM system.information_schema.volume_privileges
WHERE concat(volume_catalog, '.', volume_schema, '.', volume_name) = runtime_volume_fqn
  AND grantee = runtime_principal
ORDER BY object_type, object_name, privilege_type;
