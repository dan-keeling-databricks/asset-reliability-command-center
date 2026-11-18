-- Least-privilege access to bundle-managed Unity Catalog MLflow trace tables.
--
-- The experiment resource creates the four tables before this checked setup job runs. Dynamic
-- SQL is required because Databricks SQL does not accept parameter markers in a GRANT principal
-- clause. Every identifier is independently backtick-escaped before statement construction; the
-- values remain deployment inputs rather than executable SQL fragments.

DECLARE OR REPLACE VARIABLE trace_catalog STRING DEFAULT :catalog_name;
DECLARE OR REPLACE VARIABLE trace_schema STRING DEFAULT :schema_name;
DECLARE OR REPLACE VARIABLE trace_prefix STRING DEFAULT :table_prefix;
DECLARE OR REPLACE VARIABLE app_principal STRING DEFAULT :app_service_principal;
DECLARE OR REPLACE VARIABLE reader_group STRING DEFAULT :app_access_group;

EXECUTE IMMEDIATE concat(
  'GRANT USE CATALOG ON CATALOG `', replace(trace_catalog, '`', '``'),
  '` TO `', replace(app_principal, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT USE SCHEMA ON SCHEMA `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '` TO `', replace(app_principal, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT USE CATALOG ON CATALOG `', replace(trace_catalog, '`', '``'),
  '` TO `', replace(reader_group, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT USE SCHEMA ON SCHEMA `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '` TO `', replace(reader_group, '`', '``'), '`'
);

-- The App identity writes spans and assessments; viewers receive SELECT only.
EXECUTE IMMEDIATE concat(
  'GRANT MODIFY, SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_spans` TO `', replace(app_principal, '`', '``'), '`'
);
EXECUTE IMMEDIATE concat(
  'GRANT SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_spans` TO `', replace(reader_group, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT MODIFY, SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_logs` TO `', replace(app_principal, '`', '``'), '`'
);
EXECUTE IMMEDIATE concat(
  'GRANT SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_logs` TO `', replace(reader_group, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT MODIFY, SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_metrics` TO `', replace(app_principal, '`', '``'), '`'
);
EXECUTE IMMEDIATE concat(
  'GRANT SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_metrics` TO `', replace(reader_group, '`', '``'), '`'
);

EXECUTE IMMEDIATE concat(
  'GRANT MODIFY, SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_annotations` TO `', replace(app_principal, '`', '``'), '`'
);
EXECUTE IMMEDIATE concat(
  'GRANT SELECT ON TABLE `', replace(trace_catalog, '`', '``'), '`.`',
  replace(trace_schema, '`', '``'), '`.`', replace(trace_prefix, '`', '``'),
  '_otel_annotations` TO `', replace(reader_group, '`', '``'), '`'
);
