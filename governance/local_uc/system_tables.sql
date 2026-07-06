-- System-tables analog: SQL views over local audit + lineage logs.
--
-- On Databricks these live under ``system.access.*``. Here they are
-- registered as temp views so BI tools (Streamlit, Genie) can query them
-- uniformly.

CREATE OR REPLACE TEMP VIEW system_access_audit AS
SELECT
    from_unixtime(ts) AS event_time,
    user,
    action,
    resource,
    allowed,
    groups
FROM json.`${LAKEHOUSE_ROOT}/../governance/local_uc/audit_log.jsonl`;

CREATE OR REPLACE TEMP VIEW system_access_table_lineage AS
SELECT
    eventTime           AS event_time,
    run.runId           AS run_id,
    job.name            AS job_name,
    inputs[0].name      AS source_table,
    outputs[0].name     AS target_table
FROM json.`${LAKEHOUSE_ROOT}/../governance/lineage/events.jsonl`;

CREATE OR REPLACE TEMP VIEW system_information_schema_tables AS
SELECT * FROM json.`${LAKEHOUSE_ROOT}/../governance/local_uc/polaris_tables.json`;
