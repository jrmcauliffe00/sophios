# Sophios -> Airflow DAG Bridge (First Implementation + Two Submission Paths)

## What was added

- Added dedicated Airflow bridge module at `src/sophios/apis/python/airflow_bridge.py`.
- Added `get_airflow_dag(...)` for in-memory DAG construction.
- Added `write_airflow_dag_file(...)` to render and write a DAG module to disk (default `./dags/<dag_id>_dag.py`).
- Added `submit_airflow_dag_via_api(...)` + `submit_airflow_dag_run(...)` to trigger a DAG run via Airflow REST API without writing a local DAG file.
- Added task emission mode support:
  - `placeholder` -> always `EmptyOperator`
  - `auto` -> infer `BashOperator` from CWL `CommandLineTool` `baseCommand`/`arguments` when possible, otherwise fallback to `EmptyOperator`
- Added CLI controls:
  - `--write-airflow-dag`
  - `--airflow-dags-dir`
  - `--airflow-dag-filename`
  - `--trigger-airflow-run`
  - `--airflow-api-*` auth/base-url options
  - `--airflow-conf-json`
  - `--airflow-task-mode {placeholder,auto}`
- Updated imports to current Airflow paths (`airflow.sdk` and `airflow.providers.standard.operators.empty`).
- Implemented typed helper utilities to translate compiled Sophios/CWL step dependencies into Airflow task dependencies.
- Added import guard with a clear error if Airflow is not installed.
- Removed duplicate compile during the common path by reusing one compiled workflow object.
- Updated `examples/scripts/ichnaea_compact.py` to call the new bridge module instead of embedding bridge internals.

## How this first version works

1. Compile the Sophios `Workflow` via `get_cwl_workflow()`.
2. Normalize a serializable DAG spec (`task_ids` + `edges`) from CWL steps.
3. Use that spec to:
   - create an in-memory Airflow `DAG`, and/or
   - render a DAG Python file, and/or
   - trigger an Airflow run through API.
4. Dependency links are inferred from CWL `source` references (`step_id/output_name`).

## Current limitations / roadblocks

- Uses `EmptyOperator` placeholders only; it does not execute CWL tools yet.
- Airflow API route can trigger runs, but Airflow still must already know the DAG id.
  - Airflow's stable REST API does not natively accept arbitrary DAG Python code for registration.
  - In practice, DAG definition must still be deployed in the Airflow environment (filesystem, image, or custom ingest pipeline).
- Does not map Sophios/CWL resources (GPU, RAM, CPU, container image) to Airflow executors/operators.
- Assumes dependency extraction through common CWL `source` patterns; advanced CWL forms may need broader parsing.
- Nested subworkflows are not exploded into TaskGroups/sub-DAG structure yet.
- Task ID sanitization is basic; very large or highly similar IDs may need stricter collision/length handling.
- No integration yet with compute submission API, artifact hydration, retries policy mapping, or runtime status sync.

## Suggested next steps

- Introduce a real execution operator (e.g., `BashOperator`, `KubernetesPodOperator`, or a custom `SophiosCwlOperator`).
- Add richer typed translation models for CWL fields used during Airflow emission.
- Add tests that snapshot generated DAG source and edge topology for multi-step workflows.
- Add subworkflow mapping to `TaskGroup`.
- Add an optional "deploy DAG source" strategy for remote Airflow setups (for example, write to a mounted `dags/` volume or push to a repo consumed by git-sync).
