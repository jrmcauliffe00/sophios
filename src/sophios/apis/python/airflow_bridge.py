"""Helpers for translating Sophios workflows into Airflow DAG artifacts."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shlex
from typing import Any, TYPE_CHECKING, TypedDict, cast
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from sophios.apis.python.api import Workflow
from sophios.wic_types import Json

if TYPE_CHECKING:
    from airflow.sdk import DAG  # type: ignore[reportMissingImports]
else:
    DAG = Any


class CwlStepInputDict(TypedDict, total=False):
    """Subset of CWL input shape needed for dependency extraction."""

    source: str | list[str]


CwlStep = TypedDict(
    "CwlStep",
    {
        "id": str,
        "in": dict[str, str | list[str] | CwlStepInputDict | Json],
        "run": Json,
    },
    total=False,
)


class CwlWorkflowDoc(TypedDict, total=False):
    """Subset of compiled CWL workflow shape consumed by this adapter."""

    name: str
    steps: list[CwlStep]


class AirflowTaskSpec(TypedDict):
    """Serialized Airflow task metadata used for DAG materialization."""

    task_id: str
    operator_kind: str
    bash_command: str | None


class AirflowDagSpec(TypedDict):
    """Serializable DAG specification for runtime and file emission."""

    dag_id: str
    tasks: list[AirflowTaskSpec]
    edges: list[tuple[str, str]]


def _resolve_task_mode(task_mode: str) -> str:
    """Validate and normalize task mode options."""
    valid_modes = {"placeholder", "auto"}
    if task_mode not in valid_modes:
        raise ValueError(f"Unsupported task_mode '{task_mode}'. Expected one of: {sorted(valid_modes)}")
    return task_mode


def _extract_sources(value: object) -> list[str]:
    """Extract CWL source references from one step input mapping value."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    if isinstance(value, dict):
        source = value.get("source")
        if isinstance(source, str):
            return [source]
        if isinstance(source, list):
            return [item for item in source if isinstance(item, str)]
    return []


def _upstream_step_ids(step_in: dict[str, object]) -> set[str]:
    """Return upstream step ids for a compiled CWL step input map."""
    upstream: set[str] = set()
    for value in step_in.values():
        for source in _extract_sources(value):
            if "/" not in source:
                continue
            upstream_step, _ = source.split("/", 1)
            if upstream_step:
                upstream.add(upstream_step)
    return upstream


def _sanitize_task_id(step_id: str, *, used: set[str]) -> str:
    """Make a deterministic, Airflow-safe task id while avoiding collisions."""
    cleaned = "".join(ch if (ch.isalnum() or ch in "_-.") else "_" for ch in step_id)
    cleaned = cleaned.strip("._-")
    if not cleaned:
        cleaned = "step"
    candidate = cleaned
    suffix = 2
    while candidate in used:
        candidate = f"{cleaned}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def _command_line_from_run(run_spec: object) -> str | None:
    """Extract a concrete shell command from a CWL run object when possible."""
    if not isinstance(run_spec, dict):
        return None
    if run_spec.get("class") != "CommandLineTool":
        return None
    parts: list[str] = []

    base_command = run_spec.get("baseCommand")
    if isinstance(base_command, str):
        parts.append(base_command)
    elif isinstance(base_command, list):
        parts.extend([token for token in base_command if isinstance(token, str)])

    arguments = run_spec.get("arguments")
    if isinstance(arguments, list):
        for arg in arguments:
            if isinstance(arg, str):
                parts.append(arg)
            elif isinstance(arg, dict):
                value_from = arg.get("valueFrom")
                if isinstance(value_from, str):
                    parts.append(value_from)
    if not parts:
        return None
    return " ".join(shlex.quote(part) for part in parts)


def _task_spec_for_step(step: CwlStep, task_id: str, *, task_mode: str) -> AirflowTaskSpec:
    """Create the Airflow operator spec for one compiled workflow step."""
    if task_mode == "auto":
        bash_command = _command_line_from_run(step.get("run"))
        if bash_command:
            return AirflowTaskSpec(
                task_id=task_id,
                operator_kind="bash",
                bash_command=bash_command,
            )
    return AirflowTaskSpec(
        task_id=task_id,
        operator_kind="empty",
        bash_command=None,
    )


def _build_airflow_dag_spec(compiled: CwlWorkflowDoc, dag_id: str, *, task_mode: str) -> AirflowDagSpec:
    """Build a normalized DAG spec from compiled Sophios/CWL JSON."""
    steps = compiled.get("steps", [])
    used_task_ids: set[str] = set()
    task_by_step_id: dict[str, str] = {}
    task_spec_by_step_id: dict[str, AirflowTaskSpec] = {}
    edges: list[tuple[str, str]] = []

    for index, step in enumerate(steps):
        raw_step_id = step.get("id") or f"step_{index + 1}"
        task_id = _sanitize_task_id(raw_step_id, used=used_task_ids)
        task_by_step_id[raw_step_id] = task_id
        task_spec_by_step_id[raw_step_id] = _task_spec_for_step(step, task_id, task_mode=task_mode)

    step_by_id = {step.get("id"): step for step in steps if step.get("id")}
    for step_id, task_id in task_by_step_id.items():
        step = step_by_id.get(step_id)
        if step is None:
            continue
        step_inputs = cast(dict[str, object], step.get("in", {}))
        for upstream_step_id in sorted(_upstream_step_ids(step_inputs)):
            upstream_task_id = task_by_step_id.get(upstream_step_id)
            if upstream_task_id is not None:
                edges.append((upstream_task_id, task_id))

    return AirflowDagSpec(
        dag_id=dag_id,
        tasks=[task_spec_by_step_id[step_id] for step_id in task_by_step_id],
        edges=edges,
    )


def _render_airflow_dag_source(
    dag_spec: AirflowDagSpec,
    *,
    start_date: datetime,
    schedule: str | None,
    catchup: bool,
) -> str:
    """Render a Python Airflow DAG module from a DAG spec."""
    needs_bash_operator = any(task["operator_kind"] == "bash" for task in dag_spec["tasks"])
    lines = [
        '"""Generated by Sophios airflow bridge."""',
        "",
        "from datetime import datetime",
        "from airflow.sdk import DAG",
        "from airflow.providers.standard.operators.empty import EmptyOperator",
    ]
    if needs_bash_operator:
        lines.append("from airflow.providers.standard.operators.bash import BashOperator")
    lines.extend([
        "",
        f"with DAG(dag_id={dag_spec['dag_id']!r}, start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}), schedule={schedule!r}, catchup={catchup!r}) as dag:",
        "    tasks = {}",
    ])
    for task_spec in dag_spec["tasks"]:
        task_id = task_spec["task_id"]
        if task_spec["operator_kind"] == "bash" and task_spec["bash_command"] is not None:
            lines.append(
                f"    tasks[{task_id!r}] = BashOperator(task_id={task_id!r}, bash_command={task_spec['bash_command']!r})"
            )
        else:
            lines.append(f"    tasks[{task_id!r}] = EmptyOperator(task_id={task_id!r})")
    if dag_spec["edges"]:
        lines.append("")
    for upstream_task_id, downstream_task_id in dag_spec["edges"]:
        lines.append(f"    tasks[{upstream_task_id!r}] >> tasks[{downstream_task_id!r}]")
    lines.extend(["", "__all__ = ['dag']", ""])
    return "\n".join(lines)


def get_airflow_dag(
    sophios_workflow: Workflow,
    *,
    dag_id: str | None = None,
    start_date: datetime | None = None,
    schedule: str | None = None,
    catchup: bool = False,
    task_mode: str = "placeholder",
    compiled_workflow: Json | None = None,
) -> DAG:
    """Compile a Sophios workflow and build a basic Airflow DAG skeleton."""
    resolved_task_mode = _resolve_task_mode(task_mode)
    compiled_raw = compiled_workflow if compiled_workflow is not None else sophios_workflow.get_cwl_workflow()
    compiled = cast(CwlWorkflowDoc, compiled_raw)
    workflow_name = compiled.get("name", sophios_workflow.process_name)
    resolved_dag_id = dag_id or workflow_name
    dag_spec = _build_airflow_dag_spec(compiled, resolved_dag_id, task_mode=resolved_task_mode)
    needs_bash_operator = any(task["operator_kind"] == "bash" for task in dag_spec["tasks"])

    try:
        from airflow.sdk import DAG as AirflowDAG  # type: ignore[reportMissingImports]
        from airflow.providers.standard.operators.empty import (  # type: ignore[reportMissingImports]
            EmptyOperator,
        )
        if needs_bash_operator:
            from airflow.providers.standard.operators.bash import (  # type: ignore[reportMissingImports]
                BashOperator,
            )
        else:
            BashOperator = None
    except ImportError as exc:
        raise ImportError(
            "Apache Airflow is required for get_airflow_dag(). "
            "Install it with: pip install apache-airflow"
        ) from exc

    dag = AirflowDAG(
        dag_id=resolved_dag_id,
        start_date=start_date or datetime(2024, 1, 1),
        schedule=schedule,
        catchup=catchup,
    )

    tasks: dict[str, Any] = {}
    for task_spec in dag_spec["tasks"]:
        task_id = task_spec["task_id"]
        if task_spec["operator_kind"] == "bash" and task_spec["bash_command"] is not None and BashOperator is not None:
            tasks[task_id] = BashOperator(task_id=task_id, bash_command=task_spec["bash_command"], dag=dag)
        else:
            tasks[task_id] = EmptyOperator(task_id=task_id, dag=dag)
    for upstream_task_id, downstream_task_id in dag_spec["edges"]:
        tasks[upstream_task_id] >> tasks[downstream_task_id]
    return dag


def write_airflow_dag_file(
    sophios_workflow: Workflow,
    *,
    dags_dir: Path,
    dag_filename: str | None = None,
    dag_id: str | None = None,
    start_date: datetime | None = None,
    schedule: str | None = None,
    catchup: bool = False,
    task_mode: str = "placeholder",
    compiled_workflow: Json | None = None,
) -> Path:
    """Render and write an Airflow DAG .py file to disk."""
    resolved_task_mode = _resolve_task_mode(task_mode)
    compiled_raw = compiled_workflow if compiled_workflow is not None else sophios_workflow.get_cwl_workflow()
    compiled = cast(CwlWorkflowDoc, compiled_raw)
    resolved_dag_id = dag_id or compiled.get("name", sophios_workflow.process_name)
    dag_spec = _build_airflow_dag_spec(compiled, resolved_dag_id, task_mode=resolved_task_mode)
    dag_source = _render_airflow_dag_source(
        dag_spec,
        start_date=start_date or datetime(2024, 1, 1),
        schedule=schedule,
        catchup=catchup,
    )

    dags_dir.mkdir(parents=True, exist_ok=True)
    filename = dag_filename or f"{resolved_dag_id}_dag.py"
    dag_path = dags_dir / filename
    dag_path.write_text(dag_source, encoding="utf-8")
    return dag_path


def submit_airflow_dag_run(
    *,
    airflow_api_base_url: str,
    dag_id: str,
    run_id: str | None = None,
    conf: Json | None = None,
    api_token: str | None = None,
    api_username: str | None = None,
    api_password: str | None = None,
    verify_ssl: bool = True,
    timeout_seconds: int = 30,
) -> Json:
    """Trigger an Airflow DAG run through the stable REST API."""
    if api_username and not api_password:
        raise ValueError("api_password is required when api_username is provided")
    base_url = airflow_api_base_url.rstrip("/")
    encoded_dag_id = quote(dag_id, safe="")
    dag_url = f"{base_url}/api/v1/dags/{encoded_dag_id}"
    dag_runs_url = f"{dag_url}/dagRuns"

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    auth = HTTPBasicAuth(api_username, api_password) if api_username and api_password else None

    dag_check = requests.get(dag_url, headers=headers, auth=auth, timeout=timeout_seconds, verify=verify_ssl)
    if dag_check.status_code == 404:
        raise RuntimeError(
            f"Airflow does not know DAG '{dag_id}'. "
            "Deploy the DAG into your Airflow dags folder before triggering it via API."
        )
    if dag_check.status_code >= 400:
        raise RuntimeError(
            f"Failed to query Airflow DAG '{dag_id}' ({dag_check.status_code}): {dag_check.text}"
        )

    payload: Json = {}
    if run_id:
        payload["dag_run_id"] = run_id
    if conf is not None:
        payload["conf"] = conf
    dag_run_response = requests.post(
        dag_runs_url,
        headers=headers,
        auth=auth,
        json=payload,
        timeout=timeout_seconds,
        verify=verify_ssl,
    )
    if dag_run_response.status_code >= 400:
        raise RuntimeError(
            f"Failed to trigger Airflow DAG run ({dag_run_response.status_code}): {dag_run_response.text}"
        )
    return cast(Json, dag_run_response.json())


def submit_airflow_dag_via_api(
    sophios_workflow: Workflow,
    *,
    airflow_api_base_url: str,
    dag_id: str | None = None,
    run_id: str | None = None,
    conf: Json | None = None,
    api_token: str | None = None,
    api_username: str | None = None,
    api_password: str | None = None,
    verify_ssl: bool = True,
    timeout_seconds: int = 30,
    task_mode: str = "placeholder",
    compiled_workflow: Json | None = None,
) -> Json:
    """Build an in-memory DAG and trigger it via API without writing a local DAG file."""
    dag = get_airflow_dag(
        sophios_workflow,
        dag_id=dag_id,
        task_mode=task_mode,
        compiled_workflow=compiled_workflow,
    )
    return submit_airflow_dag_run(
        airflow_api_base_url=airflow_api_base_url,
        dag_id=dag.dag_id,
        run_id=run_id,
        conf=conf,
        api_token=api_token,
        api_username=api_username,
        api_password=api_password,
        verify_ssl=verify_ssl,
        timeout_seconds=timeout_seconds,
    )
